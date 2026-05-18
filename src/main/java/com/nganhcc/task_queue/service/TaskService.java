package com.nganhcc.task_queue.service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.nganhcc.task_queue.api.dto.EnqueueTaskRequest;
import com.nganhcc.task_queue.api.dto.TaskResponse;
import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.config.QueueProperties.QueueConfig;
import com.nganhcc.task_queue.exception.BadRequestException;
import com.nganhcc.task_queue.exception.TaskNotFoundException;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Service
public class TaskService {

    private static final String DEFAULT_QUEUE = "default";

    private final TaskRepository taskRepository;
    private final QueueProperties queueProperties;
    private final JsonMapper jsonMapper;
    private final RedisBroker redisBroker;

    public TaskService(TaskRepository taskRepository, QueueProperties queueProperties, JsonMapper jsonMapper, RedisBroker redisBroker) {
        this.taskRepository = taskRepository;
        this.queueProperties = queueProperties;
        this.jsonMapper = jsonMapper;
        this.redisBroker= redisBroker;
    }

    public TaskResponse enqueue(EnqueueTaskRequest request) {
        if (request == null) {
            throw new BadRequestException("Request body is required");
        }

        String queue = normalizeQueue(request.queue());
        String fn = requireFn(request.fn());
        JsonNode payload = requirePayload(request.payload());
        QueueConfig queueConfig = requireQueueConfig(queue);
        int maxRetries = resolveMaxRetries(request.maxRetries(), queueConfig);
        int priority = request.priority() == null ? 0: request.priority();
        String payloadJson = serializePayload(payload);

        Task task = new Task(null, queue, fn, payloadJson, maxRetries,priority, request.runAt());
        Task saved = taskRepository.save(task);

        if( saved.getRunAt() == null || !saved.getRunAt().isAfter(Instant.now())){
            redisBroker.enqueue(saved);
        }else{
            redisBroker.enqueueDelayed(saved);
        };

        return TaskResponse.from(saved, deserializePayload(saved.getPayload()));
    }

    public TaskResponse getTask(UUID id){
        Task task = taskRepository.findById(id).orElseThrow(() -> new TaskNotFoundException(id));
        return TaskResponse.from(task, deserializePayload(task.getPayload()));
    }

    public List<TaskResponse> listTasks(String queue, TaskStatus status){
        List<Task> tasks;
        if (queue != null && status != null){
            tasks= taskRepository.findByQueueAndStatus(queue,status);
        }else if(queue!= null){
            tasks = taskRepository.findByQueue(queue);
        }else if (status != null){
            tasks = taskRepository.findByStatus(status);
        }else {
            tasks=taskRepository.findAll();
        }
        return tasks.stream().map(task -> TaskResponse.from(task, deserializePayload(task.getPayload())))
        .toList();
    }

    public TaskResponse cancelTask(UUID id){
        Task task = taskRepository.findById(id).orElseThrow(()-> new TaskNotFoundException(id));
        if (task.getStatus() == TaskStatus.DONE || task.getStatus() == TaskStatus.DEAD){
            throw new BadRequestException("Cannot cancel task with status: "+ task.getStatus());
        }
        task.setStatus(TaskStatus.FAILED);
        task.setError("Task cancelled");
        Task saved= taskRepository.save(task);
        return TaskResponse.from(saved, deserializePayload(saved.getPayload()));
    }

    public TaskResponse retryTask(UUID id){
        Task task = taskRepository.findById(id).orElseThrow(()-> new TaskNotFoundException(id));
        if (task.getStatus() != TaskStatus.FAILED){
            throw new BadRequestException("Can not retry task with status: "+ task.getStatus());
        }
        task.setStatus(TaskStatus.PENDING);
        task.setRunAt(null);
        task.setStartedAt(null);
        task.setError(null);
        task.setStackTrace(null);
        Task saved= taskRepository.save(task);
        redisBroker.enqueue(saved);
        return TaskResponse.from(saved, deserializePayload(saved.getPayload()));
    }

    public int markQueuePurged(String queue){
        List<Task> tasks = taskRepository.findByQueueAndStatusIn(
            queue, 
            List.of(TaskStatus.PENDING, TaskStatus.RUNNING)
        );
        for (Task task: tasks){
            task.setStatus(TaskStatus.FAILED);
            task.setError("Queue purged");
            task.setStartedAt(null);
        }
        taskRepository.saveAll(tasks);
        return tasks.size();
    }
    private String normalizeQueue(String queue) {
        if (queue == null || queue.isBlank()) {
            return DEFAULT_QUEUE;
        }
        return queue.trim();
    }

    private String requireFn(String fn) {
        if (fn == null || fn.isBlank()) {
            throw new BadRequestException("fn is required");
        }
        return fn.trim();
    }

    private JsonNode requirePayload(JsonNode payload) {
        if (payload == null || payload.isNull()) {
            throw new BadRequestException("payload is required");
        }
        return payload;
    }

    private QueueConfig requireQueueConfig(String queue) {
        Map<String, QueueConfig> queues = queueProperties.getQueues();
        QueueConfig config = queues.get(queue);
        if (config == null) {
            throw new BadRequestException("Unknown queue: " + queue);
        }
        return config;
    }

    private int resolveMaxRetries(Integer requestedMaxRetries, QueueConfig queueConfig) {
        if (requestedMaxRetries == null) {
            return queueConfig.getMaxRetries();
        }
        if (requestedMaxRetries < 0) {
            throw new BadRequestException("maxRetries must be greater than or equal to 0");
        }
        return requestedMaxRetries;
    }

    private String serializePayload(JsonNode payload) {
        try {
            return jsonMapper.writeValueAsString(payload);
        } catch (JacksonException e) {
            throw new BadRequestException("payload must be valid JSON");
        }
    }

    private JsonNode deserializePayload(String payload) {
        try {
            return jsonMapper.readTree(payload);
        } catch (JacksonException e) {
            throw new IllegalStateException("Stored task payload is not valid JSON", e);
        }
    }
}
