package com.nganhcc.task_queue.service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.nganhcc.task_queue.api.dto.EnqueueTaskRequest;
import com.nganhcc.task_queue.api.dto.TaskEventResponse;
import com.nganhcc.task_queue.api.dto.TaskResponse;
import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.config.QueueProperties.QueueConfig;
import com.nganhcc.task_queue.exception.BadRequestException;
import com.nganhcc.task_queue.exception.ConflictException;
import com.nganhcc.task_queue.exception.TaskNotFoundException;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskEventType;
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
    private final TaskEventService taskEventService;

    public TaskService(TaskRepository taskRepository, QueueProperties queueProperties, JsonMapper jsonMapper, RedisBroker redisBroker, TaskEventService taskEventService) {
        this.taskRepository = taskRepository;
        this.queueProperties = queueProperties;
        this.jsonMapper = jsonMapper;
        this.redisBroker= redisBroker;
        this.taskEventService = taskEventService;
    }

    public TaskResponse enqueue(EnqueueTaskRequest request) {
        if (request == null) {
            throw new BadRequestException("Request body is required");
        }

        String queue = normalizeQueue(request.queue());
        String idempotencyKey = normalizeIdempotencyKey(request.idempotencyKey());
        String fn = requireFn(request.fn());
        JsonNode payload = requirePayload(request.payload());
        QueueConfig queueConfig = requireQueueConfig(queue);
        int maxRetries = resolveMaxRetries(request.maxRetries(), queueConfig);
        int priority = request.priority() == null ? 0: request.priority();
        String payloadJson = serializePayload(payload);

        if (idempotencyKey != null){
            Task existingTask = taskRepository.findByQueueAndIdempotencyKey(queue, idempotencyKey).orElse(null);
            if (existingTask != null){
                requireSameIdempotentRequest(existingTask, fn, payloadJson, maxRetries, priority, request.runAt());
                return TaskResponse.from(existingTask, deserializePayload(existingTask.getPayload()));
            }
        }

        Task task = new Task(null, queue, fn, payloadJson, maxRetries,priority, idempotencyKey, request.runAt());
        Task saved;
        try {
            saved = taskRepository.save(task);
        } catch (DataIntegrityViolationException e) {
            if (idempotencyKey == null) {
                throw e;
            }
            Task existingTask = taskRepository.findByQueueAndIdempotencyKey(queue, idempotencyKey)
                    .orElseThrow(() -> e);
            requireSameIdempotentRequest(existingTask, fn, payloadJson, maxRetries, priority, request.runAt());
            return TaskResponse.from(existingTask, deserializePayload(existingTask.getPayload()));
        }

        if( saved.getRunAt() == null || !saved.getRunAt().isAfter(Instant.now())){
            redisBroker.enqueue(saved);
        }else{
            redisBroker.enqueueDelayed(saved);
        };
        taskEventService.record(saved, TaskEventType.CREATED, "Task created");

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

        TaskStatus oldStatus = task.getStatus();
        task.setStatus(TaskStatus.FAILED);
        task.setError("Task cancelled");
        task.setStartedAt(null);
        task.setHeartbeatAt(null);
        task.setRunAt(null);
        Task saved= taskRepository.save(task);
        redisBroker.removeReadyById(task.getQueue(), task.getId());
        redisBroker.removeDelayedById(task.getId());
        if (oldStatus==TaskStatus.RUNNING){
            redisBroker.removeProcessingById(task.getQueue(), task.getId());
        }
        taskEventService.record(saved, TaskEventType.CANCELLED, "Task cancelled");
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
        task.setHeartbeatAt(null);
        task.setError(null);
        task.setStackTrace(null);
        Task saved= taskRepository.save(task);
        redisBroker.enqueue(saved);
        taskEventService.record(saved, TaskEventType.MANUAL_RETRY, "Task manually retried");
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
            task.setHeartbeatAt(null);
        }
        taskRepository.saveAll(tasks);
        tasks.forEach(task -> taskEventService.record(task, TaskEventType.QUEUE_PURGED, "Queue purged"));
        return tasks.size();
    }

    public int markDelayedPurged(){
        List<Task> tasks = taskRepository.findByStatusAndRunAtIsNotNull(TaskStatus.PENDING);
        for (Task task : tasks){
            task.setStatus(TaskStatus.FAILED);
            task.setError("Delayed queue purged");
            task.setRunAt(null);
            task.setStartedAt(null);
            task.setHeartbeatAt(null);
        }
        taskRepository.saveAll(tasks);
        tasks.forEach(task -> taskEventService.record(task, TaskEventType.DELAYED_PURGED, "Delayed queue purged"));
        return tasks.size();
    }

    public int markDlqPurged(){
        List<Task> tasks = taskRepository.findByStatus(TaskStatus.DEAD);
        for (Task task: tasks){
            task.setStatus(TaskStatus.FAILED);
            task.setError("DLQ purged");
            task.setStartedAt(null);
            task.setHeartbeatAt(null);
        }
        taskRepository.saveAll(tasks);
        tasks.forEach(task -> taskEventService.record(task, TaskEventType.DLQ_PURGED, "DLQ purged"));
        return tasks.size();
    }

    public List<TaskEventResponse> listTaskEvents(UUID id) {
        return taskEventService.listTaskEvents(id);
    }
    private String normalizeQueue(String queue) {
        if (queue == null || queue.isBlank()) {
            return DEFAULT_QUEUE;
        }
        return queue.trim();
    }

    public TaskResponse heartbeat(UUID id){
        Task task = taskRepository.findById(id).orElseThrow(()-> new TaskNotFoundException(id));
        if (task.getStatus() != TaskStatus.RUNNING){
            throw new BadRequestException("Cannot heartbeat task with status: "+ task.getStatus());
        }
        task.setHeartbeatAt(Instant.now());
        Task saved = taskRepository.save(task);
        return TaskResponse.from(saved, deserializePayload(saved.getPayload()));
    }

    private String normalizeIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return null;
        }
        String normalized = idempotencyKey.trim();
        if (normalized.length() > 200) {
            throw new BadRequestException("idempotencyKey must be 200 characters or fewer");
        }
        return normalized;
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
        validateQueueConfig(queue, config);
        return config;
    }

    private void validateQueueConfig(String queue, QueueConfig config) {
        if (config.getConcurrency() <= 0) {
            throw invalidQueueConfig(queue, "concurrency must be greater than 0");
        }
        if (config.getMaxRetries() < 0) {
            throw invalidQueueConfig(queue, "max-retries must be greater than or equal to 0");
        }
        if (config.getBaseDelayMs() < 0) {
            throw invalidQueueConfig(queue, "base-delay-ms must be greater than or equal to 0");
        }
        if (config.getMaxDelayMs() < config.getBaseDelayMs()) {
            throw invalidQueueConfig(queue, "max-delay-ms must be greater than or equal to base-delay-ms");
        }
        if (config.getJitterPercent() < 0.0 || config.getJitterPercent() > 1.0) {
            throw invalidQueueConfig(queue, "jitter-percent must be between 0.0 and 1.0");
        }
    }

    private BadRequestException invalidQueueConfig(String queue, String message) {
        return new BadRequestException("Invalid queue config for " + queue + ": " + message);
    }

    private void requireSameIdempotentRequest(
            Task existingTask,
            String fn,
            String payloadJson,
            int maxRetries,
            int priority,
            Instant runAt) {
        boolean sameRequest = existingTask.getFn().equals(fn)
                && existingTask.getPayload().equals(payloadJson)
                && existingTask.getMaxRetries() == maxRetries
                && existingTask.getPriority() == priority
                && java.util.Objects.equals(existingTask.getRunAt(), runAt);

        if (!sameRequest) {
            throw new ConflictException("Idempotency key already used with different task request");
        }
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
