package com.nganhcc.task_queue.api;

import java.util.List;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.nganhcc.task_queue.api.dto.TaskResponse;
import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.exception.TaskNotFoundException;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@RestController
public class DlqController {
    private final RedisBroker redisBroker;
    private final TaskRepository taskRepository;
    private final JsonMapper jsonMapper;

    public DlqController(RedisBroker redisBroker, JsonMapper jsonMapper, TaskRepository taskRepository) {
        this.redisBroker = redisBroker;
        this.taskRepository = taskRepository;
        this.jsonMapper = jsonMapper;
    }

    @GetMapping("/dlq")
    public List<TaskResponse> listDlq() {
        return redisBroker.listDlq().stream()
                .map(task -> TaskResponse.from(task, deserializePayload(task)))
                .toList();
    }

    @PostMapping("/dlq/{id}/replay")
    public TaskResponse replay(@PathVariable UUID id,
            @RequestParam(defaultValue = "false") boolean resetAttempts) {
        Task dlqTask = redisBroker.findDlqTask(id);
        Task task = taskRepository.findById(id).orElseThrow(() -> new TaskNotFoundException(id));
        redisBroker.removeFromDlq(dlqTask);

        task.setStatus(TaskStatus.PENDING);
        if (resetAttempts) {
            task.setAttempt(0);
        }
        task.setRunAt(null);
        task.setStartedAt(null);
        task.setError(null);
        task.setStackTrace(null);

        Task saved = taskRepository.save(task);
        redisBroker.enqueue(saved);

        return TaskResponse.from(saved, deserializePayload(saved));
    }

    private JsonNode deserializePayload(Task task) {
        try {
            return jsonMapper.readTree(task.getPayload());
        } catch (JacksonException e) {
            throw new IllegalStateException("Stored DLQ task payload is not valid JSON", e);
        }
    }
}
