package com.nganhcc.task_queue.api.dto;

import java.time.Instant;
import java.util.UUID;

import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;

import tools.jackson.databind.JsonNode;

public record TaskResponse(
        UUID id,
        String queue,
        String fn,
        JsonNode payload,
        TaskStatus status,
        int attempt,
        int maxRetries,
        int priority,
        String idempotencyKey,
        Instant heartbeatAt,
        Instant runAt,
        Instant startedAt,
        Instant createdAt,
        String result,
        String error) {

    public static TaskResponse from(Task task, JsonNode payload) {
        return new TaskResponse(
                task.getId(),
                task.getQueue(),
                task.getFn(),
                payload,
                task.getStatus(),
                task.getAttempt(),
                task.getMaxRetries(),
                task.getPriority(),
                task.getIdempotencyKey(),
                task.getHeartbeatAt(),
                task.getRunAt(),
                task.getStartedAt(),
                task.getCreatedAt(),
                task.getResult(),
                task.getError());
    }
}
