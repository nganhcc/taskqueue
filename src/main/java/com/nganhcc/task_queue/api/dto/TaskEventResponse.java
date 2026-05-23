package com.nganhcc.task_queue.api.dto;

import java.time.Instant;
import java.util.UUID;

import com.nganhcc.task_queue.model.TaskEvent;
import com.nganhcc.task_queue.model.TaskEventType;

public record TaskEventResponse(
        UUID id,
        UUID taskId,
        TaskEventType type,
        int attempt,
        String message,
        String error,
        String stackTrace,
        Instant createdAt) {

    public static TaskEventResponse from(TaskEvent event) {
        return new TaskEventResponse(
                event.getId(),
                event.getTaskId(),
                event.getType(),
                event.getAttempt(),
                event.getMessage(),
                event.getError(),
                event.getStackTrace(),
                event.getCreatedAt());
    }
}

