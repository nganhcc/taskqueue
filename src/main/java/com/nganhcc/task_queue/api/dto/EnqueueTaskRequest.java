package com.nganhcc.task_queue.api.dto;

import java.time.Instant;

import tools.jackson.databind.JsonNode;

public record EnqueueTaskRequest(
        String queue,
        String fn,
        JsonNode payload,
        Instant runAt,
        Integer maxRetries,
        Integer priority
) {
}
