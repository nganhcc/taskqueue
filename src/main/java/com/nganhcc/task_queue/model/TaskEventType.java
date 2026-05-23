package com.nganhcc.task_queue.model;

public enum TaskEventType {
    CREATED,
    STARTED,
    SUCCEEDED,
    FAILED,
    RETRY_SCHEDULED,
    DEAD_LETTERED,
    CANCELLED,
    MANUAL_RETRY,
    DLQ_REPLAYED,
    QUEUE_PURGED,
    DELAYED_PURGED,
    DLQ_PURGED
}

