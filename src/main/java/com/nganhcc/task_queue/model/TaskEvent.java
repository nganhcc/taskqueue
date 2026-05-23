package com.nganhcc.task_queue.model;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

@Entity
@Table(name = "task_events")
public class TaskEvent {

    @Id
    private UUID id;

    @Column(name = "task_id", nullable = false)
    private UUID taskId;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false, length = 50)
    private TaskEventType type;

    @Column(name = "attempt", nullable = false)
    private int attempt;

    @Column(name = "message", columnDefinition = "TEXT")
    private String message;

    @Column(name = "error", columnDefinition = "TEXT")
    private String error;

    @Column(name = "stack_trace", columnDefinition = "TEXT")
    private String stackTrace;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected TaskEvent() {
    }

    public TaskEvent(UUID id, UUID taskId, TaskEventType type, int attempt, String message, String error, String stackTrace) {
        this.id = id;
        this.taskId = taskId;
        this.type = type;
        this.attempt = attempt;
        this.message = message;
        this.error = error;
        this.stackTrace = stackTrace;
        applyDefaults();
    }

    @PrePersist
    void beforeInsert() {
        applyDefaults();
    }

    private void applyDefaults() {
        if (this.id == null) {
            this.id = UUID.randomUUID();
        }
        if (this.createdAt == null) {
            this.createdAt = Instant.now();
        }
    }

    public UUID getId() {
        return id;
    }

    public UUID getTaskId() {
        return taskId;
    }

    public TaskEventType getType() {
        return type;
    }

    public int getAttempt() {
        return attempt;
    }

    public String getMessage() {
        return message;
    }

    public String getError() {
        return error;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}

