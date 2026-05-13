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
@Table(name = "tasks")
public class Task {

    @Id
    private UUID id;

    @Column(name = "queue", nullable = false, length = 100)
    private String queue;

    @Column(name = "fn", nullable = false, length = 200)
    private String fn;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private TaskStatus status;

    @Column(name = "attempt", nullable = false)
    private int attempt;

    @Column(name = "max_retries", nullable = false)
    private int maxRetries;

    @Column(name = "run_at")
    private Instant runAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "result", columnDefinition = "TEXT")
    private String result;

    @Column(name = "error", columnDefinition = "TEXT")
    private String error;

    @Column(name = "stack_trace", columnDefinition = "TEXT")
    private String stackTrace;

    protected Task() {
    }

    public Task(UUID id, String queue, String fn, String payload, int maxRetries, Instant runAt) {
        this.id = id;
        this.queue = queue;
        this.fn = fn;
        this.payload = payload;
        this.status = TaskStatus.PENDING;
        this.attempt = 0;
        this.maxRetries = maxRetries;
        this.runAt = runAt;
    }

    @PrePersist
    void beforeInsert() {
        if (this.id == null) {
            this.id = UUID.randomUUID();
        }
        if (this.status == null) {
            this.status = TaskStatus.PENDING;
        }
        if (this.createdAt == null) {
            this.createdAt = Instant.now();
        }
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getFn() {
        return fn;
    }

    public void setFn(String fn) {
        this.fn = fn;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public Instant getRunAt() {
        return runAt;
    }

    public void setRunAt(Instant runAt) {
        this.runAt = runAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }
}
