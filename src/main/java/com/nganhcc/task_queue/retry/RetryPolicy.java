package com.nganhcc.task_queue.retry;

import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.model.Task;

@Component
public class RetryPolicy {
    public boolean shouldRetry(Task task){
        return task.getAttempt()<task.getMaxRetries();
    }
    public long nextDelayMs(Task task, long baseDelayMs, long maxDelayMs, double jitterPercent) {
    long exponentialDelay = baseDelayMs * (long) Math.pow(2, task.getAttempt());
    long cappedDelay = Math.min(exponentialDelay, maxDelayMs);

    double safeJitterPercent = Math.max(0.0, Math.min(jitterPercent, 1.0));
    double minDelay = cappedDelay * (1 - safeJitterPercent);
    double randomDelay = minDelay + (Math.random() * (cappedDelay - minDelay));

    return (long) randomDelay;
}
}
