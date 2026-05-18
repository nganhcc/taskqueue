package com.nganhcc.task_queue.retry;

import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.model.Task;

@Component
public class RetryPolicy {
    public boolean shouldRetry(Task task){
        return task.getAttempt()<task.getMaxRetries();
    }
    /*
   next retry 1 -> 1000ms
    next retry 2 -> 2000ms
    next retry 3 -> 4000ms
    */
    public long nextDelayMs(Task task, long baseDelayMs){
        return baseDelayMs * (long) Math.pow(2, task.getAttempt());
    }
}
