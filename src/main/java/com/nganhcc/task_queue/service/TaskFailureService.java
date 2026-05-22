package com.nganhcc.task_queue.service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;

import org.springframework.stereotype.Service;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.config.QueueProperties.QueueConfig;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.retry.RetryPolicy;
import com.nganhcc.task_queue.store.TaskRepository;

@Service
public class TaskFailureService {
    private final RetryPolicy retryPolicy;
    private final QueueProperties queueProperties;
    private final TaskRepository taskRepository;
    private final RedisBroker redisBroker;

    public TaskFailureService(RetryPolicy retryPolicy, QueueProperties queueProperties, TaskRepository taskRepository, RedisBroker redisBroker){
        this.retryPolicy=retryPolicy;
        this.queueProperties=queueProperties;
        this.taskRepository=taskRepository;
        this.redisBroker=redisBroker;
    }
    public void handleFailure(Task task, RuntimeException e){
        task.setAttempt(task.getAttempt()+1);
        task.setError(e.getMessage());
        task.setStartedAt(null);
        task.setHeartbeatAt(null);
        task.setStackTrace(stackTraceOf(e));
        if (retryPolicy.shouldRetry(task)){
            QueueConfig queueConfig = queueProperties.getQueues().get(task.getQueue());
            long delayMs = retryPolicy.nextDelayMs(task, queueConfig.getBaseDelayMs(), queueConfig.getMaxDelayMs(), queueConfig.getJitterPercent());
            task.setRunAt(Instant.now().plusMillis(delayMs));
            task.setStatus(TaskStatus.PENDING);
            taskRepository.save(task);

            redisBroker.enqueueDelayed(task);
        }else{
            task.setStatus(TaskStatus.DEAD);
            taskRepository.save(task);
            redisBroker.sendToDlq(task);
        }
    }

    private String stackTraceOf(Throwable throwable){
        StringWriter stringWriter = new StringWriter();
        throwable.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }
}
