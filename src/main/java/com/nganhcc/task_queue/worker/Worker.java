package com.nganhcc.task_queue.worker;

import java.time.Instant;

import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.config.QueueProperties.QueueConfig;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.retry.RetryPolicy;
import com.nganhcc.task_queue.store.TaskRepository;

@Component
public class Worker {
    private final TaskRepository taskRepository;
    private final RedisBroker redisBroker;
    private final RetryPolicy retryPolicy;
    private final QueueProperties queueProperties;
    private final HandlerRegistry handlerRegistry;

    public Worker(TaskRepository taskRepository, RedisBroker redisBroker,RetryPolicy retryPolicy, HandlerRegistry handlerRegistry, QueueProperties queueProperties){
        this.taskRepository= taskRepository;
        this.queueProperties=queueProperties;
        this.redisBroker= redisBroker;
        this.retryPolicy= retryPolicy;
        this.handlerRegistry= handlerRegistry;
    }

    public void runOnce(String queueName){
        //polledTask: from redis
        //task : from DB
        Task polledTask= redisBroker.poll(queueName);
        if (polledTask==null){
            return;
        }

        Task task= taskRepository.findById(polledTask.getId()).orElseThrow(()-> new IllegalStateException("Task not found: "+polledTask.getId()));
        if (task.getStatus() != TaskStatus.PENDING){
            redisBroker.acknowledge(polledTask);
            return;
        }
        task.setStatus(TaskStatus.RUNNING);
        task.setStartedAt(Instant.now());
        taskRepository.save(task);

        TaskHandler taskHandler = handlerRegistry.get(task.getFn());
        try{
            String result = taskHandler.handle(task);
            task.setStatus(TaskStatus.DONE);
            task.setResult(result);
            taskRepository.save(task);
        }catch(RuntimeException e){
            task.setAttempt(task.getAttempt()+1);
            task.setError(e.getMessage());
            if (retryPolicy.shouldRetry(task)){
                QueueConfig queueConfig = queueProperties.getQueues().get(task.getQueue());
                long delayMs = retryPolicy.nextDelayMs(task, queueConfig.getBaseDelayMs());
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

        redisBroker.acknowledge(polledTask);
    }
}
