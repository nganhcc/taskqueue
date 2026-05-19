package com.nganhcc.task_queue.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;


@Component
public class DelayedTaskScheduler {
    private final TaskRepository taskRepository;
    private final RedisBroker redisBroker;
    private final QueueProperties queueProperties;

    public DelayedTaskScheduler(RedisBroker redisBroker, TaskRepository taskRepository, QueueProperties queueProperties){
        this.redisBroker= redisBroker;
        this.taskRepository= taskRepository;
        this.queueProperties=queueProperties;
    }

    @Scheduled(fixedDelayString = "${taskqueue.scheduler.poll-interval-ms:1000}")
    public void moveReadyTasks(){
        String lockToken = UUID.randomUUID().toString();
        Duration ttl = Duration.ofMillis(queueProperties.getScheduler().getLockTtlMs());
        if (!redisBroker.acquireSchedulerLock(lockToken, ttl)){
            return;
        }
        try{
            while (true){
                Task delayedTask = redisBroker.pollReadyDelayed(Instant.now());
                if (delayedTask == null){
                    return;
                }
                Task dbTask = taskRepository.findById(delayedTask.getId()).orElse(null);
                if (dbTask == null || dbTask.getStatus() != TaskStatus.PENDING){
                    continue;
                }
                dbTask.setRunAt(null);
                Task saved = taskRepository.save(dbTask);
                redisBroker.enqueue(saved);
            }
        }finally{
            redisBroker.releaseSchedulerLock(lockToken);
        }
    }
}
