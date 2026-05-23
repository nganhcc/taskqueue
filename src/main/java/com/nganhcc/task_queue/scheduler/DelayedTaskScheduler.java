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
            promoteRedisDelayedTasks();
            reconcileDueDelayedTasks();
        }finally{
            redisBroker.releaseSchedulerLock(lockToken);
        }
    }

    private void promoteRedisDelayedTasks() {
        while (true){
            Instant now = Instant.now();
            Task delayedTask = redisBroker.pollReadyDelayed(now);
            if (delayedTask == null){
                return;
            }
            promoteDueTask(delayedTask.getId(), now);
        }
    }

    private void reconcileDueDelayedTasks() {
        Instant now = Instant.now();
        for (Task task : taskRepository.findByStatusAndRunAtLessThanEqual(TaskStatus.PENDING, now)){
            redisBroker.removeDelayedById(task.getId());
            promoteDueTask(task.getId(), now);
        }
    }

    private void promoteDueTask(UUID taskId, Instant now) {
        Task dbTask = taskRepository.findById(taskId).orElse(null);
        if (dbTask == null){
            return;
        }
        promoteDueTask(dbTask, now);
    }

    private void promoteDueTask(Task task, Instant now) {
        if (task.getStatus() != TaskStatus.PENDING || task.getRunAt() == null || task.getRunAt().isAfter(now)){
            return;
        }
        redisBroker.enqueue(task);
        task.setRunAt(null);
        taskRepository.save(task);
    }
}
