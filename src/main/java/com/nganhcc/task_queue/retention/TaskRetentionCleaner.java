package com.nganhcc.task_queue.retention;

import java.time.Duration;
import java.time.Instant;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.config.QueueProperties.RetentionConfig;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;

@Component
public class TaskRetentionCleaner {
    private final TaskRepository taskRepository;
    private final QueueProperties queueProperties;
    
    public TaskRetentionCleaner(TaskRepository taskRepository, QueueProperties queueProperties){
        this.taskRepository=taskRepository;
        this.queueProperties = queueProperties;
    }

    @Scheduled(fixedDelayString = "${taskqueue.retention.poll-interval-ms:3600000}")
    @Transactional
    public void cleanTerminalTasks(){
        RetentionConfig retention = queueProperties.getRetention();
        if (Boolean.FALSE.equals(retention.getEnabled())){
            return;
        }

        Instant now = Instant.now();
        deleteOlderThan(TaskStatus.DONE, retention.getDoneRetentionDays(), now);
        deleteOlderThan(TaskStatus.FAILED, retention.getFailedRetentionDays(), now);
        deleteOlderThan(TaskStatus.DEAD, retention.getDeadRetentionDays(), now);
    }

    private long deleteOlderThan(TaskStatus status, int retentionDays, Instant now){
        int safeRetentionDays = Math.max(retentionDays, 0);
        Instant cutoff = now.minus(Duration.ofDays(safeRetentionDays));
        return taskRepository.deleteByStatusAndCreatedAtBefore(status, cutoff);
    }
}
