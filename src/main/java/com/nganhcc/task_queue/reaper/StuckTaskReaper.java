package com.nganhcc.task_queue.reaper;

import java.time.Instant;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;

@Component
public class StuckTaskReaper {
    private final RedisBroker redisBroker;
    private final TaskRepository taskRepository;
    private final QueueProperties queueProperties;

    public StuckTaskReaper(RedisBroker redisBroker, TaskRepository taskRepository, QueueProperties queueProperties){
        this.redisBroker= redisBroker;
        this.taskRepository = taskRepository;
        this.queueProperties= queueProperties;
    }

    @Scheduled(fixedDelayString = "${taskqueue.reaper.poll-interval-ms:30000}")
    public void reapStuckTasks(){
        Instant cutoff = Instant.now().minusSeconds(queueProperties.getReaper().getStuckThresholdMinutes() * 60L);
        for(Task task: taskRepository.findByStatusAndStartedAtBefore(TaskStatus.RUNNING, cutoff)){
            redisBroker.removeProcessingById(task.getQueue(), task.getId());
            task.setStatus(TaskStatus.PENDING);
            task.setStartedAt(null);
            Task saved = taskRepository.save(task);
            redisBroker.enqueue(saved);
        }
    }
}
