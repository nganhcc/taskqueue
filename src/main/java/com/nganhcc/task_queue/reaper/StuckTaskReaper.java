package com.nganhcc.task_queue.reaper;

import java.time.Instant;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.service.TaskFailureService;
import com.nganhcc.task_queue.store.TaskRepository;

@Component
public class StuckTaskReaper {
    private final RedisBroker redisBroker;
    private final TaskRepository taskRepository;
    private final QueueProperties queueProperties;
    private final TaskFailureService taskFailureService;

    public StuckTaskReaper(RedisBroker redisBroker, TaskRepository taskRepository, QueueProperties queueProperties, TaskFailureService taskFailureService){
        this.redisBroker= redisBroker;
        this.taskRepository = taskRepository;
        this.queueProperties= queueProperties;
        this.taskFailureService = taskFailureService;
    }

    @Scheduled(fixedDelayString = "${taskqueue.reaper.poll-interval-ms:30000}")
    public void reapStuckTasks(){
        Instant cutoff = Instant.now().minusSeconds(queueProperties.getReaper().getStuckThresholdMinutes() * 60L);
        for(Task task: taskRepository.findByStatusAndHeartbeatAtBefore(TaskStatus.RUNNING, cutoff)){
            redisBroker.removeProcessingById(task.getQueue(), task.getId());
            taskFailureService.handleFailure(task, new RuntimeException("Task stuck in RUNNING state"));
        }
    }
}
