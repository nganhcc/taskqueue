package com.nganhcc.task_queue.scheduler;

import java.time.Instant;
import java.util.Set;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;


@Component
public class DelayedTaskScheduler {
    private final TaskRepository taskRepository;
    private final RedisBroker redisBroker;

    public DelayedTaskScheduler(RedisBroker redisBroker, TaskRepository taskRepository){
        this.redisBroker= redisBroker;
        this.taskRepository= taskRepository;
    }

    @Scheduled(fixedDelayString = "${taskqueue.scheduler.poll-interval-ms:1000}")
    public void moveReadyTasks(){
        Set<Task> readyTasks = redisBroker.findReadyDelayedTasks(Instant.now());
        for (Task task: readyTasks){
            redisBroker.removeDelayed(task);
            Task dbTask = taskRepository.findById(task.getId()).orElse(null);
            if (dbTask == null || dbTask.getStatus() != TaskStatus.PENDING){
                continue;
            }
            dbTask.setRunAt(null);
            Task saved = taskRepository.save(dbTask);
            redisBroker.enqueue(saved);
        }
    }

    
}
