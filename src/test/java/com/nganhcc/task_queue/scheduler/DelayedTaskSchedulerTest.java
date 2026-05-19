package com.nganhcc.task_queue.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;

class DelayedTaskSchedulerTest {

    @Test
    void movesReadyDelayedTasksToLiveQueue() {
        RedisBroker redisBroker = mock(RedisBroker.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        DelayedTaskScheduler scheduler = new DelayedTaskScheduler(redisBroker, taskRepository);
        Task delayedTask = task(UUID.randomUUID());
        Task dbTask = task(delayedTask.getId());
        dbTask.setRunAt(Instant.parse("2026-05-19T00:00:00Z"));

        when(redisBroker.pollReadyDelayed(any(Instant.class))).thenReturn(delayedTask).thenReturn(null);
        when(taskRepository.findById(delayedTask.getId())).thenReturn(Optional.of(dbTask));
        when(taskRepository.save(dbTask)).thenReturn(dbTask);

        scheduler.moveReadyTasks();

        assertThat(dbTask.getRunAt()).isNull();
        verify(taskRepository).save(dbTask);
        verify(redisBroker).enqueue(dbTask);
    }

    @Test
    void skipsMissingOrNonPendingTasks() {
        RedisBroker redisBroker = mock(RedisBroker.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        DelayedTaskScheduler scheduler = new DelayedTaskScheduler(redisBroker, taskRepository);
        Task missingDelayedTask = task(UUID.randomUUID());
        Task doneDelayedTask = task(UUID.randomUUID());
        Task doneDbTask = task(doneDelayedTask.getId());
        doneDbTask.setStatus(TaskStatus.DONE);

        when(redisBroker.pollReadyDelayed(any(Instant.class)))
                .thenReturn(missingDelayedTask)
                .thenReturn(doneDelayedTask)
                .thenReturn(null);
        when(taskRepository.findById(missingDelayedTask.getId())).thenReturn(Optional.empty());
        when(taskRepository.findById(doneDelayedTask.getId())).thenReturn(Optional.of(doneDbTask));

        scheduler.moveReadyTasks();

        verify(taskRepository, never()).save(any(Task.class));
        verify(redisBroker, never()).enqueue(any(Task.class));
    }

    private Task task(UUID id) {
        return new Task(id, "default", "log_message", "{}", 3, 0, null);
    }
}
