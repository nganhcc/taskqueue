package com.nganhcc.task_queue.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.config.QueueProperties.QueueConfig;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.retry.RetryPolicy;
import com.nganhcc.task_queue.service.TaskFailureService;
import com.nganhcc.task_queue.store.TaskRepository;

class WorkerTest {

    private TaskRepository taskRepository;
    private RedisBroker redisBroker;
    private HandlerRegistry handlerRegistry;
    private TaskHandler taskHandler;
    private QueueProperties queueProperties;
    private Worker worker;

    @BeforeEach
    void setUp() {
        taskRepository = mock(TaskRepository.class);
        redisBroker = mock(RedisBroker.class);
        handlerRegistry = mock(HandlerRegistry.class);
        taskHandler = mock(TaskHandler.class);

        queueProperties = new QueueProperties();
        HashMap<String, QueueConfig> queues = new HashMap<>();
        queues.put("default", queueConfig(1000));
        queueProperties.setQueues(queues);

        TaskFailureService taskFailureService = new TaskFailureService(
                new RetryPolicy(),
                queueProperties,
                taskRepository,
                redisBroker);
        worker = new Worker(taskRepository, redisBroker, handlerRegistry, queueProperties, taskFailureService);
    }

    @AfterEach
    void tearDown() {
        worker.shutdown();
    }

    @Test
    void doesNothingWhenQueueIsEmpty() {
        when(redisBroker.poll("default")).thenReturn(null);

        worker.runOnce("default");

        verify(redisBroker).poll("default");
        verifyNoInteractions(taskRepository, handlerRegistry);
    }

    @Test
    void marksTaskDoneAndAcknowledgesAfterSuccessfulHandling() {
        Task polledTask = task("log_message", 0, 3);
        Task dbTask = taskWithId(polledTask.getId(), "log_message", 0, 3);
        List<TaskStatus> savedStatuses = captureSavedStatuses();

        when(redisBroker.poll("default")).thenReturn(polledTask);
        when(taskRepository.findById(polledTask.getId())).thenReturn(Optional.of(dbTask));
        when(handlerRegistry.get("log_message")).thenReturn(taskHandler);
        when(taskHandler.handle(dbTask)).thenReturn("{\"ok\":true}");

        worker.runOnce("default");

        assertThat(savedStatuses).containsExactly(TaskStatus.RUNNING, TaskStatus.DONE);
        assertThat(dbTask.getStatus()).isEqualTo(TaskStatus.DONE);
        assertThat(dbTask.getResult()).isEqualTo("{\"ok\":true}");
        assertThat(dbTask.getAttempt()).isZero();
        verify(redisBroker).acknowledge(polledTask);
        verify(redisBroker, never()).enqueueDelayed(any(Task.class));
        verify(redisBroker, never()).sendToDlq(any(Task.class));
    }

    @Test
    void retriesFailedTaskWhenRetriesRemain() {
        Task polledTask = task("log_message", 0, 3);
        Task dbTask = taskWithId(polledTask.getId(), "log_message", 0, 3);
        List<TaskStatus> savedStatuses = captureSavedStatuses();

        when(redisBroker.poll("default")).thenReturn(polledTask);
        when(taskRepository.findById(polledTask.getId())).thenReturn(Optional.of(dbTask));
        when(handlerRegistry.get("log_message")).thenReturn(taskHandler);
        when(taskHandler.handle(dbTask)).thenThrow(new RuntimeException("boom"));

        worker.runOnce("default");

        assertThat(savedStatuses).containsExactly(TaskStatus.RUNNING, TaskStatus.PENDING);
        assertThat(dbTask.getStatus()).isEqualTo(TaskStatus.PENDING);
        assertThat(dbTask.getAttempt()).isEqualTo(1);
        assertThat(dbTask.getError()).isEqualTo("boom");
        assertThat(dbTask.getStackTrace()).contains("RuntimeException").contains("boom");
        assertThat(dbTask.getRunAt()).isNotNull();
        verify(redisBroker).enqueueDelayed(dbTask);
        verify(redisBroker).acknowledge(polledTask);
        verify(redisBroker, never()).sendToDlq(any(Task.class));
    }

    @Test
    void sendsTaskToDlqWhenRetriesAreExhausted() {
        Task polledTask = task("log_message", 2, 3);
        Task dbTask = taskWithId(polledTask.getId(), "log_message", 2, 3);
        List<TaskStatus> savedStatuses = captureSavedStatuses();

        when(redisBroker.poll("default")).thenReturn(polledTask);
        when(taskRepository.findById(polledTask.getId())).thenReturn(Optional.of(dbTask));
        when(handlerRegistry.get("log_message")).thenReturn(taskHandler);
        when(taskHandler.handle(dbTask)).thenThrow(new RuntimeException("boom"));

        worker.runOnce("default");

        assertThat(savedStatuses).containsExactly(TaskStatus.RUNNING, TaskStatus.DEAD);
        assertThat(dbTask.getStatus()).isEqualTo(TaskStatus.DEAD);
        assertThat(dbTask.getAttempt()).isEqualTo(3);
        assertThat(dbTask.getError()).isEqualTo("boom");
        assertThat(dbTask.getStackTrace()).contains("RuntimeException").contains("boom");
        verify(redisBroker).sendToDlq(dbTask);
        verify(redisBroker).acknowledge(polledTask);
        verify(redisBroker, never()).enqueueDelayed(any(Task.class));
    }

    @Test
    void retriesTaskWhenHandlerTimesOut() {
        queueProperties.getWorker().setHandlerTimeoutMs(10);
        Task polledTask = task("log_message", 0, 3);
        Task dbTask = taskWithId(polledTask.getId(), "log_message", 0, 3);
        List<TaskStatus> savedStatuses = captureSavedStatuses();

        when(redisBroker.poll("default")).thenReturn(polledTask);
        when(taskRepository.findById(polledTask.getId())).thenReturn(Optional.of(dbTask));
        when(handlerRegistry.get("log_message")).thenReturn(taskHandler);
        when(taskHandler.handle(dbTask)).thenAnswer(invocation -> {
            Thread.sleep(1000);
            return "{\"ok\":true}";
        });

        worker.runOnce("default");

        assertThat(savedStatuses).containsExactly(TaskStatus.RUNNING, TaskStatus.PENDING);
        assertThat(dbTask.getStatus()).isEqualTo(TaskStatus.PENDING);
        assertThat(dbTask.getAttempt()).isEqualTo(1);
        assertThat(dbTask.getError()).isEqualTo("Handler timed out!");
        assertThat(dbTask.getStackTrace()).contains("RuntimeException").contains("Handler timed out!");
        assertThat(dbTask.getRunAt()).isNotNull();
        verify(redisBroker).enqueueDelayed(dbTask);
        verify(redisBroker).acknowledge(polledTask);
        verify(redisBroker, never()).sendToDlq(any(Task.class));
    }

    private List<TaskStatus> captureSavedStatuses() {
        List<TaskStatus> savedStatuses = new ArrayList<>();
        when(taskRepository.save(any(Task.class))).thenAnswer(invocation -> {
            Task task = invocation.getArgument(0);
            savedStatuses.add(task.getStatus());
            return task;
        });
        return savedStatuses;
    }

    private Task task(String fn, int attempt, int maxRetries) {
        return taskWithId(UUID.randomUUID(), fn, attempt, maxRetries);
    }

    private Task taskWithId(UUID id, String fn, int attempt, int maxRetries) {
        Task task = new Task(id, "default", fn, "{}", maxRetries, 0, null);
        task.setAttempt(attempt);
        return task;
    }

    private QueueConfig queueConfig(long baseDelayMs) {
        QueueConfig queueConfig = new QueueConfig();
        queueConfig.setBaseDelayMs(baseDelayMs);
        return queueConfig;
    }
}
