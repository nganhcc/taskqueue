package com.nganhcc.task_queue.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.nganhcc.task_queue.api.dto.EnqueueTaskRequest;
import com.nganhcc.task_queue.api.dto.TaskResponse;
import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.config.QueueProperties.QueueConfig;
import com.nganhcc.task_queue.exception.BadRequestException;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class TaskServiceTest {

    private final JsonMapper jsonMapper = new JsonMapper();

    private TaskRepository taskRepository;
    private RedisBroker redisBroker;
    private TaskService taskService;

    @BeforeEach
    void setUp() {
        taskRepository = mock(TaskRepository.class);
        redisBroker = mock(RedisBroker.class);
        when(taskRepository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        QueueProperties queueProperties = new QueueProperties();
        HashMap<String, QueueConfig> queues = new HashMap<>();
        queues.put("default", queueConfig(3));
        queues.put("high_priority", queueConfig(5));
        queueProperties.setQueues(queues);

        taskService = new TaskService(taskRepository, queueProperties, jsonMapper, redisBroker);
    }

    @Test
    void createsTaskWithExplicitQueueAndMaxRetries() throws Exception {
        JsonNode payload = jsonMapper.readTree("""
                {"to":"user@example.com","subject":"Hello"}
                """);
        Instant runAt = Instant.parse("2026-05-13T12:00:00Z");

        TaskResponse response = taskService.enqueue(new EnqueueTaskRequest(
                "high_priority",
                "send_email",
                payload,
                runAt,
                7,
                9));

        ArgumentCaptor<Task> taskCaptor = ArgumentCaptor.forClass(Task.class);
        verify(taskRepository).save(taskCaptor.capture());
        Task savedTask = taskCaptor.getValue();
        verify(redisBroker).enqueue(savedTask);

        assertThat(savedTask.getQueue()).isEqualTo("high_priority");
        assertThat(savedTask.getFn()).isEqualTo("send_email");
        assertThat(savedTask.getPayload()).isEqualTo("{\"to\":\"user@example.com\",\"subject\":\"Hello\"}");
        assertThat(savedTask.getMaxRetries()).isEqualTo(7);
        assertThat(savedTask.getPriority()).isEqualTo(9);
        assertThat(savedTask.getRunAt()).isEqualTo(runAt);
        assertThat(savedTask.getStatus()).isEqualTo(TaskStatus.PENDING);
        assertThat(savedTask.getAttempt()).isZero();

        assertThat(response.queue()).isEqualTo("high_priority");
        assertThat(response.payload()).isEqualTo(payload);
    }

    @Test
    void defaultsQueueToDefault() throws Exception {
        JsonNode payload = jsonMapper.readTree("{}");

        TaskResponse response = taskService.enqueue(new EnqueueTaskRequest(
                null,
                "send_email",
                payload,
                null,
                1,
                null));

        assertThat(response.queue()).isEqualTo("default");
    }

    @Test
    void defaultsMaxRetriesFromQueueConfig() throws Exception {
        JsonNode payload = jsonMapper.readTree("{}");

        TaskResponse response = taskService.enqueue(new EnqueueTaskRequest(
                "high_priority",
                "send_email",
                payload,
                null,
                null,
                null));

        assertThat(response.maxRetries()).isEqualTo(5);
    }

    @Test
    void rejectsUnknownQueue() throws Exception {
        JsonNode payload = jsonMapper.readTree("{}");

        assertThatThrownBy(() -> taskService.enqueue(new EnqueueTaskRequest(
                "missing",
                "send_email",
                payload,
                null,
                null,
                null)))
                .isInstanceOf(BadRequestException.class)
                .hasMessage("Unknown queue: missing");
    }

    @Test
    void rejectsBlankFn() throws Exception {
        JsonNode payload = jsonMapper.readTree("{}");

        assertThatThrownBy(() -> taskService.enqueue(new EnqueueTaskRequest(
                "default",
                " ",
                payload,
                null,
                null,
                null)))
                .isInstanceOf(BadRequestException.class)
                .hasMessage("fn is required");
    }

    @Test
    void rejectsMissingPayload() {
        assertThatThrownBy(() -> taskService.enqueue(new EnqueueTaskRequest(
                "default",
                "send_email",
                null,
                null,
                null,
                null)))
                .isInstanceOf(BadRequestException.class)
                .hasMessage("payload is required");
    }

    @Test
    void rejectsNegativeMaxRetries() throws Exception {
        JsonNode payload = jsonMapper.readTree("{}");

        assertThatThrownBy(() -> taskService.enqueue(new EnqueueTaskRequest(
                "default",
                "send_email",
                payload,
                null,
                -1,
                null)))
                .isInstanceOf(BadRequestException.class)
                .hasMessage("maxRetries must be greater than or equal to 0");
    }

    @Test
    void routesImmediateTasksToLiveRedisQueue() throws Exception {
        JsonNode payload = jsonMapper.readTree("{}");

        taskService.enqueue(new EnqueueTaskRequest(
                "default",
                "send_email",
                payload,
                null,
                null,
                null));

        ArgumentCaptor<Task> taskCaptor = ArgumentCaptor.forClass(Task.class);
        verify(redisBroker).enqueue(taskCaptor.capture());
        verify(redisBroker, never()).enqueueDelayed(any(Task.class));
        assertThat(taskCaptor.getValue().getRunAt()).isNull();
    }

    @Test
    void routesFutureTasksToDelayedRedisQueue() throws Exception {
        JsonNode payload = jsonMapper.readTree("{}");
        Instant runAt = Instant.now().plusSeconds(60);

        taskService.enqueue(new EnqueueTaskRequest(
                "default",
                "send_email",
                payload,
                runAt,
                null,
                null));

        ArgumentCaptor<Task> taskCaptor = ArgumentCaptor.forClass(Task.class);
        verify(redisBroker).enqueueDelayed(taskCaptor.capture());
        verify(redisBroker, never()).enqueue(any(Task.class));
        assertThat(taskCaptor.getValue().getRunAt()).isEqualTo(runAt);
    }

    private QueueConfig queueConfig(int maxRetries) {
        QueueConfig queueConfig = new QueueConfig();
        queueConfig.setMaxRetries(maxRetries);
        return queueConfig;
    }
}
