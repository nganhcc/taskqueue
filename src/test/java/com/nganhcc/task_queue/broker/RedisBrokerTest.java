package com.nganhcc.task_queue.broker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import com.nganhcc.task_queue.model.Task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisBrokerTest {

    @Test
    void enqueuePushesSerializedTaskToQueueKey() {
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
        ZSetOperations<String, String> zSetOperations = mock(ZSetOperations.class);
        TaskSerializer taskSerializer = mock(TaskSerializer.class);
        RedisBroker redisBroker = new RedisBroker(redisTemplate, taskSerializer);
        Task task = new Task(null, "default", "send_email", "{}", 3, 0, null);

        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(taskSerializer.serialize(task)).thenReturn("{\"id\":\"task-id\"}");

        redisBroker.enqueue(task);

        verify(zSetOperations).add(eq("taskqueue:default"), eq("{\"id\":\"task-id\"}"), anyDouble());
    }

    @Test
    void pollMovesTaskFromQueueToProcessingQueue() {
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
        ListOperations<String, String> listOperations = mock(ListOperations.class);
        ZSetOperations<String, String> zSetOperations = mock(ZSetOperations.class);
        ZSetOperations.TypedTuple<String> tuple = mock(ZSetOperations.TypedTuple.class);
        TaskSerializer taskSerializer = mock(TaskSerializer.class);
        RedisBroker redisBroker = new RedisBroker(redisTemplate, taskSerializer);
        Task task = new Task(null, "default", "send_email", "{}", 3, 0, null);

        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(redisTemplate.opsForList()).thenReturn(listOperations);
        when(zSetOperations.popMin("taskqueue:default")).thenReturn(tuple);
        when(tuple.getValue()).thenReturn("{\"id\":\"task-id\"}");
        when(taskSerializer.deserialize("{\"id\":\"task-id\"}")).thenReturn(task);

        Task result = redisBroker.poll("default");

        assertThat(result).isSameAs(task);
        verify(listOperations).leftPush("taskqueue:default:processing", "{\"id\":\"task-id\"}");
    }

    @Test
    void pollReturnsNullWhenQueueIsEmpty() {
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
        ZSetOperations<String, String> zSetOperations = mock(ZSetOperations.class);
        TaskSerializer taskSerializer = mock(TaskSerializer.class);
        RedisBroker redisBroker = new RedisBroker(redisTemplate, taskSerializer);

        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.popMin("taskqueue:default")).thenReturn(null);

        Task result = redisBroker.poll("default");

        assertThat(result).isNull();
        verifyNoInteractions(taskSerializer);
    }

    @Test
    void acknowledgeRemovesSerializedTaskFromProcessingQueue() {
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
        ListOperations<String, String> listOperations = mock(ListOperations.class);
        TaskSerializer taskSerializer = mock(TaskSerializer.class);
        RedisBroker redisBroker = new RedisBroker(redisTemplate, taskSerializer);
        Task task = new Task(null, "default", "send_email", "{}", 3, 0, null);

        when(redisTemplate.opsForList()).thenReturn(listOperations);
        when(taskSerializer.serialize(task)).thenReturn("{\"id\":\"task-id\"}");

        redisBroker.acknowledge(task);

        verify(listOperations).remove("taskqueue:default:processing", 1, "{\"id\":\"task-id\"}");
    }

    @Test
    void enqueueDelayedAddsSerializedTaskToDelayedSortedSet() {
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
        ZSetOperations<String, String> zSetOperations = mock(ZSetOperations.class);
        TaskSerializer taskSerializer = mock(TaskSerializer.class);
        RedisBroker redisBroker = new RedisBroker(redisTemplate, taskSerializer);
        Instant runAt = Instant.parse("2030-01-01T00:00:00Z");
        Task task = new Task(null, "default", "send_email", "{}", 3, 0, runAt);

        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(taskSerializer.serialize(task)).thenReturn("{\"id\":\"task-id\"}");

        redisBroker.enqueueDelayed(task);

        verify(zSetOperations).add("taskqueue:delayed", "{\"id\":\"task-id\"}", runAt.toEpochMilli());
    }

    @Test
    void enqueueDelayedRejectsTaskWithoutRunAt() {
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
        TaskSerializer taskSerializer = mock(TaskSerializer.class);
        RedisBroker redisBroker = new RedisBroker(redisTemplate, taskSerializer);
        Task task = new Task(null, "default", "send_email", "{}", 3, 0, null);

        assertThatThrownBy(() -> redisBroker.enqueueDelayed(task))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Delayed task must have runAt");

        verifyNoInteractions(redisTemplate, taskSerializer);
    }
}
