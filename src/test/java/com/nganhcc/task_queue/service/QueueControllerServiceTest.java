package com.nganhcc.task_queue.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.nganhcc.task_queue.broker.RedisBroker;

class QueueControllerServiceTest {

    @Test
    void pauseDelegatesToRedisBroker() {
        RedisBroker redisBroker = mock(RedisBroker.class);
        QueueControllerService service = new QueueControllerService(redisBroker);

        service.pause("default");

        verify(redisBroker).pauseQueue("default");
    }

    @Test
    void resumeDelegatesToRedisBroker() {
        RedisBroker redisBroker = mock(RedisBroker.class);
        QueueControllerService service = new QueueControllerService(redisBroker);

        service.resume("default");

        verify(redisBroker).resumeQueue("default");
    }

    @Test
    void isPausedDelegatesToRedisBroker() {
        RedisBroker redisBroker = mock(RedisBroker.class);
        QueueControllerService service = new QueueControllerService(redisBroker);

        when(redisBroker.isQueuePaused("default")).thenReturn(true);

        assertThat(service.isPaused("default")).isTrue();
    }

    @Test
    void pausedQueuesDelegatesToRedisBroker() {
        RedisBroker redisBroker = mock(RedisBroker.class);
        QueueControllerService service = new QueueControllerService(redisBroker);

        when(redisBroker.pausedQueues()).thenReturn(Set.of("default"));

        assertThat(service.pausedQueues()).containsExactly("default");
    }
}
