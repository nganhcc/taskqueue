package com.nganhcc.task_queue.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.nganhcc.task_queue.api.dto.TaskEventResponse;
import com.nganhcc.task_queue.exception.TaskNotFoundException;
import com.nganhcc.task_queue.model.TaskEvent;
import com.nganhcc.task_queue.model.TaskEventType;
import com.nganhcc.task_queue.store.TaskEventRepository;
import com.nganhcc.task_queue.store.TaskRepository;

class TaskEventServiceTest {

    @Test
    void listsTaskEventsInRepositoryOrder() {
        TaskEventRepository taskEventRepository = mock(TaskEventRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        TaskEventService taskEventService = new TaskEventService(taskEventRepository, taskRepository);
        UUID taskId = UUID.randomUUID();
        TaskEvent event = new TaskEvent(null, taskId, TaskEventType.CREATED, 0, "Task created", null, null);

        when(taskRepository.existsById(taskId)).thenReturn(true);
        when(taskEventRepository.findByTaskIdOrderByCreatedAtAsc(taskId)).thenReturn(List.of(event));

        List<TaskEventResponse> events = taskEventService.listTaskEvents(taskId);

        assertThat(events).hasSize(1);
        assertThat(events.getFirst().taskId()).isEqualTo(taskId);
        assertThat(events.getFirst().type()).isEqualTo(TaskEventType.CREATED);
        verify(taskEventRepository).findByTaskIdOrderByCreatedAtAsc(taskId);
    }

    @Test
    void rejectsMissingTaskWhenListingEvents() {
        TaskEventRepository taskEventRepository = mock(TaskEventRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        TaskEventService taskEventService = new TaskEventService(taskEventRepository, taskRepository);
        UUID taskId = UUID.randomUUID();

        when(taskRepository.existsById(taskId)).thenReturn(false);

        assertThatThrownBy(() -> taskEventService.listTaskEvents(taskId))
                .isInstanceOf(TaskNotFoundException.class);
    }
}
