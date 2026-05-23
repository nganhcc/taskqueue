package com.nganhcc.task_queue.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import com.nganhcc.task_queue.api.dto.PageResponse;
import com.nganhcc.task_queue.api.dto.TaskEventResponse;
import com.nganhcc.task_queue.exception.BadRequestException;
import com.nganhcc.task_queue.exception.TaskNotFoundException;
import com.nganhcc.task_queue.model.TaskEvent;
import com.nganhcc.task_queue.model.TaskEventType;
import com.nganhcc.task_queue.store.TaskEventRepository;
import com.nganhcc.task_queue.store.TaskRepository;

class TaskEventServiceTest {

    @Test
    void listsTaskEventsWithDefaultChronologicalSort() {
        TaskEventRepository taskEventRepository = mock(TaskEventRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        TaskEventService taskEventService = new TaskEventService(taskEventRepository, taskRepository);
        UUID taskId = UUID.randomUUID();
        TaskEvent event = new TaskEvent(null, taskId, TaskEventType.CREATED, 0, "Task created", null, null);

        when(taskRepository.existsById(taskId)).thenReturn(true);
        when(taskEventRepository.findByTaskId(org.mockito.ArgumentMatchers.eq(taskId), any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(event)));

        PageResponse<TaskEventResponse> events = taskEventService.listTaskEvents(taskId, null, null, null);

        assertThat(events.items()).hasSize(1);
        assertThat(events.items().getFirst().taskId()).isEqualTo(taskId);
        assertThat(events.items().getFirst().type()).isEqualTo(TaskEventType.CREATED);

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(taskEventRepository).findByTaskId(org.mockito.ArgumentMatchers.eq(taskId), pageableCaptor.capture());
        Pageable pageable = pageableCaptor.getValue();
        assertThat(pageable.getPageNumber()).isZero();
        assertThat(pageable.getPageSize()).isEqualTo(50);
        Sort.Order order = pageable.getSort().getOrderFor("createdAt");
        assertThat(order).isNotNull();
        assertThat(order.getDirection()).isEqualTo(Sort.Direction.ASC);
    }

    @Test
    void listTaskEventsAppliesCustomPaginationAndSort() {
        TaskEventRepository taskEventRepository = mock(TaskEventRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        TaskEventService taskEventService = new TaskEventService(taskEventRepository, taskRepository);
        UUID taskId = UUID.randomUUID();
        TaskEvent event = new TaskEvent(null, taskId, TaskEventType.FAILED, 2, "Task execution failed", "boom", "stack");

        when(taskRepository.existsById(taskId)).thenReturn(true);
        when(taskEventRepository.findByTaskId(org.mockito.ArgumentMatchers.eq(taskId), any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(event)));

        PageResponse<TaskEventResponse> events = taskEventService.listTaskEvents(taskId, 1, 25, "attempt,desc");

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(taskEventRepository).findByTaskId(org.mockito.ArgumentMatchers.eq(taskId), pageableCaptor.capture());
        Pageable pageable = pageableCaptor.getValue();
        assertThat(pageable.getPageNumber()).isEqualTo(1);
        assertThat(pageable.getPageSize()).isEqualTo(25);
        Sort.Order order = pageable.getSort().getOrderFor("attempt");
        assertThat(order).isNotNull();
        assertThat(order.getDirection()).isEqualTo(Sort.Direction.DESC);
        assertThat(events.items().getFirst().type()).isEqualTo(TaskEventType.FAILED);
    }

    @Test
    void rejectsMissingTaskWhenListingEvents() {
        TaskEventRepository taskEventRepository = mock(TaskEventRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        TaskEventService taskEventService = new TaskEventService(taskEventRepository, taskRepository);
        UUID taskId = UUID.randomUUID();

        when(taskRepository.existsById(taskId)).thenReturn(false);

        assertThatThrownBy(() -> taskEventService.listTaskEvents(taskId, null, null, null))
                .isInstanceOf(TaskNotFoundException.class);
    }

    @Test
    void rejectsInvalidPaginationAndSort() {
        TaskEventRepository taskEventRepository = mock(TaskEventRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        TaskEventService taskEventService = new TaskEventService(taskEventRepository, taskRepository);
        UUID taskId = UUID.randomUUID();

        when(taskRepository.existsById(taskId)).thenReturn(true);

        assertThatThrownBy(() -> taskEventService.listTaskEvents(taskId, -1, 50, null))
                .isInstanceOf(BadRequestException.class)
                .hasMessage("page must be greater than or equal to 0");

        assertThatThrownBy(() -> taskEventService.listTaskEvents(taskId, 0, 201, null))
                .isInstanceOf(BadRequestException.class)
                .hasMessage("size must be between 1 and 200");

        assertThatThrownBy(() -> taskEventService.listTaskEvents(taskId, 0, 50, "queue,asc"))
                .isInstanceOf(BadRequestException.class)
                .hasMessage("Unsupported sort field: queue");

        assertThatThrownBy(() -> taskEventService.listTaskEvents(taskId, 0, 50, "createdAt,latest"))
                .isInstanceOf(BadRequestException.class)
                .hasMessage("sort direction must be asc or desc");
    }
}
