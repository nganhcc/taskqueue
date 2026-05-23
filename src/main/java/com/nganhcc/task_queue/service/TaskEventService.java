package com.nganhcc.task_queue.service;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import com.nganhcc.task_queue.api.dto.PageResponse;
import com.nganhcc.task_queue.api.dto.TaskEventResponse;
import com.nganhcc.task_queue.exception.BadRequestException;
import com.nganhcc.task_queue.exception.TaskNotFoundException;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskEvent;
import com.nganhcc.task_queue.model.TaskEventType;
import com.nganhcc.task_queue.store.TaskEventRepository;
import com.nganhcc.task_queue.store.TaskRepository;

@Service
public class TaskEventService {
    private static final int DEFAULT_PAGE = 0;
    private static final int DEFAULT_SIZE = 50;
    private static final int MAX_SIZE = 200;
    private static final String DEFAULT_SORT = "createdAt,asc";
    private static final Set<String> ALLOWED_SORT_FIELDS = Set.of("createdAt", "attempt", "type");

    private final TaskEventRepository taskEventRepository;
    private final TaskRepository taskRepository;

    public TaskEventService(TaskEventRepository taskEventRepository, TaskRepository taskRepository) {
        this.taskEventRepository = taskEventRepository;
        this.taskRepository = taskRepository;
    }

    public void record(Task task, TaskEventType type, String message) {
        record(task, type, message, task.getError(), task.getStackTrace());
    }

    public void record(Task task, TaskEventType type, String message, String error, String stackTrace) {
        TaskEvent event = new TaskEvent(
                null,
                task.getId(),
                type,
                task.getAttempt(),
                message,
                error,
                stackTrace);
        taskEventRepository.save(event);
    }

    public PageResponse<TaskEventResponse> listTaskEvents(UUID taskId, Integer page, Integer size, String sort) {
        if (!taskRepository.existsById(taskId)) {
            throw new TaskNotFoundException(taskId);
        }
        Page<TaskEvent> eventPage = taskEventRepository.findByTaskId(
                taskId,
                pageRequest(page, size, sort, DEFAULT_SORT, ALLOWED_SORT_FIELDS));
        List<TaskEventResponse> items = eventPage.getContent().stream()
                .map(TaskEventResponse::from)
                .toList();
        return PageResponse.from(eventPage, items);
    }

    private PageRequest pageRequest(Integer page, Integer size, String sort, String defaultSort, Set<String> allowedSortFields) {
        int resolvedPage = page == null ? DEFAULT_PAGE : page;
        int resolvedSize = size == null ? DEFAULT_SIZE : size;
        if (resolvedPage < 0) {
            throw new BadRequestException("page must be greater than or equal to 0");
        }
        if (resolvedSize < 1 || resolvedSize > MAX_SIZE) {
            throw new BadRequestException("size must be between 1 and 200");
        }
        return PageRequest.of(resolvedPage, resolvedSize, parseSort(sort, defaultSort, allowedSortFields));
    }

    private Sort parseSort(String sort, String defaultSort, Set<String> allowedSortFields) {
        String sortValue = sort == null || sort.isBlank() ? defaultSort : sort.trim();
        String[] parts = sortValue.split(",", -1);
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new BadRequestException("sort must use format: field,direction");
        }
        String field = parts[0].trim();
        String direction = parts[1].trim().toLowerCase();
        if (!allowedSortFields.contains(field)) {
            throw new BadRequestException("Unsupported sort field: " + field);
        }
        if (!direction.equals("asc") && !direction.equals("desc")) {
            throw new BadRequestException("sort direction must be asc or desc");
        }
        return Sort.by(Sort.Direction.fromString(direction), field);
    }
}
