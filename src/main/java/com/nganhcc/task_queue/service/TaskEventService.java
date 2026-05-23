package com.nganhcc.task_queue.service;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.nganhcc.task_queue.api.dto.TaskEventResponse;
import com.nganhcc.task_queue.exception.TaskNotFoundException;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskEvent;
import com.nganhcc.task_queue.model.TaskEventType;
import com.nganhcc.task_queue.store.TaskEventRepository;
import com.nganhcc.task_queue.store.TaskRepository;

@Service
public class TaskEventService {
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

    public List<TaskEventResponse> listTaskEvents(UUID taskId) {
        if (!taskRepository.existsById(taskId)) {
            throw new TaskNotFoundException(taskId);
        }
        return taskEventRepository.findByTaskIdOrderByCreatedAtAsc(taskId)
                .stream()
                .map(TaskEventResponse::from)
                .toList();
    }
}
