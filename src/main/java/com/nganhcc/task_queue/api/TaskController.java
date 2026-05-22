package com.nganhcc.task_queue.api;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.nganhcc.task_queue.api.dto.EnqueueTaskRequest;
import com.nganhcc.task_queue.api.dto.TaskResponse;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.service.TaskService;

@RestController
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping("/tasks")
    public ResponseEntity<TaskResponse> enqueue(@RequestBody EnqueueTaskRequest request) {
        TaskResponse response = taskService.enqueue(request);
        URI location = ServletUriComponentsBuilder
                .fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(response.id())
                .toUri();
        return ResponseEntity.created(location).body(response);
    }

    @GetMapping("/tasks")
    public ResponseEntity<List<TaskResponse>> listTasks(@RequestParam(required = false) String queue, @RequestParam(required = false) TaskStatus status){
        return ResponseEntity.ok(taskService.listTasks(queue, status));
    }

    @GetMapping("/tasks/{id}")
    public ResponseEntity<TaskResponse> getTask(@PathVariable UUID id){
        TaskResponse response = taskService.getTask(id);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/tasks/{id}/cancel")
    public ResponseEntity<TaskResponse> cancelTask(@PathVariable UUID id){
        return ResponseEntity.ok(taskService.cancelTask(id));
    }

    @PostMapping("/tasks/{id}/retry")
    public ResponseEntity<TaskResponse> retryTask(@PathVariable UUID id){
        return ResponseEntity.ok(taskService.retryTask(id));
    }

    @PostMapping("/tasks/{id}/heartbeat")
    public ResponseEntity<TaskResponse> heartbeat(@PathVariable UUID id){
        return ResponseEntity.ok(taskService.heartbeat(id));
    }
}
