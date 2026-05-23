package com.nganhcc.task_queue.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.nganhcc.task_queue.api.dto.EnqueueTaskRequest;
import com.nganhcc.task_queue.api.dto.PageResponse;
import com.nganhcc.task_queue.api.dto.TaskEventResponse;
import com.nganhcc.task_queue.api.dto.TaskResponse;
import com.nganhcc.task_queue.exception.BadRequestException;
import com.nganhcc.task_queue.model.TaskEventType;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.service.TaskService;

import tools.jackson.databind.json.JsonMapper;

@WebMvcTest(TaskController.class)
class TaskControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JsonMapper jsonMapper;

    @MockitoBean
    private TaskService taskService;

    @Test
    void enqueueReturnsCreatedTask() throws Exception {
        UUID id = UUID.fromString("9f3a1b2c-0000-0000-0000-000000000000");
        when(taskService.enqueue(any(EnqueueTaskRequest.class))).thenReturn(new TaskResponse(
                id,
                "default",
                "send_email",
                jsonMapper.readTree("""
                        {"to":"user@example.com"}
                        """),
                TaskStatus.PENDING,
                0,
                3,
                0,
                null,
                null,
                Instant.parse("2026-05-13T12:00:00Z"),
                null,
                Instant.parse("2026-05-13T10:00:00Z"),
                null,
                null));

        mockMvc.perform(post("/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {
                          "queue": "default",
                          "fn": "send_email",
                          "payload": {"to": "user@example.com"},
                          "runAt": "2026-05-13T12:00:00Z",
                          "maxRetries": 3
                        }
                        """))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "http://localhost/tasks/" + id))
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.queue").value("default"))
                .andExpect(jsonPath("$.fn").value("send_email"))
                .andExpect(jsonPath("$.payload.to").value("user@example.com"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.attempt").value(0))
                .andExpect(jsonPath("$.maxRetries").value(3));
    }

    @Test
    void enqueueReturnsBadRequestForServiceValidationErrors() throws Exception {
        when(taskService.enqueue(any(EnqueueTaskRequest.class)))
                .thenThrow(new BadRequestException("fn is required"));

        mockMvc.perform(post("/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {
                          "queue": "default",
                          "fn": " ",
                          "payload": {}
                        }
                        """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("fn is required"));
    }

    @Test
    void listTasksReturnsPaginatedResponse() throws Exception {
        UUID id = UUID.fromString("9f3a1b2c-0000-0000-0000-000000000000");
        TaskResponse task = new TaskResponse(
                id,
                "default",
                "send_email",
                jsonMapper.readTree("{}"),
                TaskStatus.PENDING,
                0,
                3,
                5,
                null,
                null,
                null,
                null,
                Instant.parse("2026-05-13T10:00:00Z"),
                null,
                null);
        when(taskService.listTasks(eq("default"), eq(TaskStatus.PENDING), eq(1), eq(25), eq("priority,desc")))
                .thenReturn(new PageResponse<>(List.of(task), 1, 25, 26, 2, false, true));

        mockMvc.perform(get("/tasks")
                .param("queue", "default")
                .param("status", "PENDING")
                .param("page", "1")
                .param("size", "25")
                .param("sort", "priority,desc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].id").value(id.toString()))
                .andExpect(jsonPath("$.items[0].priority").value(5))
                .andExpect(jsonPath("$.page").value(1))
                .andExpect(jsonPath("$.size").value(25))
                .andExpect(jsonPath("$.totalElements").value(26))
                .andExpect(jsonPath("$.totalPages").value(2))
                .andExpect(jsonPath("$.first").value(false))
                .andExpect(jsonPath("$.last").value(true));
    }

    @Test
    void listTasksReturnsBadRequestForInvalidPagination() throws Exception {
        when(taskService.listTasks(eq(null), eq(null), eq(-1), eq(50), eq(null)))
                .thenThrow(new BadRequestException("page must be greater than or equal to 0"));

        mockMvc.perform(get("/tasks")
                .param("page", "-1")
                .param("size", "50"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("page must be greater than or equal to 0"));
    }

    @Test
    void listTaskEventsReturnsEvents() throws Exception {
        UUID taskId = UUID.fromString("9f3a1b2c-0000-0000-0000-000000000000");
        UUID eventId = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
        TaskEventResponse event = new TaskEventResponse(
                eventId,
                taskId,
                TaskEventType.CREATED,
                0,
                "Task created",
                null,
                null,
                Instant.parse("2026-05-13T10:00:00Z"));
        when(taskService.listTaskEvents(eq(taskId), eq(0), eq(10), eq("createdAt,asc")))
                .thenReturn(new PageResponse<>(List.of(event), 0, 10, 1, 1, true, true));

        mockMvc.perform(get("/tasks/{id}/events", taskId)
                .param("page", "0")
                .param("size", "10")
                .param("sort", "createdAt,asc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].id").value(eventId.toString()))
                .andExpect(jsonPath("$.items[0].taskId").value(taskId.toString()))
                .andExpect(jsonPath("$.items[0].type").value("CREATED"))
                .andExpect(jsonPath("$.items[0].attempt").value(0))
                .andExpect(jsonPath("$.items[0].message").value("Task created"))
                .andExpect(jsonPath("$.page").value(0))
                .andExpect(jsonPath("$.size").value(10));
    }
}
