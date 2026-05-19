package com.nganhcc.task_queue.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;

@WebMvcTest(DlqController.class)
class DlqControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RedisBroker redisBroker;

    @MockitoBean
    private TaskRepository taskRepository;

    @Test
    void replayKeepsAttemptCountByDefault() throws Exception {
        UUID id = UUID.fromString("9f3a1b2c-0000-0000-0000-000000000001");
        Task dlqTask = deadTask(id, 3);
        Task dbTask = deadTask(id, 3);
        dbTask.setStartedAt(Instant.parse("2026-05-19T00:00:00Z"));
        dbTask.setError("boom");
        dbTask.setStackTrace("stack");

        when(redisBroker.findDlqTask(id)).thenReturn(dlqTask);
        when(taskRepository.findById(id)).thenReturn(Optional.of(dbTask));
        when(taskRepository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        mockMvc.perform(post("/dlq/{id}/replay", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.attempt").value(3))
                .andExpect(jsonPath("$.error").doesNotExist())
                .andExpect(jsonPath("$.startedAt").doesNotExist())
                .andExpect(jsonPath("$.runAt").doesNotExist());

        verify(redisBroker).removeFromDlq(dlqTask);
        verify(taskRepository).save(dbTask);
        verify(redisBroker).enqueue(dbTask);
    }

    @Test
    void replayCanResetAttemptCount() throws Exception {
        UUID id = UUID.fromString("9f3a1b2c-0000-0000-0000-000000000002");
        Task dlqTask = deadTask(id, 3);
        Task dbTask = deadTask(id, 3);

        when(redisBroker.findDlqTask(id)).thenReturn(dlqTask);
        when(taskRepository.findById(id)).thenReturn(Optional.of(dbTask));
        when(taskRepository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        mockMvc.perform(post("/dlq/{id}/replay?resetAttempts=true", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.attempt").value(0));

        verify(redisBroker).removeFromDlq(dlqTask);
        verify(taskRepository).save(dbTask);
        verify(redisBroker).enqueue(dbTask);
    }

    private Task deadTask(UUID id, int attempt) {
        Task task = new Task(id, "default", "log_message", "{\"message\":\"hello\"}", 3, 0, null);
        task.setStatus(TaskStatus.DEAD);
        task.setAttempt(attempt);
        return task;
    }
}
