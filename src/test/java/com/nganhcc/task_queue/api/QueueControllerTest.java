package com.nganhcc.task_queue.api;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.service.QueueControllerService;
import com.nganhcc.task_queue.service.TaskService;
import com.nganhcc.task_queue.worker.Worker;

@WebMvcTest(QueueController.class)
class QueueControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private QueueControllerService queueControllerService;

    @MockitoBean
    private QueueProperties queueProperties;

    @MockitoBean
    private RedisBroker redisBroker;

    @MockitoBean
    private TaskService taskService;

    @MockitoBean
    private Worker worker;

    @Test
    void purgeDelayedReturnsDeletedAndDbMarkedCount() throws Exception {
        when(redisBroker.purgeDelayed()).thenReturn(true);
        when(taskService.markDelayedPurged()).thenReturn(3);

        mockMvc.perform(post("/queues/delayed/purge"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.delayedDeleted").value(true))
                .andExpect(jsonPath("$.tasksMarkedFailed").value(3));
    }

    @Test
    void purgeDlqReturnsDeletedAndDbMarkedCount() throws Exception {
        when(redisBroker.purgeDlq()).thenReturn(true);
        when(taskService.markDlqPurged()).thenReturn(2);

        mockMvc.perform(post("/queues/dlq/purge"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.dlqDeleted").value(true))
                .andExpect(jsonPath("$.tasksMarkedFailed").value(2));
    }
}
