package com.nganhcc.task_queue.api;

import java.util.HashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.store.TaskRepository;

@RestController
public class MetricsController {
    private final TaskRepository taskRepository;
    private final QueueProperties queueProperties;
    private final RedisBroker redisBroker;
    public MetricsController(TaskRepository taskRepository, RedisBroker redisBroker, QueueProperties queueProperties){
        this.taskRepository=taskRepository;
        this.queueProperties=queueProperties;
        this.redisBroker=redisBroker;
    }

    @GetMapping("/metrics/tasks")
    public Map<String, Long> taskMetrics(){
        return Map.of(
            "pending", count(TaskStatus.PENDING),
            "running", count(TaskStatus.RUNNING),
            "done", count(TaskStatus.DONE),
            "failed", count(TaskStatus.FAILED),
            "dead", count(TaskStatus.DEAD)
        );
    }

    private long count(TaskStatus status){
        return taskRepository.countByStatus(status);
    }

    @GetMapping("/metrics/queues")
    public Map<String, Object> queueMetrics(){
        Map<String, Object> metrics = new HashMap<>();
        for (String queueName: queueProperties.getQueues().keySet()){
            long readyDepth = redisBroker.queueDepth(queueName);
            long processingDepth = redisBroker.processingDepth(queueName);
            Boolean isPaused = redisBroker.isQueuePaused(queueName);
            Boolean isDrained = isPaused && readyDepth==0 && processingDepth==0;

            metrics.put(queueName, Map.of(
                "ready", readyDepth,
                "processing", processingDepth,
                "isPaused", isPaused,
                "isDrained", isDrained
            ));
        }
        metrics.put("delayed", redisBroker.delayedDepth());
        metrics.put("dlq", redisBroker.dlqDepth());

        return metrics;
    }
}

