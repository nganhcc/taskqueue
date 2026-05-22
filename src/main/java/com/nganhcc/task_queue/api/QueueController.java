package com.nganhcc.task_queue.api;

import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.exception.BadRequestException;
import com.nganhcc.task_queue.service.QueueControllerService;
import com.nganhcc.task_queue.service.TaskService;
import com.nganhcc.task_queue.worker.Worker;

@RestController
public class QueueController {
    private final QueueControllerService queueControllerService;
    private final QueueProperties queueProperties;
    private final RedisBroker redisBroker;
    private final TaskService taskService;
    private final Worker worker;

    public QueueController(QueueControllerService queueControllerService, QueueProperties queueProperties,RedisBroker redisBroker, TaskService taskService, Worker worker){
        this.queueControllerService= queueControllerService;
        this.queueProperties=queueProperties;
        this.redisBroker=redisBroker;
        this.taskService=taskService;
        this.worker = worker;
    }

    @GetMapping("/queues")
    public Map<String, Object> listQueues(){
        return Map.of(
            "queues", queueProperties.getQueues().keySet(),
            "paused", queueControllerService.pausedQueues()
        );
    }

    @PostMapping("/queues/{queue}/pause")
    public Map<String, Object> pause(@PathVariable String queue){
        requireQueue(queue);
        queueControllerService.pause(queue);
        return queueState(queue);
    }

    @PostMapping("/queues/{queue}/resume")
    public Map<String, Object> resume(@PathVariable String queue){
        requireQueue(queue);
        queueControllerService.resume(queue);
        return queueState(queue);
    }

    @PostMapping("/queues/{queue}/drain")
    public Map<String, Object> drain(@PathVariable String queue){
        requireQueue(queue);
        queueControllerService.drain(queue);
        return Map.of(
            "queue", queue,
            "paused", queueControllerService.isPaused(queue),
            "draining", true
        );
    }

    @PostMapping("/queues/{queue}/run-once")
    public Map<String, Object> runOnce(@PathVariable String queue,
        @RequestParam(defaultValue = "false") boolean force
    ) {
        requireQueue(queue);
        boolean paused = queueControllerService.isPaused(queue);
        if (paused && !force){
            throw new BadRequestException("Can not run paused queue: " + queue);
        }
        worker.runOnce(queue);
        return Map.of(
            "queue", queue,
            "ran", true,
            "forced", force,
            "paused", paused
        );
    }

    @PostMapping("/queues/{queue}/purge")
    public Map<String, Object> purge(@PathVariable String queue){
        requireQueue(queue);
        Boolean readyDeleted = redisBroker.purgeQueue(queue);
        Boolean processingDeleted = redisBroker.purgeProcessing(queue);

        int dbTasksMarked = taskService.markQueuePurged(queue);

        return Map.of(
            "queue", queue, 
            "readyDeleted", Boolean.TRUE.equals(readyDeleted),
            "processingDeleted", Boolean.TRUE.equals(processingDeleted),
            "dbTasksMarked", dbTasksMarked
        );
    }
    @PostMapping("/queues/delayed/purge")
    public Map<String, Object> purgeDelayed(){
        Boolean delayedDeleted = redisBroker.purgeDelayed();
        int tasksMarkedFailed = taskService.markDelayedPurged();
        return Map.of(
            "delayedDeleted", Boolean.TRUE.equals(delayedDeleted),
            "tasksMarkedFailed", tasksMarkedFailed
        );
    }
    @PostMapping("/queues/dlq/purge")
    public Map<String, Object> purgeDlq() {
        Boolean dlqDeleted = redisBroker.purgeDlq();
        int tasksMarkedFailed = taskService.markDlqPurged();
        return Map.of(
            "dlqDeleted", Boolean.TRUE.equals(dlqDeleted),
            "tasksMarkedFailed", tasksMarkedFailed
        );
    }
    private void requireQueue(String queue){
        if (!queueProperties.getQueues().containsKey(queue)){
            throw new BadRequestException("Unknown queue: " + queue);
        }
    }
    private Map<String, Object> queueState(String queue){
        return Map.of(
            "queue", queue,
            "paused", queueControllerService.isPaused(queue)
        );
    }
}
