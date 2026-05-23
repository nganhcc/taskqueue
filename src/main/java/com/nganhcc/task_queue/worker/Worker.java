package com.nganhcc.task_queue.worker;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.broker.RedisBroker;
import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskEventType;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.service.TaskEventService;
import com.nganhcc.task_queue.service.TaskFailureService;
import com.nganhcc.task_queue.store.TaskRepository;

import jakarta.annotation.PreDestroy;


@Component
public class Worker {
    private final TaskRepository taskRepository;
    private final RedisBroker redisBroker;
    private final QueueProperties queueProperties;
    private final HandlerRegistry handlerRegistry;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private final TaskFailureService taskFailureService;
    private final TaskEventService taskEventService;

    public Worker(TaskRepository taskRepository, RedisBroker redisBroker, HandlerRegistry handlerRegistry, QueueProperties queueProperties, TaskFailureService taskFailureService, TaskEventService taskEventService){
        this.taskRepository= taskRepository;
        this.queueProperties=queueProperties;
        this.redisBroker= redisBroker;
        this.handlerRegistry= handlerRegistry;
        this.taskFailureService=taskFailureService;
        this.taskEventService=taskEventService;
    }

    public void runOnce(String queueName){
        //polledTask: from redis
        //task : from DB
        Task polledTask= redisBroker.poll(queueName);
        if (polledTask==null){
            return;
        }

        Task task= taskRepository.findById(polledTask.getId()).orElseThrow(()-> new IllegalStateException("Task not found: "+polledTask.getId()));
        if (task.getStatus() != TaskStatus.PENDING){
            redisBroker.acknowledge(polledTask);
            return;
        }
        task.setStatus(TaskStatus.RUNNING);
        Instant now = Instant.now();
        task.setStartedAt(now);
        task.setHeartbeatAt(now);
        taskRepository.save(task);
        taskEventService.record(task, TaskEventType.STARTED, "Task started");

        TaskHandler taskHandler = handlerRegistry.get(task.getFn());
        Future<String> future =null;
        try{
            future = executorService.submit(()-> taskHandler.handle(task));
            String result = future.get(queueProperties.getWorker().getHandlerTimeoutMs(),TimeUnit.MILLISECONDS);
            Task fresh = taskRepository.findById(polledTask.getId()).orElseThrow(()-> new IllegalStateException("Task not found: "+polledTask.getId()));
            if (fresh.getStatus() != TaskStatus.RUNNING){
                return;
            }
            fresh.setStatus(TaskStatus.DONE);
            fresh.setResult(result);
            fresh.setHeartbeatAt(null);
            taskRepository.save(fresh);
            taskEventService.record(fresh, TaskEventType.SUCCEEDED, "Task completed");
        }catch(TimeoutException e ){
            future.cancel(true);
            Task fresh = taskRepository.findById(polledTask.getId()).orElseThrow(()-> new IllegalStateException("Task not found: "+polledTask.getId()));
            if (fresh.getStatus() != TaskStatus.RUNNING){
                return;
            }
            taskFailureService.handleFailure(fresh ,new RuntimeException("Handler timed out!"));
        }catch(ExecutionException e){
            Task fresh = taskRepository.findById(polledTask.getId()).orElseThrow(()-> new IllegalStateException("Task not found: "+polledTask.getId()));
            if (fresh.getStatus() != TaskStatus.RUNNING){
                return;
            }
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException){
                taskFailureService.handleFailure(fresh, runtimeException);
            }else{
                taskFailureService.handleFailure(fresh,new RuntimeException(cause));
            }
        }catch(InterruptedException e){
            Task fresh = taskRepository.findById(polledTask.getId()).orElseThrow(()-> new IllegalStateException("Task not found: "+polledTask.getId()));
            if (fresh.getStatus() != TaskStatus.RUNNING){
                return;
            }
            Thread.currentThread().interrupt();
            taskFailureService.handleFailure(fresh, new RuntimeException("Worker interrupted", e));
        }finally{
            redisBroker.acknowledge(polledTask);
        }
    }

    @PreDestroy
    public void shutdown(){
        executorService.shutdown();
    }
}
