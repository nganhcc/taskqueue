package com.nganhcc.task_queue.worker;

import java.io.PrintWriter;
import java.io.StringWriter;
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
import com.nganhcc.task_queue.config.QueueProperties.QueueConfig;
import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;
import com.nganhcc.task_queue.retry.RetryPolicy;
import com.nganhcc.task_queue.store.TaskRepository;

import jakarta.annotation.PreDestroy;


@Component
public class Worker {
    private final TaskRepository taskRepository;
    private final RedisBroker redisBroker;
    private final RetryPolicy retryPolicy;
    private final QueueProperties queueProperties;
    private final HandlerRegistry handlerRegistry;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    

    public Worker(TaskRepository taskRepository, RedisBroker redisBroker,RetryPolicy retryPolicy, HandlerRegistry handlerRegistry, QueueProperties queueProperties){
        this.taskRepository= taskRepository;
        this.queueProperties=queueProperties;
        this.redisBroker= redisBroker;
        this.retryPolicy= retryPolicy;
        this.handlerRegistry= handlerRegistry;
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
        task.setStartedAt(Instant.now());
        taskRepository.save(task);

        TaskHandler taskHandler = handlerRegistry.get(task.getFn());
        Future<String> future =null;
        try{
            future = executorService.submit(()-> taskHandler.handle(task));
            String result = future.get(queueProperties.getWorker().getHandlerTimeoutMs(),TimeUnit.MILLISECONDS);
            task.setStatus(TaskStatus.DONE);
            task.setResult(result);
            taskRepository.save(task);
        }catch(TimeoutException e ){
            future.cancel(true);
            handleFailure(task ,new RuntimeException("Handler timed out!"));
        }catch(ExecutionException e){
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException){
                handleFailure(task, runtimeException);
            }else{
                handleFailure(task,new RuntimeException(cause));
            }
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
            handleFailure(task, new RuntimeException("Worker interrupted", e));
        }finally{
            redisBroker.acknowledge(polledTask);
        }
    }

    @PreDestroy
    public void shutdown(){
        executorService.shutdown();
    }

    private void handleFailure(Task task, RuntimeException e){
        task.setAttempt(task.getAttempt()+1);
        task.setError(e.getMessage());
        task.setStackTrace(stackTraceOf(e));
        if (retryPolicy.shouldRetry(task)){
            QueueConfig queueConfig = queueProperties.getQueues().get(task.getQueue());
            long delayMs = retryPolicy.nextDelayMs(task, queueConfig.getBaseDelayMs());
            task.setRunAt(Instant.now().plusMillis(delayMs));
            task.setStatus(TaskStatus.PENDING);
            taskRepository.save(task);

            redisBroker.enqueueDelayed(task);
        }else{
            task.setStatus(TaskStatus.DEAD);
            taskRepository.save(task);
            redisBroker.sendToDlq(task);
        }
    }

    private String stackTraceOf(Throwable throwable){
        StringWriter stringWriter = new StringWriter();
        throwable.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }
}
