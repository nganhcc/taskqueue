package com.nganhcc.task_queue.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.config.QueueProperties;
import com.nganhcc.task_queue.service.QueueControllerService;

import jakarta.annotation.PreDestroy;

@Component
public class WorkerBootstrap {
    private final Worker worker;
    private final QueueProperties queueProperties;
    private final QueueControllerService queueControllerService;
    private final ExecutorService executorService= Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean polling = new AtomicBoolean(false);

    public WorkerBootstrap(Worker worker, QueueProperties queueProperties, QueueControllerService queueControllerService){
        this.queueProperties= queueProperties;
        this.queueControllerService=queueControllerService;
        this.worker=worker;
    }

    @Scheduled(fixedDelayString = "${taskqueue.worker.poll-interval-ms:1000}")//read the delay from config application.yaml, if not exist, use 1000
    public void pollQueues(){
        /*
        Problem: @Scheduled can fire again while the previous batch of submitted worker tasks is 
        still being scheduled/running. Add a guard so only one pollQueues() batch can submit at 
        a time.
        */
        if (!polling.compareAndSet(false, true)){
            return ;
        }
        try{
            for (String queueName: queueProperties.getQueues().keySet()){
                if (queueControllerService.isPaused(queueName)){
                    continue;
                }

                int concurrency = queueProperties.getQueues().get(queueName).getConcurrency();
                for (int i=0;i< concurrency;i++){

                    executorService.submit(()-> worker.runOnce(queueName));
                }
            }
        }finally{
            polling.set(false);
        }
    }

    @PreDestroy
    public void shutdown(){
        executorService.shutdown();
    }
}
