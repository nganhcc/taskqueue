package com.nganhcc.task_queue.service;

import java.util.Set;

import org.springframework.stereotype.Service;

import com.nganhcc.task_queue.broker.RedisBroker;

@Service
public class QueueControllerService {
    private final RedisBroker redisBroker;

    public QueueControllerService(RedisBroker redisBroker){
        this.redisBroker=redisBroker;
    }

    public void pause(String queue){
        redisBroker.pauseQueue(queue);
    }

    public void drain(String queue){
        pause(queue);
    }
    
    public void resume(String queue){
        redisBroker.resumeQueue(queue);
    }
    
    public boolean isPaused(String queue){
        return redisBroker.isQueuePaused(queue);
    }

    public Set<String> pausedQueues(){
        return redisBroker.pausedQueues();
    }
}
