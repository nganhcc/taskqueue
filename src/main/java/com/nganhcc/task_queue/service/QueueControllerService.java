package com.nganhcc.task_queue.service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

@Service
public class QueueControllerService {
    private final Set<String> pausedQueues = ConcurrentHashMap.newKeySet();

    public void pause(String queue){
        pausedQueues.add(queue);
    }
    
    public void resume(String queue){
        pausedQueues.remove(queue);
    }
    
    public boolean isPaused(String queue){
        return pausedQueues.contains(queue);
    }

    public Set<String> pausedQueues(){
        return Set.copyOf(pausedQueues);
    }
}
