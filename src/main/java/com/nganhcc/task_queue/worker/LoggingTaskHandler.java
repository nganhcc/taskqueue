package com.nganhcc.task_queue.worker;

import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.model.Task;

@Component
public class LoggingTaskHandler implements TaskHandler {
    @Override
    public String fn(){
        return "log_message";
    }

    @Override
    public String handle(Task task){
        return "{\"ok\":true}";
    }
}
