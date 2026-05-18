package com.nganhcc.task_queue.exception;

import java.util.UUID;

public class TaskNotFoundException extends RuntimeException {

    public TaskNotFoundException(UUID id){
        super("Task not found: "+id);
    }
}
