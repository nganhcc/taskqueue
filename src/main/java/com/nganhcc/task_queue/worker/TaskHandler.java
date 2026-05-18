package com.nganhcc.task_queue.worker;

import com.nganhcc.task_queue.model.Task;

public interface TaskHandler {
    String fn();
    String handle(Task task);

}