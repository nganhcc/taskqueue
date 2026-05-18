package com.nganhcc.task_queue.worker;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.exception.NoHandlerFoundException;

/*
 hanlers= {
  "log_message" -> LoggingTaskHandler object,
  "send_email"  -> SendEmailTaskHandler object,
  "resize_image" -> ResizeImageTaskHandler object
}
*/
@Component
public class HandlerRegistry {
    private final Map<String, TaskHandler> handlers;
    public HandlerRegistry(List<TaskHandler> handlers){
        this.handlers= handlers.stream().collect(Collectors.toMap(TaskHandler::fn, Function.identity()));
    }
    public TaskHandler get(String fn){
        TaskHandler handler = handlers.get(fn);
        if (handler== null){
            throw new NoHandlerFoundException(fn);
        }
        return handler;
    }
}
