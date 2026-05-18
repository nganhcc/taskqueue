package com.nganhcc.task_queue.exception;

public class NoHandlerFoundException extends RuntimeException {
    public NoHandlerFoundException(String fn){
        super("No handler found for fn: "+ fn);
    }
}
