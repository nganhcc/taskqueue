package com.nganhcc.task_queue.broker;

public final class RedisKeys {
    private RedisKeys(){}
    //key
    public static String queue(String queueName){
        return "taskqueue:"+ queueName;
    }

    public static String processing(String queueName){
        return queue(queueName)+ ":processing";
    }

    public static String delayed(){
        return "taskqueue:delayed";
    }

    public static String dlq(){
        return "taskqueue:dlq";
    }
    public static String schedulerLock(){
        return "taskqueue:scheduler:lock";
    }

    public static String pausedQueues(){
        return "taskqueue:queues:paused";
    }
}
