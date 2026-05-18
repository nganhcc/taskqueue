package com.nganhcc.task_queue.broker;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.exception.TaskNotFoundException;
import com.nganhcc.task_queue.model.Task;

@Component
public class RedisBroker {
    private final RedisTemplate<String, String> redisTemplate;
    private final TaskSerializer taskSerializer;

    public RedisBroker(RedisTemplate<String, String> redisTemplate, TaskSerializer taskSerializer){
        this.redisTemplate=redisTemplate;
        this.taskSerializer= taskSerializer;
    }

    public void enqueue(Task task){
        String key = RedisKeys.queue(task.getQueue());
        String json = taskSerializer.serialize(task);
        redisTemplate.opsForZSet().add(key, json, priorityScore(task));
    }

    public Task poll(String queueName){
        String sourceKey= RedisKeys.queue(queueName);
        String destinationKey= RedisKeys.processing(queueName);
        ZSetOperations.TypedTuple<String> tuple =redisTemplate.opsForZSet().popMin(sourceKey);
        if (tuple == null || tuple.getValue() == null){
            return null;
        }
        String json = tuple.getValue();
        redisTemplate.opsForList().leftPush(destinationKey, json);
        return taskSerializer.deserialize(json);
    }

    public void removeProcessingById(String queueName, UUID id){
        String key = RedisKeys.processing(queueName);
        List<String> values = redisTemplate.opsForList().range(key, 0 ,-1);
        if (values == null || values.isEmpty()){
            return;
        }
        for (String json: values){
            Task task = taskSerializer.deserialize(json);
            if (task.getId().equals(id)){
                redisTemplate.opsForList().remove(key, 1, json);
                return;
            }
        }
    }

    public Boolean purgeQueue(String queueName){
        return redisTemplate.delete(RedisKeys.queue(queueName));
    }

    public Boolean purgeProcessing(String queueName){
        return redisTemplate.delete(RedisKeys.processing(queueName));
    }

    public Boolean purgeDelayed(){
        return redisTemplate.delete(RedisKeys.delayed());
    }

    public Boolean purgeDlq(){
        return redisTemplate.delete(RedisKeys.dlq());
    }
    
    public void acknowledge(Task task){
        String key = RedisKeys.processing(task.getQueue());
        String json= taskSerializer.serialize(task);
        redisTemplate.opsForList().remove(key, 1, json);
    }

    public void enqueueDelayed(Task task){
        if (task.getRunAt() == null){
            throw new IllegalArgumentException("Delayed task must have runAt");
        }
        String json = taskSerializer.serialize(task);
        long score=task.getRunAt().toEpochMilli();

        redisTemplate.opsForZSet().add(RedisKeys.delayed(), json ,score);

    }
    
    public Long queueDepth(String queueName){
        return redisTemplate.opsForZSet().size(RedisKeys.queue(queueName));
    }
    public Long processingDepth(String queueName){
        return redisTemplate.opsForList().size(RedisKeys.processing(queueName));
    }
    public Long delayedDepth(){
        return redisTemplate.opsForZSet().size(RedisKeys.delayed());
    }
    public Long dlqDepth(){
        return redisTemplate.opsForList().size(RedisKeys.dlq());
    }

    public Set<Task> findReadyDelayedTasks(Instant now){
        Set<String> jsonTasks= redisTemplate.opsForZSet().rangeByScore(RedisKeys.delayed(), 0, now.toEpochMilli());
        if (jsonTasks==null || jsonTasks.isEmpty()){
            return Set.of();
        }
        return jsonTasks.stream().map(taskSerializer::deserialize).collect(Collectors.toSet());
    }

    public void removeDelayed(Task task){
        String json = taskSerializer.serialize(task);
        redisTemplate.opsForZSet().remove(RedisKeys.delayed(), json);
    }
    
    public void sendToDlq(Task task){
        String json = taskSerializer.serialize(task);
        redisTemplate.opsForList().leftPush(RedisKeys.dlq(), json);
    }

    public List<Task> listDlq(){
        List<String> jsonTasks = redisTemplate.opsForList().range(RedisKeys.dlq(), 0, -1);
        if (jsonTasks == null || jsonTasks.isEmpty()){
            return List.of();
        }
        return jsonTasks.stream().map(taskSerializer::deserialize).toList();
    }

    public Task findDlqTask(UUID id){
        return listDlq().stream().filter(task -> task.getId().equals(id)).findFirst().orElseThrow(()-> new TaskNotFoundException(id));
    }

    public void removeFromDlq(Task task){
        String json = taskSerializer.serialize(task);
        redisTemplate.opsForList().remove(RedisKeys.dlq(), 1, json);
    }

    private double priorityScore(Task task){
        long createdAtMillis = task.getCreatedAt().toEpochMilli();
        return (-task.getPriority() * 1_000_000_000_000_000D) + createdAtMillis;
    }

}
