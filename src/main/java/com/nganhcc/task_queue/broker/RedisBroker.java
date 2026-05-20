package com.nganhcc.task_queue.broker;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.exception.TaskNotFoundException;
import com.nganhcc.task_queue.model.Task;

@Component
public class RedisBroker {
    private final RedisTemplate<String, String> redisTemplate;
    private final TaskSerializer taskSerializer;
    private static final RedisScript<String> POLL_SCRIPT = RedisScript.of("""
        local task = redis.call('ZRANGE', KEYS[1], 0 ,0)[1]
        if task == nil then
            return nil
        end
        redis.call('ZREM', KEYS[1], task)
        redis.call('LPUSH', KEYS[2], task)
        return task
    """, String.class);

    private static final RedisScript<String> POLL_READY_DELAYED_SCRIPT = RedisScript.of("""
        local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0 ,1)
        local task = tasks[1]
        if task == nil then
            return nil
        end
        redis.call('ZREM', KEYS[1], task)
        return task
            """, String.class);
    
    private static final RedisScript<Long> RELEASE_LOCK_SCRIPT = RedisScript.of("""
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        """, Long.class);

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
        String json = redisTemplate.execute(POLL_SCRIPT,List.of(sourceKey,destinationKey));
        if (json == null){
            return null;
        }
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
    
    public void pauseQueue(String queue){
        redisTemplate.opsForSet().add(RedisKeys.pausedQueues(),queue);
    }

    public void resumeQueue(String queue){
        redisTemplate.opsForSet().remove(RedisKeys.pausedQueues(),queue);
    }
    public boolean isQueuePaused(String queue){
        Boolean member = redisTemplate.opsForSet().isMember(RedisKeys.pausedQueues(), queue);
        return Boolean.TRUE.equals(member);
    }

    public Set<String> pausedQueues(){
        Set<String> queues = redisTemplate.opsForSet().members(RedisKeys.pausedQueues());
        return queues==null? Set.of() : queues;
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

    public Task pollReadyDelayed(Instant now){
        String json = redisTemplate.execute(
            POLL_READY_DELAYED_SCRIPT,
            List.of(RedisKeys.delayed()),
            String.valueOf(now.toEpochMilli())
        );
        if(json ==null){
            return null;
        }
        return taskSerializer.deserialize(json);
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

    public boolean acquireSchedulerLock(String token, Duration ttl){
        Boolean acquired = redisTemplate.opsForValue()
        .setIfAbsent(RedisKeys.schedulerLock(), token, ttl);
        return Boolean.TRUE.equals(acquired);
    }
    public void releaseSchedulerLock(String token){
        redisTemplate.execute(RELEASE_LOCK_SCRIPT,
            List.of(RedisKeys.schedulerLock()),token
        );
    }

    private double priorityScore(Task task){
        long createdAtMillis = task.getCreatedAt().toEpochMilli();
        return (-task.getPriority() * 1_000_000_000_000_000D) + createdAtMillis;
    }
    
}
