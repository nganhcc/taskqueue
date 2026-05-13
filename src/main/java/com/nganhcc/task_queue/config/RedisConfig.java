package com.nganhcc.task_queue.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {
    /**
     * RedisTemplate configured to use plain String keys and values.
     * All task data is serialized to JSON strings before being handed to the broker,
     * so we never want Spring to apply Java serialization here.
     */
    /*
     * Redis la open source, nhu la mot phan mem tren tang OS, giup ta quan li du lieu, truy cap du lieu nhanh,...
     * Ta muon dung dinh dang Serializer cua Redis 
     * Du lieu duoc luu, cache ngay tren Ram
        Your Spring Boot App
            ↓
        RedisTemplate<String, String>   ← You configured this
            ↓ (serializes + sends)
        Redis Server (localhost:6379)
            ↓
        Stored in RAM (Memory)
     */
    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory factory){
        RedisTemplate<String, String> template= new RedisTemplate<>();
        template.setConnectionFactory(factory);

        StringRedisSerializer serializer = new StringRedisSerializer();
        template.setKeySerializer(serializer);
        template.setValueSerializer(serializer);
        template.setHashKeySerializer(serializer);
        template.setHashValueSerializer(serializer);

        template.afterPropertiesSet();
        return template;

    }
}
