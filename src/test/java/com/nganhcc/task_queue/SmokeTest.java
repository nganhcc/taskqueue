package com.nganhcc.task_queue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
 
import static org.assertj.core.api.Assertions.assertThat;
 
@SpringBootTest
class SmokeTest {
 
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
 
    @Autowired
    private JdbcTemplate jdbcTemplate;
 
    @Test
    void redisIsReachable() {
        redisTemplate.opsForValue().set("smoke:test", "ok");
        String value = redisTemplate.opsForValue().get("smoke:test");
        assertThat(value).isEqualTo("ok");
        redisTemplate.delete("smoke:test");
    }
 
    @Test
    void postgresIsReachable() {
        Integer result = jdbcTemplate.queryForObject("SELECT 1", Integer.class);
        assertThat(result).isEqualTo(1);
    }
 
    @Test
    void tasksTableExists() {
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'tasks'",
            Integer.class
        );
        assertThat(count).isEqualTo(1);
    }
}
 