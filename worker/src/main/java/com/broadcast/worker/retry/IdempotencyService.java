package com.broadcast.worker.retry;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class IdempotencyService {
    private static final Duration DEDUPE_WINDOW = Duration.ofHours(24);
    private final StringRedisTemplate redisTemplate;

    public IdempotencyService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    public boolean markProcessingIfAbsent(String idempotencyKey){
        Boolean firstSeen = redisTemplate.opsForValue()
                .setIfAbsent("idempotency:"+idempotencyKey,"1",DEDUPE_WINDOW);
        return  Boolean.TRUE.equals(firstSeen);
    }
}
