package com.alarm.adapter.out.notification

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component
import java.time.Instant

/**
 * [파일명] RedisRateLimiter.kt
 * [수정일자] 2026-06-11
 * [기능] Redis Lua Script 기반 분산 Rate Limiter 구현
 * [관점] Outbound Infrastructure: 외부 API에 대한 초당 500회 호출 제한 사양을 보장하기 위해 Token Bucket 알고리즘을 Redis 상에서 원자적(Atomic)으로 동작시킵니다.
 */
@Component
class RedisRateLimiter(
    private val redisTemplate: StringRedisTemplate
) {
    private val key = "rate_limit:external_api"
    private val capacity = 500
    private val refillRate = 500
    private val refillPeriodSeconds = 1L

    private val luaScript = """
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local refillRate = tonumber(ARGV[2])
        local refillPeriod = tonumber(ARGV[3])
        local now = tonumber(ARGV[4])
        local requested = tonumber(ARGV[5])

        local bucket = redis.call('hgetall', key)
        local tokens = capacity
        local lastRefill = now

        if #bucket > 0 then
            for i = 1, #bucket, 2 do
                if bucket[i] == 'tokens' then
                    tokens = tonumber(bucket[i+1])
                elseif bucket[i] == 'lastRefill' then
                    lastRefill = tonumber(bucket[i+1])
                end
            end
        end

        local elapsed = now - lastRefill
        if elapsed > 0 then
            local refill = math.floor(elapsed / refillPeriod) * refillRate
            if refill > 0 then
                tokens = math.min(capacity, tokens + refill)
                lastRefill = lastRefill + math.floor(elapsed / refillPeriod) * refillPeriod
            end
        end

        if tokens >= requested then
            tokens = tokens - requested
            redis.call('hmset', key, 'tokens', tokens, 'lastRefill', lastRefill)
            return 1
        else
            redis.call('hmset', key, 'tokens', tokens, 'lastRefill', lastRefill)
            return 0
        end
    """.trimIndent()

    fun tryAcquire(tokens: Int = 1): Boolean {
        val now = Instant.now().epochSecond
        val result = redisTemplate.execute(
            DefaultRedisScript(luaScript, Long::class.java),
            listOf(key),
            capacity.toString(),
            refillRate.toString(),
            refillPeriodSeconds.toString(),
            now.toString(),
            tokens.toString()
        )
        return result == 1L
    }
}
