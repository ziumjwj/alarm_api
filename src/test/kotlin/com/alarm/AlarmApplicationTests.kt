package com.alarm

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.data.redis.core.StringRedisTemplate
import com.alarm.adapter.out.notification.RedisRateLimiter

/**
 * [대상] Spring ApplicationContext 전체
 * [범주] Smoke Test — Bean 주입/설정 오류 조기 감지
 *
 * ① contextLoads() — 에러 없이 컨텍스트 로드되는지 확인
 */

@SpringBootTest
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class AlarmApplicationTests {

    @MockBean
    lateinit var redisTemplate: StringRedisTemplate

    @MockBean
    lateinit var redisRateLimiter: RedisRateLimiter

	@Test
	fun contextLoads() {
	}

}
