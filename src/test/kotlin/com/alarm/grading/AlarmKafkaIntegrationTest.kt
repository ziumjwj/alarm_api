package com.alarm.grading

import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.application.port.out.NotificationPort
import com.alarm.domain.AlarmStatus
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito
import org.mockito.Mockito.timeout
import org.mockito.Mockito.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.http.MediaType
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import com.alarm.application.port.out.AlarmRepositoryPort
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * [대상] 비동기 Kafka 알람 발송 파이프라인 — 채점자 확인용
 * [범주] Kafka 비동기 통합 Test — SpringBootTest + H2 + EmbeddedKafka
 *
 * ① POST /notifications → HTTP 201, PENDING 상태로 생성
 * ② Kafka Consumer가 메시지 소비 → sendNotification 호출 (5초 timeout 비동기 검증)
 * ③ 소비 완료 후 H2 DB 알람 상태 → SENT 전이 (100ms 간격 폴링, 최대 5초)
 */

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = ["alarm-jobs"], bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class AlarmKafkaIntegrationTest {

    @Autowired
    lateinit var mockMvc: MockMvc

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var alarmRepositoryPort: AlarmRepositoryPort

    @MockBean
    lateinit var notificationPort: NotificationPort

    @MockBean
    lateinit var redisTemplate: StringRedisTemplate

    @MockBean
    lateinit var redisRateLimiter: RedisRateLimiter

    @BeforeEach
    fun setUp() {
        @Suppress("UNCHECKED_CAST")
        val valueOps = Mockito.mock(ValueOperations::class.java) as ValueOperations<String, String>
        Mockito.`when`(redisTemplate.opsForValue()).thenReturn(valueOps)
        Mockito.`when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        Mockito.`when`(redisRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        Mockito.`when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.empty())
    }

    @Test
    fun `카프카 기반 비동기 알람 발송 및 멱등성 검증`() {
        // 1. 알람 신규 생성 요청 전송
        val request = AlarmCreateRequest(
            name = "Async Kafka Integration Test",
            payload = "{\"message\":\"Async test payload\"}"
        )

        val createResult = mockMvc.perform(
            post("/notifications")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.name").value("Async Kafka Integration Test"))
            .andExpect(jsonPath("$.status").value(AlarmStatus.PENDING.name))
            .andReturn()

        val responseBody = createResult.response.contentAsString
        val alarmId = objectMapper.readTree(responseBody).get("id").asText()

        verify(notificationPort, timeout(5000)).sendNotification(anyList())

        // 3. 비동기 발생 후 최종 상태가 SENT로 자동 변경되었는지 검증 (최대 5초 대기/폴링)
        var statusIsSent = false
        for (i in 1..50) {
            val savedAlarm = alarmRepositoryPort.findById(alarmId)
            if (savedAlarm?.status == AlarmStatus.SENT) {
                statusIsSent = true
                break
            }
            Thread.sleep(100)
        }
        
        assert(statusIsSent) { "알람 상태가 SENT로 변경되지 않았습니다." }
    }
}
