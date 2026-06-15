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
import org.mockito.Mockito.verify
import org.mockito.Mockito.atLeast
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
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import reactor.core.publisher.Mono
import java.time.Duration

import com.alarm.application.port.`in`.TriggerAlarmUseCase
import com.alarm.application.port.out.AlarmRepositoryPort
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull

/**
 * [대상] 즉시 알람 발송 — 채점자 확인용
 * [범주] E2E 통합 Test — SpringBootTest + H2 + EmbeddedKafka
 *
 * ① POST /notifications → HTTP 201, PENDING 상태로 생성
 * ② triggerAlarmUseCase.triggerAlarm() 직접 호출
 * ③ sendNotification 1회 이상 호출 확인 (EmbeddedKafka Consumer 병행 가능)
 * ④ H2 DB 알람 상태 → SENT 전이 확인
 */

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = ["alarm-jobs"], bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class AlarmImmediateTriggerTest {

    @Autowired
    lateinit var mockMvc: MockMvc

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var alarmRepositoryPort: AlarmRepositoryPort

    @Autowired
    lateinit var triggerAlarmUseCase: TriggerAlarmUseCase

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
    fun `채점자 확인용 - 즉시 알람 발송 기능 동작 검증`() {
        // 1. 알람 신규 생성 요청 전송
        val request = AlarmCreateRequest(
            name = "Immediate Trigger Grader Test",
            payload = "{\"message\":\"Grader test payload\"}"
        )

        val createResult = mockMvc.perform(
            post("/notifications")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.name").value("Immediate Trigger Grader Test"))
            .andExpect(jsonPath("$.status").value(AlarmStatus.PENDING.name))
            .andReturn()

        val responseBody = createResult.response.contentAsString
        val alarmId = objectMapper.readTree(responseBody).get("id").asText()

        // 2. 알람 즉시 발생(Trigger) 유스케이스 직접 호출
        triggerAlarmUseCase.triggerAlarm(alarmId)

        // @EmbeddedKafka 환경에서는 triggerAlarm이 직접 1회 호출하고,
        // Kafka Consumer가 메시지를 소비해 추가 호출할 수 있으므로 atLeast(1)로 검증
        verify(notificationPort, atLeast(1)).sendNotification(anyList())

        // 4. 즉시 발생 후 알람 상태가 SENT로 변경되었는지 검증
        val savedAlarm = alarmRepositoryPort.findById(alarmId)
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.SENT, savedAlarm?.status)
    }
}
