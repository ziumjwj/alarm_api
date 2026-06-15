package com.alarm.adapter.`in`.web

import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.application.port.out.NotificationPort
import com.alarm.application.port.`in`.TriggerAlarmUseCase
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.domain.AlarmStatus
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import java.time.Duration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull

import org.springframework.kafka.test.context.EmbeddedKafka

/**
 * [대상] AlarmController — 웹 인바운드 어댑터
 * [범주] Web Adapter 통합 Test — MockMvc + H2 + EmbeddedKafka
 *
 * ① POST /notifications → HTTP 201, PENDING 상태 응답 + H2 DB 저장 확인
 * ② POST /notifications + triggerAlarm 호출 → DB 상태 SENT 전이 확인
 */

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = ["alarm-jobs"], bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class AlarmControllerTest {

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
        Mockito.`when`(notificationPort.sendNotification(anyList())).thenReturn(reactor.core.publisher.Mono.empty())
    }

    @Test
    fun `should create alarm via web adapter`() {
        val request = AlarmCreateRequest(
            name = "Web Test Alarm",
            payload = "{\"key\":\"val\"}"
        )

        val createResult = mockMvc.perform(
            post("/notifications")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.name").value("Web Test Alarm"))
            .andExpect(jsonPath("$.status").value(AlarmStatus.PENDING.name))
            .andReturn()

        val responseBody = createResult.response.contentAsString
        val alarmId = objectMapper.readTree(responseBody).get("id").asText()

        val savedAlarm = alarmRepositoryPort.findById(alarmId)
        assertNotNull(savedAlarm)
        assertEquals("Web Test Alarm", savedAlarm?.name)
    }

    @Test
    fun `should trigger alarm successfully`() {
        val request = AlarmCreateRequest(
            name = "Trigger Test Alarm",
            payload = "{\"key\":\"val\"}"
        )

        val createResult = mockMvc.perform(
            post("/notifications")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andReturn()

        val responseBody = createResult.response.contentAsString
        val alarmId = objectMapper.readTree(responseBody).get("id").asText()

        // 알람 즉시 발생 유스케이스 직접 호출
        triggerAlarmUseCase.triggerAlarm(alarmId)

        // 알람 상태가 SENT로 변경되었는지 확인
        val savedAlarm = alarmRepositoryPort.findById(alarmId)
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.SENT, savedAlarm?.status)
    }
}
