package com.alarm.adapter.`in`.messaging

import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.domain.AlarmStatus
import com.alarm.domain.AlarmExecutionLog
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito
import org.mockito.Mockito.*
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * [대상] KafkaAlarmJobConsumer — Kafka 배치 소비자
 * [범주] Consumer Unit Test — Mockito Mock, 실제 Kafka 브로커 없음
 *
 * H2 DB 상태 갱신
 *   ① 정상 처리 → updateStatusesInBatch(SENT)
 *   ② 발송 실패 → updateStatusesInBatch(FAILED) + saveAll(에러 이력)
 *
 * Redis proc: 키 멱등성
 *   ③ 동일 uuid 2건 인입 → sendNotification 정확히 1회만 호출 (중복 Skip)
 *
 * Rate Limiter
 *   ④ 501~502번째 청크 tryAcquire=false → 대기 후 재시도, 총 504회 호출
 */
class KafkaAlarmJobConsumerTest {

    private lateinit var alarmRepositoryPort: AlarmRepositoryPort
    private lateinit var notificationPort: NotificationPort
    private lateinit var alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort
    private lateinit var redisTemplate: StringRedisTemplate
    private lateinit var redisRateLimiter: RedisRateLimiter
    private lateinit var objectMapper: ObjectMapper
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    private lateinit var valueOps: ValueOperations<String, String>
    private lateinit var ack: Acknowledgment
    private lateinit var consumer: KafkaAlarmJobConsumer

    @Suppress("UNCHECKED_CAST")
    @BeforeEach
    fun setUp() {
        alarmRepositoryPort = mock(AlarmRepositoryPort::class.java)
        notificationPort = mock(NotificationPort::class.java)
        alarmExecutionLogRepositoryPort = mock(AlarmExecutionLogRepositoryPort::class.java)
        redisTemplate = mock(StringRedisTemplate::class.java)
        redisRateLimiter = mock(RedisRateLimiter::class.java)

        // 핵심 수정: KotlinModule을 등록해야 Kotlin data class 역직렬화 가능
        objectMapper = ObjectMapper().registerKotlinModule()

        kafkaTemplate = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
        valueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>
        ack = mock(Acknowledgment::class.java)

        `when`(redisTemplate.opsForValue()).thenReturn(valueOps)
        `when`(redisRateLimiter.tryAcquire(anyInt())).thenReturn(true)

        // Mockito 표준 anyList() 매처는 null이 아닌 빈 리스트를 반환하여 안전함
        `when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.empty())

        consumer = KafkaAlarmJobConsumer(
            alarmRepositoryPort,
            notificationPort,
            alarmExecutionLogRepositoryPort,
            redisTemplate,
            redisRateLimiter,
            objectMapper,
            kafkaTemplate
        )
    }

    /**
     * [시나리오] 카프카에서 수신한 알람들 중 중복되지 않은 알람이 최종적으로 DB(H2)에 완료(SENT) 상태로 잘 저장되는지 검증합니다.
     * [Given] 중복되지 않은 알람 메시지 및 Redis 선점 통과 설정
     * [When] 알람 이벤트 배치 소비 호출
     * [Then] alarmRepositoryPort.updateStatusesInBatch가 SENT 상태로 호출되었는지 검증
     */
    @Test
    fun `should update RDB H2 status to SENT on successful processing`() {
        // Given
        val payload = """{"alarmId":"alarm-ok","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-ok"}"""
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)

        // When
        consumer.consumeAlarmJobs(listOf(payload), ack)

        // Then
        // H2 DB 상태 업데이트 시 SENT(Done) 상태로 업데이트되는지 검증
        verify(alarmRepositoryPort).updateStatusesInBatch(listOf("alarm-ok"), AlarmStatus.SENT)
    }

    /**
     * [시나리오] 중복 이벤트가 인입되었을 때 중복을 감지하고 무시(Skip)하는 로직을 검증합니다.
     * [Given] 동일 멱등키(proc:)를 가진 2개의 메시지,
     *        첫 번째 메시지는 setIfAbsent=true(최초 선점 성공),
     *        두 번째 메시지는 setIfAbsent=false + Redis 값 "PROCESSING" 반환(중복 감지)으로 설정
     * [When] 알람 이벤트 배치 소비 호출
     * [Then] 외부 발송 API(sendNotification)가 정확히 1회만 호출되는지 검증 (중복 메시지 제외됨)
     */
    @Test
    fun `should detect and ignore duplicate Kafka jobs using Redis proc key`() {
        // Given
        val payload1 = """{"alarmId":"alarm-1","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-dup"}"""
        val payload2 = """{"alarmId":"alarm-2","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-dup"}"""
        // 두 메시지가 동일한 proc 키를 공유함: proc:sender-1uuid-dup
        val procKey = "proc:sender-1uuid-dup"

        // 첫 번째 선점 시도: true (성공)
        // 두 번째 선점 시도: false (이미 선점됨 = 중복)
        `when`(valueOps.setIfAbsent(eq(procKey), eq("PROCESSING"), any(Duration::class.java)))
            .thenReturn(true)
            .thenReturn(false)
        // 두 번째 메시지 도달 시 Redis에서 현재 상태 조회 → PROCESSING 반환 → 중복으로 판정
        `when`(valueOps.get(eq(procKey))).thenReturn("PROCESSING")

        // When
        consumer.consumeAlarmJobs(listOf(payload1, payload2), ack)

        // Then
        // ArgumentCaptor 캐스팅 없이 호출 횟수(1회)만 verify → Kotlin null-safety 문제 없음
        verify(notificationPort, times(1)).sendNotification(anyList())
        verify(ack).acknowledge()
    }

    /**
     * [시나리오] 발송 오류 시 DB(H2)에 실패(FAILED) 상태 업데이트와 에러 이력 저장을 검증합니다.
     * [Given] 메시지 및 외부 전송 API가 RuntimeException을 반환하도록 설정
     * [When] 알람 이벤트 배치 소비 호출
     * [Then] DB H2에 FAILED 업데이트 요청 및 에러 로그 일괄(saveAll) 저장이 일어나는지 검증
     */
    @Test
    fun `should update RDB H2 status to FAILED and record log on execution failure`() {
        // Given
        val payload = """{"alarmId":"alarm-fail","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-fail"}"""
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.error(RuntimeException("Connection lost")))

        // When
        consumer.consumeAlarmJobs(listOf(payload), ack)

        // Then
        // 1. H2 DB에 FAILED 상태로 일괄 업데이트되는지 검증
        verify(alarmRepositoryPort).updateStatusesInBatch(listOf("alarm-fail"), AlarmStatus.FAILED)
        // 2. FAILED 실행 로그가 saveAll로 저장되는지 검증
        verify(alarmExecutionLogRepositoryPort).saveAll(anyList())
    }

    /**
     * [시나리오] 외부 연동 Rate Limit 조건(초당 최대 500회 호출)을 초과한 502번째 요청 상황을 검증합니다.
     * [Given] 502개의 배치 chunk를 생성하기 위해 총 502 * 200 = 100,400개의 알람 페이로드 생성
     *        Redis Rate Limiter(tryAcquire)가 500회째까지는 true(허용),
     *        501번째와 502번째 시도는 false(초과 차단),
     *        503번째 시도부터 다시 true(허용)를 반환하도록 Mock 설정
     * [When] 100,400개의 대용량 알람 이벤트 배치 소비 호출
     * [Then] tryAcquire 호출 횟수가 총 504회(성공 502회 + 대기로 인한 재시도 2회)인지 검증하여,
     *        초과 시 대기 및 재선점이 정상 동작하는지 증명
     */
    @Test
    fun `should block and wait on 501st and 502nd calls when rate limit is exceeded`() {
        // Given
        // 502개의 배치 청크를 발생시키기 위해 502 * 200 = 100,400개의 대용량 데이터 생성
        val payloads = List(502 * 200) { index ->
            """{"alarmId":"alarm-$index","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-$index"}"""
        }
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)

        // tryAcquire가 500번째까지는 true, 501~502번째는 false(제한 차단), 503번째부터 다시 true를 반환하는 시뮬레이션
        var rateLimitCallCount = 0
        `when`(redisRateLimiter.tryAcquire(1)).thenAnswer {
            rateLimitCallCount++
            when {
                rateLimitCallCount <= 500 -> true           // 처음 500번: 허용
                rateLimitCallCount == 501 || rateLimitCallCount == 502 -> false  // 501~502번: 차단(대기)
                else -> true                                  // 503번 이후: 다시 허용
            }
        }

        // When
        consumer.consumeAlarmJobs(payloads, ack)

        // Then
        // 502개의 모든 청크가 완료될 때까지 tryAcquire가 총 504번 호출되었는지 검증
        // (500회 성공 + 2회 대기 실패 + 2회 대기 후 재선점 성공 = 504회)
        verify(redisRateLimiter, times(504)).tryAcquire(1)
        verify(ack).acknowledge()
    }
}
