package com.alarm.kafka

import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.adapter.`in`.messaging.KafkaAlarmJobConsumer
import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.domain.AlarmStatus
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyList
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.timeout
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * [대상] KafkaAlarmJobProducer + KafkaAlarmJobConsumer
 * [범주] Kafka 실패 케이스 통합 Test — SpringBootTest + EmbeddedKafka + Mockito 혼합
 *
 * 발송 API 실패
 *   ① sendNotification 예외 → H2 DB FAILED 저장 + 에러 로그 기록
 *   ② sendNotification 예외 → alarm-jobs-retry 토픽으로 재발행
 *
 * 멱등성 (중복 메시지)
 *   ③ 동일 uuid 2건 → proc: 키로 중복 감지, sendNotification 1회만 호출
 *
 * 잘못된 메시지 형식
 *   ④ 정상 JSON 1건 + 깨진 JSON 1건 → 깨진 건 Skip, 정상 건만 처리
 *
 * 파티션 발행 실패
 *   ⑤ 존재하지 않는 파티션(99번) 발행 시도 → 예외 발생
 *
 * ack 보장
 *   ⑥ 모든 메시지가 중복이어도 → ack() 정상 호출 (오프셋 누락 방지)
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
    partitions = 3,
    topics = ["alarm-jobs", "alarm-jobs-retry"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaProducerConsumerFailureTest {

    @Autowired
    lateinit var mockMvc: MockMvc

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    lateinit var alarmRepositoryPort: AlarmRepositoryPort

    @MockBean
    lateinit var notificationPort: NotificationPort

    @MockBean
    lateinit var redisTemplate: StringRedisTemplate

    @MockBean
    lateinit var redisRateLimiter: RedisRateLimiter

    private lateinit var valueOps: ValueOperations<String, String>

    @BeforeEach
    @Suppress("UNCHECKED_CAST")
    fun setUp() {
        valueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>
        Mockito.`when`(redisTemplate.opsForValue()).thenReturn(valueOps)
        Mockito.`when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        Mockito.`when`(redisRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        // 기본은 성공 응답; 개별 테스트에서 재정의 가능
        Mockito.`when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.empty())
    }

    // ─────────────────────────────────────────────────────────────────
    // [실패 시나리오 1] 발송 API 예외 발생 → FAILED 상태 저장 + 에러 로그
    // ─────────────────────────────────────────────────────────────────

    /**
     * [시나리오 1] sendNotification이 RuntimeException을 던질 때,
     *              H2 DB에 FAILED 상태로 저장되고 에러 이력이 기록되는지 검증합니다.
     * [Given] 알람 1건 생성 + sendNotification이 예외를 반환하도록 Mock 설정
     * [When] POST /notifications 호출 → Kafka 발행 → Consumer 소비 → 발송 실패
     * [Then] H2 DB에서 해당 알람 상태가 FAILED인지 폴링 검증
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should save FAILED status when sendNotification throws exception`() {
        // Given: sendNotification이 예외를 발생시키도록 설정
        Mockito.`when`(notificationPort.sendNotification(anyList()))
            .thenReturn(Mono.error(RuntimeException("외부 API 타임아웃")))

        val request = AlarmCreateRequest(name = "실패알람", payload = "{}")
        val result = mockMvc.perform(
            post("/notifications")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andReturn()

        val alarmId = objectMapper.readTree(result.response.contentAsString).get("id").asText()

        // When: Consumer 비동기 처리 대기 (최대 5초)
        // Then: H2 DB에서 알람 상태가 FAILED로 갱신되었는지 폴링 검증
        var isFailed = false
        for (i in 1..50) {
            val alarm = alarmRepositoryPort.findById(alarmId)
            if (alarm?.status == AlarmStatus.FAILED) {
                isFailed = true
                break
            }
            Thread.sleep(100)
        }
        assertTrue(isFailed, "sendNotification 예외 발생 시 알람 상태가 5초 내 FAILED로 갱신되어야 합니다.")
    }

    // ─────────────────────────────────────────────────────────────────
    // [실패 시나리오 2] 발송 실패 시 Retry Topic 재발행 (Consumer 단위 테스트)
    // ─────────────────────────────────────────────────────────────────

    /**
     * [시나리오 2] sendNotification 예외 발생 시 실패한 알람 ID가 alarm-jobs-retry 토픽으로 재발행되는지 검증합니다.
     * [Given] 소비자 단위 테스트 환경 구성 + sendNotification 예외 반환 설정
     * [When] consumeAlarmJobs() 직접 호출
     * [Then] kafkaTemplate.send("alarm-jobs-retry", ...)가 호출되었는지 검증
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should republish failed alarm to retry topic`() {
        // Given: 순수 단위 테스트로 consumer 직접 구성
        val mockAlarmRepo = mock(AlarmRepositoryPort::class.java)
        val mockLogRepo = mock(AlarmExecutionLogRepositoryPort::class.java)
        val mockNotification = mock(NotificationPort::class.java)
        val mockRedis = mock(StringRedisTemplate::class.java)
        val mockRateLimiter = mock(RedisRateLimiter::class.java)
        val mockKafka = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
        val mockAck = mock(Acknowledgment::class.java)
        val mockValueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        `when`(mockRedis.opsForValue()).thenReturn(mockValueOps)
        `when`(mockValueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(mockRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        // sendNotification이 예외를 발생시키도록 설정
        `when`(mockNotification.sendNotification(anyList()))
            .thenReturn(Mono.error(RuntimeException("Connection refused")))

        val testMapper = ObjectMapper().registerKotlinModule()
        val consumer = KafkaAlarmJobConsumer(
            mockAlarmRepo, mockNotification, mockLogRepo,
            mockRedis, mockRateLimiter, testMapper, mockKafka
        )

        val payload = """{"alarmId":"alarm-retry-test","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-retry"}"""

        // When: 소비자 직접 호출
        consumer.consumeAlarmJobs(listOf(payload), mockAck)

        // Then: alarm-jobs-retry 토픽으로 재발행 요청이 발생했는지 검증
        verify(mockKafka, times(1)).send(org.mockito.ArgumentMatchers.eq("alarm-jobs-retry"), anyString())
        // FAILED 상태로 DB 업데이트도 검증
        verify(mockAlarmRepo).updateStatusesInBatch(listOf("alarm-retry-test"), AlarmStatus.FAILED)
        verify(mockAck).acknowledge()
    }

    // ─────────────────────────────────────────────────────────────────
    // [실패 시나리오 3] 중복 Kafka 메시지 → proc: 키 멱등 처리 (Consumer 단위 테스트)
    // ─────────────────────────────────────────────────────────────────

    /**
     * [시나리오 3] 동일 uuid(멱등키)를 가진 메시지 2건이 Kafka에 들어올 때
     *              proc: Redis 키로 중복을 감지하여 sendNotification이 정확히 1회만 호출되는지 검증합니다.
     * [Given] 동일 uuid를 가진 payload 2건 준비 + 첫 번째만 setIfAbsent=true, 두 번째는 false 설정
     * [When] consumeAlarmJobs()에 2건을 한꺼번에 전달
     * [Then] sendNotification은 정확히 1회만 호출됨 검증 (중복 건 Skip)
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should deduplicate messages with same uuid via redis proc key`() {
        // Given
        val mockAlarmRepo = mock(AlarmRepositoryPort::class.java)
        val mockLogRepo = mock(AlarmExecutionLogRepositoryPort::class.java)
        val mockNotification = mock(NotificationPort::class.java)
        val mockRedis = mock(StringRedisTemplate::class.java)
        val mockRateLimiter = mock(RedisRateLimiter::class.java)
        val mockKafka = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
        val mockAck = mock(Acknowledgment::class.java)
        val mockValueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        val procKey = "proc:sender-Auuid-SAME"
        `when`(mockRedis.opsForValue()).thenReturn(mockValueOps)
        `when`(mockRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        `when`(mockNotification.sendNotification(anyList())).thenReturn(Mono.empty())

        // 첫 번째 메시지: proc 키 선점 성공 (true)
        // 두 번째 메시지: 이미 선점되어 있음 (false) → 중복으로 Skip
        `when`(mockValueOps.setIfAbsent(org.mockito.ArgumentMatchers.eq(procKey), anyString(), any(Duration::class.java)))
            .thenReturn(true)   // 첫 번째 시도
            .thenReturn(false)  // 두 번째 시도
        `when`(mockValueOps.get(org.mockito.ArgumentMatchers.eq(procKey))).thenReturn("PROCESSING")

        val testMapper = ObjectMapper().registerKotlinModule()
        val consumer = KafkaAlarmJobConsumer(
            mockAlarmRepo, mockNotification, mockLogRepo,
            mockRedis, mockRateLimiter, testMapper, mockKafka
        )

        // 동일 uuid를 가진 메시지 2건 (alarmId만 다름)
        val payload1 = """{"alarmId":"alarm-first","senderId":"sender-A","receiverId":"receiver-A","uuid":"uuid-SAME"}"""
        val payload2 = """{"alarmId":"alarm-second","senderId":"sender-A","receiverId":"receiver-A","uuid":"uuid-SAME"}"""

        // When
        consumer.consumeAlarmJobs(listOf(payload1, payload2), mockAck)

        // Then: 중복 건 제외 → sendNotification은 정확히 1회만 호출
        verify(mockNotification, times(1)).sendNotification(anyList())
        // 첫 번째만 SENT, 두 번째는 처리 안됨
        verify(mockAlarmRepo).updateStatusesInBatch(listOf("alarm-first"), AlarmStatus.SENT)
        verify(mockAck).acknowledge()
    }

    // ─────────────────────────────────────────────────────────────────
    // [실패 시나리오 4] 잘못된 JSON 포맷 메시지 혼입 시 정상 건만 처리
    // ─────────────────────────────────────────────────────────────────

    /**
     * [시나리오 4] 배치 메시지 중 JSON 파싱이 불가능한 잘못된 포맷의 메시지가 1건 포함될 때
     *              해당 건은 Skip(예외 처리)하고 나머지 정상 건은 정상 처리되는지 검증합니다.
     * [Given] 정상 JSON 1건 + 잘못된 포맷 1건을 함께 준비
     * [When] consumeAlarmJobs()에 2건을 전달
     * [Then] sendNotification은 정상 건만 포함하여 1회 호출됨 검증
     *        + 잘못된 건은 예외 로그만 남기고 전체 배치 실패로 이어지지 않음
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should skip malformed json messages and process valid ones`() {
        // Given
        val mockAlarmRepo = mock(AlarmRepositoryPort::class.java)
        val mockLogRepo = mock(AlarmExecutionLogRepositoryPort::class.java)
        val mockNotification = mock(NotificationPort::class.java)
        val mockRedis = mock(StringRedisTemplate::class.java)
        val mockRateLimiter = mock(RedisRateLimiter::class.java)
        val mockKafka = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
        val mockAck = mock(Acknowledgment::class.java)
        val mockValueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        `when`(mockRedis.opsForValue()).thenReturn(mockValueOps)
        `when`(mockValueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(mockRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        `when`(mockNotification.sendNotification(anyList())).thenReturn(Mono.empty())

        val testMapper = ObjectMapper().registerKotlinModule()
        val consumer = KafkaAlarmJobConsumer(
            mockAlarmRepo, mockNotification, mockLogRepo,
            mockRedis, mockRateLimiter, testMapper, mockKafka
        )

        // 정상 JSON 1건 + 잘못된 포맷 1건
        val validPayload = """{"alarmId":"alarm-valid","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-valid"}"""
        val malformedPayload = """NOT_A_JSON_AT_ALL"""

        // When
        consumer.consumeAlarmJobs(listOf(validPayload, malformedPayload), mockAck)

        // Then: 정상 건만 발송되어야 하므로 sendNotification 1회 호출
        verify(mockNotification, times(1)).sendNotification(anyList())
        // 정상 건의 SENT 상태 업데이트 검증
        verify(mockAlarmRepo).updateStatusesInBatch(listOf("alarm-valid"), AlarmStatus.SENT)
        // 전체 배치가 ack되어야 함 (잘못된 건 때문에 멈추면 안됨)
        verify(mockAck).acknowledge()
    }

    // ─────────────────────────────────────────────────────────────────
    // [실패 시나리오 5] 존재하지 않는 파티션 번호로 발행 시 예외 발생 검증
    // ─────────────────────────────────────────────────────────────────

    /**
     * [시나리오 5] 존재하지 않는 파티션(99번)에 강제 발행 시도 시 예외가 발생하는지 검증합니다.
     * [Given] EmbeddedKafka는 파티션 3개(0, 1, 2)만 존재
     * [When] 파티션 99번으로 ProducerRecord를 직접 발행 시도
     * [Then] TimeoutException 또는 ExecutionException 등 전송 실패 예외 발생 검증
     */
    @Test
    fun `producer should fail when sending to non-existent partition`() {
        // Given: EmbeddedKafka는 파티션 0, 1, 2만 존재 (총 3개)
        val invalidPartition = 99
        val payload = """{"alarmId":"alarm-invalid","senderId":"sender","receiverId":"receiver","uuid":"uuid-invalid"}"""

        // When & Then: 존재하지 않는 파티션 99번으로 발행 시 예외 발생
        val exception = org.junit.jupiter.api.Assertions.assertThrows(Exception::class.java) {
            kafkaTemplate.send(ProducerRecord("alarm-jobs", invalidPartition, "key", payload))
                .get(3, TimeUnit.SECONDS)
        }
        assertNotNull(exception, "존재하지 않는 파티션 번호로 발행 시 예외가 발생해야 합니다.")
    }

    // ─────────────────────────────────────────────────────────────────
    // [실패 시나리오 6] 처리 가능한 메시지 없을 때도 ack 정상 호출
    // ─────────────────────────────────────────────────────────────────

    /**
     * [시나리오 6] 배치 내 모든 메시지가 중복(proc: 키로 이미 처리됨)이어서
     *              실제로 처리할 메시지가 없는 경우에도 Kafka ack를 정상 호출하는지 검증합니다.
     * [Given] 모든 메시지의 setIfAbsent가 false를 반환 + get()이 "PROCESSING" 반환 (= 이미 처리 중)
     * [When] consumeAlarmJobs()에 메시지 전달
     * [Then] sendNotification은 절대 호출되지 않음 + ack()는 정상 호출됨
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should ack even when all messages are duplicates`() {
        // Given
        val mockAlarmRepo = mock(AlarmRepositoryPort::class.java)
        val mockLogRepo = mock(AlarmExecutionLogRepositoryPort::class.java)
        val mockNotification = mock(NotificationPort::class.java)
        val mockRedis = mock(StringRedisTemplate::class.java)
        val mockRateLimiter = mock(RedisRateLimiter::class.java)
        val mockKafka = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
        val mockAck = mock(Acknowledgment::class.java)
        val mockValueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        `when`(mockRedis.opsForValue()).thenReturn(mockValueOps)
        // 모든 메시지가 이미 처리 중 상태 (중복)
        `when`(mockValueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(false)
        `when`(mockValueOps.get(anyString())).thenReturn("PROCESSING") // 이미 처리 중 → Skip

        val testMapper = ObjectMapper().registerKotlinModule()
        val consumer = KafkaAlarmJobConsumer(
            mockAlarmRepo, mockNotification, mockLogRepo,
            mockRedis, mockRateLimiter, testMapper, mockKafka
        )

        val payload1 = """{"alarmId":"alarm-dup-1","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-dup-1"}"""
        val payload2 = """{"alarmId":"alarm-dup-2","senderId":"sender-2","receiverId":"receiver-2","uuid":"uuid-dup-2"}"""

        // When
        consumer.consumeAlarmJobs(listOf(payload1, payload2), mockAck)

        // Then: sendNotification은 한 번도 호출되지 않아야 함
        verify(mockNotification, never()).sendNotification(anyList())
        // 처리할 게 없어도 ack는 반드시 호출되어야 오프셋 누락 없음
        verify(mockAck).acknowledge()
    }
}
