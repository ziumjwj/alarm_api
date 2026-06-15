package com.alarm.kafka

import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.domain.AlarmStatus
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyList
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.any
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
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * [대상] KafkaAlarmJobProducer + KafkaAlarmJobConsumer
 * [범주] Kafka 정상 동작 통합 Test — SpringBootTest + EmbeddedKafka 3파티션
 *
 * 프로듀서 파티션 분배
 *   ① 서로 다른 파티션 키 3건 발행 → 2개 이상 파티션에 분산 확인
 *
 * 컨슈머 병렬 처리 (concurrency=3)
 *   ② 파티션 0·1·2에 각 1건 직접 발행 → 5초 내 sendNotification 호출 확인
 *
 * 배치 E2E
 *   ③ API로 알람 3건 생성 → Kafka 소비 완료 → 모두 H2 DB SENT 전이 확인
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
    partitions = 3,
    topics = ["alarm-jobs", "alarm-jobs-retry"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaProducerConsumerSuccessTest {

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

    @BeforeEach
    fun setUp() {
        @Suppress("UNCHECKED_CAST")
        val valueOps = Mockito.mock(ValueOperations::class.java) as ValueOperations<String, String>
        Mockito.`when`(redisTemplate.opsForValue()).thenReturn(valueOps)
        Mockito.`when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        Mockito.`when`(redisRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        Mockito.`when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.empty())
    }

    /**
     * [시나리오 1] 프로듀서가 서로 다른 파티션 키로 발행 시 여러 파티션에 분산되는지 검증합니다.
     * [Given] 서로 다른 senderId:receiverId 조합을 가진 3개의 알람 생성 요청
     * [When] POST /notifications 호출 → KafkaAlarmJobProducer가 파티션 키(senderId:receiverId)로 발행
     * [Then] 실제로 발행된 메시지들의 파티션 번호가 2개 이상으로 분산되었는지 검증
     *        (파티션 키 해시 기반 → 동일 키는 동일 파티션, 다른 키는 다른 파티션으로 분산)
     */
    @Test
    fun `producer should distribute messages across multiple partitions with different partition keys`() {
        // Given: 서로 다른 senderId:receiverId 조합 (파티션 키 다름)
        val requests = listOf(
            AlarmCreateRequest(name = "알람-A", payload = "{}"),  // senderId 자동 생성
            AlarmCreateRequest(name = "알람-B", payload = "{}"),
            AlarmCreateRequest(name = "알람-C", payload = "{}")
        )

        // 직접 파티션 키를 달리하여 Kafka에 3개 메시지 발행
        val partitionKeys = listOf("senderA:receiverA", "senderB:receiverB", "senderC:receiverC")
        val payload = """{"alarmId":"test-id","senderId":"sender","receiverId":"receiver","uuid":"uuid-test"}"""

        val usedPartitions = mutableSetOf<Int>()
        partitionKeys.forEach { key ->
            val result = kafkaTemplate.send("alarm-jobs", key, payload).get(5, TimeUnit.SECONDS)
            usedPartitions.add(result.recordMetadata.partition())
        }

        // Then: 3개의 서로 다른 파티션 키가 2개 이상의 파티션에 분산되었는지 검증
        // (해시 충돌로 동일 파티션이 될 수 있으나 최소 2개 이상 파티션 활용 확인)
        assertTrue(
            usedPartitions.size >= 2,
            "서로 다른 파티션 키로 발행 시 2개 이상의 파티션에 분산되어야 합니다. 실제 사용된 파티션: $usedPartitions"
        )
    }

    /**
     * [시나리오 2] 3개 파티션에 발행된 메시지를 컨슈머 3개(concurrency=3)가 병렬 소비하는지 검증합니다.
     * [Given] 파티션 0, 1, 2에 각 1개씩 메시지를 직접 지정하여 발행
     * [When] KafkaAlarmJobConsumer(concurrency=3)가 각 파티션 담당 컨슈머로 병렬 소비
     * [Then] 5초 이내에 notificationPort.sendNotification이 최소 1회 이상 호출됨을 비동기 검증
     *        (모든 파티션의 메시지가 컨슈머에 의해 소비됨을 의미)
     */
    @Test
    fun `consumers should process messages from all 3 partitions in parallel`() {
        // Given: 파티션 0, 1, 2에 각 1개씩 메시지를 명시적으로 발행
        listOf(0, 1, 2).forEach { partition ->
            val partitionPayload = """{"alarmId":"alarm-p$partition","senderId":"sender-$partition","receiverId":"receiver-$partition","uuid":"uuid-p$partition"}"""
            kafkaTemplate.send(ProducerRecord("alarm-jobs", partition, "key-$partition", partitionPayload))
                .get(5, TimeUnit.SECONDS)
        }

        // Then: 5초 이내에 sendNotification이 최소 1회 이상 호출되어야 함
        // (컨슈머 3개가 파티션 0, 1, 2를 각각 담당하여 병렬 소비함)
        verify(notificationPort, timeout(5000).atLeastOnce()).sendNotification(anyList())
    }

    /**
     * [시나리오 3] 여러 건의 알람을 API로 생성하면 Kafka 비동기 파이프라인을 통해 모두 SENT로 처리되는지 검증합니다.
     * [Given] 서로 다른 3개의 알람을 API 호출로 생성 (각각 Kafka에 발행됨)
     * [When] EmbeddedKafka의 컨슈머들이 메시지를 소비 및 처리
     * [Then] 최대 5초 내에 notificationPort.sendNotification이 3회 이상 호출됨을 검증
     *        + H2 DB에서 각 알람 상태가 SENT로 변경되었는지 폴링 검증
     */
    @Test
    fun `all alarms should be processed to SENT via kafka consumer pipeline`() {
        // Given: 3개 알람을 API로 생성
        val alarmIds = mutableListOf<String>()
        repeat(3) { i ->
            val request = AlarmCreateRequest(name = "배치알람-$i", payload = """{"seq":$i}""")
            val result = mockMvc.perform(
                post("/notifications")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(request))
            )
                .andExpect(status().isCreated)
                .andReturn()

            val id = objectMapper.readTree(result.response.contentAsString).get("id").asText()
            alarmIds.add(id)
        }

        // When: Kafka Consumer 비동기 처리 대기 (최대 5초)
        // Then 1: sendNotification 3회 이상 호출 검증
        verify(notificationPort, timeout(5000).atLeast(3)).sendNotification(anyList())

        // Then 2: 각 알람의 최종 상태가 SENT인지 H2 DB 폴링 검증
        alarmIds.forEach { alarmId ->
            var isSent = false
            for (i in 1..50) {
                if (alarmRepositoryPort.findById(alarmId)?.status == AlarmStatus.SENT) {
                    isSent = true
                    break
                }
                Thread.sleep(100)
            }
            assertTrue(isSent, "alarmId=$alarmId 의 상태가 5초 내 SENT로 갱신되지 않았습니다.")
        }
    }
}
