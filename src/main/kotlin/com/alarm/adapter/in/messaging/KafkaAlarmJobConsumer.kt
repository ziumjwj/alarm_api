package com.alarm.adapter.`in`.messaging

import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.domain.AlarmExecutionLog
import com.alarm.domain.AlarmStatus
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * [파일명] KafkaAlarmJobConsumer.kt
 * [수정일자] 2026-06-11
 * [기능] Kafka 비동기 알람 메시지 배치 소비 및 전송 처리
 * [관점] Inbound Adapter: 카프카 큐 이벤트 스트림(Inbound Event)을 구독하여 외부 API 일괄 호출 및 DB 벌크/배치 업데이트(JdbcTemplate)를 연동하고 멱등 처리를 보장합니다.
 */
@Component
class KafkaAlarmJobConsumer(
    private val alarmRepositoryPort: AlarmRepositoryPort,
    private val notificationPort: NotificationPort,
    private val alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort,
    private val redisTemplate: StringRedisTemplate,
    private val redisRateLimiter: RedisRateLimiter,
    private val objectMapper: ObjectMapper,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val logger = LoggerFactory.getLogger(KafkaAlarmJobConsumer::class.java)

    data class AlarmJobPayload(
        val alarmId: String,
        val senderId: String,
        val receiverId: String,
        val uuid: String
    )

    @KafkaListener(
        topics = ["alarm-jobs"],
        groupId = "alarm-group",
        concurrency = "3",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeAlarmJobs(payloads: List<String>, ack: Acknowledgment) {
        logger.info("Received batch of alarm job events from Kafka: size = {}", payloads.size)
        val itemsToProcess = mutableListOf<AlarmJobPayload>()

        // 1. 소비 단계 멱등성 체크 (Redis proc:)
        for (payloadStr in payloads) {
            try {
                val payload = objectMapper.readValue(payloadStr, AlarmJobPayload::class.java)
                val procKey = "proc:${payload.senderId}${payload.uuid}"
                
                // PROCESSING 상태로 선점 (TTL 3분)
                val isFirstProcessing = redisTemplate.opsForValue().setIfAbsent(procKey, "PROCESSING", Duration.ofMinutes(3))
                if (isFirstProcessing == true) {
                    itemsToProcess.add(payload)
                } else {
                    val status = redisTemplate.opsForValue().get(procKey)
                    if (status != "PROCESSING" && status != "DONE") {
                        // 만약 비정상 상태면 다시 PROCESSING 선점
                        redisTemplate.opsForValue().set(procKey, "PROCESSING", Duration.ofMinutes(3))
                        itemsToProcess.add(payload)
                    } else {
                        logger.info("Duplicate Kafka job detected and ignored: alarmId = {}", payload.alarmId)
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to parse or check idempotency for payload: {}", payloadStr, e)
            }
        }

        if (itemsToProcess.isEmpty()) {
            ack.acknowledge()
            return
        }

        // 2. 최대 200건씩 묶어 처리 (Rate Limiter 연동)
        val chunks = itemsToProcess.chunked(200)
        val successfulIds = mutableListOf<String>()
        val failedIds = mutableListOf<String>()
        val logs = mutableListOf<AlarmExecutionLog>()

        for (chunk in chunks) {
            // API 호출 제한 (초당 500회) 준수를 위한 Token 획득 대기
            while (!redisRateLimiter.tryAcquire(1)) {
                Thread.sleep(100)
            }

            val batchItems = chunk.map {
                AlarmBatchItem(
                    alarmId = it.alarmId,
                    senderId = it.senderId,
                    uuidClean = it.uuid,
                    payload = it.alarmId
                )
            }

            try {
                // 외부 API 일괄 호출 (지수 백오프 내장)
                notificationPort.sendNotification(batchItems).block()

                // 성공 시 Redis DONE 상태 변경 및 H2 상태 업데이트 목록 수집
                for (item in chunk) {
                    val procKey = "proc:${item.senderId}${item.uuid}"
                    redisTemplate.opsForValue().set(procKey, "DONE", Duration.ofMinutes(3))
                    
                    successfulIds.add(item.alarmId)
                    logs.add(AlarmExecutionLog.record(item.alarmId, "SUCCESS"))
                }
            } catch (e: Exception) {
                logger.error("Failed to execute batch API call for chunk", e)
                
                // 실패 시 Kafka 재처리 큐(Retry Topic)에 전송 및 FAILED 처리
                for (item in chunk) {
                    val procKey = "proc:${item.senderId}${item.uuid}"
                    redisTemplate.delete(procKey) // 멱등 키 제거하여 재시도 가능하게 함
                    
                    failedIds.add(item.alarmId)
                    logs.add(AlarmExecutionLog.record(item.alarmId, "FAILED", e.message))

                    // Retry topic으로 발행
                    val failedPayload = """{"alarmId":"${item.alarmId}","senderId":"${item.senderId}","receiverId":"${item.receiverId}","uuid":"${item.uuid}"}"""
                    kafkaTemplate.send("alarm-jobs-retry", failedPayload)
                }
            }
        }

        // H2 DB 벌크/배치 업데이트 수행 (성공/실패 일괄 반영)
        try {
            if (successfulIds.isNotEmpty()) {
                alarmRepositoryPort.updateStatusesInBatch(successfulIds, AlarmStatus.SENT)
            }
            if (failedIds.isNotEmpty()) {
                alarmRepositoryPort.updateStatusesInBatch(failedIds, AlarmStatus.FAILED)
            }
            if (logs.isNotEmpty()) {
                alarmExecutionLogRepositoryPort.saveAll(logs)
            }
        } catch (e: Exception) {
            logger.error("Failed to update RDB database states in batch", e)
        }

        // 수동 커밋 수행
        ack.acknowledge()
        logger.info("Successfully acknowledged Kafka batch of size = {}", payloads.size)
    }

    @KafkaListener(
        topics = ["alarm-jobs-retry"],
        groupId = "alarm-retry-group"
    )
    fun consumeRetry(record: String) {
        logger.info("Received failed alarm event to retry after 1 minute delay: {}", record)
        Thread.sleep(60000) // 1분 대기
        kafkaTemplate.send("alarm-jobs", record)
    }
}
