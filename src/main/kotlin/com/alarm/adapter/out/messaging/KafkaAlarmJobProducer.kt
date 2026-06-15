package com.alarm.adapter.out.messaging

import com.alarm.application.port.out.MessageQueuePort
import com.alarm.domain.Alarm
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

/**
 * [파일명] KafkaAlarmJobProducer.kt
 * [수정일자] 2026-06-11
 * [기능] Kafka 비동기 알람 메시지 발행 처리
 * [관점] Outbound Adapter: MessageQueuePort 구체 클래스로서, 생성된 알람에 대한 비동기 발송 작업을 카프카 브로커에 JSON 형태로 발행(Publish)합니다.
 */
@Component
class KafkaAlarmJobProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) : MessageQueuePort {

    override fun publishAlarmJob(alarm: Alarm) {
        val partitionKey = "${alarm.senderId}:${alarm.receiverId}"
        val uuidClean = alarm.id.toString().replace("-", "")
        val payload = """{"alarmId":"${alarm.id}","senderId":"${alarm.senderId}","receiverId":"${alarm.receiverId}","uuid":"$uuidClean"}"""
        kafkaTemplate.send("alarm-jobs", partitionKey, payload)
    }
}
