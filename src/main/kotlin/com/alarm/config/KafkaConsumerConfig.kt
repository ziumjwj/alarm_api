package com.alarm.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.ContainerProperties.AckMode

/**
 * [파일명] KafkaConsumerConfig.kt
 * [수정일자] 2026-06-11
 * [기능] Kafka 배치 컨슈머 리스너 팩토리 및 수동 오프셋 커밋(Manual Ack) 설정
 * [관점] Configuration: 카프카 컨슈머의 메시지 배치 수신 처리 및 수동 오프셋 Ack 모드를 설정하는 인프라스트럭처 설정 클래스입니다.
 */
@Configuration
class KafkaConsumerConfig {

    @Bean
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, String>
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory
        factory.isBatchListener = true // 배치 리스너 활성화 (List<String> 또는 List<ConsumerRecord>로 수신 가능)
        factory.containerProperties.ackMode = AckMode.MANUAL_IMMEDIATE
        return factory
    }
}
