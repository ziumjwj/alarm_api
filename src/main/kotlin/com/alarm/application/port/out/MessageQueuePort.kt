package com.alarm.application.port.out

import com.alarm.domain.Alarm

/**
 * [파일명] MessageQueuePort.kt
 * [수정일자] 2026-06-11
 * [기능] 비동기 메시지 큐 발행을 위한 아웃바운드 포트 정의
 * [관점] Outbound Port: 카프카나 RabbitMQ 등의 구현체와 무관하게, 코어 비즈니스 레이어에서 발생한 비동기 발송 작업을 이벤트 큐로 전달하는 발행용 포트 계약입니다.
 */
interface MessageQueuePort {
    fun publishAlarmJob(alarm: Alarm)
}
