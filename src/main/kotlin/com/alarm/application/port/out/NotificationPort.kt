package com.alarm.application.port.out

import reactor.core.publisher.Mono

/**
 * [파일명] NotificationPort.kt
 * [수정일자] 2026-06-11
 * [기능] 외부 알림 API 발송을 위한 아웃바운드 포트 및 데이터 모델 정의
 * [관점] Outbound Port: 외부 SMS/푸시/웹훅 API 게이트웨이 호출 사양을 선언하여, 구체적인 WebClient 전송 및 재시도 기술로부터 도메인을 격리하는 인터페이스입니다.
 */
interface NotificationPort {
    fun sendNotification(items: List<AlarmBatchItem>): Mono<Void>
}

data class AlarmBatchItem(
    val alarmId: String,
    val senderId: String,
    val uuidClean: String,
    val payload: String?
)
