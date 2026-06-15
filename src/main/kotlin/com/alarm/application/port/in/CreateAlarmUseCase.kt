package com.alarm.application.port.`in`

import com.alarm.domain.Alarm

/**
 * [파일명] CreateAlarmUseCase.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 신규 생성 비즈니스 요구사항 정의 인터페이스
 * [관점] Inbound Port: 클라이언트(Inbound Adapter)의 요청을 도메인 비즈니스 계층(AlarmService)으로 중개하는 계약 인터페이스입니다.
 */

interface CreateAlarmUseCase {
    fun createAlarm(command: CreateAlarmCommand): Alarm

    data class CreateAlarmCommand(
        val id: String,
        val senderId: String,
        val receiverId: String,
        val name: String,
        val payload: String?
    )
}
