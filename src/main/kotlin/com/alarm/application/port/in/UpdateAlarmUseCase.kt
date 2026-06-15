package com.alarm.application.port.`in`

import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus

/**
 * [파일명] UpdateAlarmUseCase.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 설정 정보 및 상태 수정 비즈니스 요구사항 정의 인터페이스
 * [관점] Inbound Port: 클라이언트의 수정 요청을 도메인 비즈니스 계층(AlarmService)으로 중개하는 계약 인터페이스입니다.
 */
interface UpdateAlarmUseCase {
    fun updateAlarm(command: UpdateAlarmCommand): Alarm

    data class UpdateAlarmCommand(
        val id: String,
        val name: String?,
        val status: AlarmStatus?,
        val payload: String?
    )
}
