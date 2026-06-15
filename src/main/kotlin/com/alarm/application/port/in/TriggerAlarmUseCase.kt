package com.alarm.application.port.`in`


/**
 * [파일명] TriggerAlarmUseCase.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 즉시 발생 비즈니스 요구사항 정의 인터페이스
 * [관점] Inbound Port: 클라이언트의 즉시 실행 요청을 도메인 비즈니스 계층(AlarmService)으로 중개하는 계약 인터페이스입니다.
 */
interface TriggerAlarmUseCase {
    fun triggerAlarm(id: String)
}
