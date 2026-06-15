package com.alarm.application.port.`in`

/**
 * [파일명] DeleteAlarmUseCase.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 삭제 및 취소 비즈니스 요구사항 정의 인터페이스
 * [관점] Inbound Port: 클라이언트의 삭제 요청을 애플리케이션 코어의 서비스 레이어에 매핑하여 제공하는 진입점 인터페이스입니다.
 */


interface DeleteAlarmUseCase {
    fun deleteAlarm(id: String)
}
