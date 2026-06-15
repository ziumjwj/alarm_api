package com.alarm.domain

import java.time.LocalDateTime
import java.util.UUID

/**
 * [파일명] AlarmExecutionLog.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 전송 실행 이력 로그 도메인 엔티티 정의
 * [관점] Domain Entity: 알람이 실제로 발송되었거나 실패한 이력 로그를 비즈니스 규칙에 맞게 팩토리 메서드(record, reconstruct)로 캡슐화한 불변 성격의 도메인 엔티티입니다.
 */
class AlarmExecutionLog private constructor(
    val id: UUID,
    val alarmId: String,
    val triggeredAt: LocalDateTime,
    val status: String, // SUCCESS, FAILED
    val errorMessage: String?
) {
    companion object {
        fun record(
            alarmId: String,
            status: String,
            errorMessage: String? = null
        ): AlarmExecutionLog {
            return AlarmExecutionLog(
                id = UUID.randomUUID(),
                alarmId = alarmId,
                triggeredAt = LocalDateTime.now(),
                status = status,
                errorMessage = errorMessage
            )
        }

        fun reconstruct(
            id: UUID,
            alarmId: String,
            triggeredAt: LocalDateTime,
            status: String,
            errorMessage: String?
        ): AlarmExecutionLog {
            return AlarmExecutionLog(id, alarmId, triggeredAt, status, errorMessage)
        }
    }
}
