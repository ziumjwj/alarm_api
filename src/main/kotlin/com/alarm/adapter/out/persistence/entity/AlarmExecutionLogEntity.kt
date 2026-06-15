package com.alarm.adapter.out.persistence.entity

import com.alarm.domain.AlarmExecutionLog
import jakarta.persistence.*
import java.time.LocalDateTime
import java.util.UUID

/**
 * [파일명] AlarmExecutionLogEntity.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 실행 이력 도메인 모델과 매핑되는 JPA RDB 테이블 엔티티 정의
 * [관점] Outbound persistence Entity: RDB(H2) 실행로그 테이블 스키마에 대응하며 도메인-DB 간 매핑 및 격리 기능을 수행합니다.
 */
@Entity
@Table(name = "alarm_execution_logs")
class AlarmExecutionLogEntity(
    @Id
    val id: UUID,

    @Column(name = "alarm_id", nullable = false)
    val alarmId: String,

    @Column(name = "triggered_at", nullable = false)
    val triggeredAt: LocalDateTime,

    @Column(nullable = false)
    val status: String, // SUCCESS, FAILED

    @Column(name = "error_message", columnDefinition = "TEXT")
    val errorMessage: String?
) {
    companion object {
        fun fromDomain(log: AlarmExecutionLog): AlarmExecutionLogEntity {
            return AlarmExecutionLogEntity(
                id = log.id,
                alarmId = log.alarmId,
                triggeredAt = log.triggeredAt,
                status = log.status,
                errorMessage = log.errorMessage
            )
        }
    }

    fun toDomain(): AlarmExecutionLog {
        return AlarmExecutionLog.reconstruct(
            id = id,
            alarmId = alarmId,
            triggeredAt = triggeredAt,
            status = status,
            errorMessage = errorMessage
        )
    }
}
