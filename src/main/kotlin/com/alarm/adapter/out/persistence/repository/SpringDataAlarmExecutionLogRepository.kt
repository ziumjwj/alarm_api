package com.alarm.adapter.out.persistence.repository

import com.alarm.adapter.out.persistence.entity.AlarmExecutionLogEntity
import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

/**
 * [파일명] SpringDataAlarmExecutionLogRepository.kt
 * [수정일자] 2026-06-11
 * [기능] Spring Data JPA 기반의 알람 실행 이력 테이블 데이터 액세스 인터페이스
 * [관점] Outbound Infrastructure: Spring Data JPA 프레임워크 기술을 활용하여 알람 전송 실행 이력을 로깅하고 DB 물리 레이어에 접근합니다.
 */

interface SpringDataAlarmExecutionLogRepository : JpaRepository<AlarmExecutionLogEntity, UUID> {
    fun findByAlarmId(alarmId: String): List<AlarmExecutionLogEntity>
}
