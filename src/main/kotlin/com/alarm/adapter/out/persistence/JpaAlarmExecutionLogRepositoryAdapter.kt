package com.alarm.adapter.out.persistence

import com.alarm.adapter.out.persistence.entity.AlarmExecutionLogEntity
import com.alarm.adapter.out.persistence.repository.SpringDataAlarmExecutionLogRepository
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.domain.AlarmExecutionLog
import org.springframework.stereotype.Component

/**
 * [파일명] JpaAlarmExecutionLogRepositoryAdapter.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 발송 실행 이력 로그 데이터베이스 저장 및 조회
 * [관점] Outbound Adapter: AlarmExecutionLogRepositoryPort 구체 클래스로서, 알람 발송 성공/실패 여부 및 에러 이력을 JPA 엔티티로 변환하여 영속화합니다.
 */
@Component
class JpaAlarmExecutionLogRepositoryAdapter(
    private val springDataAlarmExecutionLogRepository: SpringDataAlarmExecutionLogRepository
) : AlarmExecutionLogRepositoryPort {

    override fun save(log: AlarmExecutionLog): AlarmExecutionLog {
        val entity = AlarmExecutionLogEntity.fromDomain(log)
        val savedEntity = springDataAlarmExecutionLogRepository.save(entity)
        return savedEntity.toDomain()
    }

    override fun saveAll(logs: List<AlarmExecutionLog>): List<AlarmExecutionLog> {
        val entities = logs.map { AlarmExecutionLogEntity.fromDomain(it) }
        val savedEntities = springDataAlarmExecutionLogRepository.saveAll(entities)
        return savedEntities.map { it.toDomain() }
    }

    override fun findByAlarmId(alarmId: String): List<AlarmExecutionLog> {
        return springDataAlarmExecutionLogRepository.findByAlarmId(alarmId)
            .map { it.toDomain() }
    }
}
