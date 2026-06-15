package com.alarm.adapter.out.persistence

import com.alarm.adapter.out.persistence.entity.AlarmEntity
import com.alarm.adapter.out.persistence.repository.SpringDataAlarmRepository
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * [파일명] JpaAlarmRepositoryAdapter.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 데이터베이스 저장 및 조회, 벌크 업데이트 처리
 * [관점] Outbound Adapter: AlarmRepositoryPort 구체 클래스로서, JPA를 통한 영속 객체 관리 및 JdbcTemplate을 활용한 다량 건수 벌크 배치 업데이트(Bulk/Batch Update)를 구현합니다.
 */
@Component
class JpaAlarmRepositoryAdapter(
    private val springDataAlarmRepository: SpringDataAlarmRepository,
    private val jdbcTemplate: JdbcTemplate
) : AlarmRepositoryPort {

    override fun save(alarm: Alarm): Alarm {
        val entity = AlarmEntity.fromDomain(alarm)
        val savedEntity = springDataAlarmRepository.save(entity)
        return savedEntity.toDomain()
    }

    override fun findById(id: String): Alarm? {
        return springDataAlarmRepository.findById(id)
            .map { it.toDomain() }
            .orElse(null)
    }

    override fun findAll(pageable: Pageable): Page<Alarm> {
        return springDataAlarmRepository.findAll(pageable)
            .map { it.toDomain() }
    }

    override fun delete(alarm: Alarm) {
        val entity = AlarmEntity.fromDomain(alarm)
        springDataAlarmRepository.delete(entity)
    }

    override fun updateStatusesInBatch(ids: List<String>, status: AlarmStatus) {
        if (ids.isEmpty()) return
        val sql = "UPDATE alarms SET status = ?, updated_at = ? WHERE id = ?"
        jdbcTemplate.batchUpdate(sql, object : BatchPreparedStatementSetter {
            override fun setValues(ps: PreparedStatement, i: Int) {
                ps.setString(1, status.name)
                ps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()))
                ps.setString(3, ids[i])
            }
            override fun getBatchSize(): Int = ids.size
        })
    }
}
