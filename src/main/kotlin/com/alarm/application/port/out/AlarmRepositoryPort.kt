package com.alarm.application.port.out

import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable

/**
 * [파일명] AlarmRepositoryPort.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 도메인 엔티티 영속화를 위한 아웃바운드 포트 정의
 * [관점] Outbound Port: 애플리케이션 코어 레이어가 영속성 인프라 기술에 직접 결합하지 않도록 추상화된 CRUD 및 벌크 배치 저장/조회 계약 포트입니다.
 */
interface AlarmRepositoryPort {
    fun save(alarm: Alarm): Alarm
    fun findById(id: String): Alarm?
    fun findAll(pageable: Pageable): Page<Alarm>
    fun delete(alarm: Alarm)
    fun updateStatusesInBatch(ids: List<String>, status: AlarmStatus)
}
