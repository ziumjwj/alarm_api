package com.alarm.application.port.out

import com.alarm.domain.AlarmExecutionLog

/**
 * [파일명] AlarmExecutionLogRepositoryPort.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 실행 로그 영속화를 위한 아웃바운드 포트 정의
 * [관점] Outbound Port: 애플리케이션 코어 레이어가 영속성 기술(JPA 등)에 의존하지 않도록 분리하고 로그 저장을 요구하는 아웃바운드 포트 계약입니다.
 */
interface AlarmExecutionLogRepositoryPort {
    fun save(log: AlarmExecutionLog): AlarmExecutionLog
    fun saveAll(logs: List<AlarmExecutionLog>): List<AlarmExecutionLog>
    fun findByAlarmId(alarmId: String): List<AlarmExecutionLog>
}
