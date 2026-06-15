package com.alarm.application.port.`in`

import com.alarm.domain.Alarm
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable

/**
 * [파일명] GetAlarmQuery.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 단건 조회 및 전체 목록 조회 요구사항 정의 인터페이스
 * [관점] Inbound Port: 클라이언트의 읽기 전용 질의(Query) 요청을 애플리케이션 서비스 계층으로 중개하는 계약 인터페이스입니다.
 */
interface GetAlarmQuery {
    fun getAlarm(id: String): Alarm
    fun listAlarms(pageable: Pageable): Page<Alarm>
}
