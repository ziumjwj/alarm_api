package com.alarm.adapter.`in`.web.dto

import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import java.time.LocalDateTime

/**
 * [파일명] AlarmDto.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 데이터 요청 및 응답 전송 객체(DTO) 정의
 * [관점] Inbound Adapter DTO: HTTP 요청 바디 파싱 및 응답 데이터 형식을 규격화하며 도메인 엔티티를 클라이언트 스펙에 맞춰 매핑합니다.
 */
data class AlarmCreateRequest(
    val name: String,
    val payload: String? = null,
    val receiverId: String? = null
)

data class AlarmUpdateRequest(
    val name: String? = null,
    val status: AlarmStatus? = null,
    val payload: String? = null
)

data class AlarmResponse(
    val id: String,
    val senderId: String,
    val receiverId: String,
    val name: String,
    val status: AlarmStatus,
    val payload: String?,
    val createdAt: LocalDateTime,
    val updatedAt: LocalDateTime
) {
    companion object {
        fun fromDomain(alarm: Alarm): AlarmResponse {
            return AlarmResponse(
                id = alarm.id,
                senderId = alarm.senderId,
                receiverId = alarm.receiverId,
                name = alarm.name,
                status = alarm.status,
                payload = alarm.payload,
                createdAt = alarm.createdAt,
                updatedAt = alarm.updatedAt
            )
        }
    }
}
