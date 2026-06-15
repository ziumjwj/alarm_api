package com.alarm.adapter.out.persistence.entity

import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import jakarta.persistence.*
import java.time.LocalDateTime

/**
 * [파일명] AlarmEntity.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 도메인 엔티티와 매핑되는 JPA RDB 테이블 엔티티 정의
 * [관점] Outbound persistence Entity: RDB(H2) 스키마 구조와 대응하며 도메인과 DB 간의 상호 매핑(reconstruct/fromDomain) 변환 로직을 가집니다.
 */
@Entity
@Table(name = "alarms")
class AlarmEntity(
    @Id
    val id: String,

    @Column(name = "sender_id", nullable = false)
    val senderId: String,

    @Column(name = "receiver_id", nullable = false)
    val receiverId: String,

    @Column(nullable = false)
    val name: String,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    val status: AlarmStatus,

    @Column(columnDefinition = "TEXT")
    val payload: String?,

    @Column(name = "created_at", nullable = false, updatable = false)
    val createdAt: LocalDateTime,

    @Column(name = "updated_at", nullable = false)
    val updatedAt: LocalDateTime
) {
    companion object {
        fun fromDomain(alarm: Alarm): AlarmEntity {
            return AlarmEntity(
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

    fun toDomain(): Alarm {
        return Alarm.reconstruct(
            id = id,
            senderId = senderId,
            receiverId = receiverId,
            name = name,
            status = status,
            payload = payload,
            createdAt = createdAt,
            updatedAt = updatedAt
        )
    }
}
