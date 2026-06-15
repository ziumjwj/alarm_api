package com.alarm.domain

/**
 * [파일명] AlarmStatus.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 전송 처리 상태 열거형(Enum) 정의
 * [관점] Domain Value Object: 알람이 전송 대기(PENDING), 전송 완료(SENT), 전송 실패(FAILED) 중 어느 상태인지를 나타내는 도메인 레이어의 불변 상태 값 집합입니다.
 */
enum class AlarmStatus {
    PENDING,
    SENT,
    FAILED
}

