package com.alarm.adapter.out.persistence.repository

import com.alarm.adapter.out.persistence.entity.AlarmEntity
import org.springframework.data.jpa.repository.JpaRepository

/**
 * [파일명] SpringDataAlarmRepository.kt
 * [수정일자] 2026-06-11
 * [기능] Spring Data JPA 기반의 알람 테이블 데이터 액세스 인터페이스
 * [관점] Outbound Infrastructure: Spring Data JPA 프레임워크 기술을 활용하여 기본적인 CRUD 쿼리를 자동으로 처리하고 DB 물리 레이어에 접근합니다.
 */

interface SpringDataAlarmRepository : JpaRepository<AlarmEntity, String>
