package com.alarm.domain

import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * [대상] AlarmExecutionLog 도메인 엔티티
 * [범주] Domain Unit Test — 외부 의존성 없음
 *
 * ① record()   — 성공 이력: SUCCESS 상태, errorMessage = null
 * ② record()   — 실패 이력: FAILED 상태, errorMessage 내용 포함
 * ③ reconstruct() — DB 복원: id/시간/상태 필드 오염 없이 로드
 */
class AlarmExecutionLogTest {

    /**
     * [시나리오] 에러 메시지가 없는 정상적인 알람 발송 이력이 정상 기록되는지 테스트합니다.
     * [검증 항목]
     * - 로그 ID가 null이 아니게 생성되는지
     * - 알람 ID, 상태(SUCCESS), 에러메시지(null), 생성시간이 정상 매핑되는지
     */
    @Test
    fun `should record execution log correctly`() {
        // Given
        val alarmId = "alarm-123"
        val status = "SUCCESS"

        // When
        val log = AlarmExecutionLog.record(alarmId, status)

        // Then
        assertNotNull(log.id)
        assertEquals(alarmId, log.alarmId)
        assertEquals(status, log.status)
        assertNull(log.errorMessage)
        assertNotNull(log.triggeredAt)
    }

    /**
     * [시나리오] 에러 메시지가 존재하는 실패 알람 발송 이력이 정상 기록되는지 테스트합니다.
     * [검증 항목]
     * - 로그 ID가 null이 아니게 생성되는지
     * - 알람 ID, 상태(FAILED), 전달받은 에러메시지 내용이 정확히 매핑되는지
     */
    @Test
    fun `should record execution log with error message correctly`() {
        // Given
        val alarmId = "alarm-124"
        val status = "FAILED"
        val errorMessage = "Timeout connection"

        // When
        val log = AlarmExecutionLog.record(alarmId, status, errorMessage)

        // Then
        assertNotNull(log.id)
        assertEquals(alarmId, log.alarmId)
        assertEquals(status, log.status)
        assertEquals(errorMessage, log.errorMessage)
        assertNotNull(log.triggeredAt)
    }

    /**
     * [시나리오] DB 등 외부 저장소로부터 불러온 기존 데이터 기반으로 알람 이력 객체가 올바르게 복구(Reconstruct)되는지 테스트합니다.
     * [검증 항목]
     * - 기존에 저장되어 있던 고유 식별자, 발생 시점, 상태 값들이 오염 없이 그대로 로드되는지
     */
    @Test
    fun `should reconstruct execution log correctly`() {
        // Given
        val id = UUID.randomUUID()
        val alarmId = "alarm-125"
        val triggeredAt = LocalDateTime.now().minusDays(1)
        val status = "SUCCESS"
        val errorMessage = null

        // When
        val log = AlarmExecutionLog.reconstruct(id, alarmId, triggeredAt, status, errorMessage)

        // Then
        assertEquals(id, log.id)
        assertEquals(alarmId, log.alarmId)
        assertEquals(triggeredAt, log.triggeredAt)
        assertEquals(status, log.status)
        assertEquals(errorMessage, log.errorMessage)
    }
}
