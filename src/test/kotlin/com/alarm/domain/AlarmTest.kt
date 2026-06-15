package com.alarm.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * [대상] Alarm 도메인 엔티티
 * [범주] Domain Unit Test — 외부 의존성 없음
 *
 * ① 생성 — 정상 입력 시 초기 상태 PENDING
 * ② 생성 — 빈 이름 입력 시 IllegalArgumentException
 * ③ 상태 변이 — sent() 호출 시 SENT로 전이
 */
class AlarmTest {

    @Test
    fun `should create alarm with valid inputs`() {
        val alarm = Alarm.create(senderId = "12345", receiverId = "9999", name = "Test Alarm", payload = "{}")

        assertNotNull(alarm.id)
        assertEquals("Test Alarm", alarm.name)
        assertEquals(AlarmStatus.PENDING, alarm.status)
    }

    @Test
    fun `should fail to create alarm when name is blank`() {
        assertThrows<IllegalArgumentException> {
            Alarm.create(senderId = "12345", receiverId = "9999", name = "   ", payload = "{}")
        }
    }

    @Test
    fun `should update status correctly on trigger`() {
        val alarm = Alarm.create(senderId = "12345", receiverId = "9999", name = "Test Alarm", payload = "{}")
        
        alarm.sent()
        assertEquals(AlarmStatus.SENT, alarm.status)
    }
}
