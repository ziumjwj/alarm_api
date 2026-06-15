package com.alarm.application.service

import com.alarm.application.port.`in`.CreateAlarmUseCase
import com.alarm.application.port.out.*
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmExecutionLog
import com.alarm.domain.AlarmStatus
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito
import org.mockito.Mockito.*
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.web.server.ResponseStatusException
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * [대상] AlarmService — 애플리케이션 서비스 레이어
 * [범주] Service Unit Test — Mockito Mock, 외부 의존성 없음
 *
 * createAlarm
 *   ① DB에 PENDING 상태로 저장
 *   ② Kafka 큐(MessageQueue)에 발행
 *   ③ Redis 멱등키 중복 → CONFLICT(409) 예외
 *
 * triggerAlarm
 *   ④ 이미 SENT 상태 → 재발송 Skip
 *   ⑤ 발송 성공 → DB SENT 갱신 + SUCCESS 이력 기록
 *   ⑥ 발송 실패 → DB FAILED 갱신 + FAILED 이력 기록
 */
class AlarmServiceTest {

    private lateinit var alarmRepositoryPort: AlarmRepositoryPort
    private lateinit var alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort
    private lateinit var notificationPort: NotificationPort
    private lateinit var messageQueuePort: MessageQueuePort
    private lateinit var redisTemplate: StringRedisTemplate
    private lateinit var valueOps: ValueOperations<String, String>
    private lateinit var alarmService: AlarmService

    private val dummyAlarm = Alarm.create(
        id = "dummy-id",
        senderId = "sender-1",
        receiverId = "receiver-1",
        name = "Dummy",
        payload = null
    )
    private val dummyLog = AlarmExecutionLog.record("dummy", "SUCCESS")

    @Suppress("UNCHECKED_CAST")
    @BeforeEach
    fun setUp() {
        alarmRepositoryPort = mock(AlarmRepositoryPort::class.java)
        alarmExecutionLogRepositoryPort = mock(AlarmExecutionLogRepositoryPort::class.java)
        notificationPort = mock(NotificationPort::class.java)
        messageQueuePort = mock(MessageQueuePort::class.java)
        redisTemplate = mock(StringRedisTemplate::class.java)
        valueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        `when`(redisTemplate.opsForValue()).thenReturn(valueOps)

        alarmService = AlarmService(
            alarmRepositoryPort,
            alarmExecutionLogRepositoryPort,
            notificationPort,
            messageQueuePort,
            redisTemplate
        )
    }

    /**
     * [시나리오] 최초 알람 생성 시 DB에 저장되는 알람의 초기 상태를 검증합니다.
     * [Given] 생성 명령 파라미터 준비 및 Redis 중복검사 통과 설정
     * [When] 알람 생성 메서드 호출
     * [Then] DB에 저장(save) 요청된 알람의 상태(status)가 PENDING인지 검증
     */
    @Test
    fun `createAlarm - should save alarm with PENDING status in DB`() {
        // Given
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = "test-uuid-123",
            senderId = "sender-1",
            receiverId = "receiver-1",
            name = "Test Alarm",
            payload = "{}"
        )
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        
        val alarmCaptor = ArgumentCaptor.forClass(Alarm::class.java)
        `when`(alarmRepositoryPort.save(alarmCaptor.capture() ?: dummyAlarm)).thenAnswer { it.getArgument(0) }

        // When
        alarmService.createAlarm(command)

        // Then
        val savedAlarm = alarmCaptor.value
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.PENDING, savedAlarm.status) // H2 DB 저장 시 PENDING 상태 검증
    }

    /**
     * [시나리오] 알람 생성 시 카프카 큐로 메시지가 정상 발행되는지 검증합니다.
     * [Given] 생성 명령 파라미터 준비 및 Redis 중복검사 통과 설정
     * [When] 알람 생성 메서드 호출
     * [Then] MessageQueuePort의 publishAlarmJob이 호출되었는지 검증
     */
    @Test
    fun `createAlarm - should publish job to message queue`() {
        // Given
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = "test-uuid-123",
            senderId = "sender-1",
            receiverId = "receiver-1",
            name = "Test Alarm",
            payload = "{}"
        )
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(alarmRepositoryPort.save(any(Alarm::class.java) ?: dummyAlarm)).thenAnswer { it.getArgument(0) }

        // When
        alarmService.createAlarm(command)

        // Then
        verify(messageQueuePort).publishAlarmJob(any(Alarm::class.java) ?: dummyAlarm)
    }

    /**
     * [시나리오] Redis에 이미 멱등키가 등록되어 있어 중복 생성 요청인 경우 예외 발생을 검증합니다.
     * [Given] 중복 멱등키 인입 시 Redis setIfAbsent가 false를 반환하도록 설정
     * [When] 알람 생성 메서드 호출
     * [Then] ResponseStatusException(CONFLICT) 예외가 발생하는지 검증
     */
    @Test
    fun `createAlarm - should throw CONFLICT exception on duplicate request`() {
        // Given
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = "test-uuid-123",
            senderId = "sender-1",
            receiverId = "receiver-1",
            name = "Test Alarm",
            payload = "{}"
        )
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(false)

        // When & Then
        assertThrows<ResponseStatusException> {
            alarmService.createAlarm(command)
        }
        verify(alarmRepositoryPort, never()).save(any(Alarm::class.java) ?: dummyAlarm)
    }

    /**
     * [시나리오] 이미 발송 완료(SENT)인 알람은 재발송(Trigger)하지 않고 건너뛰는지 검증합니다.
     * [Given] 상태가 SENT인 알람 준비 및 findById 설정
     * [When] 알람 발송 trigger 호출
     * [Then] 외부 전송 API 호출(sendNotification)이 수행되지 않는지 검증
     */
    @Test
    fun `triggerAlarm - should skip if status is already SENT`() {
        // Given
        val alarm = Alarm.create(
            id = "alarm-123",
            senderId = "sender-1",
            receiverId = "receiver-1",
            name = "Already Sent Alarm",
            payload = "{}"
        )
        alarm.sent()
        `when`(alarmRepositoryPort.findById("alarm-123")).thenReturn(alarm)

        // When
        alarmService.triggerAlarm("alarm-123")

        // Then
        verify(notificationPort, never()).sendNotification(anyList())
    }

    /**
     * [시나리오] 알람 발송 성공 시 DB에 완료(SENT) 상태로 갱신 및 로그 기록을 검증합니다.
     * [Given] PENDING 상태의 알람 준비 및 외부 전송 API 성공 설정
     * [When] 알람 발송 trigger 호출
     * [Then] DB에 알람 상태가 SENT로 업데이트 저장되고, SUCCESS 실행 로그가 남는지 검증
     */
    @Test
    fun `triggerAlarm - should update status to SENT and record SUCCESS log on success`() {
        // Given
        val alarm = Alarm.create(
            id = "alarm-123",
            senderId = "sender-1",
            receiverId = "receiver-1",
            name = "Pending Alarm",
            payload = "{}"
        )
        `when`(alarmRepositoryPort.findById("alarm-123")).thenReturn(alarm)
        `when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.empty())
        
        val alarmCaptor = ArgumentCaptor.forClass(Alarm::class.java)
        `when`(alarmRepositoryPort.save(alarmCaptor.capture() ?: dummyAlarm)).thenAnswer { it.getArgument(0) }

        // When
        alarmService.triggerAlarm("alarm-123")

        // Then
        val savedAlarm = alarmCaptor.value
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.SENT, savedAlarm.status) // H2 DB에 SENT(Done) 상태로 업데이트되는지 검증
        
        verify(alarmExecutionLogRepositoryPort).save(any(AlarmExecutionLog::class.java) ?: dummyLog)
    }

    /**
     * [시나리오] 알람 발송 실패 시 DB에 실패(FAILED) 상태 갱신 및 에러 로그 기록을 검증합니다.
     * [Given] PENDING 상태의 알람 준비 및 외부 전송 API 오류 발생 설정
     * [When] 알람 발송 trigger 호출
     * [Then] DB에 알람 상태가 FAILED로 업데이트 저장되고, FAILED 실행 로그가 남는지 검증
     */
    @Test
    fun `triggerAlarm - should update status to FAILED and record FAILED log on failure`() {
        // Given
        val alarm = Alarm.create(
            id = "alarm-123",
            senderId = "sender-1",
            receiverId = "receiver-1",
            name = "Pending Alarm",
            payload = "{}"
        )
        `when`(alarmRepositoryPort.findById("alarm-123")).thenReturn(alarm)
        `when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.error(RuntimeException("Connection timeout")))
        
        val alarmCaptor = ArgumentCaptor.forClass(Alarm::class.java)
        `when`(alarmRepositoryPort.save(alarmCaptor.capture() ?: dummyAlarm)).thenAnswer { it.getArgument(0) }

        // When & Then
        assertThrows<RuntimeException> {
            alarmService.triggerAlarm("alarm-123")
        }

        val savedAlarm = alarmCaptor.value
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.FAILED, savedAlarm.status) // H2 DB에 FAILED 상태로 업데이트되는지 검증
        
        verify(alarmExecutionLogRepositoryPort).save(any(AlarmExecutionLog::class.java) ?: dummyLog)
    }
}
