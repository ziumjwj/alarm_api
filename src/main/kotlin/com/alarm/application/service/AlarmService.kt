package com.alarm.application.service

import com.alarm.application.port.`in`.*
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.MessageQueuePort
import com.alarm.application.port.out.NotificationPort
import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmExecutionLog
import com.alarm.domain.AlarmStatus
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.server.ResponseStatusException
import java.time.Duration

/**
 * [파일명] AlarmService.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 비즈니스 유스케이스 처리 구현체
 * [관점] Application Service: 인바운드 포트들을 구현하여 트랜잭션 경계를 설정하고, 멱등성 검증, 도메인 엔티티(Alarm) 상태 변환 제어, 아웃바운드 포트(Repository, MessageQueue, Notification) 조율 작업을 총괄합니다.
 */
@Service
@Transactional(readOnly = true)
class AlarmService(
    private val alarmRepositoryPort: AlarmRepositoryPort,
    private val alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort,
    private val notificationPort: NotificationPort,
    private val messageQueuePort: MessageQueuePort,
    private val redisTemplate: StringRedisTemplate
) : CreateAlarmUseCase, GetAlarmQuery, UpdateAlarmUseCase, DeleteAlarmUseCase, TriggerAlarmUseCase {

    @Transactional
    override fun createAlarm(command: CreateAlarmUseCase.CreateAlarmCommand): Alarm {
        // API 레벨 멱등성 체크
        val uuidClean = command.id.replace("-", "")
        val idemKey = "idem:${command.senderId}$uuidClean"
        val isFirstRequest = redisTemplate.opsForValue().setIfAbsent(
            idemKey,
            "PENDING",
            Duration.ofMinutes(5)
        )
        if (isFirstRequest != true) {
            throw ResponseStatusException(
                HttpStatus.CONFLICT,
                "Duplicate request based on Idempotency-Key"
            )
        }

        // 정적 팩토리 메서드를 사용하여 의도 명확화 및 검증 로직 강제화
        val alarm = Alarm.create(
            id = command.id,
            senderId = command.senderId,
            receiverId = command.receiverId,
            name = command.name,
            payload = command.payload
        )
        val savedAlarm = alarmRepositoryPort.save(alarm)
        messageQueuePort.publishAlarmJob(savedAlarm)
        return savedAlarm
    }

    override fun getAlarm(id: String): Alarm {
        return alarmRepositoryPort.findById(id)
            ?: throw NoSuchElementException("알람을 찾을 수 없습니다: $id")
    }

    override fun listAlarms(pageable: Pageable): Page<Alarm> {
        return alarmRepositoryPort.findAll(pageable)
    }

    @Transactional
    override fun updateAlarm(command: UpdateAlarmUseCase.UpdateAlarmCommand): Alarm {
        val alarm = alarmRepositoryPort.findById(command.id)
            ?: throw NoSuchElementException("알람을 찾을 수 없습니다: ${command.id}")

        // 도메인 내부 메서드에 행위와 상태 수정 위임
        alarm.update(
            name = command.name,
            payload = command.payload
        )
        command.status?.let {
            when (it) {
                AlarmStatus.PENDING -> alarm.markPending()
                AlarmStatus.SENT -> alarm.sent()
                AlarmStatus.FAILED -> alarm.fail()
            }
        }
        return alarmRepositoryPort.save(alarm)
    }

    @Transactional
    override fun deleteAlarm(id: String) {
        val alarm = alarmRepositoryPort.findById(id)
            ?: throw NoSuchElementException("알람을 찾을 수 없습니다: $id")
        alarmRepositoryPort.delete(alarm)
    }

    @Transactional
    override fun triggerAlarm(id: String) {
        val alarm = alarmRepositoryPort.findById(id)
            ?: throw NoSuchElementException("알람을 찾을 수 없습니다: $id")

        if (alarm.status == AlarmStatus.SENT) {
            // Idempotency: skip if already sent
            return
        }
        try {
            // 비즈니스 행위: 알람 발생 상태 변경
            alarm.sent()
            alarmRepositoryPort.save(alarm)

            // 외부 API 호출 전송
            val batchItem = AlarmBatchItem(
                alarmId = alarm.id,
                senderId = alarm.senderId,
                uuidClean = alarm.id.replace("-", ""),
                payload = alarm.payload
            )
            notificationPort.sendNotification(listOf(batchItem)).block()

            // 실행 이력 기록 (성공)
            val log = AlarmExecutionLog.record(alarm.id, "SUCCESS")
            alarmExecutionLogRepositoryPort.save(log)
        } catch (e: Exception) {
            alarm.fail()
            alarmRepositoryPort.save(alarm)

            // 실행 이력 기록 (실패)
            val log = AlarmExecutionLog.record(alarm.id, "FAILED", e.message)
            alarmExecutionLogRepositoryPort.save(log)
            throw e
        }
    }
}
