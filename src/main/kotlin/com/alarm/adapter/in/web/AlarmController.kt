package com.alarm.adapter.`in`.web

import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.adapter.`in`.web.dto.AlarmResponse
import com.alarm.application.port.`in`.*
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import java.util.UUID

/**
 * [파일명] AlarmController.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 관리를 위한 REST API 엔드포인트 제공
 * [관점] Inbound Adapter: HTTP 요청을 처리하여 도메인의 알람 생성 유스케이스(CreateAlarmUseCase)로 커맨드를 매핑 및 전달하며 API 레벨 멱등키를 추출합니다.
 */
@RestController
@Tag(name = "Alarm API", description = "단발성 알람 관리를 위한 REST API 엔드포인트")
class AlarmController(
    private val createAlarmUseCase: CreateAlarmUseCase
) {

    @PostMapping("/notifications")
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "알람 신규 생성", description = "지정된 이름과 페이로드로 알람을 생성합니다.")
    fun createAlarm(
        @RequestHeader(value = "Idempotency-Key", required = false) idempotencyKey: String?,
        @RequestBody request: AlarmCreateRequest
    ): AlarmResponse {
        val parsedId: String
        val parsedSenderId: String
        
        if (!idempotencyKey.isNullOrBlank()) {
            parsedId = idempotencyKey
            if (idempotencyKey.length > 32) {
                parsedSenderId = idempotencyKey.substring(0, idempotencyKey.length - 32)
            } else {
                parsedSenderId = "12345"
            }
        } else {
            val uuidPart = UUID.randomUUID().toString().replace("-", "")
            parsedSenderId = "12345"
            parsedId = "${parsedSenderId}${uuidPart}"
        }

        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = parsedId,
            senderId = parsedSenderId,
            receiverId = request.receiverId ?: "defaultReceiver",
            name = request.name,
            payload = request.payload
        )
        val alarm = createAlarmUseCase.createAlarm(command)
        return AlarmResponse.fromDomain(alarm)
    }
}
