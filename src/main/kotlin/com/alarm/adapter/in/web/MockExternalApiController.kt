package com.alarm.adapter.`in`.web

import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * [파일명] MockExternalApiController.kt
 * [수정일자] 2026-06-11
 * [기능] 외부 알림 발송 API 서버 Mock 구현체
 * [관점] Inbound Adapter (Mock): MockApiNotificationAdapter 가 실제로 호출하는
 *        외부 API 엔드포인트를 로컬 환경에서 가상으로 제공합니다.
 *        요청이 들어오면 수신 건수 및 페이로드를 로그로 출력하고 즉시 200 OK 를 반환합니다.
 */
@RestController
@RequestMapping("/mock-external")
class MockExternalApiController {

    private val logger = LoggerFactory.getLogger(MockExternalApiController::class.java)

    @PostMapping("/notifications/batch")
    fun receiveBatch(@RequestBody items: List<Map<String, Any?>>): Map<String, Any> {
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info("[MOCK EXTERNAL API] 알림 배치 수신 — 총 {}건", items.size)
        items.forEachIndexed { index, item ->
            logger.info(
                "  [{}] alarmId={} | senderId={} | uuid={} | payload={}",
                index + 1,
                item["alarmId"],
                item["senderId"],
                item["uuid"],
                item["payload"]
            )
        }
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return mapOf("status" to "ok", "received" to items.size)
    }
}
