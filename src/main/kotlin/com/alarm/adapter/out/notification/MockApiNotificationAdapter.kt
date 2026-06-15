package com.alarm.adapter.out.notification

import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.application.port.out.NotificationPort
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration

/**
 * [파일명] MockApiNotificationAdapter.kt
 * [수정일자] 2026-06-11
 * [기능] 외부 알림 발송 API 서버 연동 및 재시도 제어
 * [관점] Outbound Adapter: NotificationPort 구체 클래스로서, WebClient를 사용하여 외부 API를 호출하며, 429 및 5xx 에러에 대한 지수 백오프 재시도 규칙을 통제합니다.
 */
@Component
class MockApiNotificationAdapter(
    @Value("\${mock.api.url:http://localhost:8081}") private val mockApiUrl: String
) : NotificationPort {

    private val webClient = WebClient.builder().baseUrl(mockApiUrl).build()

    override fun sendNotification(items: List<AlarmBatchItem>): Mono<Void> {
        if (items.isEmpty()) return Mono.empty()
        
        val body = items.map { item ->
            mapOf(
                "alarmId" to item.alarmId,
                "senderId" to item.senderId,
                "uuid" to item.uuidClean,
                "payload" to item.payload
            )
        }

        return webClient.post()
            .uri("/notifications/batch")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(body)
            .retrieve()
            .bodyToMono(Void::class.java)
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                .filter { throwable ->
                    throwable is WebClientResponseException &&
                    (throwable.statusCode.value() == 429 || throwable.statusCode.is5xxServerError)
                }
            )
    }
}
