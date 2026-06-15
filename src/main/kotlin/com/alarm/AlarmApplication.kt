package com.alarm

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * [파일명] AlarmApplication.kt
 * [수정일자] 2026-06-11
 * [기능] 애플리케이션 진입점 및 스프링 부트 구성 클래스
 * [관점] 시스템 부팅 계층: 전체 의존성 주입 컨테이너를 부트스트랩하고 실행 환경을 구성합니다.
 */
@SpringBootApplication
class AlarmApplication

fun main(args: Array<String>) {
	runApplication<AlarmApplication>(*args)
}
