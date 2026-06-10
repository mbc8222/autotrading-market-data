# autotrading-market-data

모놀리식 아키텍처에서 MSA 아키텍처로 전환하며 AutoTrading 모놀리스에서 **시장 데이터 수집 능력**을 분리한 첫 번째 마이크로서비스.

거래소(Binance) 시장 데이터를 non-blocking으로 수집·적재하는 역할을 담당한다.

## 서비스 구성

| 영역 | 기술 | 용도 |
|---|---|---|
| 런타임 | Java 21 + Spring Boot 3.5 + Gradle | 기본 플랫폼 |
| 웹/외부 호출 | Spring WebFlux | non-blocking REST API 및 WebClient 기반 외부 API 호출 |
| DB 적재 | Spring Data R2DBC + PostgreSQL | reactive non-blocking DB 적재 |
| 스키마 관리 | Flyway | DB schema migration 버전 관리 |
| 이벤트 채널/스토어 | Reactive Redis | Redis Streams 기반 시장 데이터 이벤트 발행, 핫상태 key-value 저장 |
| 인메모리 캐시 | Caffeine + Spring Cache | 반복 조회 데이터(exchange info 등) 캐싱 |
| 장애 격리 | Resilience4j | circuit breaker, retry, rate limiter |
| 관측 | Actuator + Prometheus | health check 및 metrics 노출 |
| 로깅 | Log4j2 (JSON layout) | structured logging |
| 테스트 | Testcontainers + Reactor Test | integration / reactive 테스트 |

## 시크릿 관리

- 시크릿은 `application-local.properties`(gitignored)로 분리한다.
- `application.properties`에는 환경변수 placeholder만 유지한다.

```properties
spring.r2dbc.username=${DATABASE_USERNAME:yourId}
spring.r2dbc.password=${DATABASE_PASSWORD:yourPw}
```
