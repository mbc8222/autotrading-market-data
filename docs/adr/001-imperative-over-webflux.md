# ADR-001: WebFlux 대신 명령형 Spring Boot + Java 21 가상 스레드

- 상태: 채택 (2026-06-11)
- 결정자: 사용자 (Claude 분석 보조)

## 맥락

autotrading-market-data는 MSA 4-서비스 중 ① 마켓데이터 수신 담당이다 (바이낸스 선물 WS 수신 + REST 폴링 → DB/Redis 적재·발행).
최초 설계(2026-06-10)는 "I/O bound·다중 WS" 논거로 Spring WebFlux + R2DBC를 선택했다.

## 결정

WebFlux + R2DBC를 폐기하고 **명령형 Spring Web(MVC) + Java 21 가상 스레드(`spring.threads.virtual.enabled=true`) + JDBC(HikariCP)** 로 전환한다.
HTTP 클라이언트는 `RestClient`(Boot 3.2+, 명령형)를 사용한다.

## 근거

1. **실동시성이 한 자릿수다.** 바이낸스 WS는 combined stream으로 2~5 연결, REST 폴링은 동시 요청 몇 개,
   서버 inbound는 actuator뿐(서비스 간 통신은 Redis). reactive가 이기는 조건(수천+ 동시 연결)이 성립하지 않는다.
2. **Loom 이후 I/O bound 논거가 약화됐다.** Java 21 가상 스레드는 블로킹 스타일 코드 그대로 수만 동시성을 처리한다.
   "I/O bound면 reactive"는 가상 스레드 이전의 공식이다.
3. **backpressure는 거래소 푸시 스트림에 무력하다.** 거래소에 늦춰달라고 할 수 없으므로 버퍼링/드롭 전략은
   어느 스택이든 직접 구현해야 한다. Reactor의 핵심 이점이 이 워크로드에선 발동하지 않는다.
4. **reactive의 비용은 실재한다.** 디버깅(스택트레이스 단절), 이벤트 루프 블로킹 함정, 러닝커브.
   성능 이득 0에 비용만 지불하는 구조였다.
5. **모놀리스에서 검증된 수집 패턴**(행 기반 resume, 폴링 예외 가드, ON CONFLICT 멱등 적재)을 그대로 이식할 수 있다.

## 다시 꺼낼 조건

- ④ API 서비스의 SSE/WS 대시보드 fan-out — 다수 클라이언트로의 push는 reactive가 실제로 적합한 워크로드.
- 수신 심볼/스트림이 수백 개 규모로 늘어나는 경우 (현재 4 심볼).

## 주의 (가상 스레드, JDK 21 한정)

- `synchronized` 블록 안 블로킹 → 캐리어 스레드 pinning (JDK 24에서 해소). 직접 쓰는 코드는 `ReentrantLock` 선호.
- PostgreSQL JDBC 드라이버 최신 유지.
- 동시성 병목은 스레드가 아니라 커넥션 풀로 이동한다 — Hikari 풀 사이즈가 실질 상한.
