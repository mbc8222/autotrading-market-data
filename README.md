# autotrading-market-data

AutoTrading MSA(이벤트드리븐)의 **① 마켓데이터 수집 서비스**.

바이낸스 USDⓈ-M 선물(fapi)의 시세·파생·실시간 스트림을 수집해 **PostgreSQL**(raw 히스토리)에 적재하고
**Redis**(서비스 간 채널)로 발행한다. 분석·주문·조회는 이 서비스의 소관이 아니다.

수집 대상 심볼은 `btcusdt` · `ethusdt` · `solusdt` · `xrpusdt` (소문자 페어가 전 계층 표준 키).
**바이낸스 시장데이터는 공개 엔드포인트라 API 키가 필요 없다.**

## 스택

| 영역 | 기술 | 비고 |
|---|---|---|
| 런타임 | Java 21 + **Spring Boot 4.1** + Gradle | 가상 스레드(`spring.threads.virtual.enabled=true`) |
| 웹 | **Spring MVC**(`starter-webmvc`) | **WebFlux 아님** — [ADR-001](docs/adr/001-imperative-over-webflux.md) |
| 외부 호출 | `RestClient`(`starter-restclient`) | 명령형. Boot 4에서 http client 설정이 별도 모듈로 분리됨 |
| 실시간 수신 | jakarta.websocket (Tomcat 클라이언트) | 바이낸스 fstream combined stream |
| DB 적재 | **spring-jdbc**(`JdbcClient` + `NamedParameterJdbcTemplate.batchUpdate`) + HikariCP | **R2DBC·MyBatis 아님** — [ADR-002](docs/adr/002-spring-jdbc-over-mybatis.md) |
| 스키마 관리 | Flyway (`starter-flyway`) | DDL은 Flyway로만. 적용된 V파일은 불변 |
| 이벤트 채널 | **Redis(Lettuce, 명령형)** | Streams 발행 + 핫상태 KV |
| 캐시 | Caffeine + Spring Cache | |
| 관측 | Actuator + Micrometer/Prometheus | `/actuator/health` · `/actuator/prometheus` |
| 로깅 | Log4j2 (Logback 제외) | prod 프로파일에서 롤링 파일 appender |
| 테스트 | JUnit 5 + Testcontainers(PostgreSQL) | |

> **장애 격리는 Resilience4j를 쓰지 않는다.** boot3 전용 아티팩트가 Boot 4와 맞지 않아 제거했고,
> 재시도/백오프는 도메인 코드(`BinanceRestRetry`·`BinanceBanGuard`)가 직접 다룬다.
> 필요해지면 Spring Framework 7 내장 resilience(`@Retryable` 등)를 먼저 검토한다.

## 사전 요구 (이 저장소 밖)

이 서비스는 **PostgreSQL과 Redis가 이미 떠 있다고 가정한다.** 둘 다 이 compose 밖의 별도 컨테이너이며,
접속은 공유 외부망 `autotrading-net`에서 **컨테이너명 직결**(`postgres:5432` · `redis:6379`)로 한다.

| 필요한 것 | 값 |
|---|---|
| PostgreSQL | database `marketdata`, 스키마 `marketdata`, 소유 계정 하나 |
| Redis | 인증 설정은 환경에 따름 |
| Docker 망 | `docker network create autotrading-net` 후 postgres·redis를 합류 |

망이 없으면 `docker compose up`이 `network autotrading-net not found`로 즉시 실패한다.

## 빌드 / 실행

```bash
# 개발(local) — 호스트에서 직접 실행. 기본 프로파일 = local
./gradlew.bat compileJava     # 컴파일
./gradlew.bat bootRun         # 실행 (PostgreSQL·Redis 필요)
./gradlew.bat test            # 테스트
```

```bash
# 운영(prod) — Docker
cp deploy/market-data.env.example deploy/market-data.env   # 값 채우기
docker compose up -d --build                               # 빌드+기동, restart unless-stopped
docker compose logs -f                                     # 로그 (파일 로그는 호스트 ./logs 에도 보존)
```

이미지는 멀티스테이지 Dockerfile이 **컨테이너 안에서 `./gradlew bootJar`** 를 돌려 만든다 —
IDE의 "Build Artifacts"로 만든 jar(thin jar·MANIFEST 중복)를 쓰지 않기 위한 것이다. 런타임은 JRE 21 비루트.

기동 확인: `GET :8080/actuator/health` 의 `db`·`redis` 컴포넌트가 UP.

## 설정 / 시크릿

원칙: **JAR에는 환경 무관 공통 설정만. 접속정보는 JAR 밖에서 환경별로 주입하고 git에 올리지 않는다.**

| 환경 | 파일 | 추적 |
|---|---|---|
| 공통 | `src/main/resources/application.properties` — 포트·수집 플래그·타임아웃. **접속정보 없음** | 커밋됨 |
| 개발(local) | **프로젝트 루트**의 `application-local.properties` | gitignored (`.example`만 커밋) |
| 운영(prod) | `deploy/market-data.env` → compose `env_file` → 환경변수(relaxed binding) | gitignored (`.example`만 커밋) |

`application-local.properties`는 반드시 **루트**에 둔다. `src/main/resources`에 두면 JAR에 패키징되어 유출된다.

⚠️ **이 저장소는 public이다.** 시크릿·내부 전략 정보를 커밋하지 않는다.

## 수집·발행

| 자료 | 소스 | 방식 | 적재 / 발행 |
|---|---|---|---|
| 캔들 4 TF | `/fapi/v1/klines` | REST 백필 + 30s 폴링 | `binance_klines` · 닫힌봉 → Stream `market:kline` |
| 파생 6종 | `/futures/data` | REST 백필(30d) + 5m 폴링, bounded window | `futures_*` 6표 |
| 펀딩비 | `/fapi/v1/fundingRate` | REST 전체 히스토리 | `futures_funding_rate` |
| 마크/인덱스/예상펀딩 | WS `@markPrice@1s` | 실시간 | KV `market:mark-price:{symbol}` |
| 강제 청산 | WS `@forceOrder` | 실시간, 2s flush | `binance_liquidations` · Stream `market:liquidation` |
| 원시 체결 | WS `@aggTrade` + REST 갭 보정 | 버퍼 → 배치 적재, 60s 갭 sweep | `agg_trade`(일별 파티션) · Stream `market:aggTrade` |
| 풀북 원시 차분 | WS `@depth@100ms` + REST 스냅샷 | 로컬 북 동기화(공식 규칙) | `depth_diff`(일별 파티션) · 상위 20단 요약 → KV `market:orderbook:{symbol}` |

Redis 채널 규칙: 시장이벤트는 **Stream**(소비자가 멱등키로 중복 무시), 최신가는 **KV**(유실 OK).
pub/sub 병행 발행은 하지 않는다.

### ⚠️ 디스크 — 기동 전에 읽을 것

`agg_trade`·`depth_diff`는 **일별 파티션으로 무한히 쌓인다.** 이 서비스의 `PartitionMaintenance`는
파티션을 **만들기만 하고 지우지 않는다** — 삭제 주체는 별도 서비스(`autotrading-cold-export`)이며
Parquet 이관·검증에 성공한 파티션만 DROP한다.

**cold-export 없이 기본 설정으로 돌리면 디스크가 며칠 만에 찬다.** 실측(4심볼):

| 플래그 | 일 증가량 |
|---|---|
| `collect.depth.enabled=true` | **일 11~24GB** (`depth_diff`) |
| `collect.raw.enabled=true` | 일 ~1.3GB (`agg_trade`) |

이관 파이프라인 없이 단독으로 쓸 계획이면 `collect.depth.enabled=false`로 두는 것을 권한다.
그러면 풀북 스트림을 구독하지 않고 상위 20단 KV도 발행되지 않는다.

주요 플래그는 전부 `src/main/resources/application.properties`에 있다
(`collect.symbols` · `collect.kline.*` · `collect.futures.*` · `collect.ws.enabled` · `collect.raw.enabled` · `collect.depth.enabled`).

## 운영 메모

- **418(IP ban)** 은 `BinanceBanGuard`가 전 REST 수집기를 10분 일괄 중지한다. 계속 두드리면 ban이 연장된다.
- **429** 는 `BinanceRestRetry`가 백오프한다. 레이트리밋은 IP 단위다.
- 첫 기동은 캔들 7일 + 파생 30일 + 펀딩 전체 히스토리를 REST로 백필하므로 초반에 요청이 몰린다.
- `@Scheduled`는 예외 하나로 영구 정지될 수 있어 심볼×인터벌 단위로 try/catch를 감싼다.
- 수집 재개는 **행 기반**(DB max(ts)/hwm부터)이라 중단돼도 다음 tick이 같은 지점에서 회수한다.

## 문서

| 문서 | 내용 |
|---|---|
| [CLAUDE.md](CLAUDE.md) | 패키지 구조, 수집 패턴, 작업 원칙, 배포·설정 상세 |
| [docs/adr/001](docs/adr/001-imperative-over-webflux.md) | WebFlux 대신 명령형 + 가상 스레드를 택한 이유 |
| [docs/adr/002](docs/adr/002-spring-jdbc-over-mybatis.md) | DB 접근에 spring-jdbc 표준 도구를 택한 이유 |
