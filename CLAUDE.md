# autotrading-market-data — Claude Code 프로젝트 가이드

## 개요
AutoTrading MSA(4-서비스 이벤트드리븐)의 **① 마켓데이터 수신 서비스**.
바이낸스 선물(fapi) 데이터를 수집해 PostgreSQL(raw 히스토리)과 Redis(서비스 간 채널)에 적재·발행한다.
모놀리스(`%USERPROFILE%\IdeaProjects\AutoTrading`)와 별개 프로젝트 — 작업 전 어느 쪽이 대상인지 확인.

**스택**: **Spring Boot 4.1.x** (2026-06-11 3.5→4.1 전환), Java 21 + **가상 스레드**(`spring.threads.virtual.enabled=true`), 명령형 Web(MVC),
spring-jdbc(HikariCP) + Flyway, Redis(Lettuce), Log4j2, RestClient.
**WebFlux/R2DBC 아님** — docs/adr/001 참고. DB 접근은 MyBatis 아님 — docs/adr/002 참고.

**Boot 4 주의점** (3.x 자료와 다른 부분):
- starter 개명: `starter-web`→`starter-webmvc`, RestClient 설정 클래스는 `starter-restclient` 모듈에 분리됨
- `ClientHttpRequestFactorySettings`→**`HttpClientSettings`** 개명 (BinanceRestClientConfig 참고)
- resilience는 외부 라이브러리 대신 **Spring Framework 7 내장**(`@Retryable`·`@ConcurrencyLimit`·`@EnableResilientMethods`) 사용 방침 (resilience4j 제거됨)
- **이 repo는 public** — 시크릿·내부 전략 정보 절대 커밋 금지 (시크릿은 gitignored local 파일만)

## 빌드 / 실행
```bash
# 개발(local) — IntelliJ 또는 CLI, 호스트에서 직접 실행
./gradlew.bat compileJava     # 컴파일
./gradlew.bat bootRun         # 실행 (PostgreSQL01·redis Docker 컨테이너 필요)
./gradlew.bat test            # 테스트
```
```bash
# 운영(prod) — Docker 컨테이너 (2026-06-13 채택, deploy 상세는 아래 "배포" 절)
docker compose up -d --build  # 빌드(컨테이너 내 ./gradlew bootJar)+기동, 자동재시작
docker compose logs -f        # 로그(파일 로그는 호스트 ./logs 에도 보존)
```
인프라: Docker 컨테이너 `PostgreSQL01`(5432, DB/계정/스키마 = marketdata, 계정이 DB owner), `redis`(6379).
health: `GET /actuator/health` — db·redis 컴포넌트 UP 확인.

## 배포 (Docker, 2026-06-13 — WSL+systemd 에서 전환)
- **`Dockerfile`**(멀티스테이지): 빌드 스테이지에서 컨테이너 내 `./gradlew bootJar` → IntelliJ "Build Artifacts" 함정(thin jar/MANIFEST 중복) 원천 차단·재현성. 런타임=JRE 21, 비루트(appuser).
- **`docker-compose.yml`**: `restart: unless-stopped`(★죽으면 자동재시작+데몬 기동 시 자동기동=무중단 핵심), `SPRING_PROFILES_ACTIVE=prod`, `env_file: deploy/market-data.env`(접속정보), `8080:8080` 발행, `./logs:/var/log/autotrading` 볼륨(파일 로그 보존).
- **인프라(PostgreSQL01·redis)는 이 compose 밖 별도 컨테이너** — 건드리지 않음(재시작=수집 갭 회피). 접속은 **공유 외부망 `autotrading-net`으로 컨테이너명 직결**(`redis:6379`/`postgres:5432`, 2026-06-14 전환). 이전 `host.docker.internal:5432/6379` 우회는 간헐 드롭 원인이라 제거(`extra_hosts: host-gateway`는 폴백). redis/postgres 합류 보장=`%USERPROFILE%\docker\ensure-shared-net.sh`(재생성 후 재실행).
- 모니터링(prometheus·grafana)도 별도 compose(`%USERPROFILE%\docker`). prometheus 는 `host.docker.internal:8080` 으로 스크랩(⚠️기존 prometheus.yml 타깃 포트 일치 확인 필요).
- 절차: `cp deploy/market-data.env.example deploy/market-data.env` → 값 채움 → `docker compose up -d --build`.

## 설정 / 시크릿 (프로파일 분리, 2026-06-13 정리)
원칙: **JAR = 환경 무관 공통 설정만. 접속정보/시크릿은 JAR 밖에서 환경별 주입, git 추적 안 함.**

- `src/main/resources/application.properties` — **공통 설정만** (포트·가상스레드·collect.*·binance 타임아웃 등).
  접속정보(datasource url/계정/비번)·flyway 스키마·redis host 는 **없음**. 기본 프로파일 `spring.profiles.active=local`.
- **개발(local)**: 프로젝트 루트 `application-local.properties` (gitignored) — 실제 접속정보.
  ※ `src/main/resources` 가 아니라 **루트**에 둔다 (resources 면 JAR 에 패키징되어 유출). IntelliJ 실행 시 작업 디렉토리(루트)에서 자동 로드.
  템플릿: `application-local.properties.example` (커밋됨, 값 비움).
- **운영(prod, Docker)**: `application-prod.properties` 파일 없음. compose `env_file: deploy/market-data.env`
  의 환경변수(`SPRING_DATASOURCE_*` 등, relaxed binding)로 주입. `SPRING_PROFILES_ACTIVE=prod` 로 기본 local override.
  접속 host 는 **공유망 컨테이너명**(`SPRING_DATA_REDIS_HOST=redis`, `SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/...`, 2026-06-14 전환). 템플릿: `deploy/market-data.env.example`.
- 바이낸스 시장데이터는 **공개 엔드포인트라 API 키 불필요** (api.key/secret 설정 없음).

## 패키지 구조 (`com.autotrading.autotradingmarketdata`)
```
binance/   BinanceProperties · BinanceRestClientConfig · BinanceFuturesRestApi(fapi 전체)
           BinanceKline · FuturesRows(파생·raw·청산 row 묶음) · BinanceRestException(429/418 구분)
           BinanceBanGuard(418 전 수집기 일괄 10분 중지) · BinanceRestRetry(429 backoff 공통)
collect/   CollectProperties(공유 심볼 목록)
kline/     KlineCollectProperties · KlineCollector · KlineRepository · KlineCollectionScheduler
futures/   FuturesDataCollector(파생 7종, bounded window+hwm) · FuturesRepository
ws/        BinanceWebSocket(베이스: reconnect/circuit/라우팅) · MarkPrice·Depth·AggTrade·ForceOrder
           WebSocketStarter(ApplicationReady 일괄 연결)
raw/       BatchBuffer·AggTradeBuffer → RawPersister(전용 워커) · AggTradeRepository(갭 SQL)
           PartitionMaintenance(일별 파티션·90일 DROP) · AggTradeReconciler(60s 갭 보정)
           LiquidationRepository
publish/   MarketDataPublisher(Redis Stream + KV 발행)
resources/db/migration/  V1=binance_klines · V2=futures 7종+청산+agg_trade(파티션 부모)
docs/adr/  아키텍처 결정 기록
```

## 수집 데이터 (모놀리스 동등 + Redis 채널)

| 분류 | 소스 | 방식 | 저장/발행 |
|---|---|---|---|
| 캔들 4 TF | /fapi/v1/klines | REST 백필(7d)+30s 폴링 | `binance_klines` + 닫힌봉→Stream `market:kline`, 1m 최신가→KV |
| 파생 6종 | /futures/data (5m) | REST 백필(30d)+5m 폴링, bounded window 450×5m | `futures_*` 6테이블 |
| 펀딩비 | /fapi/v1/fundingRate | REST 전체 히스토리(2024-01~) | `futures_funding_rate` |
| 마크/인덱스/예상펀딩 | WS @markPrice@1s (/market) | 실시간 | KV `market:mark-price:{s}` (hash) |
| 호가 top20 요약 | WS @depth20@500ms (/public) | 실시간 | KV `market:orderbook:{s}` — raw 미적재(기존 결정) |
| 강제 청산 | WS @forceOrder (/market) | 실시간, 2s flush+재큐잉 | `binance_liquidations` + Stream `market:liquidation` |
| 원시 체결 | WS @aggTrade (/market) + REST 갭 보정 | 버퍼→배치 적재, 60s 갭 sweep | `agg_trade` (일별 파티션, 90일) + **Stream `market:aggTrade`**(분석 CVD/매물대 소비자용, 고빈도→2k건마다 트림, `publish.aggtrade.enabled`) |

> 418(IP ban)은 `BinanceBanGuard`로 모든 REST 수집기 10분 일괄 중지 (계속 두드리면 ban 연장).
> aggTrade는 유효성 필터(agg_id·price·qty·T 양수) — 쓰레기 행의 watermark 오염 방지(모놀리스 사고 사례).

## 관측 (observability 1단계, 2026-06-12)
- **메트릭** (`/actuator/prometheus`): WS 5종(`ws_connected`·`ws_circuit_open`·`ws_messages`·`ws_reconnects`·`ws_parse_errors`·`ws_last_message_age_seconds`, socket 태그) — BinanceWebSocket 베이스에서 계측.
  raw 5종(`raw_buffer_size/offered/dropped`, `raw_persister_written/lost`) — `RawMonitor`가 등록.
  속도(in/out rate)는 조회 측 `rate()`로 — 앱은 누적값만 노출.
- **로그**: `RawMonitor`가 15s 구조화 한 줄(`[RAW-MON] agg[size hwm(pct) in/s out/s drop] lost`) + 점유 50%·drop/lost 증가 시 WARN/ERROR. 핵심 감시 대상 = "조용한 유실"(버퍼 점유 상승이 선행지표).
- **로그 출력** (2026-06-13, `src/main/resources/log4j2-spring.xml`): Log4j2(logback 제외). 콘솔은 전 프로파일 공통 → 운영(Docker)에선 `docker compose logs`/로그 드라이버가 캡처. **prod 전용 롤링 파일**(`<SpringProfile name="prod">` arbiter): `/var/log/autotrading/market-data.log`, 일별+50MB 트리거·gz 압축·14일 경과분 자동삭제. Docker 에선 compose 볼륨 `./logs:/var/log/autotrading`로 호스트에 보존(컨테이너 재생성에도 유지). 개발(local)은 콘솔만(루트에 logs/ 안 생김).
- 미결: Discord 경보(능동 알림), Prometheus/Grafana 컨테이너(docker-compose 단계), 서비스 간 trace ID 전파(주문 경로 생길 때).

## 작업 원칙 (모놀리스에서 이식·계승)
- **행 기반 resume**: DB max(open_time)부터(inclusive) 재개. forming 봉은 `ON CONFLICT` upsert로 갱신.
- **백필=폴링 동일 로직**: 중단돼도 다음 tick이 같은 지점부터 회수.
- **폴링 예외 가드**: `@Scheduled`는 예외 1회로 정지 가능 — 심볼×인터벌 단위 try/catch. 429/418은 사이클 중단(=백오프).
- **심볼 키는 소문자 페어**("btcusdt") — URL에서만 대문자.
- **Flyway 마이그레이션은 불변** — 적용된 V파일 수정 금지, 변경은 V(n+1)로.
- **DDL은 Flyway로만** — 코드/`spring.sql.init`에서 CREATE TABLE 금지.
- `.idea/` 수정 금지, 프로젝트 루트에 임시 파일 생성 금지.

## Redis 채널 규칙 (아키텍처 2026-06-10 결정)
- 시장이벤트(닫힌 캔들) → **Stream** `market:kline` (1회 기록, MAXLEN ~100k 트림).
  소비자는 (symbol, interval, openTime) 멱등키로 중복 무시.
- 최신가 → **KV** `market:last-price:{symbol}` (유실 OK).
- 이중 publish(pub/sub 병행) 금지. 포지션 진실원천은 Binance API, raw 히스토리는 DB.

## Config 키
```properties
binance.rest-base-url=https://fapi.binance.com   # 선물 fapi (현물 api.binance.com 아님!)
binance.connect-timeout=5s / read-timeout=15s    # 폴링 주기보다 짧게
collect.kline.enabled / symbols / intervals / backfill-days / page-limit / fixed-delay
spring.datasource.* (?currentSchema=marketdata)  # 환경별 주입(개발=local 파일 / 운영=env var). application.properties 엔 없음
spring.flyway.default-schema / schemas           # url 없음 — 메인 DataSource 공유. 환경별 주입
spring.data.redis.host / port                    # 환경별 주입
```

## 다음 단계 / 미결
- **2026-06-14: 레디스/DB 접속 공유망 직결 전환 완료** — host.docker.internal 우회(analyzer가 06-14 03:57 UTC 드롭) 제거, 공유 외부망 `autotrading-net`으로 `redis:6379`/`postgres:5432` 컨테이너명 직결. `deploy/market-data.env`(redis host=`redis`·datasource host=`postgres`)·compose `networks:[default,autotrading-net]` 추가→재생성(~2초 갭은 AggTradeReconciler 복구창 1410분이 백필). 검증: WS 4종 연결·kline 수집 재개·RAW-MON drop0/lost0·공유망 직결. redis는 `restart=no`였어 `unless-stopped`로 보강함.
- **모놀리스 수집과 이중 가동 중** — 같은 데이터를 양쪽 DB(autotrading/marketdata)에 수집(특히 raw agg_trade 디스크 2배). 모놀리스 수집 중단 시점 결정 필요.
- kline 백필 깊이 7일은 임시값 — ② 분석 서비스의 데이터 소스 결정(6년 재백필 vs 모놀리스 DB 이관)과 묶어서 확정.
- ~~aggTrade의 Stream 발행은 분석 서비스 소비자 정의 후~~ → **완료(2026-06-13)**: analyzer FlowAnalyzer(CVD)·VolumeProfileAnalyzer 소비자 정의됨 → `MarketDataPublisher.publishAggTrade`로 `market:aggTrade` 발행(DB 적재 경로와 별개). ⚠️**운영 컨테이너 재배포 필요**(`docker compose up -d --build`)해야 발행 활성화.
- 재시도/서킷브레이커: Spring Framework 7 내장 `@Retryable`/`@EnableResilientMethods` 적용 검토.
- 주문 신호 모델 A/B는 보류 중.
