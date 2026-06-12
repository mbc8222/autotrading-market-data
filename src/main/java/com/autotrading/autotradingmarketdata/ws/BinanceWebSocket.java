package com.autotrading.autotradingmarketdata.ws;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 바이낸스 선물 combined-stream WebSocket 공통 베이스 (모놀리스 검증 구현 이식).
 *
 * <p>서브클래스는 {@code @ClientEndpoint} + {@code @OnOpen/@OnClose/@OnError/@OnMessage}를 선언하고
 * 각 콜백에서 {@link #opened}/{@link #closed}/{@link #errored}/{@link #dispatch}로 위임한다
 * (jakarta.websocket은 어노테이션 콜백을 런타임 엔드포인트 클래스에서 스캔하므로 베이스로 hoist 불가).
 *
 * <p>reconnect — exponential backoff(1s→cap 60s) + ±30% jitter, 최대 {@value #MAX_RETRY}회 후
 * circuit OPEN(수동 재기동), 인증 실패(401/403) 즉시 OPEN, 연결 성공 시 카운터 리셋.
 *
 * <p>중복 방지 — ① {@code reconnectScheduled} CAS로 한 끊김에 onError+onClose가 둘 다 reconnect를
 * 예약하는 것을 막고, ② 콜백이 받은 {@link Session}이 현재 세션과 다르면(이미 교체된 옛 세션) 무시한다.
 *
 * <p>★바이낸스 선물 WS는 라우팅 엔드포인트(/public·/market)로 분리 — 라우팅 없는 연결은 public만 수신
 * (market 스트림은 에러 없이 0건). 서브클래스가 {@link #streamBaseUri}로 명시한다.
 */
public abstract class BinanceWebSocket {

    private final Logger log = LogManager.getLogger(getClass());

    private static final long BASE_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 60_000;
    private static final int MAX_RETRY = 30;

    protected static final String FUTURES_PUBLIC = "wss://fstream.binance.com/public/stream?streams=";
    protected static final String FUTURES_MARKET = "wss://fstream.binance.com/market/stream?streams=";

    private final ObjectMapper jsonMapper = JsonMapper.builder().build();
    private final AtomicInteger retry = new AtomicInteger(0);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private volatile boolean circuitOpen = false;
    private volatile Session session;

    private final Counter messages;
    private final Counter reconnects;
    private final Counter parseErrors;
    private final AtomicLong lastMessageEpochMs = new AtomicLong(0);

    protected BinanceWebSocket(MeterRegistry registry) {
        String socket = name();

        messages = Counter.builder("ws.messages")
                .tag("socket", socket)
                .description("수신한 combined-stream 메시지 수")
                .register(registry);

        reconnects = Counter.builder("ws.reconnects")
                .tag("socket", socket)
                .description("reconnect 예약 횟수")
                .register(registry);

        parseErrors = Counter.builder("ws.parse.errors")
                .tag("socket", socket)
                .description("메시지 파싱 실패 수")
                .register(registry);

        Gauge.builder("ws.connected", this, ws -> ws.isStarted() ? 1 : 0)
                .tag("socket", socket)
                .description("세션 연결 여부 (1=연결)")
                .register(registry);

        Gauge.builder("ws.circuit.open", this, ws -> ws.isCircuitOpen() ? 1 : 0)
                .tag("socket", socket)
                .description("circuit OPEN 여부 (1=수동 재기동 필요)")
                .register(registry);

        // 첫 메시지 전(0)이면 -1 — 기동 직후 epoch 기준 거대값으로 false alarm 나는 것 방지
        Gauge.builder("ws.last.message.age.seconds", lastMessageEpochMs,
                        v -> v.get() == 0 ? -1 : (System.currentTimeMillis() - v.get()) / 1000.0)
                .tag("socket", socket)
                .description("마지막 메시지 수신 후 경과 초 (-1=아직 수신 없음)")
                .register(registry);
    }

    /** 구독할 combined-stream 이름들 (예: "btcusdt@aggTrade"). 빈 목록이면 연결하지 않는다. */
    protected abstract List<String> streams();

    /** combined-stream 한 건의 data 노드 처리. */
    protected abstract void onData(String stream, JsonNode data);

    /** combined-stream 베이스 URI — /public(depth) vs /market(aggTrade·markPrice·forceOrder). */
    protected abstract String streamBaseUri();

    private String name() {
        return getClass().getSimpleName();
    }

    /** 비동기 연결 시작 (WebSocketStarter 또는 reconnect가 호출). */
    public void connect() {
        if (circuitOpen) {
            log.warn("[{}] circuit OPEN — 연결 거부(수동 재기동 필요)", name());
            return;
        }
        if (streams().isEmpty()) {
            log.warn("[{}] 구독 스트림 없음 — 연결 skip", name());
            return;
        }
        CompletableFuture.runAsync(() -> {
            closeQuietly();   // 이전(half-open 가능) 세션 정리 — 그 onClose는 stale로 무시됨
            try {
                WebSocketContainer container = ContainerProvider.getWebSocketContainer();
                // 큰 메시지가 기본 8KB 텍스트버퍼를 넘으면 컨테이너가 연결을 닫는다 → 상향.
                container.setDefaultMaxTextMessageBufferSize(8 * 1024 * 1024);
                container.connectToServer(this, URI.create(streamBaseUri() + String.join("/", streams())));
                retry.set(0);
                log.info("[{}] connected — {} streams", name(), streams().size());
            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage() : "";
                if (msg.contains("401") || msg.contains("403")) {
                    circuitOpen = true;
                    log.error("[{}] auth 실패({}) — circuit OPEN", name(), msg);
                    return;
                }
                log.error("[{}] 연결 실패: {}", name(), msg);
                reconnect();
            }
        });
    }

    private void reconnect() {
        if (circuitOpen) {
            return;
        }
        if (!reconnectScheduled.compareAndSet(false, true)) {
            return;   // 이미 reconnect 예약됨 — onError+onClose 중복/storm 방지
        }
        int n = retry.incrementAndGet();
        if (n > MAX_RETRY) {
            circuitOpen = true;
            reconnectScheduled.set(false);
            log.error("[{}] reconnect 한도 초과({}회) — circuit OPEN(수동 재기동 필요)", name(), MAX_RETRY);
            return;
        }
        reconnects.increment();
        long base = Math.min(BASE_BACKOFF_MS * (1L << Math.min(n - 1, 6)), MAX_BACKOFF_MS);
        double jitter = 0.7 + ThreadLocalRandom.current().nextDouble() * 0.6;
        long delay = (long) (base * jitter);
        log.info("[{}] reconnect #{} in {}ms", name(), n, delay);
        CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS).execute(() -> {
            reconnectScheduled.set(false);
            connect();
        });
    }

    /** 현재 세션을 null로 비운 뒤 close — 그 close의 onClose는 stale 세션이라 reconnect를 트리거하지 않는다. */
    private void closeQuietly() {
        Session old = session;
        session = null;
        if (old != null && old.isOpen()) {
            try {
                old.close();
            } catch (Exception e) {
                /* ignore */
            }
        }
    }

    // ── 서브클래스 @OnXxx 콜백이 위임하는 지점 ──

    protected final void opened(Session s) {
        this.session = s;
        this.retry.set(0);
        log.info("[{}] open", name());
    }

    protected final void closed(Session s) {
        if (s != session) {
            return;   // 이미 교체된 옛 세션의 close — 무시
        }
        session = null;
        log.info("[{}] close — reconnect", name());
        reconnect();
    }

    protected final void errored(Session s, Throwable t) {
        log.warn("[{}] error: {}", name(), t == null ? "?" : t.getMessage());
        // pre-open/handshake 실패는 connect()의 catch가 처리, 옛 세션 오류는 무시.
        if (s != null && s == session) {
            reconnect();
        }
    }

    /** combined-stream 메시지({@code {stream, data}}) 파싱 후 {@link #onData} 호출. 예외는 삼킨다(연결 유지). */
    protected final void dispatch(String message) {
        messages.increment();
        lastMessageEpochMs.set(System.currentTimeMillis());
        try {
            JsonNode root = jsonMapper.readTree(message);
            JsonNode data = root.get("data");
            if (data == null || data.isNull()) {
                return;
            }
            onData(root.path("stream").asString(""), data);
        } catch (Exception e) {
            parseErrors.increment();
            log.warn("[{}] 메시지 파싱 실패: {}", name(), e.getMessage());
        }
    }

    public boolean isStarted() {
        return session != null;
    }

    public boolean isCircuitOpen() {
        return circuitOpen;
    }

    /** 운영자 명령용 — circuit 강제 해제. */
    public void resetCircuit() {
        circuitOpen = false;
        retry.set(0);
        log.info("[{}] circuit reset", name());
    }
}
