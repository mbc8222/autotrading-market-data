package com.autotrading.autotradingmarketdata.ws;

import com.autotrading.autotradingmarketdata.binance.FuturesRows.LiquidationEvent;
import com.autotrading.autotradingmarketdata.collect.CollectProperties;
import com.autotrading.autotradingmarketdata.publish.MarketDataPublisher;
import com.autotrading.autotradingmarketdata.raw.LiquidationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * 강제 청산 스트림({@code <symbol>@forceOrder}) — 이산·고신호 이벤트라 DB 적재 + Stream 발행 대상.
 * WS 수신 스레드를 I/O로 막지 않도록 enqueue만 하고, {@link #flush}가 {@value #FLUSH_MS}ms마다
 * batch로 DB 적재(멱등) 후 Stream 발행한다. 적재 실패 시 재큐잉(유실 방지).
 */
@Component
@ClientEndpoint
public class ForceOrderWebSocket extends BinanceWebSocket {

    private static final Logger log = LogManager.getLogger(ForceOrderWebSocket.class);
    private static final long FLUSH_MS = 2_000;

    private final CollectProperties collect;
    private final LiquidationRepository repository;
    private final MarketDataPublisher publisher;
    private final Queue<LiquidationEvent> pending = new ConcurrentLinkedQueue<>();

    public ForceOrderWebSocket(CollectProperties collect, LiquidationRepository repository,
                               MarketDataPublisher publisher, MeterRegistry registry) {
        super(registry);
        this.collect = collect;
        this.repository = repository;
        this.publisher = publisher;
    }

    @OnOpen
    public void onOpen(Session session) {
        opened(session);
    }

    @OnClose
    public void onClose(Session session) {
        closed(session);
    }

    @OnError
    public void onError(Session session, Throwable t) {
        errored(session, t);
    }

    @OnMessage
    public void onMessage(String message) {
        dispatch(message);
    }

    @Override
    protected String streamBaseUri() {
        return FUTURES_MARKET;   // forceOrder는 /market 라우팅
    }

    @Override
    protected List<String> streams() {
        return collect.symbols().stream().map(s -> s + "@forceOrder").toList();
    }

    @Override
    protected void onData(String stream, JsonNode d) {
        JsonNode o = d.get("o");
        if (o == null) {
            return;
        }
        String symbol = o.path("s").asString("").toLowerCase();
        if (symbol.isEmpty()) {
            return;
        }
        double price = o.path("p").asDouble(0);
        double qty = o.path("q").asDouble(0);
        pending.add(new LiquidationEvent(
                symbol,
                o.path("S").asString(""),     // side (BUY=숏 청산 / SELL=롱 청산)
                price,
                qty,
                price * qty,
                o.path("T").asLong(0)));
    }

    /** 대기 청산 이벤트를 batch 적재 + Stream 발행. 예외는 삼킨다(scheduled 영구 정지 방지). */
    @Scheduled(fixedDelay = FLUSH_MS)
    void flush() {
        List<LiquidationEvent> drained = new ArrayList<>();
        LiquidationEvent e;
        while ((e = pending.poll()) != null) {
            drained.add(e);
        }
        if (drained.isEmpty()) {
            return;
        }
        try {
            repository.batchInsert(drained);
        } catch (Exception ex) {
            pending.addAll(drained);   // 적재 실패 → 되돌려 다음 tick 재시도(유실 방지)
            log.warn("[LIQUIDATION] flush 실패 — {}건 재큐잉: {}", drained.size(), ex.toString());
            return;
        }
        try {
            publisher.publishLiquidations(drained);
        } catch (Exception ex) {
            // Stream 발행 실패는 재큐잉하지 않음(DB가 진실원천 — 재큐잉하면 DB 중복은 멱등이지만 Stream은 중복 발행됨).
            log.warn("[LIQUIDATION] Stream 발행 실패({}건): {}", drained.size(), ex.toString());
        }
    }
}
