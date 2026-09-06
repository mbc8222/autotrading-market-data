package com.autotrading.autotradingmarketdata.ws;

import com.autotrading.autotradingmarketdata.binance.FuturesRows.AggTradeRow;
import com.autotrading.autotradingmarketdata.collect.CollectProperties;
import com.autotrading.autotradingmarketdata.publish.MarketDataPublisher;
import com.autotrading.autotradingmarketdata.raw.AggTradeBuffer;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * aggTrade 스트림({@code <symbol>@aggTrade}) — 원시 집계체결 수신.
 * {@code collect.raw.enabled=true}면 {@link AggTradeBuffer}에 비차단 enqueue → 전용 워커가 배치 적재.
 * CVD 등 파생 계산은 ② 분석 서비스 소관 — 여기선 raw 적재만(Stream 발행도 소비자 정의 후).
 */
@Component
@ClientEndpoint
public class AggTradeWebSocket extends BinanceWebSocket {

    private final CollectProperties collect;
    private final AggTradeBuffer rawBuffer;
    private final boolean rawEnabled;
    private final MarketDataPublisher publisher;
    private final boolean publishEnabled;

    public AggTradeWebSocket(CollectProperties collect, AggTradeBuffer rawBuffer,
                             @Value("${collect.raw.enabled:false}") boolean rawEnabled,
                             MarketDataPublisher publisher,
                             @Value("${publish.aggtrade.enabled:true}") boolean publishEnabled,
                             MeterRegistry registry) {
        super(registry);
        this.collect = collect;
        this.rawBuffer = rawBuffer;
        this.rawEnabled = rawEnabled;
        this.publisher = publisher;
        this.publishEnabled = publishEnabled;
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
        return FUTURES_MARKET;   // aggTrade는 /market 라우팅
    }

    @Override
    protected List<String> streams() {
        if (!rawEnabled) {
            return List.of();   // raw 미수집이면 구독할 이유 없음(소비자 없는 수집 금지)
        }
        return collect.symbols().stream().map(s -> s + "@aggTrade").toList();
    }

    @Override
    protected void onData(String stream, JsonNode d) {
        String symbol = d.path("s").asString("").toLowerCase();
        double price = d.path("p").asDouble(0);
        double qty = d.path("q").asDouble(0);
        long aggId = d.path("a").asLong(0);
        long tradeTime = d.path("T").asLong(0);
        // 유효성: 정상 aggTrade는 agg_id·price·qty·T 모두 양수. 비정상(파싱 0/결측)은 skip.
        // (과거 쓰레기 행이 적재돼 갭 보정 watermark 초기화까지 오염시킨 사례 → 원천 차단.)
        if (symbol.isEmpty() || aggId <= 0 || price <= 0 || qty <= 0 || tradeTime <= 0) {
            return;
        }
        boolean buyerMaker = d.path("m").asBoolean(false);
        rawBuffer.offer(new AggTradeRow(
                symbol, aggId, price, qty,
                d.path("f").asLong(0),
                d.path("l").asLong(0),
                tradeTime,
                buyerMaker));
        // 분석 서비스 실시간 CVD/매물대 누적용 Stream 발행 (raw DB 적재와 별개 채널).
        if (publishEnabled) {
            publisher.publishAggTrade(symbol, aggId, price, qty, buyerMaker, tradeTime);
            // 핫상태 "마지막 가격" 은 체결이 원천(분 단위 kline 종가로 쓰던 것을 교체, 2026-09-06)
            publisher.publishLastPrice(symbol, price);
        }
    }
}
