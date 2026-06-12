package com.autotrading.autotradingmarketdata.ws;

import com.autotrading.autotradingmarketdata.collect.CollectProperties;
import com.autotrading.autotradingmarketdata.publish.MarketDataPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import java.util.List;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * 부분 호가창 스트림({@code <symbol>@depth20@500ms}) — 상위 20레벨에서 best bid/ask·잔량·불균형을 계산해
 * Redis 핫상태 KV로 발행. raw 호가는 적재하지 않는다(기존 결정: REST 복구 불가·HF fidelity는 retail이 못 씀).
 */
@Component
@ClientEndpoint
public class DepthWebSocket extends BinanceWebSocket {

    private final CollectProperties collect;
    private final MarketDataPublisher publisher;

    public DepthWebSocket(CollectProperties collect, MarketDataPublisher publisher,
                          MeterRegistry registry) {
        super(registry);
        this.collect = collect;
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
        return FUTURES_PUBLIC;   // depth는 /public 라우팅
    }

    @Override
    protected List<String> streams() {
        return collect.symbols().stream().map(s -> s + "@depth20@500ms").toList();
    }

    @Override
    protected void onData(String stream, JsonNode d) {
        String symbol = d.path("s").asString("").toLowerCase();
        if (symbol.isEmpty()) {
            return;
        }
        JsonNode bids = d.get("b");
        JsonNode asks = d.get("a");
        if (bids == null || asks == null || !bids.isArray() || !asks.isArray()
                || bids.isEmpty() || asks.isEmpty()) {
            return;
        }
        double bestBid = bids.get(0).path(0).asDouble(0);
        double bestAsk = asks.get(0).path(0).asDouble(0);
        double bidQty = sumQty(bids);
        double askQty = sumQty(asks);
        double total = bidQty + askQty;
        double imbalance = total > 0 ? (bidQty - askQty) / total : 0;
        publisher.publishOrderBookTop(symbol, bestBid, bestAsk, bidQty, askQty, imbalance,
                d.path("E").asLong(0));
    }

    private static double sumQty(JsonNode levels) {
        double sum = 0;
        for (JsonNode level : levels) {
            sum += level.path(1).asDouble(0);
        }
        return sum;
    }
}
