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
 * markPrice 스트림({@code <symbol>@markPrice@1s}) — 마크/인덱스 가격 + 예상 펀딩비를 1초마다 수신해
 * Redis 핫상태 KV로 발행(유실 OK — 다음 값이 덮는다).
 */
@Component
@ClientEndpoint
public class MarkPriceWebSocket extends BinanceWebSocket {

    private final CollectProperties collect;
    private final MarketDataPublisher publisher;

    public MarkPriceWebSocket(CollectProperties collect, MarketDataPublisher publisher,
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
        return FUTURES_MARKET;   // markPrice는 /market 라우팅
    }

    @Override
    protected List<String> streams() {
        return collect.symbols().stream().map(s -> s + "@markPrice@1s").toList();
    }

    @Override
    protected void onData(String stream, JsonNode d) {
        String symbol = d.path("s").asString("").toLowerCase();
        if (symbol.isEmpty()) {
            return;
        }
        publisher.publishMarkPrice(symbol,
                d.path("p").asDouble(0),    // mark price
                d.path("i").asDouble(0),    // index price
                d.path("r").asDouble(0),    // funding rate
                d.path("T").asLong(0),      // next funding time
                d.path("E").asLong(0));     // event time
    }
}
