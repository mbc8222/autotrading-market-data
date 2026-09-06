package com.autotrading.autotradingmarketdata.ws;

import com.autotrading.autotradingmarketdata.collect.CollectProperties;
import com.autotrading.autotradingmarketdata.depth.DepthCollector;
import com.autotrading.autotradingmarketdata.depth.DepthRows.DiffEvent;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * 차분 호가 스트림({@code <symbol>@depth@100ms}) — 풀북의 원천. 이벤트를 {@link DepthCollector}에 넘긴다
 * (로컬 북 동기화·원시 적재·KV 요약은 거기서). {@code collect.depth.enabled=false}면 구독하지 않는다.
 *
 * <p>2026-09-06: 이전 {@code @depth20@500ms}(상위 20단 요약, raw 미적재)를 대체. 상위 20단 KV 는 로컬 북에서 만든다.
 */
@Component
@ClientEndpoint
public class DepthWebSocket extends BinanceWebSocket {

    private final CollectProperties collect;
    private final DepthCollector collector;   // null 이면 비활성

    public DepthWebSocket(CollectProperties collect, ObjectProvider<DepthCollector> collector,
                          MeterRegistry registry) {
        super(registry);
        this.collect = collect;
        this.collector = collector.getIfAvailable();
    }

    @OnOpen
    public void onOpen(Session session) {
        if (collector != null) {
            collector.onConnected();
        }
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
        if (collector == null) {
            return List.of();
        }
        return collect.symbols().stream().map(s -> s + "@depth@100ms").toList();
    }

    @Override
    protected void onData(String stream, JsonNode d) {
        String symbol = d.path("s").asString("").toLowerCase();
        if (symbol.isEmpty() || collector == null) {
            return;
        }
        collector.onEvent(new DiffEvent(symbol, d.path("E").asLong(0), d.path("U").asLong(0),
                d.path("u").asLong(0), d.path("pu").asLong(0), levels(d.get("b")), levels(d.get("a"))),
                System.currentTimeMillis());
    }

    private static List<String[]> levels(JsonNode arr) {
        if (arr == null || !arr.isArray()) {
            return List.of();
        }
        List<String[]> out = new ArrayList<>(arr.size());
        for (JsonNode l : arr) {
            out.add(new String[] {l.path(0).asString("0"), l.path(1).asString("0")});
        }
        return out;
    }
}
