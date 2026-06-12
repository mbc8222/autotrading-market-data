package com.autotrading.autotradingmarketdata.ws;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 기동 완료 후 전 WS 연결 — ApplicationReady 시점이라 Flyway(테이블)·파티션 준비 이후다.
 */
@Component
@ConditionalOnProperty(prefix = "collect.ws", name = "enabled", havingValue = "true", matchIfMissing = true)
public class WebSocketStarter {

    private static final Logger log = LogManager.getLogger(WebSocketStarter.class);

    private final List<BinanceWebSocket> sockets;

    public WebSocketStarter(List<BinanceWebSocket> sockets) {
        this.sockets = sockets;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        log.info("[WS] {}개 클라이언트 연결 시작", sockets.size());
        sockets.forEach(BinanceWebSocket::connect);
    }
}
