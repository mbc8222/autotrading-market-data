package com.autotrading.autotradingmarketdata.binance;

import java.time.Duration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

/**
 * 418(IP ban) 공용 가드 — ban은 IP 단위라서 한 수집기가 맞으면 모든 REST 수집기가 함께 멈춰야 한다.
 * 계속 두드리면 ban이 연장되므로(바이낸스 정책) {@value #PAUSE_MINUTES}분 일괄 중지.
 */
@Component
public class BinanceBanGuard {

    private static final Logger log = LogManager.getLogger(BinanceBanGuard.class);
    private static final int PAUSE_MINUTES = 10;

    private volatile long pausedUntil = 0;

    public void banned(String source) {
        pausedUntil = System.currentTimeMillis() + Duration.ofMinutes(PAUSE_MINUTES).toMillis();
        log.error("[BAN-GUARD] 418 IP ban ({}) — 전체 REST 수집 {}분 중지", source, PAUSE_MINUTES);
    }

    public boolean isPaused() {
        return System.currentTimeMillis() < pausedUntil;
    }
}
