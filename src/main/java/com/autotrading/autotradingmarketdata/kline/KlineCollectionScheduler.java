package com.autotrading.autotradingmarketdata.kline;

import com.autotrading.autotradingmarketdata.binance.BinanceBanGuard;
import com.autotrading.autotradingmarketdata.binance.BinanceRestException;
import com.autotrading.autotradingmarketdata.binance.RateBucket;
import com.autotrading.autotradingmarketdata.collect.CollectProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 캔들 폴링 — @Scheduled는 예외 1회로 영구 정지될 수 있으므로 조합 단위로 가드한다(모놀리스 runPoll 원칙).
 * fixedDelay는 메서드 종료 후부터 대기하므로 사이클 중첩은 발생하지 않는다.
 * ★이 수집기는 {@code /fapi/v1/klines} 만 쓰므로 {@link RateBucket#FAPI} 양동이만 본다(2026-08-21).
 * 종전에는 전역 가드라 {@code /futures/data/basis} 가 받은 밴에 79~105분씩 같이 멈췄다 —
 * 실제로는 밴 중에도 klines 는 계속 200 을 받고 있었다.
 */
@Component
@ConditionalOnProperty(prefix = "collect.kline", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KlineCollectionScheduler {

    private static final Logger log = LogManager.getLogger(KlineCollectionScheduler.class);

    private final KlineCollector collector;
    private final KlineCollectProperties properties;
    private final CollectProperties collect;
    private final BinanceBanGuard banGuard;

    public KlineCollectionScheduler(KlineCollector collector, KlineCollectProperties properties,
                                    CollectProperties collect, BinanceBanGuard banGuard) {
        this.collector = collector;
        this.properties = properties;
        this.collect = collect;
        this.banGuard = banGuard;
    }

    @Scheduled(fixedDelayString = "${collect.kline.fixed-delay:30s}", initialDelayString = "5s")
    public void poll() {
        if (banGuard.isPaused(RateBucket.FAPI)) {
            return;
        }
        for (String symbol : collect.symbols()) {
            for (String interval : properties.intervals()) {
                try {
                    int saved = collector.collect(symbol, interval);
                    if (saved > 0) {
                        log.info("Collected klines: symbol={}, interval={}, rows={}", symbol, interval, saved);
                    }
                } catch (BinanceRestException e) {
                    if (e.isBanned()) {
                        banGuard.banned(e.bucket(), "kline", e.retryAfterSec(), e.bannedUntilMs(), e.getMessage());
                        return;
                    }
                    if (e.isRateLimited()) {
                        banGuard.rateLimited(e.bucket(), "kline", e.retryAfterSec());
                        return;
                    }
                    log.error("Binance API error: symbol={}, interval={}", symbol, interval, e);
                } catch (Exception e) {
                    log.error("Kline collection failed: symbol={}, interval={}", symbol, interval, e);
                }
            }
        }
    }
}
