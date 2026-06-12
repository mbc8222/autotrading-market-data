package com.autotrading.autotradingmarketdata.kline;

import com.autotrading.autotradingmarketdata.binance.BinanceBanGuard;
import com.autotrading.autotradingmarketdata.binance.BinanceRestException;
import com.autotrading.autotradingmarketdata.collect.CollectProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 캔들 폴링 — @Scheduled는 예외 1회로 영구 정지될 수 있으므로 조합 단위로 가드한다(모놀리스 runPoll 원칙).
 * fixedDelay는 메서드 종료 후부터 대기하므로 사이클 중첩은 발생하지 않는다.
 * 418(ban)은 {@link BinanceBanGuard}로 전 수집기 일괄 장기 중지, 429는 이번 사이클만 중단(다음 delay가 백오프).
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
        if (banGuard.isPaused()) {
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
                        banGuard.banned("kline");
                        return;
                    }
                    if (e.isRateLimited()) {
                        log.warn("Rate limited by Binance — aborting this cycle");
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
