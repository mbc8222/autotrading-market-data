package com.autotrading.autotradingmarketdata.kline;

import com.autotrading.autotradingmarketdata.binance.BinanceFuturesRestApi;
import com.autotrading.autotradingmarketdata.binance.BinanceKline;
import com.autotrading.autotradingmarketdata.binance.BinanceRestRetry;
import com.autotrading.autotradingmarketdata.publish.MarketDataPublisher;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

/**
 * 캔들 수집 — 모놀리스에서 검증된 수집 원칙 이식:
 * <ul>
 *   <li>행 기반 resume: DB max(open_time)부터(inclusive) 재개 — forming 봉은 upsert로 갱신.</li>
 *   <li>백필=폴링 동일 로직: 중단돼도 다음 tick이 같은 지점부터 회수.</li>
 *   <li>페이지 간 {@value #THROTTLE_MS}ms throttle(weight 여유) + 429 backoff 재시도, 418은 즉시 전파.</li>
 * </ul>
 * Stream 발행은 따라잡은(caught-up) 마지막 페이지의 닫힌 봉만 — 백필 중 과거 봉으로 채널을 범람시키지 않는다.
 */
@Service
public class KlineCollector {

    private static final Logger log = LogManager.getLogger(KlineCollector.class);
    private static final long THROTTLE_MS = 350;

    private final BinanceFuturesRestApi api;
    private final KlineRepository repository;
    private final MarketDataPublisher publisher;
    private final KlineCollectProperties properties;

    public KlineCollector(BinanceFuturesRestApi api, KlineRepository repository,
                          MarketDataPublisher publisher, KlineCollectProperties properties) {
        this.api = api;
        this.repository = repository;
        this.publisher = publisher;
        this.properties = properties;
    }

    /** @return 이번 사이클에 적재(upsert)한 봉 수 */
    public int collect(String symbol, String interval) {
        long now = System.currentTimeMillis();
        long startTime = repository.findMaxOpenTime(symbol, interval)
                .orElseGet(() -> Instant.ofEpochMilli(now).minus(Duration.ofDays(properties.backfillDays())))
                .toEpochMilli();

        int total = 0;
        while (true) {
            final long from = startTime;
            List<BinanceKline> page = BinanceRestRetry.fetchWithRetry(
                    () -> api.getKlines(symbol, interval, from, properties.pageLimit()),
                    symbol + " " + interval, log);
            if (page.isEmpty()) {
                break;
            }
            repository.upsertAll(symbol, interval, page, now);
            total += page.size();

            if (page.size() < properties.pageLimit()) {
                publishCaughtUpPage(symbol, interval, page, now);
                break;
            }
            startTime = page.get(page.size() - 1).openTime() + 1;
            BinanceRestRetry.sleep(THROTTLE_MS);
        }
        return total;
    }

    private void publishCaughtUpPage(String symbol, String interval, List<BinanceKline> page, long now) {
        List<BinanceKline> closed = page.stream()
                .filter(k -> k.isClosedAt(now))
                .toList();
        publisher.publishClosedKlines(symbol, interval, closed);
        // last-price KV 는 AggTradeWebSocket 이 체결마다 쓴다(2026-09-06). 여기서 분 단위 종가로 덮어쓰면
        // 방금 들어온 체결가를 묵은 값으로 되돌리므로 호출을 뺐다.
    }
}
