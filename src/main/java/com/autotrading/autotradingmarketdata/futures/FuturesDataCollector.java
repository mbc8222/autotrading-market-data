package com.autotrading.autotradingmarketdata.futures;

import com.autotrading.autotradingmarketdata.binance.BinanceBanGuard;
import com.autotrading.autotradingmarketdata.binance.BinanceFuturesRestApi;
import com.autotrading.autotradingmarketdata.binance.BinanceRestException;
import com.autotrading.autotradingmarketdata.binance.BinanceRestRetry;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.FundingRow;
import com.autotrading.autotradingmarketdata.collect.CollectProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 파생상품 데이터 수집기 — {@code /futures/data} 통계 6종 + funding rate (모놀리스 검증 로직 이식).
 *
 * <p>백필과 폴링이 동일한 catchUp 로직을 공유 — 각 시리즈는 hwm(메모리) 또는 DB max(ts)부터 행 단위로 따라잡는다.
 *
 * <p>안전장치 —
 * ① 절단 없음: 한 window가 PAGE({@value #PAGE})개를 절대 넘지 않게 시간 폭 제한(450×5m) →
 *    거래소의 start/end 정렬 가정과 무관하게 window 내 모든 행이 반환됨.
 * ② 멱등: 모든 insert가 자연키 ON CONFLICT DO NOTHING.
 * ③ resume: hwm 없으면 DB max(ts)+1부터. 실패(재시도 소진)는 전진하지 않고 중단 → 다음 tick 재개.
 * ④ rate limit: 호출 사이 {@value #THROTTLE_MS}ms throttle + 429 backoff, 418은 BanGuard 일괄 중지.
 * ⑤ {@code /futures/data}는 거래소가 ~{@value #BACKFILL_DAYS}일만 보존 → 최초 백필 floor clamp.
 */
@Component
@ConditionalOnProperty(prefix = "collect.futures", name = "enabled", havingValue = "true", matchIfMissing = true)
public class FuturesDataCollector {

    private static final Logger log = LogManager.getLogger(FuturesDataCollector.class);

    private static final String PERIOD = "5m";
    private static final long PERIOD_MS = 300_000L;
    private static final int PAGE = 500;                                  // /futures/data 상한
    private static final long WINDOW_MS = (PAGE - 50) * PERIOD_MS;        // 450개 × 5m — 절단 불가 보장
    private static final long DAY_MS = 86_400_000L;
    private static final int BACKFILL_DAYS = 30;                          // /futures/data 보존 한계
    private static final long FUNDING_START = 1_704_067_200_000L;         // 2024-01-01
    private static final int FUNDING_PAGE = 1000;
    private static final long THROTTLE_MS = 250;

    /** 사이클마다 증가 — 심볼 시작 위치 회전용(밴 손실을 심볼 간에 균등화). */
    private long rotation = 0;

    private final BinanceFuturesRestApi api;
    private final FuturesRepository repository;
    private final CollectProperties collect;
    private final BinanceBanGuard banGuard;

    /** 시리즈별(테이블:키) 마지막 적재 ts — 백필/폴링 공유 증분 cursor. */
    private final Map<String, Long> highWater = new ConcurrentHashMap<>();

    public FuturesDataCollector(BinanceFuturesRestApi api, FuturesRepository repository,
                                CollectProperties collect, BinanceBanGuard banGuard) {
        this.api = api;
        this.repository = repository;
        this.collect = collect;
        this.banGuard = banGuard;
    }

    /** 첫 실행이 30일 백필을 겸한다(행 기반 resume이라 백필=폴링 동일 로직). */
    @Scheduled(fixedDelayString = "${collect.futures.fixed-delay:5m}", initialDelayString = "10s")
    public void poll() {
        if (banGuard.isPaused()) {
            return;
        }
        try {
            // 심볼 순서를 매 사이클 회전한다. 고정 순서면 밴이 늘 같은 지점에서 잘려
            // 선두 심볼(BTC)만 상시 성공하고 뒤쪽 3심볼이 만성 지연된다(2026-08-20 실측).
            // 회전하면 최소한 손실이 심볼 간에 균등해지고, 어느 심볼도 영구히 굶지 않는다.
            List<String> symbols = rotated(collect.symbols());
            for (String symbol : symbols) {
                catchUp("oi", "futures_open_interest_hist", "symbol", symbol, r -> r.ts(),
                        (s, e) -> api.oiHist(symbol, PERIOD, PAGE, s, e), repository::insertOiHist);
                BinanceRestRetry.sleep(THROTTLE_MS);
                catchUp("topPos", "futures_top_position_ratio", "symbol", symbol, r -> r.ts(),
                        (s, e) -> api.topPositionRatio(symbol, PERIOD, PAGE, s, e),
                        rows -> repository.insertLsRatio("futures_top_position_ratio", rows));
                BinanceRestRetry.sleep(THROTTLE_MS);
                catchUp("topAcct", "futures_top_account_ratio", "symbol", symbol, r -> r.ts(),
                        (s, e) -> api.topAccountRatio(symbol, PERIOD, PAGE, s, e),
                        rows -> repository.insertLsRatio("futures_top_account_ratio", rows));
                BinanceRestRetry.sleep(THROTTLE_MS);
                catchUp("globalLs", "futures_global_ls_ratio", "symbol", symbol, r -> r.ts(),
                        (s, e) -> api.globalLsRatio(symbol, PERIOD, PAGE, s, e),
                        rows -> repository.insertLsRatio("futures_global_ls_ratio", rows));
                BinanceRestRetry.sleep(THROTTLE_MS);
                catchUp("taker", "futures_taker_ratio", "symbol", symbol, r -> r.ts(),
                        (s, e) -> api.takerRatio(symbol, PERIOD, PAGE, s, e), repository::insertTaker);
                BinanceRestRetry.sleep(THROTTLE_MS);
                catchUp("basis", "futures_basis", "pair", symbol, r -> r.ts(),
                        (s, e) -> api.basis(symbol, PERIOD, PAGE, s, e), repository::insertBasis);
                BinanceRestRetry.sleep(THROTTLE_MS);
                catchUpFunding(symbol);
                // 심볼 사이 간격 — 아래 series 간 간격과 함께 버스트를 평탄화한다.
                BinanceRestRetry.sleep(THROTTLE_MS);
            }
            banGuard.cycleSucceeded();
        } catch (BinanceRestException e) {
            if (e.isBanned()) {
                banGuard.banned("futures", e.retryAfterSec());
                return;
            }
            log.warn("[FUTURES] 사이클 중단(다음 tick 재개): {}", e.getMessage());
        } catch (Exception e) {
            log.error("[FUTURES] 폴링 실패(다음 tick 계속)", e);
        }
    }

    /**
     * 사이클마다 시작 심볼을 한 칸 밀어 순서를 회전한다.
     * 밴으로 사이클이 중간에 끊겨도 다음 사이클은 다른 심볼부터 시작하므로 손실이 균등해진다.
     */
    private List<String> rotated(List<String> symbols) {
        if (symbols.size() <= 1) {
            return symbols;
        }
        int off = (int) (Math.floorMod(rotation++, symbols.size()));
        List<String> out = new ArrayList<>(symbols.size());
        for (int i = 0; i < symbols.size(); i++) {
            out.add(symbols.get((off + i) % symbols.size()));
        }
        return out;
    }

    /** /futures/data 시리즈 1개: cursor부터 now까지 bounded window로 따라잡는다. */
    private <T> void catchUp(String label, String table, String keyCol, String key,
                             ToLongFunction<T> tsOf, WindowFetch<T> fetch, Consumer<List<T>> insert) {
        long now = System.currentTimeMillis();
        long floor = now - (long) BACKFILL_DAYS * DAY_MS;
        // 보존 한계(~30일)보다 오래된 지점은 빈 응답 → cursor를 floor로 clamp해 빈 window walk 회피.
        long cursor = Math.max(resolveCursor(table, "ts", keyCol, key, floor), floor);
        long persisted = 0;
        long maxSeen = -1;

        while (cursor < now) {
            final long from = cursor;
            final long end = Math.min(cursor + WINDOW_MS, now);
            List<T> rows = BinanceRestRetry.fetchWithRetry(() -> fetch.fetch(from, end), key + " " + label, log);
            if (!rows.isEmpty()) {
                insert.accept(rows);
                persisted += rows.size();
                long m = maxTs(rows, tsOf);
                if (m > maxSeen) {
                    maxSeen = m;
                }
            }
            cursor = end + 1;
            if (end >= now) {
                break;
            }
            BinanceRestRetry.sleep(THROTTLE_MS);
        }
        if (maxSeen > 0) {
            // hwm은 실제 마지막 행 ts — 다음 tick이 미발행분을 다시 집어가도록(꼬리 누락 방지).
            highWater.put(hwmKey(table, key), maxSeen);
        }
        if (persisted > 0) {
            log.info("[FUTURES] {} {} +{}건", key, label, persisted);
        }
    }

    /** funding: cursor(없으면 2024-01-01)부터 끝까지 1000개씩. */
    private void catchUpFunding(String symbol) {
        long cursor = resolveCursor("futures_funding_rate", "funding_time", "symbol", symbol, FUNDING_START);
        long persisted = 0;

        while (true) {
            final long from = cursor;
            List<FundingRow> page = BinanceRestRetry.fetchWithRetry(
                    () -> api.fundingHistory(symbol, from, FUNDING_PAGE), symbol + " funding", log);
            if (page.isEmpty()) {
                break;
            }
            repository.insertFunding(page);
            persisted += page.size();

            long maxTs = maxTs(page, FundingRow::fundingTime);
            if (maxTs < cursor) {
                break;   // 전진 없음(이상 응답) — 무한루프 방지
            }
            highWater.put(hwmKey("futures_funding_rate", symbol), maxTs);
            cursor = maxTs + 1;
            if (page.size() < FUNDING_PAGE) {
                break;
            }
            BinanceRestRetry.sleep(THROTTLE_MS);
        }
        if (persisted > 0) {
            log.info("[FUTURES] {} funding +{}건", symbol, persisted);
        }
    }

    /** 시작 cursor — 메모리 hwm 우선, 없으면 DB max(ts)+1, 그것도 없으면 floor. */
    private long resolveCursor(String table, String tsCol, String keyCol, String key, long floor) {
        Long hw = highWater.get(hwmKey(table, key));
        if (hw != null) {
            return hw + 1;
        }
        return repository.findMaxTs(table, tsCol, keyCol, key)
                .map(ts -> ts.toEpochMilli() + 1)
                .orElse(floor);
    }

    private static <T> long maxTs(List<T> rows, ToLongFunction<T> tsOf) {
        long max = Long.MIN_VALUE;
        for (T r : rows) {
            long ts = tsOf.applyAsLong(r);
            if (ts > max) {
                max = ts;
            }
        }
        return max;
    }

    private static String hwmKey(String table, String key) {
        return table + ":" + key;
    }

    @FunctionalInterface
    private interface WindowFetch<T> {
        List<T> fetch(long startTime, long endTime);
    }
}
