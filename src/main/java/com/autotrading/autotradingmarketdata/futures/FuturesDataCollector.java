package com.autotrading.autotradingmarketdata.futures;

import com.autotrading.autotradingmarketdata.binance.BinanceBanGuard;
import com.autotrading.autotradingmarketdata.binance.BinanceFuturesRestApi;
import com.autotrading.autotradingmarketdata.binance.BinanceRestException;
import com.autotrading.autotradingmarketdata.binance.RateBucket;
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
    /**
     * /futures/data 계열 호출 간격. 250ms(≈3 req/s)에서 1s 로 늦춘다 — 실측상 이 계열은
     * 가중치 헤더도 Retry-After 도 주지 않는 별도 한도이고, 초당 3건 구간에서 418 이 걸렸다.
     * 한 사이클 28요청 × 1s ≈ 28초로 수집 주기(5분) 안에 충분히 들어가므로 신선도 손실은 없다.
     */
    private static final long THROTTLE_MS = 1_000;
    /** basis 폴링 주기 — 5분에서 30분으로. WINDOW_MS(37.5h) 덕에 5분 해상도 행은 그대로 다 들어온다. */
    private static final long BASIS_INTERVAL_MS = 30 * 60_000L;

    /** 사이클마다 증가 — 심볼 시작 위치 회전용(밴 손실을 심볼 간에 균등화). */
    private long rotation = 0;

    /** basis 마지막 폴링 시각 — BASIS_INTERVAL_MS 간격 유지용. 0 = 기동 후 첫 사이클에 즉시 1회. */
    private volatile long lastBasisAt = 0;

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

    /**
     * 첫 실행이 30일 백필을 겸한다(행 기반 resume이라 백필=폴링 동일 로직).
     *
     * <p>★2026-08-21: 한 사이클을 <b>세 패스</b>로 쪼갰다. 종전에는 심볼 루프 안에서 7종을
     * 연달아 호출했고, {@code basis} 에서 난 예외가 {@code poll()} 까지 올라가 <b>남은 심볼이
     * 통째로 스킵</b>됐다(관측된 418 502건이 전부 basis 였으므로 사실상 매번 그랬다).
     * 패스를 나누면 basis 가 죽어도 나머지가 살고, 각 패스가 자기 양동이만 본다.
     */
    @Scheduled(fixedDelayString = "${collect.futures.fixed-delay:5m}", initialDelayString = "10s")
    public void poll() {
        // 심볼 순서를 매 사이클 회전한다. 고정 순서면 밴이 늘 같은 지점에서 잘려
        // 선두 심볼(BTC)만 상시 성공하고 뒤쪽 3심볼이 만성 지연된다(2026-08-20 실측).
        List<String> symbols = rotated(collect.symbols());
        pollStats(symbols);
        pollFunding(symbols);
        pollBasis(symbols);
    }

    /** 패스 1 — {@code /futures/data} 통계 5종. 밴 유발 이력 0건이라 5분 주기를 유지한다. */
    private void pollStats(List<String> symbols) {
        if (banGuard.isPaused(RateBucket.FUTURES_DATA)) {
            return;
        }
        try {
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
            }
            banGuard.cycleSucceeded(RateBucket.FUTURES_DATA);
        } catch (BinanceRestException e) {
            if (!reportLimit(e, "futures-stats")) {
                log.warn("[FUTURES] 통계 패스 중단(다음 tick 재개): {}", e.getMessage());
            }
        } catch (Exception e) {
            log.error("[FUTURES] 통계 패스 실패(다음 tick 계속)", e);
        }
    }

    /**
     * 패스 2 — funding. 이건 {@code /fapi/v1/fundingRate} 라 <b>FAPI 양동이</b>다.
     * 종전에는 심볼 루프 안에 섞여 있어서 basis 밴에 같이 끌려갔다.
     */
    private void pollFunding(List<String> symbols) {
        if (banGuard.isPaused(RateBucket.FAPI)) {
            return;
        }
        try {
            for (String symbol : symbols) {
                catchUpFunding(symbol);
                BinanceRestRetry.sleep(THROTTLE_MS);
            }
            banGuard.cycleSucceeded(RateBucket.FAPI);
        } catch (BinanceRestException e) {
            if (!reportLimit(e, "futures-funding")) {
                log.warn("[FUTURES] funding 패스 중단(다음 tick 재개): {}", e.getMessage());
            }
        } catch (Exception e) {
            log.error("[FUTURES] funding 패스 실패(다음 tick 계속)", e);
        }
    }

    /**
     * 패스 3 — basis. 관측된 418 <b>502건이 전부</b> 이 엔드포인트에서 났고, 응답이 지목하는 IP 가
     * 우리 것이 아니라 바이낸스 내부 주소({@code 10.119.x.x}, 9개가 번갈아)라 <b>우리 요청량과
     * 무관하게</b> 밴이 난다({@link RateBucket} 참조).
     *
     * <p>그래서 두 가지를 한다 — ① 별도 양동이로 격리해 나머지 6종을 오염시키지 않는다.
     * ② 주기를 {@value #BASIS_INTERVAL_MS}ms 로 늦춘다. {@code WINDOW_MS} 가 37.5시간이라
     * 30분마다 받아도 5분 해상도 행이 <b>빠짐없이</b> 들어오므로 데이터 손실은 0이고,
     * 포화된 프록시에 걸릴 기회만 1/6 로 준다.
     */
    private void pollBasis(List<String> symbols) {
        long now = System.currentTimeMillis();
        if (now - lastBasisAt < BASIS_INTERVAL_MS) {
            return;
        }
        if (banGuard.isPaused(RateBucket.FUTURES_DATA_BASIS)) {
            return;
        }
        lastBasisAt = now;
        for (String symbol : symbols) {
            try {
                catchUp("basis", "futures_basis", "pair", symbol, r -> r.ts(),
                        (s, e) -> api.basis(symbol, PERIOD, PAGE, s, e), repository::insertBasis);
                BinanceRestRetry.sleep(THROTTLE_MS);
            } catch (BinanceRestException e) {
                if (reportLimit(e, "futures-basis")) {
                    return;   // 밴/쿨다운 — 남은 심볼도 두드리지 않는다
                }
                log.warn("[FUTURES] {} basis 보류(다음 주기 재개): {}", symbol, e.getMessage());
            } catch (Exception e) {
                log.error("[FUTURES] {} basis 실패(다음 주기 계속)", symbol, e);
            }
        }
        banGuard.cycleSucceeded(RateBucket.FUTURES_DATA_BASIS);
    }

    /**
     * 418/429 를 <b>예외가 실어온 양동이</b>로 가드에 전달한다.
     * 반환 true = 레이트리밋이므로 이번 패스를 중단해야 함.
     */
    private boolean reportLimit(BinanceRestException e, String source) {
        if (e.isBanned()) {
            banGuard.banned(e.bucket(), source, e.retryAfterSec(), e.bannedUntilMs(), e.getMessage());
            return true;
        }
        if (e.isRateLimited()) {
            banGuard.rateLimited(e.bucket(), source, e.retryAfterSec());
            return true;
        }
        return false;
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
