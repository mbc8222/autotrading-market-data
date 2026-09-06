package com.autotrading.autotradingmarketdata.depth;

import com.autotrading.autotradingmarketdata.binance.BinanceBanGuard;
import com.autotrading.autotradingmarketdata.binance.BinanceFuturesRestApi;
import com.autotrading.autotradingmarketdata.binance.BinanceRestException;
import com.autotrading.autotradingmarketdata.binance.RateBucket;
import com.autotrading.autotradingmarketdata.collect.CollectProperties;
import com.autotrading.autotradingmarketdata.depth.DepthRows.DiffEvent;
import com.autotrading.autotradingmarketdata.depth.DepthRows.Row;
import com.autotrading.autotradingmarketdata.depth.DepthRows.Snapshot;
import com.autotrading.autotradingmarketdata.publish.MarketDataPublisher;
import com.autotrading.autotradingmarketdata.raw.DepthBuffer;
import com.autotrading.autotradingmarketdata.raw.DepthRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 풀북 수집 — WS diff(100ms)로 심볼별 {@link LocalBook}을 유지하고, 스냅샷·diff 레벨을 원시 그대로
 * {@link DepthBuffer}에 넣는다(전용 워커가 {@code depth_diff} 배치 적재). 상위 20단 요약은 KV 로 발행.
 *
 * <p>심볼별 상태: needSnapshot → (이벤트 버퍼링 + 스냅샷 1회 요청) → 스냅샷 적용 → 버퍼 재생 → live.
 * live 에서 pu≠last_u 면 갭 → needSnapshot 복귀(사건 기록). WS 재연결 시 전 심볼 needSnapshot.
 * 스냅샷 REST 는 {@link RateBucket#FAPI}(weight 20) — 밴 중이면 요청하지 않는다.
 *
 * <p>파생물(1분 요약 등)은 만들지 않는다 — 수집기는 원시만 적재, 요약은 관측 계층 몫.
 */
@Component
@ConditionalOnProperty(name = "collect.depth.enabled", havingValue = "true")
public class DepthCollector {

    private static final Logger log = LogManager.getLogger(DepthCollector.class);
    private static final int SNAPSHOT_LIMIT = 1000;
    private static final int PENDING_CAP = 20_000;
    private static final long KV_INTERVAL_MS = 500;
    private static final long UNITS_RETRY_MS = 10_000;

    private final CollectProperties collect;
    private final BinanceFuturesRestApi api;
    private final DepthBuffer buffer;
    private final DepthRepository repository;
    private final MarketDataPublisher publisher;
    private final BinanceBanGuard banGuard;
    private final ExecutorService async = Executors.newVirtualThreadPerTaskExecutor();

    private final Map<String, SymbolState> states = new ConcurrentHashMap<>();
    private volatile long unitsFetchedAt = 0;

    private final Counter gaps;
    private final Counter snapshots;
    private final Counter unitErrors;

    private static final class SymbolState {
        final String symbol;
        SymbolUnits units;
        LocalBook book;
        volatile boolean needSnapshot = true;
        final AtomicBoolean snapshotInFlight = new AtomicBoolean(false);
        final ArrayDeque<DiffEvent> pending = new ArrayDeque<>();
        long lastKvMs = 0;
        volatile long lastEventMs = 0;

        SymbolState(String symbol) {
            this.symbol = symbol;
        }
    }

    public DepthCollector(CollectProperties collect, BinanceFuturesRestApi api, DepthBuffer buffer,
                          DepthRepository repository, MarketDataPublisher publisher, BinanceBanGuard banGuard,
                          MeterRegistry registry) {
        this.collect = collect;
        this.api = api;
        this.buffer = buffer;
        this.repository = repository;
        this.publisher = publisher;
        this.banGuard = banGuard;
        for (String s : collect.symbols()) {
            SymbolState st = new SymbolState(s.toLowerCase());
            states.put(st.symbol, st);
            // 적재 정지 감시용 — SQL 로 depth_diff 를 훑지 않고(인덱스 없음) 수집기가 직접 낸다.
            Gauge.builder("depth.last.event.age.seconds", st,
                            x -> x.lastEventMs == 0 ? -1 : (System.currentTimeMillis() - x.lastEventMs) / 1000.0)
                    .tag("symbol", st.symbol)
                    .description("마지막 diff 적용 후 경과 초 (-1=아직 없음)")
                    .register(registry);
        }
        gaps = Counter.builder("depth.gaps").description("diff 순번 갭(재동기화) 횟수").register(registry);
        snapshots = Counter.builder("depth.snapshots").description("REST 스냅샷 적용 횟수").register(registry);
        unitErrors = Counter.builder("depth.unit.errors").description("가격/수량 단위 불일치").register(registry);
    }

    @PreDestroy
    void stop() {
        async.shutdownNow();
    }

    /** WS 연결(재연결 포함) — 전 심볼의 북을 무효화하고 스냅샷 대기로. */
    public void onConnected() {
        for (SymbolState st : states.values()) {
            synchronized (st) {
                if (st.book != null) {
                    st.book.invalidate();
                }
                st.needSnapshot = true;
                st.pending.clear();
            }
            event(st.symbol, "ws_connect", null);
        }
    }

    /** WS 스레드에서 호출. 절대 차단하지 않는다(DB·REST 는 가상 스레드로). */
    public void onEvent(DiffEvent ev, long recvMs) {
        SymbolState st = states.get(ev.symbol());
        if (st == null) {
            return;
        }
        if (st.units == null && !ensureUnits(st)) {
            return;   // exchangeInfo 실패 — 다음 이벤트에서 재시도(10s 간격)
        }
        synchronized (st) {
            if (st.needSnapshot) {
                if (st.pending.size() >= PENDING_CAP) {
                    st.pending.pollFirst();
                }
                st.pending.addLast(ev);
                requestSnapshot(st, "buffered");
                return;
            }
            applyLive(st, ev, recvMs);
        }
    }

    // ── 내부 (st 락 안에서 호출) ──

    private void applyLive(SymbolState st, DiffEvent ev, long recvMs) {
        List<Row> rows = new ArrayList<>();
        LocalBook.Result r;
        try {
            r = st.book.apply(ev, rows);
        } catch (IllegalArgumentException e) {
            unitErrors.increment();
            event(st.symbol, "unit_error", e.getMessage());
            st.needSnapshot = true;
            st.pending.addLast(ev);
            requestSnapshot(st, "unit_error");
            return;
        }
        switch (r) {
            case APPLIED -> {
                for (Row row : rows) {
                    buffer.offer(withRecv(row, recvMs));
                }
                st.lastEventMs = recvMs;
                publishKv(st, ev.eventMs());
            }
            case GAP -> {
                gaps.increment();
                event(st.symbol, "gap", "pu=" + ev.prevU() + " last_u=" + st.book.lastU());
                st.needSnapshot = true;
                st.pending.addLast(ev);
                requestSnapshot(st, "gap");
            }
            default -> { /* DROPPED / UNSYNCED — 무시 */ }
        }
    }

    private void requestSnapshot(SymbolState st, String why) {
        if (banGuard.isPaused(RateBucket.FAPI)) {
            return;   // 밴 해제 후 다음 이벤트가 다시 요청
        }
        if (!st.snapshotInFlight.compareAndSet(false, true)) {
            return;
        }
        async.submit(() -> fetchSnapshot(st, why));
    }

    private void fetchSnapshot(SymbolState st, String why) {
        try {
            for (int attempt = 0; attempt < 5; attempt++) {
                try {
                    if (attempt > 0) {
                        Thread.sleep(300L * attempt);
                    }
                    Snapshot snap = api.depthSnapshot(st.symbol, SNAPSHOT_LIMIT);
                    long recvMs = System.currentTimeMillis();
                    synchronized (st) {
                        if (st.book == null) {
                            st.book = new LocalBook(st.units);
                        }
                        List<Row> rows = st.book.applySnapshot(snap, recvMs);
                        for (Row row : rows) {
                            buffer.offer(row);
                        }
                        int applied = 0;
                        int dropped = 0;
                        st.needSnapshot = false;
                        DiffEvent ev;
                        while ((ev = st.pending.pollFirst()) != null) {
                            long before = st.book.lastU();
                            applyLive(st, ev, recvMs);
                            if (st.needSnapshot) {
                                break;   // 재생 중 갭 — 새 요청이 이미 예약됨
                            }
                            if (st.book.lastU() != before) {
                                applied++;
                            } else {
                                dropped++;
                            }
                        }
                        snapshots.increment();
                        event(st.symbol, "snapshot", "why=" + why + " lastUpdateId=" + snap.lastUpdateId()
                                + " levels=" + rows.size() + " replay applied=" + applied + " dropped=" + dropped);
                        log.info("[DEPTH] {} 스냅샷({}) lastUpdateId={} 레벨={} 재생 applied={} dropped={}",
                                st.symbol, why, snap.lastUpdateId(), rows.size(), applied, dropped);
                    }
                    return;
                } catch (BinanceRestException e) {
                    if (e.isBanned()) {
                        banGuard.banned(e.bucket(), "depth-snapshot", e.retryAfterSec(), e.bannedUntilMs(), e.getMessage());
                        return;
                    }
                    if (e.isRateLimited()) {
                        banGuard.rateLimited(e.bucket(), "depth-snapshot", e.retryAfterSec());
                        return;
                    }
                    log.warn("[DEPTH] {} 스냅샷 실패({}): {}", st.symbol, attempt, e.getMessage());
                } catch (IllegalArgumentException e) {
                    unitErrors.increment();
                    event(st.symbol, "unit_error", "snapshot: " + e.getMessage());
                    log.error("[DEPTH] {} 스냅샷 단위 불일치 — 수집 불가: {}", st.symbol, e.getMessage());
                    return;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    log.warn("[DEPTH] {} 스냅샷 실패({}): {}", st.symbol, attempt, e.toString());
                }
            }
            event(st.symbol, "snapshot_fail", "5회 실패 — 다음 이벤트에서 재시도");
        } finally {
            st.snapshotInFlight.set(false);
        }
    }

    private void publishKv(SymbolState st, long eventMs) {
        if (eventMs - st.lastKvMs < KV_INTERVAL_MS) {
            return;
        }
        Map.Entry<Long, Long> bb = st.book.bestBid();
        Map.Entry<Long, Long> ba = st.book.bestAsk();
        if (bb == null || ba == null) {
            return;
        }
        st.lastKvMs = eventMs;
        double bidQty = st.units.qty(st.book.topBidSteps(20));
        double askQty = st.units.qty(st.book.topAskSteps(20));
        double total = bidQty + askQty;
        double imbalance = total > 0 ? (bidQty - askQty) / total : 0;
        publisher.publishOrderBookTop(st.symbol, st.units.price(bb.getKey()), st.units.price(ba.getKey()),
                bidQty, askQty, imbalance, eventMs);
    }

    private boolean ensureUnits(SymbolState st) {
        long now = System.currentTimeMillis();
        if (now - unitsFetchedAt < UNITS_RETRY_MS) {
            return false;
        }
        unitsFetchedAt = now;
        try {
            Map<String, SymbolUnits> all = api.symbolUnits(collect.symbols());
            for (SymbolState s : states.values()) {
                SymbolUnits u = all.get(s.symbol);
                if (u != null) {
                    s.units = u;
                    repository.upsertUnits(u);
                    log.info("[DEPTH] {} price_unit=10^-{} qty_unit=10^-{} (filter tick={} step={})",
                            s.symbol, u.pricePrecision(), u.qtyPrecision(), u.tickSize(), u.stepSize());
                }
            }
            return st.units != null;
        } catch (Exception e) {
            log.warn("[DEPTH] exchangeInfo 실패: {}", e.toString());
            return false;
        }
    }

    private void event(String symbol, String kind, String detail) {
        async.submit(() -> {
            try {
                repository.insertEvent(symbol, kind, detail);
            } catch (Exception e) {
                log.warn("[DEPTH] {} 사건 기록 실패({}): {}", symbol, kind, e.toString());
            }
        });
    }

    private static Row withRecv(Row r, long recvMs) {
        return new Row(r.symbol(), r.eventMs(), recvMs, r.kind(), r.side(), r.priceTicks(), r.qtySteps(),
                r.u(), r.firstU(), r.prevU());
    }
}
