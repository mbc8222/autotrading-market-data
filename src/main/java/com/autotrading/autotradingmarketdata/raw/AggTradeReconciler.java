package com.autotrading.autotradingmarketdata.raw;

import com.autotrading.autotradingmarketdata.binance.BinanceBanGuard;
import com.autotrading.autotradingmarketdata.binance.BinanceFuturesRestApi;
import com.autotrading.autotradingmarketdata.binance.BinanceRestException;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.AggTradeRow;
import com.autotrading.autotradingmarketdata.collect.CollectProperties;
import com.autotrading.autotradingmarketdata.raw.AggTradeRepository.AggIdGap;
import com.autotrading.autotradingmarketdata.raw.AggTradeRepository.AggIdScan;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 원시 집계체결(agg_trade) 갭 탐지형 REST 보정기 (모놀리스 검증 구현 이식) —
 * WS-only 수집의 누락(재기동·끊김·버스트 drop)을 {@value #SWEEP_MS}ms마다 메운다.
 *
 * <p>agg_id는 심볼별 빠짐없이 순차이므로 DB에서 빠진 id = 진짜 갭. 매 sweep 심볼별로:
 * ① floor(24h 복구창 하한)로 watermark clamp — 복구 불가 갭은 포기.
 * ② watermark 위 ~ grace({@value #GRACE_MS}ms 전) 아래를 빠른 집계로 판정 — 연속이면 watermark만 전진(REST 0회).
 * ③ 빈틈이 있으면 누락 구간만 REST로 fetch → 멱등 적재. 완전히 못 메운 갭에서 멈춰 watermark를 안 넘긴다.
 *
 * <p>전용 데몬 스레드(공유 스케줄러와 분리 — 큰 갭 복구가 캔들·파생 폴링을 굶기지 않음).
 * 429는 backoff 재시도, 418은 {@link BinanceBanGuard} 일괄 중지. grace는 WS 정착(재연결 backoff 최대 60s
 * + flush) 미만 구간을 가짜 갭으로 오인하지 않게 한다. watermark는 인메모리 — 재기동 시 floor부터 재구성.
 */
@Component
@ConditionalOnProperty(name = "collect.raw.enabled", havingValue = "true")
public class AggTradeReconciler {

    private static final Logger log = LogManager.getLogger(AggTradeReconciler.class);

    private static final long INITIAL_MS = 90_000;        // 캔들·파생 백필 weight 경합 회피
    private static final long SWEEP_MS = 60_000;
    private static final long GRACE_MS = 120_000;
    private static final int HISTORY_MIN = 24 * 60 - 30;  // 23h30m: REST 24h 보존에 30분 안전여유
    private static final long HISTORY_MS = HISTORY_MIN * 60_000L;
    private static final int PAGE = 1_000;
    private static final long THROTTLE_MS = 350;
    private static final int MAX_RETRY = 5;
    private static final long RL_BACKOFF_MS = 5_000;

    private final CollectProperties collect;
    private final BinanceFuturesRestApi api;
    private final AggTradeRepository repository;
    private final BinanceBanGuard banGuard;

    /** 심볼 → 연속 확인된 마지막 agg_id(이 아래는 빈틈 없음/복구 포기). */
    private final Map<String, Long> watermark = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private Thread worker;

    public AggTradeReconciler(CollectProperties collect, BinanceFuturesRestApi api,
                              AggTradeRepository repository, BinanceBanGuard banGuard) {
        this.collect = collect;
        this.api = api;
        this.repository = repository;
        this.banGuard = banGuard;
    }

    @PostConstruct
    void start() {
        worker = new Thread(this::loop, "agg-reconcile");
        worker.setDaemon(true);
        worker.start();
        log.info("[AGG-RECON] 갭 보정 워커 시작 (sweep {}s, grace {}s, 복구창 {}분)",
                SWEEP_MS / 1000, GRACE_MS / 1000, HISTORY_MIN);
    }

    @PreDestroy
    void stop() {
        running = false;
        if (worker != null) {
            worker.interrupt();
        }
    }

    private void loop() {
        if (!sleep(INITIAL_MS)) {
            return;
        }
        while (running) {
            try {
                sweep();
            } catch (Exception e) {
                log.warn("[AGG-RECON] sweep 실패: {}", e.toString());
            }
            if (!sleep(SWEEP_MS)) {
                return;
            }
        }
    }

    private void sweep() {
        if (banGuard.isPaused()) {
            return;
        }
        long now = System.currentTimeMillis();
        long graceCut = now - GRACE_MS;
        long floorTs = now - HISTORY_MS;
        for (String symbol : collect.symbols()) {
            try {
                reconcileSymbol(symbol, graceCut, floorTs);
            } catch (BinanceRestException e) {
                if (e.isBanned()) {
                    banGuard.banned("agg-reconcile");
                    return;   // 이번 sweep 전체 중단
                }
                log.warn("[AGG-RECON] {} REST 보정 보류(다음 sweep 재시도): {}", symbol, e.getMessage());
            } catch (Exception e) {
                log.warn("[AGG-RECON] {} 보정 실패: {}", symbol, e.toString());
            }
        }
    }

    private void reconcileSymbol(String symbol, long graceCut, long floorTs) {
        Long floorAggId = repository.minAggIdSince(symbol, floorTs);
        if (floorAggId == null) {
            return;   // 24h 복구창 내 데이터 없음
        }
        long floor = floorAggId - 1;
        // 24h 밖으로 처진 watermark는 floor로 끌어올림 — 복구 불가 갭 재스캔 stuck 방지.
        long w = Math.max(watermark.getOrDefault(symbol, floor), floor);

        AggIdScan scan = repository.scanAbove(symbol, w, floorTs, graceCut);
        if (scan.cnt() == 0 || scan.maxId() == null) {
            watermark.put(symbol, w);
            return;
        }
        long hi = scan.maxId();
        if (scan.cnt() == hi - w) {
            watermark.put(symbol, hi);   // (w, hi] 연속 — 전진만(REST 미호출)
            return;
        }

        // 빈틈 존재 → 누락 구간만 보정. 완전히 못 메운 갭에서 멈춰 watermark를 안 넘긴다(영구 누락 방지).
        List<AggIdGap> gaps = repository.findGaps(symbol, w, floorTs, graceCut);
        long advanceTo = hi;
        int filled = 0;
        for (AggIdGap g : gaps) {
            long expected = g.toId() - g.fromId() + 1;
            int got = fillRange(symbol, g.fromId(), g.toId());
            filled += got;
            if (got < expected) {
                advanceTo = g.fromId() - 1;   // 미완 → 이 갭부터 다음 sweep 재시도
                break;
            }
        }
        long newW = Math.max(w, advanceTo);
        watermark.put(symbol, newW);
        if (filled > 0) {
            log.info("[AGG-RECON] {} 갭 보정 +{}건 (watermark→{})", symbol, filled, newW);
        }
    }

    /** [fromId, toId] 누락 구간을 페이지네이션으로 fetch·적재. 반환=fetch 건수(=expected면 완전 보정). */
    private int fillRange(String symbol, long fromId, long toId) {
        int total = 0;
        long cursor = fromId;
        while (cursor <= toId) {
            int limit = (int) Math.min(PAGE, toId - cursor + 1);
            List<AggTradeRow> rows = fetchWithRetry(symbol, cursor, limit);
            if (rows.isEmpty()) {
                break;   // 해당 구간 미제공 → 호출 측이 미완으로 처리
            }
            repository.batchInsert(rows);
            total += rows.size();
            long lastId = rows.get(rows.size() - 1).aggId();
            if (lastId < cursor) {
                break;   // 전진 없음(이상 응답) — 무한루프 방지
            }
            cursor = lastId + 1;
            if (!sleep(THROTTLE_MS)) {
                break;
            }
        }
        return total;
    }

    /** aggTrades 호출 + 429 backoff 재시도. 418은 즉시 전파(sweep가 BanGuard로 중지). */
    private List<AggTradeRow> fetchWithRetry(String symbol, long fromId, int limit) {
        int attempt = 0;
        while (true) {
            try {
                return api.aggTrades(symbol, fromId, limit);
            } catch (BinanceRestException e) {
                if (e.isBanned() || ++attempt > MAX_RETRY) {
                    throw e;
                }
                if (!sleep((e.isRateLimited() ? RL_BACKOFF_MS : 1_000) * attempt)) {
                    throw e;
                }
            }
        }
    }

    private boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
