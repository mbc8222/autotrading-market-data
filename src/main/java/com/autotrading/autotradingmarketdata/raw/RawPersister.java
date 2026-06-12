package com.autotrading.autotradingmarketdata.raw;

import com.autotrading.autotradingmarketdata.binance.FuturesRows.AggTradeRow;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 원시 버퍼(aggTrade)를 전용 데몬 워커가 연속 drain → 배치 적재 (모놀리스 이식).
 *
 * <p>공유 스케줄러와 무관(REST 폴링과 경합 없음), tick 게이팅 없이 DB가 받는 만큼 연속 적재.
 * 한 INSERT의 바인드 파라미터가 PostgreSQL 한도(65535)를 넘지 않도록 배치를 {@value #BATCH}건으로 제한.
 * 적재 1회 재시도 후 실패분은 drop(손실 카운트). {@code collect.raw.enabled=true}일 때만 활성.
 */
@Component
@ConditionalOnProperty(name = "collect.raw.enabled", havingValue = "true")
public class RawPersister {

    private static final Logger log = LogManager.getLogger(RawPersister.class);
    private static final int BATCH = 5_000;     // 5000 × 8컬럼 = 40k params < 65535
    private static final long IDLE_MS = 150;    // 버퍼 빌 때 busy-spin 방지

    private final AggTradeBuffer buffer;
    private final AggTradeRepository repository;

    private final AtomicLong written = new AtomicLong(0);
    private final AtomicLong lost = new AtomicLong(0);

    private volatile boolean running = true;
    private Thread worker;

    public RawPersister(AggTradeBuffer buffer, AggTradeRepository repository) {
        this.buffer = buffer;
        this.repository = repository;
    }

    @PostConstruct
    void start() {
        worker = new Thread(this::loop, "raw-flush-agg");
        worker.setDaemon(true);
        worker.start();
        log.info("[RAW] 전용 flush 워커 시작");
    }

    /** 종료 시 남은 버퍼를 best-effort로 비우고 정리. */
    @PreDestroy
    void stop() {
        running = false;
        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void loop() {
        while (running || buffer.size() > 0) {
            List<AggTradeRow> rows = buffer.drain(BATCH);
            if (rows.isEmpty()) {
                sleep(IDLE_MS);
                continue;
            }
            if (tryInsert(rows) || tryInsert(rows)) {
                written.addAndGet(rows.size());
            } else {
                lost.addAndGet(rows.size());
                log.warn("[RAW] 적재 2회 실패 — {}건 drop(누적 손실 {})", rows.size(), lost.get());
            }
        }
    }

    private boolean tryInsert(List<AggTradeRow> rows) {
        try {
            repository.batchInsert(rows);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long writtenCount() {
        return written.get();
    }

    public long lostCount() {
        return lost.get();
    }
}
