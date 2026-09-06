package com.autotrading.autotradingmarketdata.raw;

import com.autotrading.autotradingmarketdata.depth.DepthRows.Row;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * {@link DepthBuffer} 전용 flush 워커 — {@link RawPersister}와 같은 규율(비차단 drain, 배치 상한, 2회 실패 시 drop).
 * 10컬럼 × 5000행 = 50k 바인드 < 65535. {@code collect.depth.enabled=true}일 때만 활성.
 */
@Component
@ConditionalOnProperty(name = "collect.depth.enabled", havingValue = "true")
public class DepthPersister {

    private static final Logger log = LogManager.getLogger(DepthPersister.class);
    private static final int BATCH = 5_000;
    private static final long IDLE_MS = 150;

    private final DepthBuffer buffer;
    private final DepthRepository repository;
    private final AtomicLong written = new AtomicLong(0);
    private final AtomicLong lost = new AtomicLong(0);
    private volatile boolean running = true;
    private Thread worker;

    public DepthPersister(DepthBuffer buffer, DepthRepository repository, MeterRegistry registry) {
        this.buffer = buffer;
        this.repository = repository;
        Gauge.builder("depth.buffer.size", buffer, BatchBuffer::size)
                .description("depth_diff 버퍼 현재 깊이").register(registry);
        FunctionCounter.builder("depth.buffer.dropped", buffer, BatchBuffer::droppedCount)
                .description("버퍼 상한 초과로 버린 누적 행 수 — 증가 = 유실 진행 중").register(registry);
        FunctionCounter.builder("depth.persister.written", this, DepthPersister::writtenCount)
                .description("depth_diff 에 적재한 누적 행 수").register(registry);
        FunctionCounter.builder("depth.persister.lost", this, DepthPersister::lostCount)
                .description("적재 재시도 실패로 버린 누적 행 수 — 증가 = DB 쓰기 장애").register(registry);
    }

    @PostConstruct
    void start() {
        worker = new Thread(this::loop, "raw-flush-depth");
        worker.setDaemon(true);
        worker.start();
        log.info("[DEPTH] 전용 flush 워커 시작");
    }

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
            List<Row> rows = buffer.drain(BATCH);
            if (rows.isEmpty()) {
                sleep(IDLE_MS);
                continue;
            }
            if (tryInsert(rows) || tryInsert(rows)) {
                written.addAndGet(rows.size());
            } else {
                lost.addAndGet(rows.size());
                log.warn("[DEPTH] 적재 2회 실패 — {}건 drop(누적 손실 {})", rows.size(), lost.get());
            }
        }
    }

    private boolean tryInsert(List<Row> rows) {
        try {
            repository.batchInsert(rows);
            return true;
        } catch (Exception e) {
            log.debug("[DEPTH] 적재 실패: {}", e.toString());
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
