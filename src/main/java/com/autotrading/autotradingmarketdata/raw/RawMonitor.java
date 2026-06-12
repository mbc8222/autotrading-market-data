package com.autotrading.autotradingmarketdata.raw;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * raw 적재 파이프라인 모니터 (모놀리스 RawMonitor 이식 — Discord 경보 대신 메트릭+로그).
 *
 * <p>이 파이프라인의 고장 모드는 "조용한 유실" — WS 수신을 절대 막지 않는 설계라 DB가 느려지면
 * 예외 없이 버퍼가 차오르다 drop이 시작된다. 버퍼 점유율 상승이 유실의 선행지표.
 *
 * <ul>
 *   <li>메트릭(Prometheus): {@code raw.buffer.size/offered/dropped}, {@code raw.persister.written/lost}
 *       — 속도(in/out rate)는 조회 측이 {@code rate()}로 계산.</li>
 *   <li>로그: {@value #SAMPLE_MS}ms 샘플로 구간 피크(hwm)를 잡고 {@value #REPORT_MS}ms마다 구조화 한 줄.
 *       점유율 {@value #WARN_PCT}% 이상·drop/lost 증가 시 WARN/ERROR.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "collect.raw.enabled", havingValue = "true")
public class RawMonitor {

    private static final Logger log = LogManager.getLogger(RawMonitor.class);

    private static final long SAMPLE_MS = 1_000;
    private static final long REPORT_MS = 15_000;
    private static final int WARN_PCT = 50;

    private final AggTradeBuffer buffer;
    private final RawPersister persister;

    private volatile int hwm = 0;
    private long lastOffered = 0;
    private long lastWritten = 0;
    private long lastDropped = 0;
    private long lastLost = 0;
    private long lastReportMs = System.currentTimeMillis();

    public RawMonitor(AggTradeBuffer buffer, RawPersister persister, MeterRegistry registry) {
        this.buffer = buffer;
        this.persister = persister;

        Gauge.builder("raw.buffer.size", buffer, BatchBuffer::size)
                .description("agg_trade 버퍼 현재 깊이")
                .register(registry);
        FunctionCounter.builder("raw.buffer.offered", buffer, BatchBuffer::offeredCount)
                .description("버퍼에 들어온 누적 행 수 (WS 수신·유효성 통과분)")
                .register(registry);
        FunctionCounter.builder("raw.buffer.dropped", buffer, BatchBuffer::droppedCount)
                .description("버퍼 상한 초과로 버린 누적 행 수 — 증가 = 유실 진행 중")
                .register(registry);
        FunctionCounter.builder("raw.persister.written", persister, RawPersister::writtenCount)
                .description("DB에 적재한 누적 행 수")
                .register(registry);
        FunctionCounter.builder("raw.persister.lost", persister, RawPersister::lostCount)
                .description("적재 재시도 실패로 버린 누적 행 수 — 증가 = DB 쓰기 장애")
                .register(registry);
    }

    /** 구간 피크 샘플(빠른 주기) — 15s 평균에 묻히는 순간 포화를 잡는다. */
    @Scheduled(fixedDelayString = "1s")
    void sample() {
        int size = buffer.size();
        if (size > hwm) {
            hwm = size;
        }
    }

    /** 구조화 health 로그 + 임계 감지. 예외는 던지지 않는다(scheduled 정지 방지 — 산술뿐이라 사실상 무위험). */
    @Scheduled(fixedDelayString = "15s", initialDelayString = "15s")
    void report() {
        long now = System.currentTimeMillis();
        double dtSec = Math.max(0.001, (now - lastReportMs) / 1000.0);

        long offered = buffer.offeredCount();
        long written = persister.writtenCount();
        long dropped = buffer.droppedCount();
        long lost = persister.lostCount();
        long inRate = Math.round((offered - lastOffered) / dtSec);
        long outRate = Math.round((written - lastWritten) / dtSec);
        int pct = (int) (100L * hwm / buffer.capacity());

        log.info("[RAW-MON] agg[size={} hwm={}({}%) in={}/s out={}/s drop={}] lost={}",
                buffer.size(), hwm, pct, inRate, outRate, dropped, lost);

        if (pct >= WARN_PCT) {
            log.warn("[RAW-MON] 버퍼 점유 {}% (hwm={}) — drain 지연, 유실 임박 선행지표", pct, hwm);
        }
        if (dropped > lastDropped) {
            log.error("[RAW-MON] 유실 발생 — drop +{} (누적 {}). DB 적재가 유입을 못 따라감",
                    dropped - lastDropped, dropped);
        }
        if (lost > lastLost) {
            log.error("[RAW-MON] 적재 실패 유실 — lost +{} (누적 {}). DB 쓰기 오류 확인",
                    lost - lastLost, lost);
        }

        lastOffered = offered;
        lastWritten = written;
        lastDropped = dropped;
        lastLost = lost;
        hwm = 0;
        lastReportMs = now;
    }
}
