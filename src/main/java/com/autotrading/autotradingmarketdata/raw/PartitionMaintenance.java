package com.autotrading.autotradingmarketdata.raw;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * agg_trade 일별 파티션 유지 — [어제~모레] 앞당겨 생성.
 * 자식 파티션({@code agg_trade_pYYYYMMDD})이 없으면 해당 시각 INSERT가 실패하므로 기동 시 + 6h마다 보장.
 * {@code collect.raw.enabled=true}일 때만 활성. DDL 실패는 삼킨다(기동/스케줄 정지 방지).
 * 모든 식별자·범위는 코드 계산값만 사용(injection 무관).
 *
 * <p>★ 삭제는 이 클래스가 하지 않는다(2026-09-06). 파티션 DROP 의 유일한 주체는 cold-export 컨테이너
 * (PycharmProjects/autotrading-cold-export)이며, Parquet 이관·정수 검증이 끝난 파티션만 지운다.
 * 예전 "90일 경과 무조건 DROP"은 이관 여부를 보지 않아 Parquet 없이 데이터를 잃을 수 있어 제거했다.
 * 이관이 멈추면 핫 파티션이 쌓일 뿐 손실은 없다(설계 정본: obsidian AutoTrading_콜드저장_설계_2026-09-06).
 */
@Component
@ConditionalOnProperty(name = "collect.raw.enabled", havingValue = "true")
public class PartitionMaintenance {

    private static final Logger log = LogManager.getLogger(PartitionMaintenance.class);

    private static final long DAY_MS = 86_400_000L;
    /** 일별 파티션 부모들 — agg_trade(trade_time) · depth_diff(event_time, 2026-09-06). */
    private static final List<String> PARENTS = List.of("agg_trade", "depth_diff");
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final JdbcTemplate jdbc;

    public PartitionMaintenance(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // ApplicationReady라 Flyway(부모 테이블) 완료 후 실행. @Order로 WS 연결(WebSocketStarter)보다 먼저.
    @EventListener(ApplicationReadyEvent.class)
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public void onReady() {
        maintain();
    }

    // 주기 유지 + 기동 시 실패(DB 미준비) 대비 5분 후 재시도.
    @Scheduled(fixedDelayString = "6h", initialDelayString = "5m")
    void scheduled() {
        maintain();
    }

    private void maintain() {
        long today = System.currentTimeMillis() / DAY_MS;
        for (String parent : PARENTS) {
            for (long day = today - 1; day <= today + 2; day++) {
                ensure(parent, day);
            }
        }
    }

    private void ensure(String parent, long epochDay) {
        String name = partitionName(parent, epochDay);
        try {
            jdbc.execute("CREATE TABLE IF NOT EXISTS " + name + " PARTITION OF " + parent
                    + " FOR VALUES FROM ('" + isoUtc(epochDay) + "') TO ('" + isoUtc(epochDay + 1) + "')");
        } catch (Exception e) {
            log.warn("[PARTITION] {} 생성 실패: {}", name, e.getMessage());
        }
    }

    private static String partitionName(String parent, long epochDay) {
        return parent + "_p" + LocalDate.ofEpochDay(epochDay).format(DAY);
    }

    private static String isoUtc(long epochDay) {
        return Instant.ofEpochMilli(epochDay * DAY_MS).toString();   // 예: 2026-06-11T00:00:00Z
    }
}
