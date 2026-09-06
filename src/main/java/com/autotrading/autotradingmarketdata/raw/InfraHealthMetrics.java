package com.autotrading.autotradingmarketdata.raw;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 인프라(PostgreSQL·Redis) 상태를 수집기가 직접 잰다 — 별도 exporter 컨테이너 대체(2026-09-06).
 *
 * <p>감시 스택을 Prometheus+Grafana 둘로 줄이기 위한 것. 수집기가 인프라에 못 붙으면 그것이 곧 수집 장애이므로
 * 여기서 재는 것이 맞고, 수집기 자체가 죽으면 {@code up{job="market-data"}} 가 잡는다.
 * 10s 주기, 각 검사는 실패해도 다른 검사를 막지 않는다. 게이지는 스크레이프 시점의 마지막 검사 결과.
 */
@Component
public class InfraHealthMetrics {

    private static final Logger log = LogManager.getLogger(InfraHealthMetrics.class);

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;

    private final AtomicInteger pgUp = new AtomicInteger(0);
    private final AtomicInteger redisUp = new AtomicInteger(0);
    private final AtomicLong pgConnections = new AtomicLong(-1);
    private final AtomicLong pgMaxConnections = new AtomicLong(-1);
    private final AtomicLong redisUsedMemory = new AtomicLong(-1);

    public InfraHealthMetrics(JdbcTemplate jdbc, StringRedisTemplate redis, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.redis = redis;
        Gauge.builder("infra.postgres.up", pgUp, AtomicInteger::get)
                .description("PostgreSQL SELECT 1 성공(1) — 수집기 기준").register(registry);
        Gauge.builder("infra.redis.up", redisUp, AtomicInteger::get)
                .description("Redis PING 성공(1) — 수집기 기준").register(registry);
        Gauge.builder("infra.postgres.connections", pgConnections, AtomicLong::get)
                .description("pg_stat_activity 연결 수(-1=미측정)").register(registry);
        Gauge.builder("infra.postgres.max.connections", pgMaxConnections, AtomicLong::get)
                .description("max_connections 설정값(-1=미측정)").register(registry);
        Gauge.builder("infra.redis.used.memory.bytes", redisUsedMemory, AtomicLong::get)
                .description("Redis used_memory (-1=미측정)").register(registry);
    }

    @Scheduled(fixedDelayString = "10s", initialDelayString = "5s")
    void probe() {
        try {
            Long[] r = jdbc.queryForObject(
                    "SELECT ARRAY[(SELECT count(*) FROM pg_stat_activity), current_setting('max_connections')::bigint]",
                    (rs, n) -> (Long[]) rs.getArray(1).getArray());
            pgUp.set(1);
            if (r != null && r.length == 2) {
                pgConnections.set(r[0]);
                pgMaxConnections.set(r[1]);
            }
        } catch (Exception e) {
            pgUp.set(0);
            log.warn("[INFRA] PostgreSQL 검사 실패: {}", e.toString());
        }
        try {
            Properties info = redis.getRequiredConnectionFactory().getConnection().serverCommands().info("memory");
            redisUp.set(1);
            String used = info == null ? null : info.getProperty("used_memory");
            redisUsedMemory.set(used == null ? -1 : Long.parseLong(used.trim()));
        } catch (Exception e) {
            redisUp.set(0);
            log.warn("[INFRA] Redis 검사 실패: {}", e.toString());
        }
    }
}
