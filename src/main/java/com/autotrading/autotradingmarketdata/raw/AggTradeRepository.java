package com.autotrading.autotradingmarketdata.raw;

import com.autotrading.autotradingmarketdata.binance.FuturesRows.AggTradeRow;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

/**
 * agg_trade 적재 + 갭 탐지 SQL (모놀리스 agg_trade.xml 이식).
 * 적재는 ON CONFLICT (symbol, agg_id, trade_time) DO NOTHING — WS·REST 보정 공유 멱등.
 */
@Repository
public class AggTradeRepository {

    /** (afterAggId, ~] 구간 건수·최대 agg_id — cnt == maxId-afterAggId면 연속(선두 갭 포함 판정). */
    public record AggIdScan(long cnt, Long maxId) {
    }

    /** 누락 agg_id 연속 구간 [fromId, toId]. */
    public record AggIdGap(long fromId, long toId) {
    }

    private static final String INSERT_SQL = """
            INSERT INTO agg_trade (symbol, agg_id, price, qty, first_trade_id, last_trade_id, trade_time, is_buyer_maker)
            VALUES (:symbol, :aggId, :price, :qty, :firstTradeId, :lastTradeId, :tradeTime, :buyerMaker)
            ON CONFLICT (symbol, agg_id, trade_time) DO NOTHING
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public AggTradeRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void batchInsert(List<AggTradeRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        SqlParameterSource[] batch = rows.stream()
                .map(r -> (SqlParameterSource) new MapSqlParameterSource()
                        .addValue("symbol", r.symbol())
                        .addValue("aggId", r.aggId())
                        .addValue("price", r.price())
                        .addValue("qty", r.qty())
                        .addValue("firstTradeId", r.firstTradeId())
                        .addValue("lastTradeId", r.lastTradeId())
                        .addValue("tradeTime", ts(r.tradeTime()))
                        .addValue("buyerMaker", r.buyerMaker()))
                .toArray(SqlParameterSource[]::new);
        jdbc.batchUpdate(INSERT_SQL, batch);
    }

    /**
     * 체결시각 {@code sinceTs} 이상 구간의 floor agg_id(가장 이른 체결). 없으면 empty.
     * agg_id↔trade_time 단조성으로 24h 복구창 하한을 인덱스 LIMIT 1로 싸게 얻는다.
     */
    public Long minAggIdSince(String symbol, long sinceTs) {
        List<Long> rows = jdbc.query("""
                        SELECT agg_id FROM agg_trade
                        WHERE symbol = :symbol AND trade_time >= :sinceTs
                        ORDER BY trade_time ASC, agg_id ASC
                        LIMIT 1
                        """,
                new MapSqlParameterSource().addValue("symbol", symbol).addValue("sinceTs", ts(sinceTs)),
                (rs, n) -> rs.getLong(1));
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** watermark 초과 ~ [floorTs, beforeTs] 구간의 건수·최대 agg_id. floorTs로 파티션 프루닝. */
    public AggIdScan scanAbove(String symbol, long afterAggId, long floorTs, long beforeTs) {
        return jdbc.queryForObject("""
                        SELECT count(*) AS cnt, max(agg_id) AS max_id
                        FROM agg_trade
                        WHERE symbol = :symbol AND agg_id > :afterAggId
                          AND trade_time >= :floorTs AND trade_time <= :beforeTs
                        """,
                new MapSqlParameterSource()
                        .addValue("symbol", symbol).addValue("afterAggId", afterAggId)
                        .addValue("floorTs", ts(floorTs)).addValue("beforeTs", ts(beforeTs)),
                (rs, n) -> new AggIdScan(rs.getLong("cnt"),
                        rs.getObject("max_id") == null ? null : rs.getLong("max_id")));
    }

    /**
     * 누락 agg_id 연속 구간들(오름차순). 첫 행의 lag 기본값을 watermark로 둬
     * [watermark+1, 첫행-1] 선두 갭도 잡는다(WS가 watermark 직후를 drop한 케이스).
     */
    public List<AggIdGap> findGaps(String symbol, long afterAggId, long floorTs, long beforeTs) {
        return jdbc.query("""
                        SELECT prev_id + 1 AS from_id, agg_id - 1 AS to_id
                        FROM (
                            SELECT agg_id, lag(agg_id, 1, :afterAggId) OVER (ORDER BY agg_id) AS prev_id
                            FROM agg_trade
                            WHERE symbol = :symbol AND agg_id > :afterAggId
                              AND trade_time >= :floorTs AND trade_time <= :beforeTs
                        ) t
                        WHERE agg_id - prev_id > 1
                        ORDER BY from_id
                        """,
                new MapSqlParameterSource()
                        .addValue("symbol", symbol).addValue("afterAggId", afterAggId)
                        .addValue("floorTs", ts(floorTs)).addValue("beforeTs", ts(beforeTs)),
                (rs, n) -> new AggIdGap(rs.getLong("from_id"), rs.getLong("to_id")));
    }

    private static Timestamp ts(long epochMilli) {
        return Timestamp.from(Instant.ofEpochMilli(epochMilli));
    }
}
