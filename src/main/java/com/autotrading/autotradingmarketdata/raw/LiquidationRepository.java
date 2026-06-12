package com.autotrading.autotradingmarketdata.raw;

import com.autotrading.autotradingmarketdata.binance.FuturesRows.LiquidationEvent;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

/** binance_liquidations 적재 — 자연키 ON CONFLICT DO NOTHING(재큐잉 재시도와 호환되는 멱등). */
@Repository
public class LiquidationRepository {

    private static final String INSERT_SQL = """
            INSERT INTO binance_liquidations (symbol, side, price, qty, quote_qty, trade_time)
            VALUES (:symbol, :side, :price, :qty, :quoteQty, :tradeTime)
            ON CONFLICT ON CONSTRAINT uq_liquidation DO NOTHING
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public LiquidationRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void batchInsert(List<LiquidationEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        SqlParameterSource[] batch = events.stream()
                .map(e -> (SqlParameterSource) new MapSqlParameterSource()
                        .addValue("symbol", e.symbol())
                        .addValue("side", e.side())
                        .addValue("price", e.price())
                        .addValue("qty", e.qty())
                        .addValue("quoteQty", e.quoteQty())
                        .addValue("tradeTime", Timestamp.from(Instant.ofEpochMilli(e.tradeTime()))))
                .toArray(SqlParameterSource[]::new);
        jdbc.batchUpdate(INSERT_SQL, batch);
    }
}
