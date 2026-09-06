package com.autotrading.autotradingmarketdata.raw;

import com.autotrading.autotradingmarketdata.depth.DepthRows.Row;
import com.autotrading.autotradingmarketdata.depth.SymbolUnits;
import java.sql.Timestamp;
import java.util.List;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

/** {@code depth_diff}(원시 차분) · {@code depth_sync_events} · {@code depth_symbol_units}. */
@Repository
public class DepthRepository {

    private static final String INSERT_SQL = """
            INSERT INTO depth_diff (symbol, event_time, recv_time, kind, side, price_ticks, qty_steps, u, first_u, prev_u)
            VALUES (:symbol, :eventTime, :recvTime, :kind, :side, :priceTicks, :qtySteps, :u, :firstU, :prevU)
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public DepthRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void batchInsert(List<Row> rows) {
        if (rows.isEmpty()) {
            return;
        }
        SqlParameterSource[] batch = rows.stream()
                .map(r -> (SqlParameterSource) new MapSqlParameterSource()
                        .addValue("symbol", r.symbol())
                        .addValue("eventTime", new Timestamp(r.eventMs()))
                        .addValue("recvTime", new Timestamp(r.recvMs()))
                        .addValue("kind", String.valueOf(r.kind()))
                        .addValue("side", String.valueOf(r.side()))
                        .addValue("priceTicks", r.priceTicks())
                        .addValue("qtySteps", r.qtySteps())
                        .addValue("u", r.u())
                        .addValue("firstU", r.firstU())
                        .addValue("prevU", r.prevU()))
                .toArray(SqlParameterSource[]::new);
        jdbc.batchUpdate(INSERT_SQL, batch);
    }

    public void insertEvent(String symbol, String kind, String detail) {
        jdbc.update("INSERT INTO depth_sync_events (symbol, kind, detail) VALUES (:symbol, :kind, :detail)",
                new MapSqlParameterSource().addValue("symbol", symbol).addValue("kind", kind)
                        .addValue("detail", detail));
    }

    public void upsertUnits(SymbolUnits u) {
        jdbc.update("""
                        INSERT INTO depth_symbol_units (symbol, price_precision, qty_precision, tick_size, step_size)
                        VALUES (:symbol, :pp, :qp, CAST(:tick AS numeric), CAST(:step AS numeric))
                        ON CONFLICT (symbol) DO UPDATE SET price_precision = EXCLUDED.price_precision,
                            qty_precision = EXCLUDED.qty_precision, tick_size = EXCLUDED.tick_size,
                            step_size = EXCLUDED.step_size, updated_at = now()
                        """,
                new MapSqlParameterSource().addValue("symbol", u.symbol()).addValue("pp", u.pricePrecision())
                        .addValue("qp", u.qtyPrecision()).addValue("tick", u.tickSize()).addValue("step", u.stepSize()));
    }
}
