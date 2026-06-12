package com.autotrading.autotradingmarketdata.futures;

import com.autotrading.autotradingmarketdata.binance.FuturesRows.BasisRow;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.FundingRow;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.LsRatioRow;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.OiHistRow;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.TakerRatioRow;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

/**
 * 파생 7종 적재 — 모든 insert가 자연키 ON CONFLICT DO NOTHING(멱등, 백필·폴링 공유).
 * resume 기준은 시리즈별 DB max(ts).
 */
@Repository
public class FuturesRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public FuturesRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insertOiHist(List<OiHistRow> rows) {
        batch("""
                INSERT INTO futures_open_interest_hist (symbol, ts, sum_oi, sum_oi_value)
                VALUES (:symbol, :ts, :sumOi, :sumOiValue)
                ON CONFLICT (symbol, ts) DO NOTHING
                """, rows, (p, r) -> p.addValue("symbol", r.symbol()).addValue("ts", ts(r.ts()))
                .addValue("sumOi", r.sumOi()).addValue("sumOiValue", r.sumOiValue()));
    }

    public void insertLsRatio(String table, List<LsRatioRow> rows) {
        // table은 코드 상수(enum)에서만 옴 — 식별자 보간 안전.
        batch("INSERT INTO " + table + """
                 (symbol, ts, long_short_ratio, long_account, short_account)
                VALUES (:symbol, :ts, :ratio, :longAcct, :shortAcct)
                ON CONFLICT (symbol, ts) DO NOTHING
                """, rows, (p, r) -> p.addValue("symbol", r.symbol()).addValue("ts", ts(r.ts()))
                .addValue("ratio", r.longShortRatio()).addValue("longAcct", r.longAccount())
                .addValue("shortAcct", r.shortAccount()));
    }

    public void insertTaker(List<TakerRatioRow> rows) {
        batch("""
                INSERT INTO futures_taker_ratio (symbol, ts, buy_sell_ratio, buy_vol, sell_vol)
                VALUES (:symbol, :ts, :ratio, :buyVol, :sellVol)
                ON CONFLICT (symbol, ts) DO NOTHING
                """, rows, (p, r) -> p.addValue("symbol", r.symbol()).addValue("ts", ts(r.ts()))
                .addValue("ratio", r.buySellRatio()).addValue("buyVol", r.buyVol()).addValue("sellVol", r.sellVol()));
    }

    public void insertBasis(List<BasisRow> rows) {
        batch("""
                INSERT INTO futures_basis (pair, contract_type, ts, futures_price, index_price, basis, basis_rate, annualized_rate)
                VALUES (:pair, 'PERPETUAL', :ts, :futuresPrice, :indexPrice, :basis, :basisRate, :annualizedRate)
                ON CONFLICT (pair, contract_type, ts) DO NOTHING
                """, rows, (p, r) -> p.addValue("pair", r.pair()).addValue("ts", ts(r.ts()))
                .addValue("futuresPrice", r.futuresPrice()).addValue("indexPrice", r.indexPrice())
                .addValue("basis", r.basis()).addValue("basisRate", r.basisRate())
                .addValue("annualizedRate", r.annualizedRate()));
    }

    public void insertFunding(List<FundingRow> rows) {
        batch("""
                INSERT INTO futures_funding_rate (symbol, funding_time, funding_rate, mark_price)
                VALUES (:symbol, :fundingTime, :fundingRate, :markPrice)
                ON CONFLICT (symbol, funding_time) DO NOTHING
                """, rows, (p, r) -> p.addValue("symbol", r.symbol()).addValue("fundingTime", ts(r.fundingTime()))
                .addValue("fundingRate", r.fundingRate()).addValue("markPrice", r.markPrice()));
    }

    /** 시리즈 resume 기준 — DB max(ts). table/tsCol/keyCol은 코드 상수에서만 온다(식별자 보간 안전). */
    public Optional<Instant> findMaxTs(String table, String tsCol, String keyCol, String key) {
        Timestamp max = jdbc.queryForObject(
                "SELECT max(" + tsCol + ") FROM " + table + " WHERE " + keyCol + " = :key",
                new MapSqlParameterSource().addValue("key", key),
                Timestamp.class);
        return Optional.ofNullable(max).map(Timestamp::toInstant);
    }

    private <T> void batch(String sql, List<T> rows, BiConsumer<MapSqlParameterSource, T> binder) {
        if (rows.isEmpty()) {
            return;
        }
        SqlParameterSource[] batch = rows.stream()
                .map(r -> {
                    MapSqlParameterSource p = new MapSqlParameterSource();
                    binder.accept(p, r);
                    return (SqlParameterSource) p;
                })
                .toArray(SqlParameterSource[]::new);
        jdbc.batchUpdate(sql, batch);
    }

    private static Timestamp ts(long epochMilli) {
        return Timestamp.from(Instant.ofEpochMilli(epochMilli));
    }
}
