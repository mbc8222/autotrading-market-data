package com.autotrading.autotradingmarketdata.kline;

import com.autotrading.autotradingmarketdata.binance.BinanceKline;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

/**
 * binance_klines 적재 — ON CONFLICT upsert로 forming 봉 갱신과 재수집 멱등성을 동시에 보장.
 * DB 접근은 spring-jdbc 표준 도구 사용 (docs/adr/002).
 */
@Repository
public class KlineRepository {

    private static final String UPSERT_SQL = """
            INSERT INTO binance_klines (
                symbol, interval, open_time, open_price, high_price, low_price, close_price,
                volume, close_time, quote_asset_volume, number_of_trades,
                taker_buy_base_asset_volume, taker_buy_quote_asset_volume, is_closed
            ) VALUES (
                :symbol, :interval, :openTime, :open, :high, :low, :close,
                :volume, :closeTime, :quoteAssetVolume, :numberOfTrades,
                :takerBuyBase, :takerBuyQuote, :isClosed
            )
            ON CONFLICT (symbol, interval, open_time) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                close_time = EXCLUDED.close_time,
                quote_asset_volume = EXCLUDED.quote_asset_volume,
                number_of_trades = EXCLUDED.number_of_trades,
                taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
                taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
                is_closed = EXCLUDED.is_closed,
                updated_at = now()
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public KlineRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void upsertAll(String symbol, String interval, List<BinanceKline> klines, long nowEpochMilli) {
        if (klines.isEmpty()) {
            return;
        }
        SqlParameterSource[] batch = klines.stream()
                .map(k -> new MapSqlParameterSource()
                        .addValue("symbol", symbol)
                        .addValue("interval", interval)
                        .addValue("openTime", Timestamp.from(Instant.ofEpochMilli(k.openTime())))
                        .addValue("open", k.open())
                        .addValue("high", k.high())
                        .addValue("low", k.low())
                        .addValue("close", k.close())
                        .addValue("volume", k.volume())
                        .addValue("closeTime", Timestamp.from(Instant.ofEpochMilli(k.closeTime())))
                        .addValue("quoteAssetVolume", k.quoteAssetVolume())
                        .addValue("numberOfTrades", k.numberOfTrades())
                        .addValue("takerBuyBase", k.takerBuyBaseAssetVolume())
                        .addValue("takerBuyQuote", k.takerBuyQuoteAssetVolume())
                        .addValue("isClosed", k.isClosedAt(nowEpochMilli)))
                .toArray(SqlParameterSource[]::new);
        jdbc.batchUpdate(UPSERT_SQL, batch);
    }

    /** 행 기반 resume의 기준점 — 마지막으로 저장된 봉의 open_time(보통 forming 봉). */
    public Optional<Instant> findMaxOpenTime(String symbol, String interval) {
        Timestamp max = jdbc.queryForObject(
                "SELECT max(open_time) FROM binance_klines WHERE symbol = :symbol AND interval = :interval",
                new MapSqlParameterSource()
                        .addValue("symbol", symbol)
                        .addValue("interval", interval),
                Timestamp.class);
        return Optional.ofNullable(max).map(Timestamp::toInstant);
    }
}
