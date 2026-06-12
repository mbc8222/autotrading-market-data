package com.autotrading.autotradingmarketdata.publish;

import com.autotrading.autotradingmarketdata.binance.BinanceKline;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.LiquidationEvent;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Redis 채널 발행 — 아키텍처 채널 규칙(2026-06-10):
 * <ul>
 *   <li>시장이벤트(닫힌 캔들·청산)는 Stream에 1회 기록 (완전성·순서, 이중 publish 금지).
 *       소비자는 자연키를 멱등키로 중복을 무시해야 한다.</li>
 *   <li>핫상태(최신가·마크가격·호가 top)는 key-value (유실 OK, 최신값만 의미).</li>
 * </ul>
 * raw aggTrade는 Stream 미발행 — 소비자(분석)가 정의되기 전까지 DB만(소비자 없는 채널 금지 원칙).
 */
@Component
public class MarketDataPublisher {

    public static final String KLINE_STREAM_KEY = "market:kline";
    public static final String LIQUIDATION_STREAM_KEY = "market:liquidation";
    private static final String LAST_PRICE_KEY_PREFIX = "market:last-price:";
    private static final String MARK_PRICE_KEY_PREFIX = "market:mark-price:";
    private static final String ORDER_BOOK_KEY_PREFIX = "market:orderbook:";
    private static final long KLINE_STREAM_MAX_LENGTH = 100_000;
    private static final long LIQUIDATION_STREAM_MAX_LENGTH = 50_000;

    private final StringRedisTemplate redis;

    public MarketDataPublisher(StringRedisTemplate redis) {
        this.redis = redis;
    }

    // ── Stream (완전성·순서) ──

    public void publishClosedKlines(String symbol, String interval, List<BinanceKline> closedKlines) {
        for (BinanceKline k : closedKlines) {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("symbol", symbol);
            fields.put("interval", interval);
            fields.put("openTime", String.valueOf(k.openTime()));
            fields.put("closeTime", String.valueOf(k.closeTime()));
            fields.put("open", k.open().toPlainString());
            fields.put("high", k.high().toPlainString());
            fields.put("low", k.low().toPlainString());
            fields.put("close", k.close().toPlainString());
            fields.put("volume", k.volume().toPlainString());
            redis.opsForStream().add(KLINE_STREAM_KEY, fields);
        }
        if (!closedKlines.isEmpty()) {
            redis.opsForStream().trim(KLINE_STREAM_KEY, KLINE_STREAM_MAX_LENGTH, true);
        }
    }

    public void publishLiquidations(List<LiquidationEvent> events) {
        for (LiquidationEvent e : events) {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("symbol", e.symbol());
            fields.put("side", e.side());
            fields.put("price", String.valueOf(e.price()));
            fields.put("qty", String.valueOf(e.qty()));
            fields.put("quoteQty", String.valueOf(e.quoteQty()));
            fields.put("tradeTime", String.valueOf(e.tradeTime()));
            redis.opsForStream().add(LIQUIDATION_STREAM_KEY, fields);
        }
        if (!events.isEmpty()) {
            redis.opsForStream().trim(LIQUIDATION_STREAM_KEY, LIQUIDATION_STREAM_MAX_LENGTH, true);
        }
    }

    // ── key-value 핫상태 (유실 OK) ──

    public void publishLastPrice(String symbol, BinanceKline latest) {
        redis.opsForValue().set(LAST_PRICE_KEY_PREFIX + symbol, latest.close().toPlainString());
    }

    public void publishMarkPrice(String symbol, double markPrice, double indexPrice,
                                 double fundingRate, long nextFundingTime, long eventTime) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("markPrice", String.valueOf(markPrice));
        fields.put("indexPrice", String.valueOf(indexPrice));
        fields.put("fundingRate", String.valueOf(fundingRate));
        fields.put("nextFundingTime", String.valueOf(nextFundingTime));
        fields.put("eventTime", String.valueOf(eventTime));
        redis.opsForHash().putAll(MARK_PRICE_KEY_PREFIX + symbol, fields);
    }

    public void publishOrderBookTop(String symbol, double bestBid, double bestAsk,
                                    double bidQty, double askQty, double imbalance, long eventTime) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("bestBid", String.valueOf(bestBid));
        fields.put("bestAsk", String.valueOf(bestAsk));
        fields.put("bidQty", String.valueOf(bidQty));
        fields.put("askQty", String.valueOf(askQty));
        fields.put("imbalance", String.valueOf(imbalance));
        fields.put("eventTime", String.valueOf(eventTime));
        redis.opsForHash().putAll(ORDER_BOOK_KEY_PREFIX + symbol, fields);
    }
}
