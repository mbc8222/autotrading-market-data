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
    public static final String AGG_TRADE_STREAM_KEY = "market:aggTrade";
    private static final String LAST_PRICE_KEY_PREFIX = "market:last-price:";
    private static final String MARK_PRICE_KEY_PREFIX = "market:mark-price:";
    private static final String ORDER_BOOK_KEY_PREFIX = "market:orderbook:";
    private static final long KLINE_STREAM_MAX_LENGTH = 100_000;
    private static final long LIQUIDATION_STREAM_MAX_LENGTH = 50_000;
    // aggTrade는 고빈도(초당 수백~수천) — CVD 롤링 윈도우 + 여유분만 보존. 소비자(분석 CVD)는 멱등키(aggId)로 중복 무시.
    private static final long AGG_TRADE_STREAM_MAX_LENGTH = 500_000;
    private static final long AGG_TRADE_TRIM_EVERY = 2_000;

    private final StringRedisTemplate redis;
    // 고빈도 경로 — 매 XADD마다 trim하면 round-trip이 2배라, N건마다 한 번만 approximate trim.
    private final java.util.concurrent.atomic.AtomicLong aggTradeAddCount = new java.util.concurrent.atomic.AtomicLong();

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

    /**
     * 원시 체결 1건을 Stream에 발행 — 분석 서비스의 실시간 CVD/매물대 누적용.
     * 자연키(aggId)를 멱등키로 소비자가 중복을 무시한다. 고빈도라 trim은 {@value #AGG_TRADE_TRIM_EVERY}건마다.
     */
    public void publishAggTrade(String symbol, long aggId, double price, double qty,
                                boolean buyerMaker, long tradeTime) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("symbol", symbol);
        fields.put("aggId", String.valueOf(aggId));
        fields.put("price", String.valueOf(price));
        fields.put("qty", String.valueOf(qty));
        fields.put("isBuyerMaker", buyerMaker ? "1" : "0");
        fields.put("tradeTime", String.valueOf(tradeTime));
        redis.opsForStream().add(AGG_TRADE_STREAM_KEY, fields);
        if (aggTradeAddCount.incrementAndGet() % AGG_TRADE_TRIM_EVERY == 0) {
            redis.opsForStream().trim(AGG_TRADE_STREAM_KEY, AGG_TRADE_STREAM_MAX_LENGTH, true);
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
