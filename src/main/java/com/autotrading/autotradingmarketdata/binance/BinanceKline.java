package com.autotrading.autotradingmarketdata.binance;

import java.math.BigDecimal;
import java.util.List;

/**
 * /fapi/v1/klines 응답 배열 1행. openTime/closeTime은 epoch ms.
 */
public record BinanceKline(
        long openTime,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume,
        long closeTime,
        BigDecimal quoteAssetVolume,
        long numberOfTrades,
        BigDecimal takerBuyBaseAssetVolume,
        BigDecimal takerBuyQuoteAssetVolume
) {

    public static BinanceKline from(List<Object> values) {
        if (values.size() < 11) {
            throw new IllegalArgumentException("Unexpected Binance kline payload: size=" + values.size());
        }
        return new BinanceKline(
                asLong(values.get(0)),
                asDecimal(values.get(1)),
                asDecimal(values.get(2)),
                asDecimal(values.get(3)),
                asDecimal(values.get(4)),
                asDecimal(values.get(5)),
                asLong(values.get(6)),
                asDecimal(values.get(7)),
                asLong(values.get(8)),
                asDecimal(values.get(9)),
                asDecimal(values.get(10))
        );
    }

    public boolean isClosedAt(long epochMilli) {
        return closeTime < epochMilli;
    }

    private static BigDecimal asDecimal(Object value) {
        return new BigDecimal(value.toString());
    }

    private static long asLong(Object value) {
        return Long.parseLong(value.toString());
    }
}
