package com.autotrading.autotradingmarketdata.depth;

import java.math.BigDecimal;

/**
 * 심볼별 정수 단위 — 가격은 {@code 10^-pricePrecision}, 수량은 {@code 10^-quantityPrecision} 의 정수배로 저장한다.
 *
 * <p>★ exchangeInfo 의 {@code PRICE_FILTER.tickSize} 는 실제 호가 해상도와 다를 수 있다
 * (실측 2026-09-06: SOLUSDT tickSize 0.01 인데 호가창에 105.0020). 자릿수(precision)는 모든 가격 문자열이
 * 그 정수배임을 거래소가 보장하므로 이것을 단위로 쓴다. 정수배가 아니면 예외 — 조용히 반올림하지 않는다.
 */
public record SymbolUnits(String symbol, int pricePrecision, int qtyPrecision, String tickSize, String stepSize) {

    public long priceTicks(String price) {
        return toUnits(price, pricePrecision);
    }

    public long qtySteps(String qty) {
        return toUnits(qty, qtyPrecision);
    }

    /** {@code price_ticks} → 가격(double). KV 발행용. */
    public double price(long ticks) {
        return BigDecimal.valueOf(ticks).movePointLeft(pricePrecision).doubleValue();
    }

    public double qty(long steps) {
        return BigDecimal.valueOf(steps).movePointLeft(qtyPrecision).doubleValue();
    }

    static long toUnits(String value, int precision) {
        BigDecimal scaled = new BigDecimal(value).movePointRight(precision);
        if (scaled.stripTrailingZeros().scale() > 0) {
            throw new IllegalArgumentException(value + " 는 10^-" + precision + " 의 정수배가 아님");
        }
        return scaled.longValueExact();
    }
}
