package com.autotrading.autotradingmarketdata.binance;

/**
 * 바이낸스 REST 오류 — 상태코드를 보존해 호출 측이 재시도/중단을 판단한다.
 * 429 = rate limit(짧은 backoff 후 재시도 가능), 418 = IP ban(계속 두드리면 ban 연장 — 장기 중단 필수).
 */
public class BinanceRestException extends RuntimeException {

    private final int statusCode;

    public BinanceRestException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }

    public boolean isRateLimited() {
        return statusCode == 429;
    }

    public boolean isBanned() {
        return statusCode == 418;
    }
}
