package com.autotrading.autotradingmarketdata.binance;

/**
 * 바이낸스 REST 오류 — 상태코드를 보존해 호출 측이 재시도/중단을 판단한다.
 * 429 = rate limit(짧은 backoff 후 재시도 가능), 418 = IP ban(계속 두드리면 ban 연장 — 장기 중단 필수).
 *
 * <p>★{@code Retry-After} 를 함께 실어 나른다(2026-08-20). 바이낸스 418 은 재범 시 밴 기간이
 * 단계적으로 늘어나는데(2분 → … → 3일), 고정 10분 중지로 재개하면 실제 밴이 아직 안 풀린 상태에서
 * 다시 두드려 밴이 연장된다. 7일간 494회 밴이 이 되먹임의 결과였다. 서버가 알려주는 해제 시각을
 * 그대로 따르는 것이 유일하게 정확한 값이다.
 */
public class BinanceRestException extends RuntimeException {

    private final int statusCode;
    /** 응답의 Retry-After(초). 없으면 0. */
    private final long retryAfterSec;
    /** X-MBX-USED-WEIGHT-1M — 분당 사용 가중치. 없으면 -1. */
    private final long usedWeight1m;

    public BinanceRestException(int statusCode, String message) {
        this(statusCode, message, 0, -1);
    }

    public BinanceRestException(int statusCode, String message, long retryAfterSec, long usedWeight1m) {
        super(message);
        this.statusCode = statusCode;
        this.retryAfterSec = retryAfterSec;
        this.usedWeight1m = usedWeight1m;
    }

    public int statusCode() {
        return statusCode;
    }

    public long retryAfterSec() {
        return retryAfterSec;
    }

    public long usedWeight1m() {
        return usedWeight1m;
    }

    public boolean isRateLimited() {
        return statusCode == 429;
    }

    public boolean isBanned() {
        return statusCode == 418;
    }
}
