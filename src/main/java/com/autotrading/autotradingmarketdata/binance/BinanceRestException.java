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
    /** 418 본문의 {@code banned until <epoch ms>} — 없으면 0. */
    private final long bannedUntilMs;
    /** 이 요청이 속한 레이트리밋 양동이 — 밴/쿨다운을 양동이 단위로 걸기 위해 경로에서 판정한다. */
    private final RateBucket bucket;

    private static final String MARKER = "banned until ";

    public BinanceRestException(int statusCode, String message) {
        this(statusCode, message, 0, -1, RateBucket.FAPI);
    }

    public BinanceRestException(int statusCode, String message, long retryAfterSec, long usedWeight1m,
                                RateBucket bucket) {
        super(message);
        this.statusCode = statusCode;
        this.retryAfterSec = retryAfterSec;
        this.usedWeight1m = usedWeight1m;
        this.bucket = bucket == null ? RateBucket.FAPI : bucket;
        this.bannedUntilMs = parseBannedUntil(message);
    }

    /**
     * {@code /futures/data/*} 는 Retry-After 도 가중치 헤더도 주지 않는다(실측 2026-08-20).
     * 대신 418 본문이 정확한 해제 시각을 준다:
     * {@code {"code":-1003,"msg":"Way too many requests; IP(10.x.x.x) banned until 1787179851822. ..."}}
     * 실측된 밴은 <b>약 2시간</b>이었는데 수집기는 10분만 쉬고 재개해 매번 다시 두드렸고,
     * 그 재요청이 밴을 갱신해 7일간 494회의 되먹임이 됐다. 이 값이 그 고리를 끊는 유일한 근거다.
     */
    private static long parseBannedUntil(String message) {
        if (message == null) {
            return 0;
        }
        int i = message.indexOf(MARKER);
        if (i < 0) {
            return 0;
        }
        int j = i + MARKER.length();
        int k = j;
        while (k < message.length() && Character.isDigit(message.charAt(k))) {
            k++;
        }
        if (k - j < 10) {
            return 0;
        }
        try {
            return Long.parseLong(message.substring(j, k));
        } catch (NumberFormatException e) {
            return 0;
        }
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

    /** 418 본문이 알려준 해제 시각(epoch ms). 없으면 0. */
    public long bannedUntilMs() {
        return bannedUntilMs;
    }

    /** 이 요청이 속한 레이트리밋 양동이. */
    public RateBucket bucket() {
        return bucket;
    }

    public boolean isRateLimited() {
        return statusCode == 429;
    }

    public boolean isBanned() {
        return statusCode == 418;
    }
}
