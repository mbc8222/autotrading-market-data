package com.autotrading.autotradingmarketdata.binance;

import java.util.function.Supplier;
import org.apache.logging.log4j.Logger;

/**
 * REST 호출 공통 재시도.
 *
 * <p>418(ban)은 재시도하지 않고 즉시 전파한다(호출 측이 {@link BinanceBanGuard} 로 장기 중지).
 *
 * <p>★429는 <b>양동이에 따라 다르게</b> 다룬다(2026-08-21). 종전에는 양동이 구분 없이 5회
 * 재시도했는데, 공유 양동이({@code /futures/data/basis})에서는 그 재시도가 곧 418 승격이었다:
 * <pre>
 *   2026-08-16 05:15:37  429  /futures/data/basis
 *   2026-08-16 05:15:42  418  &lt;&lt;&lt; BAN     ← 재시도 1회, 5초 만에 2시간 밴
 * </pre>
 * 공식 문서상 418 은 "429 를 받고도 계속 요청한 IP"에 걸린다. 남이 채운 양동이를 5초 뒤 다시
 * 두드려봐야 빠졌을 리가 없으므로, 공유 양동이는 <b>재시도하지 않고</b> 호출 측이 쿨다운을 건다.
 * FAPI 는 우리 트래픽이 원인이라 백오프가 실제로 듣기 때문에 재시도를 유지한다.
 *
 * <p>재시도 소진 시 마지막 예외 전파 — 호출 측이 구간을 중단하고 다음 tick 이 같은 지점부터 재개한다.
 */
public final class BinanceRestRetry {

    private static final int MAX_RETRY = 5;
    private static final long RATE_LIMIT_BACKOFF_MS = 5_000;
    private static final long DEFAULT_BACKOFF_MS = 1_000;

    private BinanceRestRetry() {
    }

    public static <T> T fetchWithRetry(Supplier<T> call, String label, Logger log) {
        int attempt = 0;
        while (true) {
            try {
                return call.get();
            } catch (BinanceRestException e) {
                if (e.isBanned()) {
                    throw e;
                }
                // 공유 양동이의 429 — 재시도 금지. 호출 측이 양동이 쿨다운으로 물러난다.
                if (e.isRateLimited() && e.bucket() != RateBucket.FAPI) {
                    throw e;
                }
                if (++attempt > MAX_RETRY) {
                    throw e;
                }
                long backoff = backoffMs(e, attempt);
                log.warn("[RETRY] {} {} — {}ms 후 재시도({}/{})", label, e.getMessage(), backoff, attempt, MAX_RETRY);
                sleep(backoff);
            }
        }
    }

    /** 서버가 Retry-After 를 주면 그 값을 그대로 쓴다 — 추측보다 항상 정확하다(418 에서 얻은 교훈). */
    private static long backoffMs(BinanceRestException e, int attempt) {
        if (e.isRateLimited()) {
            return e.retryAfterSec() > 0
                    ? e.retryAfterSec() * 1000
                    : RATE_LIMIT_BACKOFF_MS * attempt;
        }
        return DEFAULT_BACKOFF_MS * attempt;
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
