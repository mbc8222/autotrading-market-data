package com.autotrading.autotradingmarketdata.binance;

import java.util.function.Supplier;
import org.apache.logging.log4j.Logger;

/**
 * REST 호출 공통 재시도 — 429는 5s×시도, 기타 오류는 1s×시도로 최대 {@value #MAX_RETRY}회.
 * 418(ban)은 재시도하지 않고 즉시 전파한다(호출 측이 {@link BinanceBanGuard}로 장기 중지).
 * 재시도 소진 시 마지막 예외 전파 — 호출 측이 구간을 중단하고 다음 tick이 같은 지점부터 재개한다.
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
                if (e.isBanned() || ++attempt > MAX_RETRY) {
                    throw e;
                }
                long backoff = (e.isRateLimited() ? RATE_LIMIT_BACKOFF_MS : DEFAULT_BACKOFF_MS) * attempt;
                log.warn("[RETRY] {} {} — {}ms 후 재시도({}/{})", label, e.getMessage(), backoff, attempt, MAX_RETRY);
                sleep(backoff);
            }
        }
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
