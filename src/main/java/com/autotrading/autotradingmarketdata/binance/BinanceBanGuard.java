package com.autotrading.autotradingmarketdata.binance;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

/**
 * 418(IP ban) 공용 가드 — ban은 IP 단위라서 한 수집기가 맞으면 모든 REST 수집기가 함께 멈춰야 한다.
 *
 * <p>★고정 10분 중지를 폐기한다(2026-08-20 실측). 바이낸스 418 은 재범 시 밴 기간이 단계적으로
 * 늘어나는데, 고정 10분으로 재개하면 아직 안 풀린 밴을 다시 두드려 <b>밴이 연장</b>된다. 그러면
 * 다음 재개도 실패하는 되먹임이 되고, 실제로 7일간 <b>494회</b> 밴이 걸렸다. 증상은 매 사이클
 * 같은 지점(BTC 5종 직후)에서 잘려 나머지 3심볼이 상시 20분 지연되는 형태로 나타났다.
 *
 * <p>해제 시각은 추측할 값이 아니라 서버가 알려주는 값이다 — {@code Retry-After} 를 그대로 따른다.
 * 헤더가 없을 때만 연속 밴 횟수에 따라 지수적으로 물러난다(고정값은 "얼마가 맞는지 모른다"는
 * 사실을 숨기고 항상 같은 실수를 반복하게 만든다).
 */
@Component
public class BinanceBanGuard {

    private static final Logger log = LogManager.getLogger(BinanceBanGuard.class);

    /** Retry-After 부재 시 기본 대기 — 연속 밴마다 2배. */
    private static final long BASE_PAUSE_MS = Duration.ofMinutes(10).toMillis();
    /** 지수 후퇴 상한 — 바이낸스 최대 밴이 3일이라 그보다 짧게 잡아 과도한 정지를 피한다. */
    private static final long MAX_PAUSE_MS = Duration.ofHours(2).toMillis();
    /** Retry-After 가 비현실적으로 작을 때의 하한(경계 오차·시계 오차 흡수). */
    private static final long MIN_PAUSE_MS = Duration.ofSeconds(30).toMillis();
    /** 이 시간 안에 다시 밴이면 "연속"으로 보고 후퇴를 키운다. */
    private static final long ESCALATION_WINDOW_MS = Duration.ofMinutes(30).toMillis();

    private volatile long pausedUntil = 0;
    private volatile long lastBanAt = 0;
    private volatile int consecutive = 0;
    private final AtomicLong totalBans = new AtomicLong();
    /** 가장 최근 응답의 분당 사용 가중치(-1 = 미관측). 상한 근접을 사전에 보기 위한 값. */
    private volatile long usedWeight1m = -1;

    public BinanceBanGuard(MeterRegistry registry) {
        Gauge.builder("binance.ban.paused", this, g -> g.isPaused() ? 1 : 0)
                .description("REST 수집이 418 밴으로 중지 중이면 1 — 수집 공백의 직접 원인")
                .register(registry);
        Gauge.builder("binance.ban.remaining.seconds", this, BinanceBanGuard::remainingSeconds)
                .description("밴 해제까지 남은 초")
                .register(registry);
        Gauge.builder("binance.used.weight.1m", this, g -> g.usedWeight1m)
                .description("X-MBX-USED-WEIGHT-1M — 분당 사용 가중치(-1=미관측). 상한 근접 사전 감시용")
                .register(registry);
        FunctionCounter.builder("binance.ban.total", totalBans, AtomicLong::get)
                .description("418 밴 누적 횟수 — 증가 = 요청 빈도가 한계를 넘고 있다")
                .register(registry);
    }

    /** 418 수신. {@code retryAfterSec}=0 이면 헤더 부재 → 지수 후퇴. */
    public void banned(String source, long retryAfterSec) {
        long now = System.currentTimeMillis();
        consecutive = (now - lastBanAt <= ESCALATION_WINDOW_MS) ? consecutive + 1 : 1;
        lastBanAt = now;
        totalBans.incrementAndGet();

        long pause;
        String basis;
        if (retryAfterSec > 0) {
            pause = Math.max(retryAfterSec * 1000, MIN_PAUSE_MS);
            basis = "Retry-After=" + retryAfterSec + "s";
        } else {
            pause = Math.min(BASE_PAUSE_MS * (1L << Math.min(consecutive - 1, 6)), MAX_PAUSE_MS);
            basis = "헤더없음 → 지수후퇴(연속 " + consecutive + "회)";
        }
        pausedUntil = now + pause;
        log.error("[BAN-GUARD] 418 IP ban ({}) — 전체 REST 수집 {}분 중지 [{}] 누적 {}회",
                source, pause / 60_000, basis, totalBans.get());
    }

    /** 사이클이 밴 없이 끝났을 때 호출 — 연속 카운터를 풀어 다음 밴이 과도하게 길어지지 않게 한다. */
    public void cycleSucceeded() {
        if (consecutive > 0 && System.currentTimeMillis() - lastBanAt > ESCALATION_WINDOW_MS) {
            consecutive = 0;
        }
    }

    /** 429/정상 응답에서 관측한 사용 가중치 갱신. */
    public void observeWeight(long weight) {
        if (weight >= 0) {
            usedWeight1m = weight;
        }
    }

    public boolean isPaused() {
        return System.currentTimeMillis() < pausedUntil;
    }

    public double remainingSeconds() {
        long left = pausedUntil - System.currentTimeMillis();
        return left > 0 ? left / 1000.0 : 0;
    }
}
