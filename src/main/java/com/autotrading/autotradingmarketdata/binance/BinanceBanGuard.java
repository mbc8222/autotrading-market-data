package com.autotrading.autotradingmarketdata.binance;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

/**
 * 429/418 가드 — <b>{@link RateBucket} 단위</b>로 중지한다.
 *
 * <p>★2026-08-21 이전에는 단일 전역 중지였다. 전제는 "418 은 IP 단위 밴이니 한 수집기가 맞으면
 * 전부 멈춰야 한다"였고, 응답 본문이 문자 그대로 {@code IP(x.x.x.x) banned until ...} 이라
 * 그렇게 믿을 만했다. <b>그러나 틀렸다</b> — 응답이 지목하는 IP 가 경로마다 다르다
 * ({@link RateBucket} 참조). 그 결과 {@code /futures/data/basis} 하나가 받은 밴으로
 * {@code /fapi} 계열(kline·aggTrades)까지 2시간씩 세우고 있었고, 그 때문에 08-19~08-20 의
 * 22h56m agg_trade 갭 복구가 복구창 밖으로 밀려났다. 밴 중에도 kline 은 계속 성공했다는 사실이
 * 로그에 매일 쌓여 있었지만 아무도 그 조합을 보지 않았다.
 *
 * <p>정책 —
 * <ul>
 *   <li><b>418</b>: 본문 {@code banned until} → {@code Retry-After} → 지수후퇴 순으로 해제
 *       시각을 정한다. 추측은 마지막 수단이다(고정 10분 재개가 7일간 494회 밴의 원인이었다).</li>
 *   <li><b>429</b>: 418 의 예고다. FAPI 는 우리가 원인이라 짧게 물러나고, 공유 양동이는
 *       재시도가 곧 418 승격이므로 길게 물러난다. 실측 사례:
 *       {@code 05:15:37 429 → 05:15:42 418} — 재시도 1회에 2시간 밴.</li>
 * </ul>
 */
@Component
public class BinanceBanGuard {

    private static final Logger log = LogManager.getLogger(BinanceBanGuard.class);

    /** 418: Retry-After·본문 모두 없을 때의 기본 대기 — 연속 밴마다 2배. */
    private static final long BASE_PAUSE_MS = Duration.ofMinutes(10).toMillis();
    /** 418 지수 후퇴 상한 — 바이낸스 최대 밴이 3일이라 그보다 짧게 잡아 과도한 정지를 피한다. */
    private static final long MAX_PAUSE_MS = Duration.ofHours(3).toMillis();
    /** Retry-After 가 비현실적으로 작을 때의 하한(경계 오차·시계 오차 흡수). */
    private static final long MIN_PAUSE_MS = Duration.ofSeconds(30).toMillis();
    /** 이 시간 안에 다시 걸리면 "연속"으로 보고 후퇴를 키운다. */
    private static final long ESCALATION_WINDOW_MS = Duration.ofMinutes(30).toMillis();

    /** 429(FAPI): 우리 트래픽이 원인이라 백오프가 실제로 듣는다 — 짧게. */
    private static final long FAPI_429_BASE_MS = Duration.ofSeconds(10).toMillis();
    private static final long FAPI_429_MAX_MS = Duration.ofSeconds(60).toMillis();
    /**
     * 429(공유 양동이): 남의 트래픽이 원인이라 5초 뒤 다시 물어봐야 빠졌을 리 없다.
     * 물러나는 비용은 "그 사이클 한 번 거름"인데 안 물러난 비용은 2시간이다 —
     * 비대칭이 압도적이라 보수적으로 간다. /futures/data 는 30일 보존이라 데이터 손실은 0.
     */
    private static final long SHARED_429_BASE_MS = Duration.ofSeconds(60).toMillis();
    private static final long SHARED_429_MAX_MS = Duration.ofMinutes(15).toMillis();

    private final Map<RateBucket, AtomicLong> pausedUntil = new EnumMap<>(RateBucket.class);
    private final Map<RateBucket, AtomicLong> lastBanAt = new EnumMap<>(RateBucket.class);
    private final Map<RateBucket, AtomicLong> consecutiveBan = new EnumMap<>(RateBucket.class);
    private final Map<RateBucket, AtomicLong> last429At = new EnumMap<>(RateBucket.class);
    private final Map<RateBucket, AtomicLong> consecutive429 = new EnumMap<>(RateBucket.class);
    private final Map<RateBucket, AtomicLong> banTotal = new EnumMap<>(RateBucket.class);
    private final Map<RateBucket, AtomicLong> cooldownTotal = new EnumMap<>(RateBucket.class);

    /** 가장 최근 응답의 분당 사용 가중치(-1 = 미관측). /futures/data 는 이 헤더를 주지 않는다. */
    private volatile long usedWeight1m = -1;

    public BinanceBanGuard(MeterRegistry registry) {
        for (RateBucket b : RateBucket.values()) {
            pausedUntil.put(b, new AtomicLong());
            lastBanAt.put(b, new AtomicLong());
            consecutiveBan.put(b, new AtomicLong());
            last429At.put(b, new AtomicLong());
            consecutive429.put(b, new AtomicLong());
            banTotal.put(b, new AtomicLong());
            cooldownTotal.put(b, new AtomicLong());

            Gauge.builder("binance.ban.paused", this, g -> g.isPaused(b) ? 1 : 0)
                    .tag("bucket", b.tag())
                    .description("해당 양동이가 418/429 로 중지 중이면 1 — 수집 공백의 직접 원인")
                    .register(registry);
            Gauge.builder("binance.ban.remaining.seconds", this, g -> g.remainingSeconds(b))
                    .tag("bucket", b.tag())
                    .description("중지 해제까지 남은 초")
                    .register(registry);
            FunctionCounter.builder("binance.ban.total", banTotal.get(b), AtomicLong::get)
                    .tag("bucket", b.tag())
                    .description("418 누적 — fapi 가 0 이 아니면 양동이 분리 가설이 반증된 것이다")
                    .register(registry);
            FunctionCounter.builder("binance.cooldown.total", cooldownTotal.get(b), AtomicLong::get)
                    .tag("bucket", b.tag())
                    .description("429 쿨다운 누적 — 418 로 승격되지 않고 여기서 멈췄다는 뜻")
                    .register(registry);
        }
        Gauge.builder("binance.used.weight.1m", this, g -> g.usedWeight1m)
                .description("X-MBX-USED-WEIGHT-1M — 분당 사용 가중치(-1=미관측). 상한 2400 근접 감시용")
                .register(registry);
    }

    /**
     * 418 수신. {@code detail} 은 응답 본문 — 바이낸스는 여기에
     * {@code IP(x.x.x.x) banned until <epoch>} 형태로 <b>실제 해제 시각</b>을 넣는다.
     * {@code /futures/data/*} 는 Retry-After·가중치 헤더를 주지 않으므로 본문이 유일한 근거다.
     */
    public void banned(RateBucket bucket, String source, long retryAfterSec, long bannedUntilMs, String detail) {
        long now = System.currentTimeMillis();
        long n = bump(consecutiveBan, lastBanAt, bucket, now);
        banTotal.get(bucket).incrementAndGet();

        long pause;
        String basis;
        if (bannedUntilMs > now) {
            pause = (bannedUntilMs - now) + 30_000;
            basis = "본문 banned-until 준수(" + (pause / 60_000) + "분)";
        } else if (retryAfterSec > 0) {
            pause = Math.max(retryAfterSec * 1000, MIN_PAUSE_MS);
            basis = "Retry-After=" + retryAfterSec + "s";
        } else {
            pause = Math.min(BASE_PAUSE_MS * (1L << Math.min(n - 1, 6)), MAX_PAUSE_MS);
            basis = "헤더없음 -> 지수후퇴(연속 " + n + "회)";
        }
        extend(bucket, now + pause);
        log.error("[BAN-GUARD] 418 ban bucket={} ({}) — 해당 양동이 {}분 중지 [{}] 누적 {}회 | {}",
                bucket.tag(), source, pause / 60_000, basis, banTotal.get(bucket).get(),
                detail == null ? "(본문없음)" : abbreviate(detail));
    }

    /**
     * 429 수신 — 418 의 직전 단계다. 여기서 확실히 물러나면 밴을 피할 수 있다.
     *
     * <p>양동이 전체를 재우는 이유: 같은 양동이를 쓰는 다른 수집기가 이 사실을 모르면
     * 계속 두드려 418 로 승격시킨다(백필이 weight 1800/min 을 쓰는 중에 kline 이 429 를
     * 받아도 갭 보정기는 모르던 구조).
     */
    public void rateLimited(RateBucket bucket, String source, long retryAfterSec) {
        long now = System.currentTimeMillis();
        long n = bump(consecutive429, last429At, bucket, now);
        cooldownTotal.get(bucket).incrementAndGet();

        long pause;
        String basis;
        if (retryAfterSec > 0) {
            pause = retryAfterSec * 1000;
            basis = "Retry-After=" + retryAfterSec + "s";
        } else if (bucket == RateBucket.FAPI) {
            pause = Math.min(FAPI_429_BASE_MS * (1L << Math.min(n - 1, 3)), FAPI_429_MAX_MS);
            basis = "자체 양동이 -> 짧은 후퇴(연속 " + n + "회)";
        } else {
            pause = Math.min(SHARED_429_BASE_MS * (1L << Math.min(n - 1, 4)), SHARED_429_MAX_MS);
            basis = "공유 양동이 -> 긴 후퇴(연속 " + n + "회, 재시도 없음)";
        }
        extend(bucket, now + pause);
        log.warn("[BAN-GUARD] 429 bucket={} ({}) — 해당 양동이 {}초 쿨다운 [{}] 누적 {}회",
                bucket.tag(), source, pause / 1000, basis, cooldownTotal.get(bucket).get());
    }

    /** 사이클이 무사고로 끝났을 때 호출 — 연속 카운터를 풀어 다음 후퇴가 과도해지지 않게 한다. */
    public void cycleSucceeded(RateBucket bucket) {
        long now = System.currentTimeMillis();
        if (now - lastBanAt.get(bucket).get() > ESCALATION_WINDOW_MS) {
            consecutiveBan.get(bucket).set(0);
        }
        if (now - last429At.get(bucket).get() > ESCALATION_WINDOW_MS) {
            consecutive429.get(bucket).set(0);
        }
    }

    public boolean isPaused(RateBucket bucket) {
        return System.currentTimeMillis() < pausedUntil.get(bucket).get();
    }

    public double remainingSeconds(RateBucket bucket) {
        long left = pausedUntil.get(bucket).get() - System.currentTimeMillis();
        return left > 0 ? left / 1000.0 : 0;
    }

    /** 정상 응답에서 관측한 사용 가중치 갱신. */
    public void observeWeight(long weight) {
        if (weight >= 0) {
            usedWeight1m = weight;
        }
    }

    /** 이미 더 긴 중지가 걸려 있으면 줄이지 않는다. */
    private void extend(RateBucket bucket, long until) {
        pausedUntil.get(bucket).accumulateAndGet(until, Math::max);
    }

    private long bump(Map<RateBucket, AtomicLong> counter, Map<RateBucket, AtomicLong> lastAt,
                      RateBucket bucket, long now) {
        long prev = lastAt.get(bucket).getAndSet(now);
        long n = (now - prev <= ESCALATION_WINDOW_MS) ? counter.get(bucket).get() + 1 : 1;
        counter.get(bucket).set(n);
        return n;
    }

    private static String abbreviate(String s) {
        String one = s.replaceAll("\\s+", " ").trim();
        return one.length() <= 300 ? one : one.substring(0, 300) + "…";
    }
}
