package com.autotrading.autotradingmarketdata.binance;

/**
 * 바이낸스 레이트리밋 양동이 — 밴은 양동이 단위로 걸리므로 중지도 양동이 단위여야 한다.
 *
 * <p>★2026-08-21 실측: 레이트리밋 응답이 지목하는 IP 가 경로마다 다르다.
 * <pre>
 *   /fapi/v1/klines      → IP(x.x.x.x)  = 우리 공인 IP        (2건)
 *   /futures/data/basis  → IP(10.119.x.x)      = 바이낸스 내부 주소   (23건, 9개가 번갈아)
 * </pre>
 * 100% 분리된다. 즉 {@code /futures/data/basis} 는 내부 프록시를 거쳐 서빙되고 그 <b>프록시
 * 주소</b>로 레이트리밋이 집계된다. 그 양동이는 우리만 쓰는 게 아니라서 우리 요청량과 무관하게
 * 넘친다 — 문서상 한도(1000 req/5min)의 2.4%만 쓰는데 밴이 났고, throttle 을 250ms→1s 로
 * 4배 늦춰도 그대로였다. 7일간 494회 밴이 이것이다.
 *
 * <p>따라서 하나의 가드로 다룰 수 없다. 통제 가능한 것(FAPI)은 백오프로 통제하고,
 * 통제 불가능한 것(BASIS)은 격리해 나머지를 오염시키지 않게 한다.
 */
public enum RateBucket {

    /** {@code /fapi/*} — 공인 IP 기준, weight 2400/min. 우리 트래픽이 원인이므로 백오프가 유효하다. */
    FAPI,

    /** {@code /futures/data/*} (basis 제외) — 5종 모두 밴 유발 이력 0건. */
    FUTURES_DATA,

    /**
     * {@code /futures/data/basis} — 관측된 418 <b>502건이 전부</b> 여기서 났다(예외 0).
     * 공유 프록시 기준이라 백오프가 듣지 않으므로 별도 양동이로 격리한다.
     *
     * <p>분리는 미확인 가설도 함께 검정한다 — basis 밴 중에 나머지 5종이 되는지는 한 번도
     * 관측된 적이 없다(항상 basis 보다 먼저 실행됐고, 밴이 나면 공용 가드가 전부 세웠다).
     * 분리 후 5종이 성공하면 양동이가 실제로 다르다는 뜻이고, 418 을 받으면 합쳐야 한다.
     */
    FUTURES_DATA_BASIS;

    /** 요청 경로 → 양동이. 알 수 없는 경로는 보수적으로 FAPI(가장 엄격한 관리 대상). */
    public static RateBucket of(String path) {
        if (path == null) {
            return FAPI;
        }
        if (path.startsWith("/futures/data/basis")) {
            return FUTURES_DATA_BASIS;
        }
        if (path.startsWith("/futures/data/")) {
            return FUTURES_DATA;
        }
        return FAPI;
    }

    /** 메트릭 태그값. */
    public String tag() {
        return name().toLowerCase();
    }
}
