package com.autotrading.autotradingmarketdata.depth;

import java.util.List;

/** 풀북 수집의 데이터 묶음 — WS diff 이벤트, REST 스냅샷, DB 행. */
public final class DepthRows {

    private DepthRows() {
    }

    /** {@code <symbol>@depth@100ms} 한 건. bids/asks 는 [price, qty] 문자열 쌍(거래소 원문 그대로). */
    public record DiffEvent(String symbol, long eventMs, long firstU, long lastU, long prevU,
                            List<String[]> bids, List<String[]> asks) {
    }

    /** {@code /fapi/v1/depth?limit=1000}. */
    public record Snapshot(long lastUpdateId, long eventMs, List<String[]> bids, List<String[]> asks) {
    }

    /**
     * {@code depth_diff} 한 행. kind: S=스냅샷 레벨, D=diff 레벨. side: b/a.
     * 스냅샷 행은 u=lastUpdateId, firstU=prevU=0.
     */
    public record Row(String symbol, long eventMs, long recvMs, char kind, char side,
                      long priceTicks, long qtySteps, long u, long firstU, long prevU) {
    }
}
