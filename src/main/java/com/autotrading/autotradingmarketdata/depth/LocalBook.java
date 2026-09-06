package com.autotrading.autotradingmarketdata.depth;

import com.autotrading.autotradingmarketdata.depth.DepthRows.DiffEvent;
import com.autotrading.autotradingmarketdata.depth.DepthRows.Row;
import com.autotrading.autotradingmarketdata.depth.DepthRows.Snapshot;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 로컬 호가창 — I/O 없는 순수 상태기계. 스레드 안전하지 않다(호출자가 심볼 단위로 직렬화).
 *
 * <p>동기화 규칙(바이낸스 USDⓈ-M 공식 문서):
 * <ul>
 *   <li>스냅샷 이전 이벤트(u &lt; lastUpdateId)는 버린다</li>
 *   <li>첫 적용 이벤트는 U ≤ lastUpdateId ≤ u</li>
 *   <li>이후 이벤트는 pu == 직전 u — 아니면 갭(재동기화)</li>
 * </ul>
 * 이벤트를 먼저 버퍼링한 뒤 스냅샷을 받아야 첫 이벤트가 위 조건을 만족한다(선요청 시 가짜 갭).
 */
public final class LocalBook {

    public enum Result { APPLIED, DROPPED, GAP, UNSYNCED }

    private final SymbolUnits units;
    private final TreeMap<Long, Long> bids = new TreeMap<>(Comparator.reverseOrder());   // price_ticks → qty_steps
    private final TreeMap<Long, Long> asks = new TreeMap<>();
    private long lastU = -1;
    private boolean synced = false;

    public LocalBook(SymbolUnits units) {
        this.units = units;
    }

    /** 스냅샷으로 북을 교체하고 원시 행(kind=S)을 돌려준다. 이후 첫 diff 가 조건을 만족해야 synced. */
    public List<Row> applySnapshot(Snapshot s, long recvMs) {
        bids.clear();
        asks.clear();
        List<Row> rows = new ArrayList<>(s.bids().size() + s.asks().size());
        for (String[] l : s.bids()) {
            long p = units.priceTicks(l[0]);
            long q = units.qtySteps(l[1]);
            if (q > 0) {
                bids.put(p, q);
            }
            rows.add(new Row(units.symbol(), s.eventMs(), recvMs, 'S', 'b', p, q, s.lastUpdateId(), 0, 0));
        }
        for (String[] l : s.asks()) {
            long p = units.priceTicks(l[0]);
            long q = units.qtySteps(l[1]);
            if (q > 0) {
                asks.put(p, q);
            }
            rows.add(new Row(units.symbol(), s.eventMs(), recvMs, 'S', 'a', p, q, s.lastUpdateId(), 0, 0));
        }
        lastU = s.lastUpdateId();
        synced = false;
        return rows;
    }

    /**
     * diff 적용. APPLIED 면 {@code out} 에 원시 행(kind=D)이 추가된다.
     * 단위 불일치는 {@link IllegalArgumentException} — 북은 변경되지 않은 상태로 남는다(호출자가 재동기화).
     */
    public Result apply(DiffEvent ev, List<Row> out) {
        if (lastU < 0) {
            return Result.UNSYNCED;
        }
        if (!synced) {
            if (ev.lastU() < lastU) {
                return Result.DROPPED;
            }
            if (!(ev.firstU() <= lastU && lastU <= ev.lastU())) {
                return Result.GAP;
            }
        } else if (ev.prevU() != lastU) {
            return Result.GAP;
        }
        // 단위 변환을 먼저 전부 끝내고(예외 가능) 그 다음 북을 바꾼다 — 반쯤 적용된 북을 남기지 않는다.
        List<Row> rows = new ArrayList<>(ev.bids().size() + ev.asks().size());
        for (String[] l : ev.bids()) {
            rows.add(new Row(units.symbol(), ev.eventMs(), 0, 'D', 'b', units.priceTicks(l[0]), units.qtySteps(l[1]),
                    ev.lastU(), ev.firstU(), ev.prevU()));
        }
        for (String[] l : ev.asks()) {
            rows.add(new Row(units.symbol(), ev.eventMs(), 0, 'D', 'a', units.priceTicks(l[0]), units.qtySteps(l[1]),
                    ev.lastU(), ev.firstU(), ev.prevU()));
        }
        for (Row r : rows) {
            TreeMap<Long, Long> side = r.side() == 'b' ? bids : asks;
            if (r.qtySteps() == 0) {
                side.remove(r.priceTicks());
            } else {
                side.put(r.priceTicks(), r.qtySteps());
            }
        }
        synced = true;
        lastU = ev.lastU();
        out.addAll(rows);
        return Result.APPLIED;
    }

    /** 재연결 등으로 더 이상 신뢰할 수 없을 때. 다음 스냅샷까지 UNSYNCED. */
    public void invalidate() {
        lastU = -1;
        synced = false;
    }

    public boolean isSynced() {
        return synced;
    }

    public long lastU() {
        return lastU;
    }

    public Map.Entry<Long, Long> bestBid() {
        return bids.firstEntry();
    }

    public Map.Entry<Long, Long> bestAsk() {
        return asks.firstEntry();
    }

    /** 상위 n 단 수량 합(스텝). KV 요약용. */
    public long topBidSteps(int n) {
        return topSteps(bids, n);
    }

    public long topAskSteps(int n) {
        return topSteps(asks, n);
    }

    private static long topSteps(TreeMap<Long, Long> side, int n) {
        long sum = 0;
        int i = 0;
        for (long q : side.values()) {
            if (i++ >= n) {
                break;
            }
            sum += q;
        }
        return sum;
    }

    public int bidLevels() {
        return bids.size();
    }

    public int askLevels() {
        return asks.size();
    }
}
