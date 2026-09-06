package com.autotrading.autotradingmarketdata.depth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.autotrading.autotradingmarketdata.depth.DepthRows.DiffEvent;
import com.autotrading.autotradingmarketdata.depth.DepthRows.Row;
import com.autotrading.autotradingmarketdata.depth.DepthRows.Snapshot;
import com.autotrading.autotradingmarketdata.depth.LocalBook.Result;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LocalBookTest {

    private static final SymbolUnits U = new SymbolUnits("btcusdt", 2, 3, "0.10", "0.001");

    private static DiffEvent ev(long first, long last, long prev, List<String[]> bids, List<String[]> asks) {
        return new DiffEvent("btcusdt", 1000, first, last, prev, bids, asks);
    }

    private static List<String[]> lv(String... pairs) {
        List<String[]> out = new ArrayList<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.add(new String[] {pairs[i], pairs[i + 1]});
        }
        return out;
    }

    @Test
    void 단위는_자릿수_기준이고_정수배가_아니면_예외() {
        assertEquals(8000010, U.priceTicks("80000.10"));
        assertEquals(1, U.qtySteps("0.001"));
        SymbolUnits sol = new SymbolUnits("solusdt", 4, 2, "0.0100", "0.01");
        assertEquals(1050020, sol.priceTicks("105.0020"));   // tickSize 0.01 로는 못 담던 값
        assertThrows(IllegalArgumentException.class, () -> U.priceTicks("80000.105"));
    }

    @Test
    void 동기화_규칙_첫_이벤트는_스냅샷을_걸쳐야_한다() {
        LocalBook b = new LocalBook(U);
        List<Row> out = new ArrayList<>();
        assertEquals(Result.UNSYNCED, b.apply(ev(1, 2, 0, lv(), lv()), out));
        b.applySnapshot(new Snapshot(100, 0, lv("10.00", "1.000"), lv("10.10", "2.000")), 0);
        assertEquals(Result.DROPPED, b.apply(ev(90, 99, 89, lv(), lv()), out));    // u < lastUpdateId
        assertEquals(Result.GAP, b.apply(ev(101, 105, 99, lv(), lv()), out));      // U > lastUpdateId
        assertEquals(Result.APPLIED, b.apply(ev(95, 105, 94, lv("10.00", "0.500"), lv()), out));
        assertTrue(b.isSynced());
        assertEquals(105, b.lastU());
        assertEquals(Result.APPLIED, b.apply(ev(106, 110, 105, lv(), lv()), out));
        assertEquals(Result.GAP, b.apply(ev(112, 115, 111, lv(), lv()), out));    // pu != last_u
        assertEquals(1, out.size());
        assertEquals('D', out.get(0).kind());
        assertEquals(500, out.get(0).qtySteps());
    }

    @Test
    void 수량_0은_레벨_제거이고_최우선가가_따라간다() {
        LocalBook b = new LocalBook(U);
        List<Row> out = new ArrayList<>();
        List<Row> snap = b.applySnapshot(new Snapshot(1, 0, lv("10.00", "1.000", "9.90", "1.000"),
                lv("10.10", "1.000")), 0);
        assertEquals(3, snap.size());
        assertEquals('S', snap.get(0).kind());
        assertEquals(Result.APPLIED, b.apply(ev(1, 2, 0, lv("10.00", "0.000"), lv()), out));
        assertEquals(990, b.bestBid().getKey());
        assertEquals(1010, b.bestAsk().getKey());
        assertEquals(1, b.bidLevels());
    }

    @Test
    void 단위_불일치_이벤트는_북을_바꾸지_않는다() {
        LocalBook b = new LocalBook(U);
        List<Row> out = new ArrayList<>();
        b.applySnapshot(new Snapshot(1, 0, lv("10.00", "1.000"), lv("10.10", "1.000")), 0);
        assertThrows(IllegalArgumentException.class,
                () -> b.apply(ev(1, 2, 0, lv("10.00", "0.000", "10.005", "1.000"), lv()), out));
        assertEquals(1000, b.bestBid().getKey());   // 첫 레벨(제거)도 적용되지 않았다
        assertEquals(1, b.lastU());
        assertFalse(b.isSynced());
        assertTrue(out.isEmpty());
    }

    @Test
    void 무효화하면_스냅샷까지_UNSYNCED() {
        LocalBook b = new LocalBook(U);
        b.applySnapshot(new Snapshot(1, 0, lv("10.00", "1.000"), lv("10.10", "1.000")), 0);
        b.invalidate();
        assertEquals(Result.UNSYNCED, b.apply(ev(1, 2, 0, lv(), lv()), new ArrayList<>()));
        assertFalse(b.isSynced());
    }
}
