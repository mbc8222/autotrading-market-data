package com.autotrading.autotradingmarketdata.raw;

import com.autotrading.autotradingmarketdata.depth.DepthRows.Row;
import org.springframework.stereotype.Component;

/** 원시 호가 차분 버퍼 — 실측 1,300행/s(4심볼·조용한 시간대) → 상한 2M ≈ 25분 완충. */
@Component
public class DepthBuffer extends BatchBuffer<Row> {

    public DepthBuffer() {
        super(2_000_000);
    }
}
