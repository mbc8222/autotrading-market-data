package com.autotrading.autotradingmarketdata.raw;

import com.autotrading.autotradingmarketdata.binance.FuturesRows.AggTradeRow;
import org.springframework.stereotype.Component;

/** 원시 집계체결 버퍼 — WS 수신과 배치 적재 사이의 비차단 완충. */
@Component
public class AggTradeBuffer extends BatchBuffer<AggTradeRow> {

    public AggTradeBuffer() {
        super(200_000);
    }
}
