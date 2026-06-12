package com.autotrading.autotradingmarketdata.binance;

/**
 * 파생/raw 수집 row 묶음 — 통계·집계 데이터라 모놀리스와 동일하게 double 정밀도(처리량·저장 우선).
 * 가격 진실원천인 kline만 BigDecimal({@link BinanceKline}).
 */
public final class FuturesRows {

    private FuturesRows() {
    }

    /** Open Interest 통계 (/futures/data/openInterestHist). ts = epoch ms. */
    public record OiHistRow(String symbol, double sumOi, double sumOiValue, long ts) {
    }

    /** 롱/숏 비율 3종 공용 (top position / top account / global). */
    public record LsRatioRow(String symbol, double longShortRatio, double longAccount, double shortAccount, long ts) {
    }

    /** Taker 매수/매도 거래량 비율. */
    public record TakerRatioRow(String symbol, double buySellRatio, double buyVol, double sellVol, long ts) {
    }

    /** Basis(선물-현물 괴리), contractType=PERPETUAL. */
    public record BasisRow(String pair, double futuresPrice, double indexPrice, double basis,
                           double basisRate, double annualizedRate, long ts) {
    }

    /** Funding Rate 정산 히스토리 (8h). */
    public record FundingRow(String symbol, double fundingRate, long fundingTime, double markPrice) {
    }

    /** 원시 집계체결 (WS @aggTrade / REST /fapi/v1/aggTrades 공용). */
    public record AggTradeRow(String symbol, long aggId, double price, double qty,
                              long firstTradeId, long lastTradeId, long tradeTime, boolean buyerMaker) {
    }

    /** 강제 청산 이벤트 (WS @forceOrder). */
    public record LiquidationEvent(String symbol, String side, double price, double qty,
                                   double quoteQty, long tradeTime) {
    }
}
