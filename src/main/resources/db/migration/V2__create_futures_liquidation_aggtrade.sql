-- 파생 7종 + 강제 청산 + 원시 집계체결.
-- 통계/raw는 모놀리스 검증 스키마를 따라 DOUBLE PRECISION(처리량·저장 우선) — 가격 진실원천인 binance_klines만 NUMERIC.
-- 시각은 서비스 컨벤션대로 TIMESTAMPTZ(UTC).

-- ─── 파생상품 통계 (/futures/data, period 5m, 거래소 보존 ~30일) ───

CREATE TABLE futures_open_interest_hist (
    symbol        VARCHAR(32)      NOT NULL,
    ts            TIMESTAMPTZ      NOT NULL,
    sum_oi        DOUBLE PRECISION NOT NULL,
    sum_oi_value  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE futures_top_position_ratio (
    symbol           VARCHAR(32)      NOT NULL,
    ts               TIMESTAMPTZ      NOT NULL,
    long_short_ratio DOUBLE PRECISION NOT NULL,
    long_account     DOUBLE PRECISION NOT NULL,
    short_account    DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE futures_top_account_ratio (
    symbol           VARCHAR(32)      NOT NULL,
    ts               TIMESTAMPTZ      NOT NULL,
    long_short_ratio DOUBLE PRECISION NOT NULL,
    long_account     DOUBLE PRECISION NOT NULL,
    short_account    DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE futures_global_ls_ratio (
    symbol           VARCHAR(32)      NOT NULL,
    ts               TIMESTAMPTZ      NOT NULL,
    long_short_ratio DOUBLE PRECISION NOT NULL,
    long_account     DOUBLE PRECISION NOT NULL,
    short_account    DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE futures_taker_ratio (
    symbol         VARCHAR(32)      NOT NULL,
    ts             TIMESTAMPTZ      NOT NULL,
    buy_sell_ratio DOUBLE PRECISION NOT NULL,
    buy_vol        DOUBLE PRECISION NOT NULL,
    sell_vol       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE futures_basis (
    pair            VARCHAR(32)      NOT NULL,
    contract_type   VARCHAR(20)      NOT NULL,
    ts              TIMESTAMPTZ      NOT NULL,
    futures_price   DOUBLE PRECISION,
    index_price     DOUBLE PRECISION,
    basis           DOUBLE PRECISION,
    basis_rate      DOUBLE PRECISION,
    annualized_rate DOUBLE PRECISION,
    PRIMARY KEY (pair, contract_type, ts)
);

-- Funding Rate (8h 정산, 전체 히스토리)
CREATE TABLE futures_funding_rate (
    symbol       VARCHAR(32)      NOT NULL,
    funding_time TIMESTAMPTZ      NOT NULL,
    funding_rate DOUBLE PRECISION NOT NULL,
    mark_price   DOUBLE PRECISION,
    PRIMARY KEY (symbol, funding_time)
);

-- ─── 강제 청산 (WS @forceOrder) ───
--   side: SELL = 롱 포지션 청산(강제 매도), BUY = 숏 포지션 청산.
--   qty 포함 자연키: cascade 시 동일 symbol/side/price/ms의 서로 다른 청산을 구분.
CREATE TABLE binance_liquidations (
    symbol     VARCHAR(32)      NOT NULL,
    side       VARCHAR(8)       NOT NULL,
    price      DOUBLE PRECISION NOT NULL,
    qty        DOUBLE PRECISION NOT NULL,
    quote_qty  DOUBLE PRECISION NOT NULL,
    trade_time TIMESTAMPTZ      NOT NULL,
    CONSTRAINT uq_liquidation UNIQUE (symbol, side, price, qty, trade_time)
);
CREATE INDEX idx_binance_liquidations_symbol_time
    ON binance_liquidations (symbol, trade_time DESC);

-- ─── 원시 집계체결 (WS @aggTrade + REST 갭 보정, collect.raw.enabled 게이트) ───
--   일별 RANGE 파티션 — 자식 파티션은 PartitionMaintenance가 런타임 생성/90일 DROP.
--   처리량 우선: surrogate PK 없음. 인덱스는 부모에 선언 → 자식에 전파.
CREATE TABLE agg_trade (
    symbol         VARCHAR(32)      NOT NULL,
    agg_id         BIGINT           NOT NULL,
    price          DOUBLE PRECISION NOT NULL,
    qty            DOUBLE PRECISION NOT NULL,
    first_trade_id BIGINT,
    last_trade_id  BIGINT,
    trade_time     TIMESTAMPTZ      NOT NULL,
    is_buyer_maker BOOLEAN          NOT NULL
) PARTITION BY RANGE (trade_time);

CREATE INDEX idx_agg_trade_symbol_time ON agg_trade (symbol, trade_time);
-- WS·REST 보정 양쪽 멱등(ON CONFLICT DO NOTHING) + agg_id 갭 탐지용 유일키.
-- PG 규칙상 파티션 키(trade_time) 포함 필수 — agg_id가 심볼별 유일이라 (symbol, agg_id) 전역 유일과 동치.
CREATE UNIQUE INDEX uq_agg_trade_id ON agg_trade (symbol, agg_id, trade_time);
-- 체결 매물대(VP) 단일패스 index-only 스캔용 covering index.
CREATE INDEX idx_agg_trade_vp ON agg_trade (symbol, trade_time) INCLUDE (price, qty, is_buyer_maker);
