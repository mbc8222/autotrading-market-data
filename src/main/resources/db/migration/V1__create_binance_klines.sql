-- 바이낸스 선물(fapi) 캔들. 심볼은 소문자 페어("btcusdt"), 시각은 TIMESTAMPTZ(UTC).
CREATE TABLE binance_klines (
    symbol                       VARCHAR(32)     NOT NULL,
    interval                     VARCHAR(8)      NOT NULL,
    open_time                    TIMESTAMPTZ     NOT NULL,
    open_price                   NUMERIC(38, 18) NOT NULL,
    high_price                   NUMERIC(38, 18) NOT NULL,
    low_price                    NUMERIC(38, 18) NOT NULL,
    close_price                  NUMERIC(38, 18) NOT NULL,
    volume                       NUMERIC(38, 18) NOT NULL,
    close_time                   TIMESTAMPTZ     NOT NULL,
    quote_asset_volume           NUMERIC(38, 18) NOT NULL,
    number_of_trades             BIGINT          NOT NULL,
    taker_buy_base_asset_volume  NUMERIC(38, 18) NOT NULL,
    taker_buy_quote_asset_volume NUMERIC(38, 18) NOT NULL,
    is_closed                    BOOLEAN         NOT NULL,
    created_at                   TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, interval, open_time)
);

CREATE INDEX idx_binance_klines_close_time
    ON binance_klines (symbol, interval, close_time DESC);
