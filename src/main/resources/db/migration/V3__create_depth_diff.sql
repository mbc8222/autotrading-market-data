-- ─── 원시 호가 차분 (WS @depth@100ms + REST 스냅샷, collect.depth.enabled 게이트, 2026-09-06) ───
--   kind: S = 스냅샷 레벨(REST depth?limit=1000, u=lastUpdateId, first_u=prev_u=0) / D = diff 레벨.
--   가격·수량은 정수: price = price_ticks × 10^-price_precision, qty = qty_steps × 10^-qty_precision
--   (단위는 depth_symbol_units. exchangeInfo tickSize 는 실제 해상도와 다를 수 있어 자릿수를 단위로 쓴다).
--   일별 RANGE 파티션 — 자식은 PartitionMaintenance 가 생성, DROP 은 cold-export 가 Parquet 이관·검증 후.
--   ★인덱스 없음: 하루 ~1.1억 행이라 btree 하나가 ~3GB/일. 핫(3일)은 파티션 프루닝 + 순차 스캔,
--     분석은 콜드 Parquet(정렬·행그룹 통계)에서. 필요해지면 (symbol, event_time) 부터 추가.
CREATE TABLE depth_diff (
    symbol       VARCHAR(32)  NOT NULL,
    event_time   TIMESTAMPTZ  NOT NULL,   -- 거래소 이벤트 시각 E
    recv_time    TIMESTAMPTZ  NOT NULL,   -- 수신 시각
    kind         CHAR(1)      NOT NULL,   -- S | D
    side         CHAR(1)      NOT NULL,   -- b | a
    price_ticks  BIGINT       NOT NULL,
    qty_steps    BIGINT       NOT NULL,   -- 0 = 레벨 제거
    u            BIGINT       NOT NULL,   -- 이벤트 마지막 update id
    first_u      BIGINT       NOT NULL,   -- 이벤트 첫 update id (U)
    prev_u       BIGINT       NOT NULL    -- 직전 이벤트 u (pu)
) PARTITION BY RANGE (event_time);

CREATE TABLE depth_symbol_units (
    symbol          VARCHAR(32) PRIMARY KEY,
    price_precision INT         NOT NULL,
    qty_precision   INT         NOT NULL,
    tick_size       NUMERIC     NOT NULL,   -- exchangeInfo PRICE_FILTER (참고용)
    step_size       NUMERIC     NOT NULL,   -- exchangeInfo LOT_SIZE (참고용)
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 동기화 사건: 무결성 대장. DB 유니크 제약이 없는 depth_diff 의 신뢰성은 이 표 + u 연속성으로 판정한다.
CREATE TABLE depth_sync_events (
    id      BIGSERIAL   PRIMARY KEY,
    symbol  VARCHAR(32) NOT NULL,
    ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind    VARCHAR(16) NOT NULL,   -- ws_connect | snapshot | snapshot_fail | gap | unit_error
    detail  TEXT
);
CREATE INDEX idx_depth_sync_events_symbol_ts ON depth_sync_events (symbol, ts);
