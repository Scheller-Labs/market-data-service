-- ============================================================
-- 003_options_enhancements.sql
-- Options trading enhancements for 2-10 day strategy support
--
-- Changes:
--   1. Extend options_snapshots with rho, underlying_price, iv_percentile
--   2. Add iv_rank_history table for daily IV rank tracking
--   3. Add performance indexes for options chain queries
-- ============================================================


-- ============================================================
-- 1. Extend options_snapshots table
--
-- rho: rate sensitivity Greek (important for longer-dated options)
-- underlying_price: spot price at snapshot time (needed for moneyness)
-- iv_percentile: % of days with lower IV in lookback window
--   (distinct from iv_rank which uses min/max range normalization)
-- ============================================================

ALTER TABLE options_snapshots
    ADD COLUMN IF NOT EXISTS rho               DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS underlying_price  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS iv_percentile     DOUBLE PRECISION;


-- ============================================================
-- 2. New table: iv_rank_history
--
-- Stores daily IV rank snapshots for efficient historical queries.
-- Separating from options_snapshots allows lightweight IV rank queries
-- without loading full chain data.
--
-- iv_rank:       (current_iv - iv_52w_low) / (iv_52w_high - iv_52w_low) * 100
-- iv_percentile: % of trading days in lookback with lower IV than today
-- ============================================================

CREATE TABLE IF NOT EXISTS iv_rank_history (
    recorded_at   DATE             NOT NULL,
    symbol        TEXT             NOT NULL,
    iv_rank       DOUBLE PRECISION NOT NULL,   -- 0-100 scale
    iv_percentile DOUBLE PRECISION NOT NULL,   -- 0-100 scale
    current_iv    DOUBLE PRECISION NOT NULL,   -- raw IV value (e.g. 0.25 = 25%)
    iv_52w_high   DOUBLE PRECISION NOT NULL,   -- 52-week IV high
    iv_52w_low    DOUBLE PRECISION NOT NULL,   -- 52-week IV low
    provider      TEXT             NOT NULL DEFAULT '',
    fetched_at    TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    CONSTRAINT iv_rank_history_pkey PRIMARY KEY (recorded_at, symbol)
);


-- ============================================================
-- 3. Performance indexes for options workflow
--
-- Options chain queries by strategy agents are heavily filtered by:
--   - symbol + expiration_date (building spreads, computing max pain)
--   - symbol + option_type (call/put screens)
--   - symbol + snapshot_at (latest snapshot retrieval)
-- ============================================================

-- Primary access pattern: all strikes for a given symbol + expiration
CREATE INDEX IF NOT EXISTS idx_options_expiration
    ON options_snapshots (symbol, expiration_date, snapshot_at DESC);

-- Max pain query: needs OI grouped by strike for a specific expiration
CREATE INDEX IF NOT EXISTS idx_options_oi_by_strike
    ON options_snapshots (symbol, expiration_date, option_type, strike);

-- IV rank history time-series access
CREATE INDEX IF NOT EXISTS idx_iv_rank_symbol_date
    ON iv_rank_history (symbol, recorded_at DESC);
