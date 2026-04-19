-- ============================================================
-- 001_initial_schema.sql
-- Market Data Service — TimescaleDB Schema
-- ============================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- OHLCV DAILY
-- ============================================================
CREATE TABLE IF NOT EXISTS ohlcv_daily (
    timestamp       TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          BIGINT          NOT NULL,
    adj_close       DOUBLE PRECISION,
    provider        TEXT            NOT NULL,
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT ohlcv_daily_pkey PRIMARY KEY (timestamp, symbol)
);

SELECT create_hypertable('ohlcv_daily', 'timestamp',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ============================================================
-- OHLCV INTRADAY
-- ============================================================
CREATE TABLE IF NOT EXISTS ohlcv_intraday (
    timestamp       TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    interval        TEXT            NOT NULL,   -- '1m','5m','15m','1h'
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          BIGINT          NOT NULL,
    provider        TEXT            NOT NULL,
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT ohlcv_intraday_pkey PRIMARY KEY (timestamp, symbol, interval)
);

SELECT create_hypertable('ohlcv_intraday', 'timestamp',
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE
);

-- ============================================================
-- FUNDAMENTALS (snapshot per quarter/fetch)
-- ============================================================
CREATE TABLE IF NOT EXISTS fundamentals (
    snapshot_date   DATE            NOT NULL,
    symbol          TEXT            NOT NULL,
    pe_ratio        DOUBLE PRECISION,
    eps             DOUBLE PRECISION,
    revenue         BIGINT,
    market_cap      BIGINT,
    debt_to_equity  DOUBLE PRECISION,
    roe             DOUBLE PRECISION,
    sector          TEXT,
    industry        TEXT,
    raw_data        JSONB,
    provider        TEXT            NOT NULL,
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT fundamentals_pkey PRIMARY KEY (snapshot_date, symbol)
);

-- ============================================================
-- EARNINGS
-- ============================================================
CREATE TABLE IF NOT EXISTS earnings (
    report_date     DATE            NOT NULL,
    symbol          TEXT            NOT NULL,
    eps_actual      DOUBLE PRECISION,
    eps_estimate    DOUBLE PRECISION,
    eps_surprise    DOUBLE PRECISION,
    revenue_actual  BIGINT,
    revenue_estimate BIGINT,
    fiscal_quarter  TEXT,
    fiscal_year     INT,
    provider        TEXT            NOT NULL,
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT earnings_pkey PRIMARY KEY (report_date, symbol)
);

-- ============================================================
-- NEWS SENTIMENT
-- ============================================================
CREATE TABLE IF NOT EXISTS news_sentiment (
    published_at    TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    headline        TEXT            NOT NULL,
    source          TEXT,
    sentiment_score DOUBLE PRECISION,   -- -1.0 to 1.0
    sentiment_label TEXT,               -- positive/neutral/negative
    url             TEXT,
    provider        TEXT            NOT NULL,
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT news_sentiment_pkey UNIQUE (published_at, symbol, headline)
);

SELECT create_hypertable('news_sentiment', 'published_at',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ============================================================
-- OPTIONS SNAPSHOTS
-- ============================================================
CREATE TABLE IF NOT EXISTS options_snapshots (
    snapshot_at         TIMESTAMPTZ     NOT NULL,
    symbol              TEXT            NOT NULL,
    expiration_date     DATE            NOT NULL,
    strike              DOUBLE PRECISION NOT NULL,
    option_type         TEXT            NOT NULL,    -- 'call' | 'put'
    bid                 DOUBLE PRECISION,
    ask                 DOUBLE PRECISION,
    last                DOUBLE PRECISION,
    volume              INT,
    open_interest       INT,
    implied_volatility  DOUBLE PRECISION,
    delta               DOUBLE PRECISION,
    gamma               DOUBLE PRECISION,
    theta               DOUBLE PRECISION,
    vega                DOUBLE PRECISION,
    iv_rank             DOUBLE PRECISION,
    provider            TEXT            NOT NULL,
    fetched_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT options_pkey PRIMARY KEY (snapshot_at, symbol, expiration_date, strike, option_type)
);

SELECT create_hypertable('options_snapshots', 'snapshot_at',
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE
);

-- ============================================================
-- DIVIDENDS
-- ============================================================
CREATE TABLE IF NOT EXISTS dividends (
    ex_date         DATE            NOT NULL,
    symbol          TEXT            NOT NULL,
    amount          DOUBLE PRECISION NOT NULL,
    pay_date        DATE,
    declaration_date DATE,
    provider        TEXT            NOT NULL,
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT dividends_pkey PRIMARY KEY (ex_date, symbol)
);

-- ============================================================
-- COVERAGE MANIFEST (also mirrored in SQLite for fast local checks)
-- ============================================================
CREATE TABLE IF NOT EXISTS coverage_map (
    id              SERIAL          PRIMARY KEY,
    symbol          TEXT            NOT NULL,
    data_type       TEXT            NOT NULL,   -- 'ohlcv','fundamentals', etc.
    interval        TEXT,                        -- '1d','1h',etc.
    start_date      DATE            NOT NULL,
    end_date        DATE            NOT NULL,
    provider        TEXT            NOT NULL,
    row_count       INT,
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT coverage_unique UNIQUE (symbol, data_type, interval, start_date, end_date, provider)
);

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_symbol     ON ohlcv_daily (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_intraday_symbol  ON ohlcv_intraday (symbol, interval, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fundamentals_symbol    ON fundamentals (symbol, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_earnings_symbol        ON earnings (symbol, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_news_symbol            ON news_sentiment (symbol, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_options_symbol         ON options_snapshots (symbol, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_coverage_symbol_type   ON coverage_map (symbol, data_type);
