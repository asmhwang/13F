-- New tables for the ranking pipelines. Idempotent; every scoring module runs
-- this at startup so required tables always exist.

-- ---- Phase 1: prices + benchmark -------------------------------------------
CREATE TABLE IF NOT EXISTS prices (
    ticker     TEXT NOT NULL,
    date       DATE NOT NULL,
    close      REAL,
    adj_close  REAL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_prices_ticker ON prices(ticker);

CREATE TABLE IF NOT EXISTS benchmark (        -- ^SP500TR total-return series
    date       DATE PRIMARY KEY,
    adj_close  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS price_fetch_log (  -- incremental bookkeeping
    ticker     TEXT PRIMARY KEY,
    first_date DATE,
    last_date  DATE,
    status     TEXT,                          -- 'ok' | 'no_data' | 'error'
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---- Phase 2: fund ranking -------------------------------------------------
CREATE TABLE IF NOT EXISTS fund_eligibility (
    fund_id     TEXT PRIMARY KEY,
    eligible    INTEGER NOT NULL,
    fail_reason TEXT
);

CREATE TABLE IF NOT EXISTS holding_returns (
    fund_id            TEXT NOT NULL,
    quarter_date       DATE NOT NULL,
    ticker             TEXT NOT NULL,   -- resolved ticker, or the cusip when unresolved
    position_value_usd REAL,
    three_yr_return    REAL,            -- NULL when excluded
    data_quality_flag  TEXT,
    PRIMARY KEY (fund_id, quarter_date, ticker)
);

CREATE TABLE IF NOT EXISTS fund_quarterly_scores (
    fund_id                 TEXT NOT NULL,
    quarter_date            DATE NOT NULL,
    qps_raw                 REAL,
    qps_excess              REAL,
    benchmark_return        REAL,
    positions_included      INTEGER,
    positions_excluded_null INTEGER,
    PRIMARY KEY (fund_id, quarter_date)
);

CREATE TABLE IF NOT EXISTS fund_tws (
    fund_id                   TEXT PRIMARY KEY,
    tws                       REAL,
    quarters_scored           INTEGER,
    oldest_quarter_included   DATE,
    one_hit_wonder_flag       INTEGER,
    best_quarter_contribution REAL
);

CREATE TABLE IF NOT EXISTS fund_turnover (
    fund_id                TEXT PRIMARY KEY,
    avg_turnover_rate      REAL,
    turnover_multiplier    REAL,
    quarter_pairs_measured INTEGER
);

CREATE TABLE IF NOT EXISTS fund_consistency (
    fund_id           TEXT PRIMARY KEY,
    qps_stdev         REAL,
    consistency_score REAL
);

CREATE TABLE IF NOT EXISTS fund_rankings (
    fund_id                   TEXT PRIMARY KEY,
    fund_name                 TEXT,
    rank                      INTEGER,
    final_score               REAL,
    tws_raw                   REAL,
    avg_turnover_rate         REAL,
    turnover_multiplier       REAL,
    consistency_score         REAL,
    one_hit_wonder_flag       INTEGER,
    best_quarter_contribution REAL,
    quarters_of_data          INTEGER,
    avg_position_count        REAL,
    avg_aum                   REAL,
    eligible                  INTEGER,
    fail_reason               TEXT
);

-- ---- Phase 3: fundamentals (current quarter) -------------------------------
CREATE TABLE IF NOT EXISTS fundamentals (
    ticker            TEXT NOT NULL,
    as_of_date        DATE NOT NULL,
    market_cap        REAL,
    shares_out        REAL,
    pe_ratio          REAL,
    pe_available      INTEGER,
    gross_margin_pct  REAL,
    source            TEXT,
    PRIMARY KEY (ticker, as_of_date)
);

CREATE TABLE IF NOT EXISTS sectors (
    ticker  TEXT PRIMARY KEY,
    sector  TEXT NOT NULL
);
