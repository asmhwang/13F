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
