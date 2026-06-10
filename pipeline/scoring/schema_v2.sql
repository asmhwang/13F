-- V2 ranking tables (side-by-side with v1; nothing here touches v1 tables).
-- Idempotent; v2 scoring modules run this at startup.

-- One row per fund x consecutive-quarter clone window:
-- buy the filed portfolio at disclosure (filed_date), hold to the next
-- disclosure. Non-overlapping by construction.
CREATE TABLE IF NOT EXISTS fund_clone_windows_v2 (
    fund_id          TEXT NOT NULL,
    start_period     DATE NOT NULL,    -- period_of_report of the filed portfolio
    end_period       DATE NOT NULL,    -- next period_of_report
    start_date       DATE,             -- original filed_date of start_period
    end_date         DATE,             -- original filed_date of end_period
    clone_return     REAL,             -- value-weighted return of priced positions
    benchmark_return REAL,             -- S&P 500 TR over the same window
    excess_return    REAL,             -- clone - benchmark
    coverage         REAL,             -- priced value / total equity value (0-1)
    positions_priced INTEGER,
    positions_total  INTEGER,
    valid            INTEGER,          -- 1 = feeds skill stats, 0 = excluded
    invalid_reason   TEXT,             -- 'low_coverage' | 'no_benchmark' | 'bad_window'
    PRIMARY KEY (fund_id, start_period)
);

CREATE TABLE IF NOT EXISTS fund_rankings_v2 (
    fund_id           TEXT PRIMARY KEY,
    fund_name         TEXT,
    rank              INTEGER,
    score             REAL,            -- 0-100 percentile of shrunk_ir_annual
    shrunk_ir_annual  REAL,            -- shrunk mean excess / stdev * sqrt(4)
    ir_annual         REAL,            -- unshrunk annualized information ratio
    t_stat            REAL,            -- mean / (stdev / sqrt(n))
    mean_excess_q     REAL,            -- mean quarterly excess return
    stdev_excess_q    REAL,
    n_windows         INTEGER,
    win_rate          REAL,            -- fraction of windows with excess > 0
    recent_4q_excess  REAL,            -- sum of the last 4 window excess returns
    avg_coverage      REAL,
    median_positions  REAL,
    median_aum        REAL,
    top10_weight_med  REAL,            -- median top-10 concentration (0-1)
    first_window      DATE,
    last_window       DATE,
    eligible          INTEGER NOT NULL,
    fail_reason       TEXT             -- 'insufficient_windows' | 'no_dispersion' | NULL
);

CREATE TABLE IF NOT EXISTS stock_rankings_v2 (
    ticker            TEXT PRIMARY KEY,
    company_name      TEXT,
    sector            TEXT,
    rank              INTEGER,
    score             REAL,            -- 0-100 percentile of raw_score
    raw_score         REAL,            -- sum over backers: skill x conviction x mults
    n_backers         INTEGER,         -- positive-skill ranked funds holding it
    top_fund_id       TEXT,
    top_fund_name     TEXT,            -- highest-skill backer
    top_fund_skill    REAL,            -- that fund's shrunk_ir_annual
    max_weight        REAL,            -- largest portfolio weight among backers
    avg_weight        REAL,
    new_buys          INTEGER,         -- backers that opened this quarter
    adds              INTEGER,         -- backers that increased >= 20%
    trims             INTEGER,         -- backers that trimmed >= 20%
    avg_tenure        REAL,            -- avg consecutive quarters held
    market_cap        REAL,
    pe_ratio          REAL,
    pe_available      INTEGER,
    price_fresh       INTEGER,         -- 1 = traded within 7 days of quarter end
    as_of_date        DATE
);
