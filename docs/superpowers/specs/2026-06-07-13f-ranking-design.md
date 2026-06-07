# 13F Ranking — Fund & Stock Ranking Pipeline — Design Spec

**Date:** 2026-06-07
**Status:** Approved
**Source spec:** `13F_ranking_developer_spec_v2.md` (+ matching PDFs)

---

## Overview

Turn the ingested 13F data into two ranked, website-displayed outputs:

1. **Fund Rankings** — which small, concentrated funds have the best long-term selection track records (7-stage pipeline).
2. **Stock Rankings** — which stocks the top funds are most convicted on now (6-stage pipeline), shown raw + filtered.

Scoring is framed as a **Holding Period Return Simulation**: did a fund's picks, held statically for 3 years from the as-of date, beat the S&P 500? It measures selection skill, not actual fund performance.

---

## Context: the developer spec assumes a data model we do not have

The developer spec was written against an idealized schema (`holdings.ticker`, `funds`, `portfolios`, `prices`). The real ingested DB is filings-shaped and has **no price, benchmark, or fundamental data**. Roughly **70% of the work is data acquisition**, not the scoring math.

### Real schema → spec terms (adapter mapping)

| Spec term | Real source |
|---|---|
| `funds.fund_id` | `filers.cik` |
| `funds.fund_name` | `filers.name` |
| `funds.first_filing_date` | `MIN(filings.period_of_report)` per cik |
| `holdings.quarter_date` | `filings.period_of_report` |
| filing publication date | `filings.filed_date` |
| `holdings.position_value_usd` | `holdings.value_thousands * 1000` |
| `holdings.share_count` | `holdings.shares` |
| `holdings.ticker` | `securities.ticker` via `holdings.cusip` join (**~36% resolved**) |
| `portfolios.total_portfolio_value_usd` | `SUM(value_thousands*1000)` per filing |
| exclude options/derivatives | `WHERE put_call IS NULL OR put_call = ''` |
| `prices` | **does not exist → Phase 1 builds it** |
| S&P 500 benchmark | **does not exist → Phase 1 builds it** |
| sector / P/E / margin / shares-out | **does not exist → Phase 3 builds it** |

### Data inventory (as of brainstorm)

- 31 filers, 3,001 filings, **history 1999-2025** (deep enough for 5yr weed + 3yr forward).
- 5.3M holdings rows; ~9% are options (`put_call` set), excluded.
- `securities`: 44,450 distinct CUSIPs, **15,892 resolved to tickers (36%)**. Remainder largely non-US-equity / bonds / expired / unmappable; `cusip.py` stores a NULL sentinel for confirmed no-match. All three API keys (OpenFIGI, Polygon, Finnhub) are set.

---

## Data sources (free, no paid tier)

| Need | Source | Key | Notes |
|---|---|---|---|
| Adjusted daily prices | Yahoo v8 chart (`yfinance` 1.0, already installed) | none | Deep history, split/div adjusted. Direct endpoint fallback. |
| S&P 500 benchmark | Yahoo `^SP500TR` (total-return index) | none | True total return (incl. dividends). |
| Sector, market cap, shares (current) | Finnhub `/stock/profile2` + `/stock/metric` | `FINNHUB_API_KEY` | Current snapshot only; 60 req/min free. |
| Sector, shares, margin, EPS (historical) | SEC EDGAR companyfacts (`data.sec.gov/api/xbrl/...`) | none | Authoritative, historical, for regression training. |
| CUSIP→ticker | existing `cusip.py` (OpenFIGI → Polygon) | set | Unchanged. |

Rejected: Stooq (now JS proof-of-work gated, unscriptable); keyed price sources Tiingo/AlphaVantage/TwelveData/FMP (free tiers throttle deep history too hard).

---

## Architecture & module layout

```
pipeline/
  prices.py            [P1] Yahoo price + ^SP500TR ingest      → prices, benchmark
  fundamentals.py      [P3] Finnhub + SEC fundamentals ingest  → fundamentals, sectors
  scoring/
    __init__.py
    schema.sql         all new result tables + indexes
    adapter.py         real-schema → spec-term helpers (the mapping table above)
    fund_pipeline.py   [P2] stages 1-7  → fund_rankings (+ intermediates)
    stock_pipeline.py  [P4] stages 1-6  → stock_rankings_raw / _filtered
    runner.py          CLI orchestration; wired into refresh.sh / refresh.bat
app.py                 [P5] +2 pages: Fund Rankings, Stock Rankings
tests/                 unit tests per stage on fixture data
```

Conventions inherited from the repo: flat `pipeline/` modules, CTE-style SQL (heed the correlated-subquery / composite-index lessons in git history), **materialized result tables** so Streamlit reads stay cheap under `@st.cache_data`. Compute that is awkward in SQL (decay weights, stdev, percentile, regression) is done in pandas/numpy; everything is written back to tables.

---

## Phasing

Each phase is an independent ship with its own `writing-plans` cycle, built and tested before the next.

| Phase | Builds | Depends on | Primary output |
|---|---|---|---|
| **P1** | Price + benchmark ingest | — | `prices`, `benchmark`, coverage report |
| **P2** | Fund Ranking (7 stages) | P1 | `fund_rankings` |
| **P3** | Fundamentals ingest | — | `fundamentals`, `sectors` |
| **P4** | Stock Ranking (6 stages) | P1, P2, P3 | `stock_rankings_raw`, `stock_rankings_filtered` |
| **P5** | Website pages | P2, P4 | Fund + Stock Streamlit pages |

---

## Phase 1 — Price + benchmark ingest (`pipeline/prices.py`)

**Tables**

```sql
CREATE TABLE prices (
    ticker      TEXT NOT NULL,
    date        DATE NOT NULL,
    close       REAL,
    adj_close   REAL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX idx_prices_ticker ON prices(ticker);

CREATE TABLE benchmark (          -- ^SP500TR total-return series
    date        DATE PRIMARY KEY,
    adj_close   REAL NOT NULL
);

CREATE TABLE price_fetch_log (    -- incremental bookkeeping
    ticker        TEXT PRIMARY KEY,
    first_date    DATE,
    last_date     DATE,
    status        TEXT,           -- 'ok' | 'no_data' | 'error'
    fetched_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Scope (keeps it free + bounded):** only tickers held by the 31 funds (resolved `securities.ticker`, equity only). Per-ticker date window = `[first holding quarter, last holding quarter + 3 years]`, capped at today. Incremental: skip ticker/date ranges already in `prices`; `price_fetch_log` records `no_data`/`error` so dead tickers aren't retried forever.

**Fetch:** `yfinance` with `auto_adjust=False` (keep raw `close` + computed `adj_close`); direct Yahoo v8 endpoint as fallback. Polite throttle + retry; reuse the `http_cache`-style caching pattern.

**Coverage report** (printed + optional table): for the most recent quarter and overall, the % of holdings **by value** that have (a) a usable price at the as-of date and (b) a price at as-of + 3yr. This quantifies real data quality before any scoring runs.

**Out of scope for P1:** fundamentals.

---

## Phase 2 — Fund Ranking pipeline (`pipeline/scoring/fund_pipeline.py`)

All stages operate on eligible funds only, via `adapter.py`. The global `current_quarter_date` = most recent `period_of_report` across all filings, applied identically to every fund.

**Stage 1 — Weeding** → `fund_eligibility(fund_id, eligible, fail_reason)`. Filter OUT if any: `MAX(position_value) > $100M`; `COUNT(equity tickers) > 30`; `(today − first_filing_date) < 5y`; `last_filing < current_quarter_date`. `fail_reason ∈ {position_too_large, too_many_positions, insufficient_history, inactive, insufficient_scoreable_quarters, null}`.

**Stage 2 — Forward return join** → `holding_returns(fund_id, quarter_date, ticker, position_value_usd, three_yr_return, data_quality_flag)`. `3yr_return = (fwd_price − base_price)/base_price`. **Base price = `adj_close` at the as-of date (`filed_date`, see Resolved Ambiguities); forward = as-of + 3yr.** Look-ahead rule: forward price = latest `adj_close` with `date ≤ as_of+3yr`. Missing entirely → `null_excluded`. CUSIP unresolved → `cusip_unresolved`. `data_quality_flag ∈ {clean, last_price, spinoff, spinoff_partial, null_excluded, cusip_unresolved}`.

**Stage 3 — QPS** → `fund_quarterly_scores(fund_id, quarter_date, qps_raw, qps_excess, benchmark_return, positions_included, positions_excluded_null)`. `weight(i)=pos_value(i)/Σpos_value` (renormalized over non-null positions); `raw_QPS=Σ weight·3yr_return`; `benchmark_return` from `benchmark` over the as-of→+3yr window; `excess_QPS = raw_QPS − benchmark_return`. Only quarters with 3yr-forward data.

**Stage 4 — TWS** → `fund_tws(fund_id, tws, quarters_scored, oldest_quarter_included, one_hit_wonder_flag, best_quarter_contribution)`. Requires ≥6 scoreable quarters (else `insufficient_scoreable_quarters` → ineligible). `λ=0.85`, `w(t)=λ^(quarters_from_most_recent)`, `TWS=Σ w·excess_QPS / Σ w`. One-hit-wonder: if `MAX(w·excess_QPS)/Σ(w·excess_QPS) > 0.50` → flag + `TWS *= 0.75`.

**Stage 5 — Turnover** → `fund_turnover(fund_id, avg_turnover_rate, turnover_multiplier, quarter_pairs_measured)`. `turnover(t)=|tickers in t-1 not in t| / |tickers in t-1|`; `multiplier = CLAMP(1 − avg_turnover·0.5, 0.5, 1.0)`.

**Stage 6 — Consistency** → `fund_consistency(fund_id, qps_stdev, consistency_score)`. `qps_stdev = STDEV(excess_QPS)`; `consistency_score = 1 − PERCENT_RANK(qps_stdev)` across all eligible funds (relative).

**Stage 7 — Composite** → `fund_rankings` (full output columns per source spec: rank, final_score 0-100, tws_raw, avg_turnover_rate, turnover_multiplier, consistency_score, one_hit_wonder_flag, best_quarter_contribution, quarters_of_data, avg_position_count, avg_aum, eligible, fail_reason). `raw_composite = TWS·turnover_multiplier·0.70 + consistency_score·0.30`; `final_score = (raw_composite − MIN)/(MAX − MIN) · 100`.

---

## Phase 3 — Fundamentals ingest (`pipeline/fundamentals.py`)

**Tables**

```sql
CREATE TABLE fundamentals (
    ticker            TEXT NOT NULL,
    as_of_date        DATE NOT NULL,    -- quarter being described
    market_cap        REAL,
    shares_out        REAL,
    pe_ratio          REAL,
    pe_available      INTEGER,          -- 0/1
    gross_margin_pct  REAL,
    source            TEXT,             -- 'finnhub' | 'sec'
    PRIMARY KEY (ticker, as_of_date)
);
CREATE TABLE sectors (
    ticker  TEXT PRIMARY KEY,
    sector  TEXT NOT NULL
);
```

**Current quarter:** Finnhub `/stock/profile2` (sector via `finnhubIndustry`, market cap, shares) + `/stock/metric` (P/E, gross margin). **Historical (regression training):** SEC companyfacts — `EntityCommonStockSharesOutstanding`×price → market cap; `GrossProfit`/`Revenues` → margin; `EarningsPerShareDiluted` + price → P/E; SIC code (from EDGAR submissions metadata) → sector bucket. 52-week range is computed in P4 from the `prices` table per the source spec's null rules.

---

## Phase 4 — Stock Ranking pipeline (`pipeline/scoring/stock_pipeline.py`)

**Stage 1 — Universe:** stocks held by top-50% funds (`fund_rankings.rank ≤ total_eligible/2`) in the most recent quarter.

**Stage 2 — Signals** per stock: `fund_conviction`, `holder_count`, `net_change_pct`, `avg_relative_size`, `avg_tenure` (consecutive quarters held, resets on exit/re-entry) — formulas per source spec.

**Stage 3 — Fundamental variables:** market cap (required), 52wk_range_position + 52wk_partial (computed from `prices` with the spec's 4-week/52-week null rules), P/E + pe_available, sector dummy, gross_margin. Pulled from P3 tables.

**Stage 4 — Regression** → predict `3yr_return` (from `holding_returns`) on the feature set (signals + `log(market_cap)` + 52wk + P/E dummies + one-hot sectors). Training set = historical stock×quarter observations with 3yr-forward data, top-50% funds. **Default OLS** (hand-rolled numpy normal equations; weighting switchable). `sector_adjusted_score = raw_score − sector_mean_score`.

**Stage 5 — Confidence** → `stock_confidence`: composite of weighted_holder_score (30%), avg_tenure_score (25%), avg_relative_size (20%), direction_agreement (15%), data_quality_score (10%); each normalized 0-1; bucketed by percentile (≥67th High / 33-67 Medium / <33 Low), recomputed per quarter.

**Stage 6 — Outputs:** `stock_rankings_raw` (all columns per source spec) and `stock_rankings_filtered` (`confidence_flag != Low`, `300M ≤ market_cap ≤ 4B`, `0.1 ≤ 52wk_range_position ≤ 0.9`, `holder_count ≥ 3`, top-N by `sector_adjusted_score`, target 30-75 names).

---

## Phase 5 — Website (`app.py`)

Two new pages added to the existing `st.radio` view switch, reusing `inject_css`, `kpi_row`, `shdr`, and `@st.cache_data` loaders.

- **Fund Rankings:** table (Rank, Fund, Score, Avg AUM, Positions, Quarters, Turnover); filters (score/AUM/positions); sort any column; fund detail (holdings, historical QPS chart, turnover history); score tooltip (3 components); persistent staleness label.
- **Stock Rankings:** Raw / Filtered tabs; table (Rank, Ticker, Company, Sector, Score, Confidence, #Funds, Net Change, Avg Tenure); net-change color (green buy / red sell); confidence badge; filters (sector/confidence/market-cap); sort; stock detail (which qualifying funds hold it, weight, quarters); staleness disclaimer.

---

## Resolved ambiguities (spec contradictory/silent — chosen defaults, all config-switchable)

1. **As-of price date.** Source Stage 2 says `quarter_date`; Stage 4 says "use price at time of filing, NOT quarter end." → Use **`filed_date`** as the as-of for base price (info is public then; removes look-ahead from the ≤45-day filing lag). Forward = `filed_date + 3yr`. Config flag switches to `quarter_date`.
2. **Regression weights.** Source says "weighted" but never defines weights → **OLS / equal weight** default; flag to weight by `avg_relative_size`.
3. **Forward price / missing data.** Forward = latest `adj_close` with `date ≤ as_of+3yr`; missing entirely → `null_excluded` + renormalize weights; CUSIP unresolved → excluded.

## Scope cuts (v2 — flagged, free-data limits / YAGNI)

- **Full spinoff dual-leg tracking** (Stage 2): v1 relies on Yahoo split/adjusted pricing and flags spinoffs; no manual leg-splitting.
- **Macro-shock quarter exclusion** (Stage 6 consistency): source spec marks future-iteration.

---

## Dependencies

Add to `requirements.txt`: `yfinance` (already installed in env), `numpy` (already transitive via pandas — pin explicitly). No scikit-learn / statsmodels.

## Recalculation / refresh integration

Quarterly per source spec. `runner.py` chains: ingest → resolve CUSIPs → fetch prices → fetch fundamentals → fund pipeline → stock pipeline. Wired into `refresh.sh` / `refresh.bat` and the app's refresh action. Result tables are rebuilt idempotently.

## Testing strategy

Per-stage unit tests on small fixture data (a few synthetic funds/holdings/prices with hand-computed expected scores). Look-ahead rule, weight renormalization, one-hit-wonder discount, turnover clamp, percentile relativity, and confidence bucketing each get targeted tests. Coverage report sanity-checked against the real DB before P2 runs.

---

## Files changed (by phase)

| Phase | Files |
|---|---|
| P1 | `pipeline/prices.py` (new), `pipeline/scoring/schema.sql` (new), `requirements.txt`, tests |
| P2 | `pipeline/scoring/{adapter,fund_pipeline,runner}.py` (new), tests |
| P3 | `pipeline/fundamentals.py` (new), schema additions, tests |
| P4 | `pipeline/scoring/stock_pipeline.py` (new), tests |
| P5 | `app.py`, `refresh.sh`, `refresh.bat` |
