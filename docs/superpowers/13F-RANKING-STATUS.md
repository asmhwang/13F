# 13F Ranking Feature — Build Status & Resume Guide

**Last updated:** 2026-06-07
**Branch:** built on `feat/13f-ranking`, merged to `main`.
**Resume anchor:** this file. Read it first after clearing chat.

This feature turns ingested 13F filings into two ranked outputs — **Fund Rankings** (which small, concentrated funds pick well long-term) and **Stock Rankings** (which stocks those funds are most convicted on) — both as raw + filtered views, eventually shown on the Streamlit site.

Source spec: `docs/superpowers/specs/2026-06-07-13f-ranking-design.md`
Per-phase plans: `docs/superpowers/plans/2026-06-07-13f-*.md`

---

## The process we're using

For each phase: **brainstorm → design spec → implementation plan (TDD, bite-sized) → subagent-driven execution** (one implementer subagent, then a spec-compliance review subagent, then an opus code-quality review subagent, then a fix pass). Every phase ends green with per-task commits.

Phases are independent ships, dependency-ordered:
`P1 prices → P2 fund ranking → P3 fundamentals → P4 stock ranking → P5 website`.

---

## What's DONE (P1–P4, backend complete) ✅

All on `feat/13f-ranking`, 38 commits, **55 tests passing**, no new paid services (all free/no-key or keys already in `.env`).

### P1 — Price + benchmark ingest  `pipeline/prices.py`
- Yahoo v8 chart endpoint (keyless), adjusted close, deep history. Benchmark = `^SP500TR` (S&P 500 total return).
- Tables: `prices(ticker,date,close,adj_close)`, `benchmark(date,adj_close)`, `price_fetch_log`.
- Scope per ticker = `[first holding quarter, last+3yr]`, incremental via fetch log. Junk-ticker filter (`NOT GLOB '*[0-9]*'`) drops ~3,389 FIGI/SEDOL codes. Value-weighted coverage report. CLI: `python3 -m pipeline.prices`.

### P2 — Fund ranking (7 stages)  `pipeline/scoring/fund_pipeline.py` + `adapter.py`
- Stages: weeding → 3yr forward returns (as-of = **`filed_date`**) → QPS vs benchmark → time-weighted score (λ=0.85, ≥6 quarters, one-hit-wonder ×0.75) → turnover multiplier → consistency percentile → composite (0.70/0.30, 0–100, ranked).
- Tables: `fund_eligibility`, `holding_returns`, `fund_quarterly_scores`, `fund_tws`, `fund_turnover`, `fund_consistency`, `fund_rankings`. Truncate-rebuild idempotent. CLI: `python3 -m pipeline.scoring.fund_pipeline`.

### P3 — Current-quarter fundamentals  `pipeline/fundamentals.py`
- Finnhub `/stock/profile2` (sector, market cap, shares) + `/stock/metric` (P/E, gross margin) for the ranked-fund stock universe.
- Tables: `fundamentals(ticker,as_of_date,...)`, `sectors(ticker,sector)`. CLI: `python3 -m pipeline.fundamentals`.

### P4 — Stock ranking (6 stages)  `pipeline/scoring/stock_pipeline.py`
- Top-half universe (`rank ≤ ceil(n/2)`) → per-stock signals (fund_conviction, holder_count, net_change_pct, avg_relative_size, avg_tenure) → OLS regression on 3yr return (numpy `lstsq`, **falls back to fund_conviction when <8 training rows**) + sector adjustment → 5-component confidence (High/Medium/Low) → raw + filtered outputs.
- Tables: `stock_signals`, `stock_confidence`, `stock_rankings_raw`, `stock_rankings_filtered`. CLI: `python3 -m pipeline.scoring.stock_pipeline`.
- Only new dependency in the whole feature: `numpy>=1.26`.

---

## What's LEFT

### P5 — Website (the only remaining phase) ⏳
Wire the result tables into `app.py` as two Streamlit pages (currently `app.py` has **no** `fund_rankings`/`stock_*` readers):
- **Fund Rankings page:** table (Rank, Fund, Score, Avg AUM, Positions, Quarters, Turnover), filters (score/AUM/positions), sort, fund detail (holdings + historical QPS chart + turnover history), score tooltip (3 components), staleness label.
- **Stock Rankings page:** Raw / Filtered tabs, table (Rank, Ticker, Company, Sector, Score, Confidence, #Funds, Net Change, Avg Tenure), net-change color (green buy / red sell), confidence badge, filters (sector/confidence/market-cap), sort, stock detail (which funds hold it + weight + quarters), staleness disclaimer.
- Wire the pipeline into `refresh.sh`/`refresh.bat` + the app refresh action (recalc order: ingest → resolve CUSIPs → prices → fundamentals → fund pipeline → stock pipeline).

### Operational data runs (not code — make the rankings real)
Rankings are currently **sparse/thin** (see Data Reality). To populate real data:
1. `python3 -m pipeline.prices`  — **full price backfill** (~12k tickers × ~0.5s ≈ 2–3 hrs, hits Yahoo a lot). Only ~20 tickers + the benchmark are loaded so far.
2. `python3 -m pipeline.fundamentals`  — full fundamentals for the universe (only 3 tickers loaded via a `--limit` smoke so far).
3. Re-run `fund_pipeline` then `stock_pipeline`.
4. Add more **small** filers (the 31 seed filers are mostly large institutions that fail the "small concentrated fund" weed).

---

## What we SKIPPED / DEFERRED (intentional, documented)

| Item | Why deferred | When needed |
|---|---|---|
| **Historical SEC-XBRL fundamentals (P3b)** | Heavy XBRL pipeline; ~no payoff on a 1-fund universe. Probe-confirmed feasible (SEC `company_tickers.json` + `companyconcept` for Revenues/EPS/GrossProfit/shares all 200). | To give P4's regression historical fundamental features (market_cap/P/E/margin over time). |
| **market_cap / P/E / gross_margin as regression features** | No historical values exist (only current from Finnhub), so they can't be train/predict-consistent. They are **display + filter inputs only** today. | After P3b. |
| **Full spinoff dual-leg return tracking** (fund Stage 2) | Yahoo adj_close absorbs most corp actions; manual leg-splitting is high-effort, low-marginal-value. Spinoffs are flagged, not split. | v2, if precision demands it. |
| **Macro-shock quarter exclusion** (consistency) | Source spec itself marks it future-iteration. | v2. |
| **Full CUSIP resolution** | Only ~36% of CUSIPs resolve to tickers (many are bonds/foreign/expired; resolver stores a NULL sentinel). | Optionally re-run `pipeline.cusip` / add sources to widen coverage. |
| **Concurrency / batching of price + fundamentals fetches** | Serial loops are fine for the tiny current universe. | If the universe grows to hundreds+. |

---

## Data reality (why output looks thin — NOT bugs)

- Only **1 fund passes weeding: Baupost Group** (`cik 1061768`). 23 of 31 seed filers fail `position_too_large`, 5 `inactive`, 2 `too_many_positions` — they're large institutions, not the "small concentrated funds" the spec targets.
- With 1 ranked fund: stock universe = Baupost's ~22 holdings; the regression hits its fund_conviction fallback (~11 priced returns, < 8 threshold gets... actually 1 training row); `stock_rankings_filtered` = 0 (the `holder_count ≥ 3` filter needs ≥3 funds, and `market_cap` is NULL for most until full fundamentals run).
- Coverage is ~22% because only ~20 tickers are priced AND only 36% of CUSIPs resolve.
- **All of this is correct given the current data.** The math is verified on fixtures; real rankings need the operational data runs above + more small filers.

---

## Key decisions locked (so you don't re-litigate)

- **As-of price date = `filed_date`** (not quarter end) — kills the ≤45-day look-ahead. Config-switchable.
- **Regression = OLS** via numpy `lstsq` (no sklearn/statsmodels), with a fund_conviction fallback for tiny/singular training sets.
- **Data sources, all free:** Yahoo (prices + `^SP500TR`), Finnhub (current fundamentals), SEC (deferred historical). Polygon stays as the CUSIP resolver only. Stooq rejected (JS anti-bot).
- **Schema adapter** maps the real filings schema to the spec's terms (`cik`→fund_id, `value_thousands*1000`, `securities` ticker join, `put_call` options filter) — see `pipeline/scoring/adapter.py`.

---

## How to resume after clearing chat

1. `git checkout main && git pull` (the feature is merged here). Latest feature commits are the P1–P4 work; this file is the index.
2. Re-read this file + `docs/superpowers/specs/2026-06-07-13f-ranking-design.md`.
3. **To build P5:** brainstorm/spec the website page wiring, then plan, then subagent-driven execution (same process). `app.py` is ~1,300 lines; the new pages plug into its existing `st.radio` view switch and `@st.cache_data` loaders.
4. **To make data real first:** run the three operational commands under "Operational data runs", then re-run the fund + stock pipelines, then build P5 against populated tables.
5. Memory: a MemPalace `13F-platform` wing (rooms `decisions`, `backend`) has per-phase drawers with full detail if needed.

**Test everything still green:** `python3 -m pytest -q` → expect 55 passed.
