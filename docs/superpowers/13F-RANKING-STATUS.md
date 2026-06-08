# 13F Ranking Feature — Build Status & Resume Guide

**Last updated:** 2026-06-08
**Branch:** P1–P4 on `feat/13f-ranking`, P5 on `feat/13f-website` — both merged to `main` (latest merge `fba117c`), pushed to `origin`.
**Resume anchor:** this file. Read it first after clearing chat.

This feature turns ingested 13F filings into two ranked outputs — **Fund Rankings** (which small, concentrated funds pick well long-term) and **Stock Rankings** (which stocks those funds are most convicted on) — both as raw + filtered views, shown on the Streamlit site.

**Status in one line: backend (P1–P4) + website (P5) are DONE and live; price/fundamental data is fully backfilled. The ONLY thing left is adding more small filers so more than one fund ranks.**

Source specs: `docs/superpowers/specs/2026-06-07-13f-ranking-design.md`, `docs/superpowers/specs/2026-06-07-13f-rankings-website-design.md`
Per-phase plans: `docs/superpowers/plans/2026-06-07-13f-*.md`

---

## The process we're using

For each phase: **brainstorm → design spec → implementation plan (TDD, bite-sized) → subagent-driven execution** (one implementer subagent, then a spec-compliance review subagent, then an opus code-quality review subagent, then a fix pass). Every phase ends green with per-task commits.

Phases are independent ships, dependency-ordered (ALL DONE):
`P1 prices ✅ → P2 fund ranking ✅ → P3 fundamentals ✅ → P4 stock ranking ✅ → P5 website ✅`.

---

## What's DONE (P1–P5, backend + website complete) ✅

P1–P4 on `feat/13f-ranking` (38 commits); P5 on `feat/13f-website` (14 commits). **65 tests passing.** No new paid services (all free/no-key or keys already in `.env`). Only new dep across the whole feature: `numpy>=1.26`.

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

### P5 — Website (Apple-flavored Streamlit pages)  `webui/`
- New isolated package: `webui/data.py` (pure SQL→DataFrame queries + `@st.cache_data` wrappers), `webui/components.py` (pure fmt/color/filter helpers + render helpers), `webui/theme.py` (scoped `.rk-` CSS + Emil-Kowalski motion), `webui/fund_rankings.py`, `webui/stock_rankings.py`.
- **Fund Rankings page:** hero + staleness, numeric KPI strip, score/sort filters, animated ranking list, "Inspect a fund" → `st.dialog` modal (excess-QPS Plotly chart + turnover).
- **Stock Rankings page:** Raw / Filtered tabs, sector/confidence filters, confidence badges, green/red net-change, "Inspect a stock" → modal (market cap / P/E / 52wk / margin + holders table). Filtered tab shows a graceful empty-state.
- Wired into `app.py` (two `st.radio` views + dispatch + sidebar filter guard + `theme.inject()`); recalc chain appended to `refresh.sh`/`.bat` (`ingest → CUSIPs → prices → fundamentals → fund → stock`).
- Design pass applied **emil-design-eng** (capped stagger, gated hover, sub-400ms eased entries, reduced-motion), **impeccable** (numeric KPIs, tabular figures, balanced hero, lighter cards), **taste-skill** (killed AI-purple chip, single Apple-blue accent). Verified via headless-Chrome screenshot + Streamlit `AppTest`.
- Tests: `tests/test_webui_data.py`, `tests/test_webui_components.py` (10 new). The 3 existing views (Single Filer, Cross-Filer, Conviction Scores) are untouched.
- **Run the site:** `streamlit run app.py` → pick "Fund Rankings" / "Stock Rankings" in the sidebar.

### Operational data backfill — DONE (2026-06-08)
Ran the full chain `prices → fundamentals → fund_pipeline → stock_pipeline`:
- **Price coverage 21.8% → 98.2%** (9,914 tickers, ~20.6M rows; 2,149 failed = delisted/FIGI-junk 404s, expected).
- Fundamentals: 22 tickers (the ranked-fund universe), all with sector/market-cap/P/E/margin.
- Stock raw rankings are now **regression-backed with real market caps** (no more conviction fallback).

---

## What's LEFT (one thing)

**Add more small filers.** Everything else is done. The rankings still show **1 fund (Baupost)** and an **empty filtered stock list** — not a bug, a data-population gap:
- 30 of the 31 seed filers fail the "small concentrated fund" weed (single position > $100M, or > 30 positions, or < 5yr history, or inactive). Only **Baupost** passes.
- With 1 ranked fund, every stock has `holder_count = 1`, so the filtered gate (`holder_count ≥ 3`) yields **0** by construction. The other filtered gates already pass (mcap 300M–4B: 5 stocks, confidence≠Low: 15, 52wk-range 0.1–0.9: 17).
- **To unblock:** ingest ≥2 more genuinely small, concentrated funds (search EDGAR or supply CIKs) → re-run `fund_pipeline` then `stock_pipeline` (or just `bash refresh.sh`). Once ≥3 funds rank and ≥3 hold the same stock, multi-fund signals + the filtered tab populate.

Optional alternative (no new filers): the filtered `holder_count ≥ 3` threshold is a spec-documented "revisit after first run" knob — lowering it to 1 in `pipeline/scoring/stock_pipeline.py` would surface a filtered list against the single fund immediately.

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

## Data reality (post-backfill 2026-06-08 — what's real, what's still thin)

- **Prices: real.** 98.2% value-weighted coverage (9,914 tickers, ~20.6M rows). The stock regression now trains on real 3yr returns (no more fund_conviction fallback).
- **Fundamentals: real** for the 22-stock universe (sector, market cap, P/E, gross margin all populated).
- **Still 1 ranked fund: Baupost Group** (`cik 1061768`). 30 of 31 seed filers fail the small-concentrated weed (`position_too_large` / `too_many_positions` / `inactive` / `insufficient_history`). This is the **only** remaining gap — see "What's LEFT".
- **`stock_rankings_filtered` = 0** purely because `holder_count ≥ 3` is impossible with 1 fund (every stock has holder_count = 1). NOT market_cap (now populated) and NOT prices (now 98%).
- CUSIP resolution ~36% (15,892 / 44,450 securities have tickers) — many are bonds/foreign/expired; widening it is optional, not blocking.
- **All correct given the data.** Adding small filers is the single lever that makes fund + filtered-stock rankings rich.

---

## Key decisions locked (so you don't re-litigate)

- **As-of price date = `filed_date`** (not quarter end) — kills the ≤45-day look-ahead. Config-switchable.
- **Regression = OLS** via numpy `lstsq` (no sklearn/statsmodels), with a fund_conviction fallback for tiny/singular training sets.
- **Data sources, all free:** Yahoo (prices + `^SP500TR`), Finnhub (current fundamentals), SEC (deferred historical). Polygon stays as the CUSIP resolver only. Stooq rejected (JS anti-bot).
- **Schema adapter** maps the real filings schema to the spec's terms (`cik`→fund_id, `value_thousands*1000`, `securities` ticker join, `put_call` options filter) — see `pipeline/scoring/adapter.py`.

---

## How to resume after clearing chat

1. `git checkout main && git pull` (everything is merged + pushed; this file is the index).
2. Re-read this file. Specs: `specs/2026-06-07-13f-ranking-design.md` (backend) + `specs/2026-06-07-13f-rankings-website-design.md` (website).
3. **See it run:** `streamlit run app.py` → sidebar "Fund Rankings" / "Stock Rankings".
4. **The one open task — add small filers** (makes rankings multi-fund + fills the filtered tab):
   - Find ≥2 genuinely small, concentrated 13F filers (single position < $100M, ≤ 30 positions, ≥ 5yr history, filed most recent quarter). Search EDGAR via `python3 -m pipeline.edgar` helpers or supply CIKs.
   - Ingest them (`pipeline.ingest`), then `bash refresh.sh` (runs CUSIPs → prices → fundamentals → fund → stock). Prices/fundamentals are incremental, so this is fast now.
   - Re-open the site; fund rankings + filtered stocks populate once ≥3 funds rank.
   - Alternative with no new filers: lower the filtered `holder_count ≥ 3` knob in `pipeline/scoring/stock_pipeline.py` to surface a filtered list against Baupost alone.
5. Memory: a MemPalace `13F-platform` wing (rooms `decisions`, `backend`) has per-phase + P5 + backfill drawers with full detail.

**Test everything still green:** `python3 -m pytest -q` → expect **65 passed**.
