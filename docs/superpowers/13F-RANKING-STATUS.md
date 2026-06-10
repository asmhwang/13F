# 13F Ranking Feature — Build Status & Resume Guide

**Last updated:** 2026-06-09
**Branch:** P1–P4 on `feat/13f-ranking`, P5 on `feat/13f-website` — both merged to `main`. Unit-bug fix + filer expansion (2026-06-09) committed on `fix/13f-value-units-and-filer-expansion` (`86e6929`), branched off `main`, **not yet merged or pushed**.
**Resume anchor:** this file. Read it first after clearing chat.

This feature turns ingested 13F filings into two ranked outputs — **Fund Rankings** (which small, concentrated funds pick well long-term) and **Stock Rankings** (which stocks those funds are most convicted on) — both as raw + filtered views, shown on the Streamlit site.

**Status in one line: DONE and live. Backend (P1–P4) + website (P5) complete; a critical 13F unit-of-value bug was found and fixed; 24 small/mid filers ingested → 9 funds now rank (was 1), 135 raw + 9 filtered stocks. 71 tests pass.**

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

## Add-small-filers session (2026-06-09) — DONE ✅

Goal was "add small filers so >1 fund ranks." Doing it surfaced a critical data bug; fixing it + broadening the fund universe got us to **9 ranked funds, 135 raw + 9 filtered stocks**.

### 1. Critical fix — 13F value-unit bug (was corrupting ~4 funds 1000×)
- **Root cause:** 13F `<value>` units are *mixed* post-2022 — most filers switched to whole dollars, but several still report thousands (price-validated: Baupost 14/14, T. Rowe 15/15, Tieton 14/14, Duquesne 13/14; partial: RenTech, Tweedy Browne, Wedgewood, GreenhavenRd, Giverny, Baillie Gifford). The old blanket `period >= "2022-12-31" → //1000` (ingest.py) corrupted the thousands-filers 1000×.
- **Proof:** Baupost 2026-03-31 AMZN raw XML `<value>649543</value>`, 3,118,754 shares → $208/sh only if value is thousands ($649.5M) — matches real AMZN. The `//1000` had stored it as $649K.
- **Consequence:** Baupost was the *only* "ranked fund" in the old baseline **purely because of this bug** — its real $649M positions looked like $649K and slipped under the $100M weed gate. With correct units Baupost's max position is $650M → correctly weeded out (`position_too_large`).
- **Fix (TDD):** new pure `parser.detect_value_divisor(holdings)` — median implied price/share; `>= 1` ⇒ dollars (÷1000), else already-thousands (÷1). Self-contained at ingest (no prices-table dependency), subsumes the pre/post-2022 split. Wired into `ingest.py`. Re-ingested all **3,695** filings `--force`; Baupost AUM $0.01B→$5.12B; post-2022 corruption 79 filings → 0. Tests: `tests/test_value_units.py` (5).

### 2. Broadened the fund weed gate (user-approved) — `pipeline/scoring/fund_pipeline.py`
- `_MAX_POSITIONS` 30→**55**, `_POSITION_LIMIT_THOUSANDS` 100_000→**200_000** ($200M).
- **Why:** with correct units the strict gate left only 2 tiny funds (GreenhavenRd, Wedgewood) that **share zero stocks** — the filtered tab was structurally impossible. The multi-fund consensus on mega-caps lives in slightly-larger concentrated funds the strict gate rejected. Broadening admits them. Tests updated (mid/broad eligible fixtures).

### 3. Filtered-tab holder_count knob 3→1 (user-approved) — `pipeline/scoring/stock_pipeline.py`
- Even with 9 funds, **no** small/mid-cap (300M–4B) is co-held by ≥2 of the top funds — concentrated funds only co-hold mega-caps (GOOGL/AAPL ~$4T, above the cap). The "≥3 funds agree on a 300M–4B stock" premise is structurally empty.
- Lowered `_MIN_FILTERED_HOLDERS` 3→1 (the spec's "revisit after first run" knob), reframing the filtered tab as **"top-fund high-conviction small/mid-cap ideas"**. Extracted a testable `passes_filtered_gate(...)`. Tab now shows 9 names (HRMY, BBSI, AMR, CUBI, MYE, NBBK, ZUMZ, VPG, ACIC). Test: `test_passes_filtered_gate`.

### 4. Filers ingested (24 new CIKs)
Eligible after broadening (9 ranked): Dalal Street/Pabrai (#1, score 100), Tieton, Giverny, Semper Augustus, Wedgewood, Voss, Mar Vista, Greenhaven Road, Punch Card. Ingested-but-weeded (honest fails): Semper/Voss/etc. were *admitted*; ineligible ones = inactive (Cove Street, Roumell, Ensemble, Mittleman, Intrepid, Sasco, Aravt — all stopped filing) or too-broad (Gator 90 pos, Pinnacle 955, Donald Smith 61, Polaris 84) or `<5yr` (Alta Fox, Praetorian). Also added `EDGAR_USER_AGENT` to `.env` (was missing → IP-block risk).

### Optional follow-ups (not blocking)
- **Backfill prices for 145 missing eligible-fund tickers** (current eligible-fund coverage 82%; missing ones are mostly delisted). A full re-price of the new funds' deep history (~2,900 tickers) was started then **killed** — Yahoo was throttling at ~15s/ticker (~12h ETA) and most are delisted junk held only by ineligible funds. Run a *targeted* fetch later if return precision matters.
- Re-run `bash refresh.sh` quarterly as before (chain unchanged).

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

## Data reality (post-2026-06-09 session — what's real)

- **Value units: now correct per-filing** (see fix #1 above). Baupost et al. no longer 1000× off.
- **Prices: real**, ~98% value-weighted on the established universe; new eligible-fund coverage 82% (145 tickers, mostly delisted, optionally backfillable).
- **Fundamentals: real** for the ranked-stock universe (213 tickers, 198 with sector; market cap, P/E, gross margin populated).
- **9 ranked funds** (was 1): Dalal Street/Pabrai #1, then Tieton, Giverny, Semper Augustus, Wedgewood, Voss, Mar Vista, Greenhaven Road, Punch Card. Latest quarter 2026-03-31.
- **Stock rankings:** 135 raw, **9 filtered** (small/mid-cap conviction picks). `stock_rankings_filtered` populated after the holder_count 3→1 knob (fix #3); ≥2-holder consensus on small/mid-caps does not exist with concentrated funds.
- CUSIP resolution ~36% (16,374 / 45,464 securities) — many are bonds/foreign/expired; widening it is optional, not blocking.
- **All correct given the data.**

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
