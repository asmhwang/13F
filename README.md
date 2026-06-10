# 13F

A pipeline and dashboard for ingesting, normalizing, and analyzing SEC 13F-HR filings. Tracks institutional investors ($100M+ AUM) quarterly and surfaces conviction scores, position changes, and cross-filer patterns.

**Key features:**
- Ingests filings from SEC EDGAR (XML and legacy text formats), with HTTP disk caching for fast re-runs
- Handles amendments (13F-HR/A) correctly — RESTATEMENTs replace, NEW HOLDINGS amendments union with the original
- Normalizes the SEC's Q4 2022 unit change (value field switched from thousands to raw dollars), detected per filing
- Resolves CUSIPs to tickers/names via OpenFIGI
- 40 pre-configured seed filers across activist, long/short, growth, value, macro, large asset manager, and small/mid concentrated categories
- Search and add any EDGAR 13F filer (~6,000 institutions) directly from the dashboard — full history ingested in the background
- Streamlit dashboard with single-filer deep-dives, cross-filer comparison, and conviction scoring
- **Two ranking methodologies, side by side** (see [Methodology](#methodology--how-the-two-ranking-versions-are-made)):
  - **v1 — Holding Period Return Simulation**: funds scored on whether their picks beat the S&P 500 over a static 3-year hold; stocks scored by a regression over top-fund consensus signals
  - **v2 — Clone Returns + Best Ideas**: funds scored by a shrunk information ratio on non-overlapping quarterly "copy the filing" returns; stocks scored as the skill-weighted best ideas of concentrated managers (no regression)
- **Walk-forward backtest** (`pipeline/backtest.py`, results in [BACKTEST.md](BACKTEST.md)): point-in-time comparison of both versions against the S&P 500 TR — v2 outperformed v1 on every metric over 40 quarters (2015–2025)

---

## Setup

**Requirements:** Python 3.11+

**macOS / Linux:**

```bash
pip3 install -r requirements.txt
```

**Windows:**

```bat
pip install -r requirements.txt
```

> On Windows, use `python` instead of `python3` in all commands below.

---

## Project Structure

```
13F/
├── app.py               # Streamlit dashboard (all 7 views)
├── refresh.sh           # One-shot refresh: ingest → CUSIPs → prices → fundamentals → rankings (v1 + v2)
├── BACKTEST.md          # Walk-forward backtest results: v1 vs v2 vs S&P 500 TR
├── requirements.txt
├── .env                 # Local secrets (gitignored)
├── .env.example         # Template for .env
├── data/
│   ├── 13f.db           # SQLite database (created on first run)
│   ├── backtest_results.csv  # Per-epoch backtest detail
│   ├── refresh.log      # Log output from refresh runs
│   └── http_cache/      # Disk cache for EDGAR HTTP responses
├── pipeline/
│   ├── database.py      # Schema + DB helpers + amendment resolution (effective_filings)
│   ├── edgar.py         # SEC EDGAR API client, filer search, seed filer list
│   ├── parser.py        # 13F filing parser (XML + legacy text formats)
│   ├── ingest.py        # CLI ingestion script
│   ├── cusip.py         # CUSIP → ticker resolver (OpenFIGI + Polygon, online)
│   ├── cusip_local.py   # Offline CUSIP resolution: prefix chaining + name matching
│   ├── prices.py        # Daily prices + S&P 500 TR benchmark (Yahoo, keyless)
│   ├── fundamentals.py  # Current-quarter fundamentals (Finnhub)
│   ├── queries.py       # Analytical queries (conviction scores, QoQ changes, etc.)
│   ├── backtest.py      # Walk-forward backtest harness (point-in-time v1 vs v2)
│   └── scoring/
│       ├── adapter.py           # Filings schema → spec terms; price/benchmark lookups
│       ├── fund_pipeline.py     # v1 fund ranking (7 stages; supports as_of= for backtests)
│       ├── stock_pipeline.py    # v1 stock ranking (6 stages; supports as_of=)
│       ├── fund_pipeline_v2.py  # v2 fund ranking (clone returns → shrunk IR)
│       ├── stock_pipeline_v2.py # v2 stock ranking (best-ideas score)
│       └── schema_v2.sql        # v2 result tables
├── webui/               # Fund/Stock Rankings pages, v1 + v2 (theme, components, data layer)
└── tests/               # 94 tests (pipelines, parser, rankings, v2 scoring math, webui)
```

---

## Step 1 — Ingest Data

All commands are run from the repo root.

**Quickstart — ingest the 40 seed institutions, latest quarter only:**

```bash
python3 -m pipeline.ingest --seed --latest-only
```

**Ingest all history for the seed filers (first run fetches from EDGAR; subsequent runs complete in seconds from cache):**

```bash
python3 -m pipeline.ingest --seed
```

**Ingest a specific filer by CIK:**

```bash
python3 -m pipeline.ingest --cik 0001067983
```

**Ingest a specific filer since a given date:**

```bash
python3 -m pipeline.ingest --cik 0001067983 --since 2022-01-01
```

**Flags:**

| Flag | Description |
|---|---|
| `--seed` | Ingest all 40 pre-configured seed filers |
| `--all-tracked` | Ingest every filer already in the database (seed + dashboard-added) |
| `--cik <CIK>` | Ingest a single filer by SEC CIK |
| `--latest-only` | Only fetch the most recent filing per filer |
| `--since <YYYY-MM-DD>` | Skip filings filed before this date |
| `--force` | Re-ingest filings already in the database |
| `--db <path>` | Use a custom database path (default: `data/13f.db`) |

Filings already in the database are skipped automatically. HTTP responses are cached to `data/http_cache/` — re-runs that hit only the cache complete in a few seconds regardless of history size.

**Seed filers** (40 pre-configured in `pipeline/edgar.py`):

| Category | Filers |
|---|---|
| Legacy | Berkshire Hathaway, Pershing Square, Renaissance Technologies, Bridgewater Associates, Gates Foundation Trust, Appaloosa Management, Tiger Global, Coatue Management, Viking Global, Lone Pine Capital |
| Activist | Elliott Investment Management, Starboard Value, ValueAct Capital, Third Point |
| Long/Short | D.E. Shaw, Two Sigma Investments, Citadel Advisors, Point72 Asset Management, Baupost Group |
| Growth | ARK Investment Management, Baillie Gifford, Ruane Cunniff & Goldfarb |
| Value | Tweedy Browne, Greenlight Capital |
| Macro / Family Office | Duquesne Family Office |
| Large Asset Managers | BlackRock Advisors, Vanguard Group, FMR LLC (Fidelity), T. Rowe Price Associates, Franklin Resources, Capital Research Global Investors |
| Small / Mid Concentrated | Dalal Street (Pabrai), Tieton Capital, Giverny Capital, Semper Augustus, Wedgewood Partners, Voss Capital, Mar Vista, Greenhaven Road, Punch Card — these are the funds that actually rank |

**Adding filers from the dashboard:** Use the "Add New Filer" search box in the sidebar to find and add any of the ~6,000 EDGAR 13F filers by name. Full filing history is ingested in the background — the UI stays responsive and updates automatically when complete.

> **Note on amendments:** 13F-HR/A amendments come in two SEC kinds and are resolved by type: a **RESTATEMENT** replaces the original information table; a **NEW HOLDINGS** amendment (e.g. positions revealed after confidential treatment expires) is *unioned* with it. Tiny amendments labeled RESTATEMENT (<50% of the original's positions) are treated as additive — pre-2013 confidential-treatment releases were habitually mislabeled. The resolution lives in the `effective_filings` table (`pipeline/database.py:rebuild_effective_filings`), rebuilt automatically after every ingest, and all queries read from it.

> **Note on SEC unit change:** Around the 2022-12-31 reporting period the SEC moved the `<value>` field in 13F filings from thousands of dollars toward whole dollars — but adoption is uneven (large filers like Berkshire switched; others such as Baupost, T. Rowe Price, and Tieton still report in thousands). A blanket period-based rule corrupts the stragglers by 1000×, so the pipeline detects the unit **per filing** from the median implied price per share (value ÷ shares) and normalizes each filing to thousands accordingly (`pipeline/parser.py:detect_value_divisor`).

---

## Step 2 — Resolve CUSIPs to Tickers

Maps every CUSIP in the database to a ticker symbol, company name, and exchange via the [OpenFIGI API](https://www.openfigi.com/api).

**Set up your API key (recommended):**

```bash
cp .env.example .env
# edit .env and set OPENFIGI_API_KEY=your_key_here
```

Get a free key at [openfigi.com/api](https://www.openfigi.com/api). The key is loaded automatically from `.env` — no `export` needed.

**Run the resolver:**

```bash
python3 -m pipeline.cusip
```

Without a key the resolver still works but is rate-limited (~2+ hours for a full history). With a key it completes in ~5 minutes. Already-resolved CUSIPs are skipped on re-runs. Expect ~50–55% match rate — older, delisted, and non-equity instruments won't resolve.

**Offline second pass — historical CUSIPs:**

```bash
python3 -m pipeline.cusip_local
```

OpenFIGI only knows *current* CUSIPs, but a 20+ year database is full of dead ones (pre-reverse-split Citigroup, old GE, renamed issuers). This pass recovers them with no network, using three rules: same first 8 characters as a resolved security (check-digit variant of the same issue), same 6-character issuer prefix when the issuer maps to exactly one ticker, and normalized issuer-name matching (handles legacy filings whose name field embeds table fragments). Bond CUSIPs (letters in the issue code), preferreds/warrants/units, and junk tickers are excluded; every mapping is logged with its rule and evidence in `cusip_resolution_log` for auditing. Runs automatically in `refresh.sh` (step 2b) — after it resolves new tickers, the next `pipeline.prices` run backfills their price history automatically.

---

## Step 3 — Build the Fund & Stock Rankings

Turns the ingested filings into ranked outputs shown on the dashboard, in **two parallel methodologies**: v1 (Fund Rankings / Stock Rankings) and v2 (Fund Rankings v2 / Stock Rankings v2). Both run side by side; see the [Methodology](#methodology--how-the-two-ranking-versions-are-made) section for how each works and the [backtest](BACKTEST.md) for how they compare.

**Prerequisites (`.env`):**

| Key | Used by | Where to get it |
|---|---|---|
| `EDGAR_USER_AGENT` | all EDGAR fetches | No signup — set to `Your Name you@example.com` (SEC requires it; missing = IP-block risk) |
| `OPENFIGI_API_KEY` | Step 2 (CUSIP→ticker) | Free at [openfigi.com/api](https://www.openfigi.com/api) |
| `FINNHUB_API_KEY` | fundamentals (sector, market cap, P/E, margin) | Free at [finnhub.io](https://finnhub.io) |

Prices and the benchmark come from Yahoo's public chart endpoint — no key needed.

**Run the chain in order** (each step is incremental/idempotent — safe to re-run):

```bash
python3 -m pipeline.prices                      # 1. daily prices + ^SP500TR benchmark for every held ticker
python3 -m pipeline.fundamentals                # 2. current-quarter fundamentals for the ranked-fund universe
python3 -m pipeline.scoring.fund_pipeline       # 3. v1 fund rankings  → prints {'eligible': N, 'ranked': N}
python3 -m pipeline.scoring.stock_pipeline      # 4. v1 stock rankings → prints {'universe': N, 'ranked': N, 'filtered': N}
python3 -m pipeline.scoring.fund_pipeline_v2    # 5. v2 fund rankings  → prints {'ranked': N, 'valid_windows': N}
python3 -m pipeline.scoring.stock_pipeline_v2   # 6. v2 stock rankings → prints {'universe': N, 'backers': N}
```

Notes per step:
- **Prices** — first full run is slow (Yahoo throttles; thousands of tickers). Re-runs only fetch new/uncovered tickers and fresh bars. `--coverage` prints a value-weighted coverage report; `--limit N` for a smoke test. Delisted/junk tickers 404 once and are never retried.
- **Fundamentals** — ~1 request/second on the free tier; the universe is only the stocks held by ranked funds (~200 tickers ≈ 4 min). Run it *after* the fund pipeline once, or just run the chain twice the very first time (it reads the previous run's `fund_rankings`).
- **All four scoring pipelines** are truncate-rebuild: every run is a clean recompute, safe to re-run anytime. The mechanics of each are described in [Methodology](#methodology--how-the-two-ranking-versions-are-made) below.

**See the results:** `streamlit run app.py` → sidebar **Fund Rankings** / **Stock Rankings** (v1) and **Fund Rankings v2** / **Stock Rankings v2**. Click "Inspect a fund" for the excess-return chart; "Inspect a stock" for fundamentals + the holders table.

**Troubleshooting:**
- *v1 Fund Rankings page empty* → no fund passed the v1 gates; check `sqlite3 data/13f.db "SELECT fund_id, fail_reason FROM fund_eligibility WHERE eligible = 0"`, then ingest more small/concentrated filers (sidebar search or `pipeline.ingest --cik`). v2 ranks far more funds (no size gates) — check that tab too.
- *Stock score column shows "—" / regression fell back* → too few training rows; usually means prices are missing — re-run `pipeline.prices` and check `--coverage`.
- *v1 Filtered tab empty* → no ranked-fund holding currently sits in the $300M–$4B band with non-Low confidence; the Raw tab is the full universe.
- *Stale numbers after adding a filer* → run the full chain (or `bash refresh.sh`); pages cache for 5 minutes.

---

## Methodology — how the two ranking versions are made

Both versions answer the same two questions — *which managers pick stocks well, and what are they convicted on right now* — with different machinery. They share the same ingested data (effective filings, resolved tickers, adjusted prices, ^SP500TR benchmark) and write to separate tables, so the dashboard can show them side by side.

### v1 — "Holding Period Return Simulation" (original spec)

**Fund ranking** (`pipeline/scoring/fund_pipeline.py`, 7 stages):

1. **Weeding** — hard eligibility gates: ≤55 equity positions, no single position >$200M, ≥5 years of filing history, filed in the most recent quarter. Failures recorded in `fund_eligibility.fail_reason`.
2. **Forward returns** — every holding gets a 3-year static-hold return, priced from the filing's *disclosure date* (no look-ahead: the position wasn't public until then). Delistings use the last traded price; unresolved CUSIPs are excluded and weights renormalized.
3. **QPS** — each quarter's value-weighted portfolio return minus the S&P 500 TR over the same 3-year window. A quarter is only scoreable once its window has elapsed.
4. **TWS** — time-weighted average of excess QPS with λ=0.85 decay per calendar quarter (recent quarters count more); requires ≥6 scoreable quarters; funds whose record hinges on one quarter get a 25% "one-hit wonder" discount.
5. **Turnover** — funds that churn positions get a multiplier penalty (0.5–1.0).
6. **Consistency** — percentile rank of the stdev of excess QPS (steadier = better).
7. **Composite** — `0.70 × penalized TWS + 0.30 × consistency`, min-max normalized to 0–100, ranked.

**Stock ranking** (`pipeline/scoring/stock_pipeline.py`, 6 stages): universe = stocks held by the top half of ranked funds. Five conviction signals per stock (rank-weighted fund conviction, holder count, net QoQ positioning, average portfolio weight, holding tenure) feed an **OLS regression trained on historical 3-year forward returns**; predictions are sector-demeaned, graded with a 5-component confidence score (High/Medium/Low), and published as a raw universe plus a filtered small/mid-cap ($300M–$4B) investable list.

**Known weaknesses** (the reason v2 exists): adjacent 3-year windows overlap ~92%, so the consistency/one-hit-wonder statistics measure autocorrelation rather than skill; the regression is circular (funds qualify by `final_score`, which was fit on the same forward returns the regression predicts); the composite mixes units (a return × 0.7 + a percentile × 0.3) and min-max scaling lets one outlier fund compress everyone else; the $200M position gate expels successful funds as they grow; and the newest scoreable quarter is always 3 years stale.

### v2 — Clone Returns + Best Ideas

**Fund ranking** (`pipeline/scoring/fund_pipeline_v2.py`):

1. **Clone windows** — for every pair of consecutive quarterly filings, simulate buying the fund's filed portfolio at its disclosure date and holding it until the next disclosure (what a 13F copier could actually have earned). Each window's value-weighted return minus the S&P 500 TR over the *same dates* is one observation — and windows never overlap, so the observations are statistically independent. Windows with <60% of book value priced are excluded (`fund_clone_windows_v2.invalid_reason`).
2. **Skill score** — the per-window excess returns are summarized as a **shrunk information ratio**: `mean_shrunk = mean × n/(n+8)` (empirical-Bayes shrinkage toward zero — a 50-quarter record moves the needle far more than an 8-quarter hot streak), then `shrunk_IR = mean_shrunk / stdev × √4` (annualized). Consistency is built in: volatile excess returns lower the IR directly, so no separate consistency score, one-hit-wonder flag, or decay constant is needed. A t-stat and win rate are stored alongside for transparency.
3. **Ranking** — funds with ≥12 valid windows are ranked by shrunk IR and scored as a **percentile (0–100)**, so a single outlier can't compress the scale. There are no size gates: position count, AUM, and top-10 concentration are descriptive columns, not filters.

**Stock ranking** (`pipeline/scoring/stock_pipeline_v2.py`) — no regression, no consensus requirement. A stock's score is the sum, over every **backer** holding it, of:

```
skill × conviction × recency × tenure

backer      = ranked fund with positive shrunk IR and a concentrated book
              (median ≤100 positions — index-scale filers are skilled but
              their 3,000th position isn't an "idea")
skill       = the fund's shrunk IR
conviction  = position weight ÷ the fund's median position weight (capped 8×)
              — measures emphasis within that manager's own book
recency     = ×1.25 new buy · ×1.10 added ≥20% · ×0.85 trimmed ≥20% · ×1.0 held
tenure      = +2% per consecutive quarter held (capped at 10)
```

This is the Cohen–Polk–Silli "best ideas" result operationalized: the top-weighted positions of skilled concentrated managers are informative *individually*, so one 22% Elliott position outranks a stock that six mediocre funds each hold at 1%. Scores are percentile-ranked 0–100; every component (backers, top fund, max weight, new buys/adds/trims, tenure) is stored and shown in the UI, so any rank can be decomposed by eye.

### How they compare

The walk-forward backtest (`python3 -m pipeline.backtest`, full writeup in [BACKTEST.md](BACKTEST.md)) rebuilds both methodologies point-in-time at 40 quarters (2015–2025), buys each top-20 list 50 days after quarter end (when the filings were actually public), and measures quarterly excess vs the S&P 500 TR: **v1 −0.23%/quarter (47.5% hit rate, lags the index); v2 +0.95%/quarter (67.5% hit rate, +400% cumulative vs +325% for the index)**. The v2 edge is consistent but not yet statistically significant (t=0.73) — read it as "clearly better than v1," not "proven alpha."

---

## Step 4 — Launch the Dashboard

```bash
streamlit run app.py
```

Then open **http://localhost:8501** in your browser.

**Single Filer view:**
- AUM, position count, and largest holding KPIs
- Portfolio composition donut chart (labeled by ticker)
- Top holdings bar chart
- Quarter-over-quarter position change chart (requires 2+ periods ingested)
- Full holdings table with ticker, weight %, and share count

**Cross-Filer Overview view:**
- AUM comparison across all loaded institutions
- Most widely held securities by breadth (# of institutions) and aggregate value
- Overlap heatmap: which firms hold which top securities

**Conviction Scores view:**
- Securities ranked by conviction score: `num_institutions × log(1 + avg_portfolio_weight%) × (1 + net_buyer_ratio)`
- Rewards securities that are widely held, carry meaningful position sizes, and are being bought/increased vs sold
- Net buyer ratio = fraction of holders who opened or grew their position vs prior quarter (0.5 default when no prior data)
- Scatter plots: score vs. avg weight, and breadth vs. concentration
- Full sortable table with score, # institutions, avg weight, net buyer ratio, and aggregate value

**Fund Rankings view (v1):**
- Ranked list of eligible funds with score bar, avg AUM, position count, quarters of data, turnover, and time-weighted score
- Score/sort filters; "one-hit wonder" chip for funds whose record hinges on a single quarter
- "Inspect a fund" → modal with the historical excess-return chart (3yr forward vs S&P 500 TR) and turnover detail

**Stock Rankings view (v1):**
- **Raw tab**: full ranked universe with confidence badge, score, holder count, net change, avg tenure; sector + confidence filters
- **Filtered tab**: the investable subset (small/mid-cap, non-Low confidence, mid 52-week range)
- "Inspect a stock" → modal with market cap, P/E, 52-week range position, gross margin, and the ranked funds holding it

**Fund Rankings v2 view:**
- Funds ranked by shrunk information ratio on non-overlapping clone windows — columns: percentile score, shrunk IR, t-stat, window count, win rate
- "Inspect a fund" → per-window excess-return bars + cumulative line (each bar is one independent quarter), coverage detail

**Stock Rankings v2 view:**
- Best-ideas ranking with backers count, max portfolio weight, activity chips (new buys / adds / trims), and tenure
- Filters: sector, minimum backers, market-cap band (small/mid vs large)
- "Inspect a stock" → modal with the top backer's skill, price-data freshness, and every v2-ranked fund holding it with weight and quarters held

All views filter out options (puts/calls), zero-value rows, and resolve amendments automatically (see the amendments note above).

---

## Keeping Data Current

13F filings are published quarterly (~45 days after each quarter end). Run the refresh script — it executes the whole chain (ingest → CUSIPs online + offline → prices → fundamentals → v1 rankings → v2 rankings) for every tracked filer, including ones added from the dashboard:

**macOS / Linux:**
```bash
bash refresh.sh
```

**Windows:**
```bat
refresh.bat
```

Output is logged to `data/refresh.log`. Already-ingested filings and already-resolved CUSIPs are skipped, so the script completes in seconds when nothing is new.

**Automatic weekly refresh:**

macOS / Linux (cron):
```bash
# Runs every Monday at 7am — add to crontab with: crontab -e
0 7 * * 1 /path/to/13F/refresh.sh
```

Windows (Task Scheduler): open Task Scheduler, create a Basic Task, set trigger to Weekly on Monday, and set the action to run `refresh.bat` in the repo directory.

A **Refresh Data** button in the dashboard sidebar runs the same script on demand and reloads the UI when complete.

---

## Querying the Database Directly

The database is a standard SQLite file at `data/13f.db`. You can query it from Python:

```python
from pipeline.database import get_connection
from pipeline.queries import top_holdings, conviction_scores, position_changes, filer_summary, available_periods

conn = get_connection()

# See what periods are loaded
periods = available_periods(conn)

# Top holdings across all filers for a period
rows = top_holdings(periods[0], top_n=20, conn=conn)

# Conviction scores (weights breadth + position size + net buyer momentum)
scores = conviction_scores(periods[0], min_filers=3, conn=conn)
for r in scores:
    print(r["name_of_issuer"], r["conviction_score"])

# Quarter-over-quarter position changes for a single filer
changes = position_changes("0001067983", period_new=periods[0], period_old=periods[1], conn=conn)
for r in changes:
    print(r["name_of_issuer"], r["status"], r["pct_change"])

# High-level stats for a filer in a period
summary = filer_summary("0001067983", periods[0], conn=conn)
print(summary["num_positions"], summary["total_aum_thousands"])

# Ranking outputs (populated by the scoring pipelines, Step 3)
for r in conn.execute("SELECT rank, fund_name, final_score FROM fund_rankings ORDER BY rank"):
    print(r["rank"], r["fund_name"], r["final_score"])
for r in conn.execute("SELECT rank, ticker, confidence_flag FROM stock_rankings_filtered ORDER BY rank"):
    print(r["rank"], r["ticker"], r["confidence_flag"])

# v2 ranking outputs
for r in conn.execute("SELECT rank, fund_name, shrunk_ir_annual, t_stat FROM fund_rankings_v2 WHERE eligible = 1 ORDER BY rank"):
    print(r["rank"], r["fund_name"], r["shrunk_ir_annual"], r["t_stat"])
for r in conn.execute("SELECT rank, ticker, n_backers, top_fund_name FROM stock_rankings_v2 ORDER BY rank LIMIT 20"):
    print(r["rank"], r["ticker"], r["n_backers"], r["top_fund_name"])
```

Key ranking tables — v1: `fund_rankings`, `fund_quarterly_scores`, `fund_eligibility` (with `fail_reason`), `stock_rankings_raw`, `stock_rankings_filtered`, `stock_signals`, `stock_confidence`. v2: `fund_rankings_v2`, `fund_clone_windows_v2` (one row per clone window with validity flags), `stock_rankings_v2`. Shared: `effective_filings` (amendment resolution), `cusip_resolution_log` (offline CUSIP mapping audit trail).

Or open it with any SQLite client (e.g. [DB Browser for SQLite](https://sqlitebrowser.org/)).
