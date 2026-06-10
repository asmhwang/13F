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
- **Fund Rankings**: small, concentrated funds ranked by long-term selection skill (3yr forward returns vs S&P 500 TR, time-weighted, turnover- and consistency-adjusted)
- **Stock Rankings**: the stocks those top funds are most convicted on — full raw universe + a filtered small/mid-cap investable list, with confidence grades

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
├── app.py               # Streamlit dashboard (all 5 views)
├── refresh.sh           # One-shot refresh: ingest → CUSIPs → prices → fundamentals → rankings
├── requirements.txt
├── .env                 # Local secrets (gitignored)
├── .env.example         # Template for .env
├── data/
│   ├── 13f.db           # SQLite database (created on first run)
│   ├── refresh.log      # Log output from refresh runs
│   └── http_cache/      # Disk cache for EDGAR HTTP responses
├── pipeline/
│   ├── database.py      # Schema + DB helpers + amendment resolution (effective_filings)
│   ├── edgar.py         # SEC EDGAR API client, filer search, seed filer list
│   ├── parser.py        # 13F filing parser (XML + legacy text formats)
│   ├── ingest.py        # CLI ingestion script
│   ├── cusip.py         # CUSIP → ticker resolver (OpenFIGI)
│   ├── prices.py        # Daily prices + S&P 500 TR benchmark (Yahoo, keyless)
│   ├── fundamentals.py  # Current-quarter fundamentals (Finnhub)
│   ├── queries.py       # Analytical queries (conviction scores, QoQ changes, etc.)
│   └── scoring/
│       ├── adapter.py        # Filings schema → spec terms; price/benchmark lookups
│       ├── fund_pipeline.py  # Fund ranking (7 stages)
│       └── stock_pipeline.py # Stock ranking (6 stages)
├── webui/               # Fund Rankings + Stock Rankings pages (theme, components, data layer)
└── tests/               # 78 tests (pipelines, parser, rankings, webui)
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

---

## Step 3 — Build the Fund & Stock Rankings

Turns the ingested filings into two ranked outputs shown on the dashboard: **Fund Rankings** (small, concentrated funds ranked by long-term stock-selection skill) and **Stock Rankings** (the stocks those funds are most convicted on, raw + filtered views).

**Prerequisites (`.env`):**

| Key | Used by | Where to get it |
|---|---|---|
| `EDGAR_USER_AGENT` | all EDGAR fetches | No signup — set to `Your Name you@example.com` (SEC requires it; missing = IP-block risk) |
| `OPENFIGI_API_KEY` | Step 2 (CUSIP→ticker) | Free at [openfigi.com/api](https://www.openfigi.com/api) |
| `FINNHUB_API_KEY` | fundamentals (sector, market cap, P/E, margin) | Free at [finnhub.io](https://finnhub.io) |

Prices and the benchmark come from Yahoo's public chart endpoint — no key needed.

**Run the chain in order** (each step is incremental/idempotent — safe to re-run):

```bash
python3 -m pipeline.prices                   # 1. daily prices + ^SP500TR benchmark for every held ticker
python3 -m pipeline.fundamentals             # 2. current-quarter fundamentals for the ranked-fund universe
python3 -m pipeline.scoring.fund_pipeline    # 3. fund rankings  → prints {'eligible': N, 'ranked': N}
python3 -m pipeline.scoring.stock_pipeline   # 4. stock rankings → prints {'universe': N, 'ranked': N, 'filtered': N}
```

Notes per step:
- **Prices** — first full run is slow (Yahoo throttles; thousands of tickers). Re-runs only fetch new/uncovered tickers and fresh bars. `--coverage` prints a value-weighted coverage report; `--limit N` for a smoke test. Delisted/junk tickers 404 once and are never retried.
- **Fundamentals** — ~1 request/second on the free tier; the universe is only the stocks held by ranked funds (~200 tickers ≈ 4 min). Run it *after* the fund pipeline once, or just run the chain twice the very first time (it reads the previous run's `fund_rankings`).
- **Fund pipeline** — 7 stages: eligibility weeding → 3yr forward returns (as-of = original filing date) → quarterly score vs S&P 500 TR → time-weighted score (λ=0.85 by calendar quarter, ≥6 scoreable quarters) → turnover penalty → consistency percentile → composite (0–100, ranked). Truncate-rebuild: every run is a clean recompute.
- **Stock pipeline** — 6 stages over the top half of ranked funds: per-stock conviction signals → OLS regression on 3yr forward return (falls back to fund conviction when <8 training rows) → sector adjustment → 5-component confidence (High/Medium/Low) → raw + filtered outputs. The filtered tab = small/mid-cap ($300M–$4B), non-Low confidence, mid 52-week range, held by a top fund.

**See the results:** `streamlit run app.py` → sidebar **Fund Rankings** / **Stock Rankings**. Click "Inspect a fund" for the excess-return chart and turnover detail; "Inspect a stock" for fundamentals + the holders table.

**Eligibility rules (why a fund does/doesn't rank):** a fund must have ≤55 positions, no single position >$200M, ≥5 years of filing history, a filing in the most recent quarter, and ≥6 scoreable quarters (a quarter is scoreable only once its 3-year forward window has elapsed). Failure reasons are recorded in the `fund_eligibility` table:

```bash
sqlite3 data/13f.db "SELECT fund_id, fail_reason FROM fund_eligibility WHERE eligible = 0 LIMIT 20"
```

**Troubleshooting:**
- *Fund Rankings page empty* → no fund passed the gates; check `fund_eligibility` above, then ingest more small/concentrated filers (sidebar search or `pipeline.ingest --cik`).
- *Stock score column shows "—" / regression fell back* → too few training rows; usually means prices are missing — re-run `pipeline.prices` and check `--coverage`.
- *Filtered tab empty* → no ranked-fund holding currently sits in the $300M–$4B band with non-Low confidence; the Raw tab is the full universe.
- *Stale numbers after adding a filer* → run the full chain (or `bash refresh.sh`); pages cache for 5 minutes.

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

**Fund Rankings view:**
- Ranked list of eligible funds with score bar, avg AUM, position count, quarters of data, turnover, and time-weighted score
- Score/sort filters; "one-hit wonder" chip for funds whose record hinges on a single quarter
- "Inspect a fund" → modal with the historical excess-return chart (3yr forward vs S&P 500 TR) and turnover detail

**Stock Rankings view:**
- **Raw tab**: full ranked universe with confidence badge, score, holder count, net change, avg tenure; sector + confidence filters
- **Filtered tab**: the investable subset (small/mid-cap, non-Low confidence, mid 52-week range)
- "Inspect a stock" → modal with market cap, P/E, 52-week range position, gross margin, and the ranked funds holding it

All views filter out options (puts/calls), zero-value rows, and resolve amendments automatically (see the amendments note above).

---

## Keeping Data Current

13F filings are published quarterly (~45 days after each quarter end). Run the refresh script — it executes the whole chain (ingest → CUSIPs → prices → fundamentals → fund rankings → stock rankings) for every tracked filer, including ones added from the dashboard:

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
```

Key ranking tables: `fund_rankings`, `fund_quarterly_scores`, `fund_eligibility` (with `fail_reason`), `stock_rankings_raw`, `stock_rankings_filtered`, `stock_signals`, `stock_confidence`, `effective_filings` (amendment resolution).

Or open it with any SQLite client (e.g. [DB Browser for SQLite](https://sqlitebrowser.org/)).
