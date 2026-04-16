# 13F

A pipeline and dashboard for ingesting, normalizing, and analyzing SEC 13F-HR filings. Tracks institutional investors ($100M+ AUM) quarterly and surfaces conviction scores, position changes, and cross-filer patterns.

**Key features:**
- Ingests filings from SEC EDGAR (XML and legacy text formats), with HTTP disk caching for fast re-runs
- Handles amendments (13F-HR/A) correctly — always uses the latest amended filing per period
- Normalizes the SEC's Q4 2022 unit change (value field switched from thousands to raw dollars)
- Resolves CUSIPs to tickers/names via OpenFIGI
- 31 pre-configured seed filers across activist, long/short, growth, value, macro, and large asset manager categories
- Search and add any EDGAR 13F filer (~6,000 institutions) directly from the dashboard — full history ingested in the background
- Streamlit dashboard with single-filer deep-dives, cross-filer comparison, and conviction scoring

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
├── app.py               # Streamlit dashboard
├── refresh.sh           # One-shot refresh: ingest + resolve CUSIPs
├── requirements.txt
├── .env                 # Local secrets (gitignored)
├── .env.example         # Template for .env
├── data/
│   ├── 13f.db           # SQLite database (created on first run)
│   ├── refresh.log      # Log output from refresh runs
│   └── http_cache/      # Disk cache for EDGAR HTTP responses
├── pipeline/
│   ├── database.py      # Schema + DB helpers
│   ├── edgar.py         # SEC EDGAR API client, filer search, seed filer list
│   ├── parser.py        # 13F filing parser (XML + legacy text formats)
│   ├── ingest.py        # CLI ingestion script
│   ├── cusip.py         # CUSIP → ticker resolver (OpenFIGI)
│   └── queries.py       # Analytical queries (conviction scores, QoQ changes, etc.)
└── tests/
    └── test_edgar_search.py  # Unit tests for EDGAR filer search
```

---

## Step 1 — Ingest Data

All commands are run from the repo root.

**Quickstart — ingest the 31 seed institutions, latest quarter only:**

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
| `--seed` | Ingest all 31 pre-configured seed filers |
| `--cik <CIK>` | Ingest a single filer by SEC CIK |
| `--latest-only` | Only fetch the most recent filing per filer |
| `--since <YYYY-MM-DD>` | Skip filings filed before this date |
| `--force` | Re-ingest filings already in the database |
| `--db <path>` | Use a custom database path (default: `data/13f.db`) |

Filings already in the database are skipped automatically. HTTP responses are cached to `data/http_cache/` — re-runs that hit only the cache complete in a few seconds regardless of history size.

**Seed filers** (31 pre-configured in `pipeline/edgar.py`):

| Category | Filers |
|---|---|
| Legacy | Berkshire Hathaway, Pershing Square, Renaissance Technologies, Bridgewater Associates, Gates Foundation Trust, Appaloosa Management, Tiger Global, Coatue Management, Viking Global, Lone Pine Capital |
| Activist | Elliott Investment Management, Starboard Value, ValueAct Capital, Third Point |
| Long/Short | D.E. Shaw, Two Sigma Investments, Citadel Advisors, Point72 Asset Management, Baupost Group |
| Growth | ARK Investment Management, Baillie Gifford, Ruane Cunniff & Goldfarb |
| Value | Tweedy Browne, Greenlight Capital |
| Macro / Family Office | Duquesne Family Office |
| Large Asset Managers | BlackRock Advisors, Vanguard Group, FMR LLC (Fidelity), T. Rowe Price Associates, Franklin Resources, Capital Research Global Investors |

**Adding filers from the dashboard:** Use the "Add New Filer" search box in the sidebar to find and add any of the ~6,000 EDGAR 13F filers by name. Full filing history is ingested in the background — the UI stays responsive and updates automatically when complete.

> **Note on amendments:** When a filer files a 13F-HR/A amendment, only the most recently filed version for that period is used. All queries automatically deduplicate amendments.

> **Note on SEC unit change:** Starting with periods ending on or after 2022-12-31, the SEC changed the `<value>` field in 13F XML filings from thousands of dollars to raw dollars. The pipeline normalizes this automatically on ingestion.

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

## Step 3 — Launch the Dashboard

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

All views filter out options (puts/calls), zero-value rows, and duplicate amendments automatically.

---

## Keeping Data Current

13F filings are published quarterly (~45 days after each quarter end). Run the refresh script to pull the latest filings and resolve any new CUSIPs:

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
```

Or open it with any SQLite client (e.g. [DB Browser for SQLite](https://sqlitebrowser.org/)).
