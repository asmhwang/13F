# 13F

There are 13F files produced by the SEC which track institutional investors ($100M+ AUM) quarterly. Our goal is to create actionable insights after ingesting these files to create conviction scores and notice patterns for different securities.

---

## Setup

**Requirements:** Python 3.11+

Install dependencies:

```bash
pip3 install -r requirements.txt
```

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
└── pipeline/
    ├── database.py      # Schema + DB helpers
    ├── edgar.py         # SEC EDGAR API client + seed filer list
    ├── parser.py        # 13F filing parser (XML + legacy text formats)
    ├── ingest.py        # CLI ingestion script
    ├── cusip.py         # CUSIP → ticker resolver (OpenFIGI)
    └── queries.py       # Analytical queries (conviction scores, QoQ changes, etc.)
```

---

## Step 1 — Ingest Data

All commands are run from the repo root.

**Quickstart — ingest the 10 seed institutions, latest quarter only:**

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
| `--seed` | Ingest all 10 pre-configured seed filers |
| `--cik <CIK>` | Ingest a single filer by SEC CIK |
| `--latest-only` | Only fetch the most recent filing per filer |
| `--since <YYYY-MM-DD>` | Skip filings filed before this date |
| `--force` | Re-ingest filings already in the database |
| `--db <path>` | Use a custom database path (default: `data/13f.db`) |

Filings already in the database are skipped automatically. HTTP responses are cached to `data/http_cache/` — re-runs that hit only the cache complete in a few seconds regardless of history size.

**Seed filers** (pre-configured in `pipeline/edgar.py`):
- Berkshire Hathaway
- Pershing Square Capital Management
- Renaissance Technologies
- Bridgewater Associates
- Bill & Melinda Gates Foundation Trust
- Appaloosa Management
- Tiger Global Management
- Coatue Management
- Viking Global Investors
- Lone Pine Capital

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
- Securities ranked by conviction score: `num_institutions × log(1 + avg_portfolio_weight%)`
- Rewards securities that are both widely held and carry meaningful position sizes
- Scatter plots: score vs. avg weight, and breadth vs. concentration
- Full sortable table with score, # institutions, avg weight, and aggregate value

---

## Keeping Data Current

13F filings are published quarterly (~45 days after each quarter end). Run the refresh script to pull the latest filings and resolve any new CUSIPs:

```bash
bash refresh.sh
```

Output is logged to `data/refresh.log`. Already-ingested filings and already-resolved CUSIPs are skipped, so the script completes in seconds when nothing is new.

**Automatic weekly refresh (cron):**

```bash
# Runs every Monday at 7am — add to crontab with: crontab -e
0 7 * * 1 /path/to/13F/refresh.sh
```

A **Refresh Data** button in the dashboard sidebar runs the same script on demand and reloads the UI when complete.

---

## Querying the Database Directly

The database is a standard SQLite file at `data/13f.db`. You can query it from Python:

```python
from pipeline.database import get_connection
from pipeline.queries import top_holdings, conviction_scores, available_periods

conn = get_connection()

# See what periods are loaded
periods = available_periods(conn)

# Top holdings across all filers for a period
rows = top_holdings(periods[0], top_n=20, conn=conn)

# Conviction scores (weights breadth + position size)
scores = conviction_scores(periods[0], min_filers=3, conn=conn)
for r in scores:
    print(r["name_of_issuer"], r["conviction_score"])
```

Or open it with any SQLite client (e.g. [DB Browser for SQLite](https://sqlitebrowser.org/)).
