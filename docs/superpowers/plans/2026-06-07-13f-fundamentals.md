# 13F Phase 3 — Fundamentals Ingest (current-quarter) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate per-stock current-quarter fundamentals (market cap, shares, P/E, gross margin) and sector classification for every stock held by a ranked fund, using the keyed Finnhub API.

**Architecture:** One new module `pipeline/fundamentals.py` fetches `/stock/profile2` (sector, market cap, shares) and `/stock/metric` (P/E, gross margin) from Finnhub for the stock universe (tickers held in the current quarter by funds in `fund_rankings`), and upserts into two new tables `fundamentals` and `sectors`. HTTP is isolated behind one mockable helper. Reuses `pipeline.scoring.adapter` for schema init and current-quarter lookup.

**Tech Stack:** Python 3.13, stdlib `sqlite3`, `requests`, `python-dotenv` (all already deps), `pytest` + `unittest.mock`. **No new dependencies.** Requires `FINNHUB_API_KEY` in `.env` (already set).

---

## Scope (read first)

This phase delivers **current-quarter** fundamentals + sectors only — the inputs Phase 4's stock-ranking *outputs* need (market-cap band filter, sector dummies/adjustment, P/E and gross-margin columns, all at the most recent quarter). It is deliberately **not** the historical SEC-XBRL fundamentals extraction. Rationale: the eligible universe is currently ~1 fund / ~11 tickers, so historical fundamental training data for the P4 regression would be near-empty; building the SEC companyfacts pipeline now is low ROI. That work is recorded as a future **P3b** (probe-confirmed feasible: SEC `company_tickers.json` + `companyconcept` endpoints for `Revenues`, `EarningsPerShareDiluted`, `GrossProfit`, `dei/EntityCommonStockSharesOutstanding` all return 200). Phase 4 will treat historical market_cap/P/E/margin as optional regression features and document their absence until P3b lands.

Field conversions (probe-confirmed against AAPL):
- `profile2.marketCapitalization` is in **millions USD** → store `× 1_000_000`.
- `profile2.shareOutstanding` is in **millions** → store `× 1_000_000`.
- `profile2.finnhubIndustry` → `sectors.sector` (e.g. "Technology").
- `metric.peTTM` → `pe_ratio` (`pe_available = 0` and `pe_ratio = 0` when missing/≤0, per the design spec's "use 0 for P/E in regression").
- `metric.grossMarginTTM` is a **percent** (e.g. 47.86) → store as-is in `gross_margin_pct` (NULL when absent).

---

## File structure

| File | Responsibility |
|---|---|
| `pipeline/scoring/schema.sql` | **Append** the Phase-3 tables `fundamentals`, `sectors` (idempotent). |
| `pipeline/fundamentals.py` | Finnhub client (`_finnhub_get`, `fetch_profile`, `fetch_metrics`), `universe_tickers`, `ingest_fundamentals`, CLI. |
| `tests/test_fundamentals.py` | Unit tests (HTTP mocked) + ingest integration test. |

All work on branch `feat/13f-ranking`.

---

### Task 1: Schema + Finnhub profile fetch

**Files:**
- Modify: `pipeline/scoring/schema.sql`
- Create: `pipeline/fundamentals.py`
- Test: `tests/test_fundamentals.py`

- [ ] **Step 1: Append Phase-3 tables to `pipeline/scoring/schema.sql`**

```sql

-- ---- Phase 3: fundamentals (current quarter) -------------------------------
CREATE TABLE IF NOT EXISTS fundamentals (
    ticker            TEXT NOT NULL,
    as_of_date        DATE NOT NULL,
    market_cap        REAL,
    shares_out        REAL,
    pe_ratio          REAL,
    pe_available      INTEGER,
    gross_margin_pct  REAL,
    source            TEXT,
    PRIMARY KEY (ticker, as_of_date)
);

CREATE TABLE IF NOT EXISTS sectors (
    ticker  TEXT PRIMARY KEY,
    sector  TEXT NOT NULL
);
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_fundamentals.py`:

```python
"""Tests for pipeline.fundamentals (Phase 3 current-quarter fundamentals)."""
from unittest.mock import patch

from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter
from pipeline import fundamentals


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return db, conn


def test_fetch_profile_converts_millions(tmp_path):
    payload = {"name": "Apple Inc", "finnhubIndustry": "Technology",
               "marketCapitalization": 4514012.29, "shareOutstanding": 14687.36}
    with patch("pipeline.fundamentals._finnhub_get", return_value=payload):
        prof = fundamentals.fetch_profile("AAPL")
    assert prof["sector"] == "Technology"
    assert round(prof["market_cap"]) == round(4514012.29 * 1_000_000)
    assert round(prof["shares_out"]) == round(14687.36 * 1_000_000)


def test_fetch_profile_missing_fields(tmp_path):
    with patch("pipeline.fundamentals._finnhub_get", return_value={}):
        prof = fundamentals.fetch_profile("ZZZ")
    assert prof["sector"] is None
    assert prof["market_cap"] is None
    assert prof["shares_out"] is None
```

- [ ] **Step 3: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_fundamentals.py -k fetch_profile -v`
Expected: FAIL (module/function missing).

- [ ] **Step 4: Create `pipeline/fundamentals.py`**

```python
"""
Current-quarter fundamentals ingest from Finnhub.

For every stock held in the most recent quarter by a ranked fund, fetch sector,
market cap, shares, P/E and gross margin and upsert into the fundamentals and
sectors tables. Requires FINNHUB_API_KEY in .env.

Run directly:
    python3 -m pipeline.fundamentals            # ingest universe
    python3 -m pipeline.fundamentals --limit 5  # first 5 tickers (smoke)
"""

import os
import sqlite3
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from pipeline.database import DB_PATH, get_connection
from pipeline.scoring import adapter

_FINNHUB_BASE = "https://finnhub.io/api/v1"
_RATE_SLEEP = 1.1            # free tier: 60 req/min -> ~1s apart
_MAX_RETRIES = 3


def _api_key() -> str | None:
    return os.environ.get("FINNHUB_API_KEY") or None


def _finnhub_get(path: str, params: dict) -> dict:
    """GET {base}{path}?... with the API token, retrying on 429."""
    p = dict(params)
    p["token"] = _api_key()
    url = f"{_FINNHUB_BASE}{path}"
    resp = None
    for attempt in range(_MAX_RETRIES + 1):
        resp = requests.get(url, params=p, timeout=20)
        if resp.status_code == 429:
            if attempt < _MAX_RETRIES:
                time.sleep(5 * (2 ** attempt))
                continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return {}


def _millions(v) -> float | None:
    return v * 1_000_000 if isinstance(v, (int, float)) and v else None


def fetch_profile(ticker: str) -> dict:
    """{sector, market_cap, shares_out} from Finnhub /stock/profile2."""
    data = _finnhub_get("/stock/profile2", {"symbol": ticker})
    return {
        "sector": data.get("finnhubIndustry") or None,
        "market_cap": _millions(data.get("marketCapitalization")),
        "shares_out": _millions(data.get("shareOutstanding")),
    }
```

- [ ] **Step 5: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_fundamentals.py -k fetch_profile -v`
Expected: 2 PASS.

- [ ] **Step 6: Commit**

```bash
git add pipeline/scoring/schema.sql pipeline/fundamentals.py tests/test_fundamentals.py
git commit -m "feat(fundamentals): schema + Finnhub profile fetch"
```

---

### Task 2: Finnhub metrics fetch (`fetch_metrics`)

**Files:**
- Modify: `pipeline/fundamentals.py`
- Test: `tests/test_fundamentals.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_fundamentals.py`:

```python
def test_fetch_metrics_pe_and_margin():
    payload = {"metric": {"peTTM": 36.83, "grossMarginTTM": 47.86}}
    with patch("pipeline.fundamentals._finnhub_get", return_value=payload):
        m = fundamentals.fetch_metrics("AAPL")
    assert round(m["pe_ratio"], 2) == 36.83
    assert m["pe_available"] == 1
    assert round(m["gross_margin_pct"], 2) == 47.86


def test_fetch_metrics_missing_pe_uses_zero():
    payload = {"metric": {"peTTM": None, "grossMarginTTM": None}}
    with patch("pipeline.fundamentals._finnhub_get", return_value=payload):
        m = fundamentals.fetch_metrics("ZZZ")
    assert m["pe_ratio"] == 0.0
    assert m["pe_available"] == 0
    assert m["gross_margin_pct"] is None


def test_fetch_metrics_negative_pe_unavailable():
    payload = {"metric": {"peTTM": -12.0, "grossMarginTTM": 10.0}}
    with patch("pipeline.fundamentals._finnhub_get", return_value=payload):
        m = fundamentals.fetch_metrics("LOSS")
    assert m["pe_ratio"] == 0.0
    assert m["pe_available"] == 0
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_fundamentals.py -k fetch_metrics -v`
Expected: FAIL (`fetch_metrics` not defined).

- [ ] **Step 3: Implement `fetch_metrics`**

Add to `pipeline/fundamentals.py`:

```python
def fetch_metrics(ticker: str) -> dict:
    """
    {pe_ratio, pe_available, gross_margin_pct} from Finnhub /stock/metric.
    A non-positive or missing P/E is treated as unavailable: pe_available=0 and
    pe_ratio=0 (so it contributes nothing in the downstream regression).
    """
    metric = _finnhub_get("/stock/metric", {"symbol": ticker, "metric": "all"}).get("metric", {})
    pe = metric.get("peTTM")
    if isinstance(pe, (int, float)) and pe > 0:
        pe_ratio, pe_available = float(pe), 1
    else:
        pe_ratio, pe_available = 0.0, 0
    gm = metric.get("grossMarginTTM")
    gross_margin_pct = float(gm) if isinstance(gm, (int, float)) else None
    return {"pe_ratio": pe_ratio, "pe_available": pe_available,
            "gross_margin_pct": gross_margin_pct}
```

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_fundamentals.py -k fetch_metrics -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/fundamentals.py tests/test_fundamentals.py
git commit -m "feat(fundamentals): Finnhub P/E + gross margin fetch"
```

---

### Task 3: Stock universe (`universe_tickers`)

**Files:**
- Modify: `pipeline/fundamentals.py`
- Test: `tests/test_fundamentals.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_fundamentals.py`:

```python
def _add_filing(conn, cik, period, filed, acc):
    conn.execute("INSERT INTO filings(cik,accession_number,period_of_report,filed_date) "
                 "VALUES (?,?,?,?)", (cik, acc, period, filed))
    return conn.execute("SELECT id FROM filings WHERE accession_number=?", (acc,)).fetchone()[0]


def test_universe_tickers_current_quarter_ranked_funds(tmp_path):
    _db_, conn = _db(tmp_path)
    cq = adapter.current_quarter_date(conn)  # None when empty -> set explicitly below
    cq = "2024-12-31"
    # ranked fund 'r' holds AAA + BBB this quarter; unranked fund 'u' holds CCC
    conn.execute("INSERT INTO filers(cik,name) VALUES ('r','Ranked'),('u','Unranked')")
    conn.execute("INSERT INTO fund_rankings(fund_id,fund_name,rank,final_score,eligible) "
                 "VALUES ('r','Ranked',1,100.0,1)")
    fr = _add_filing(conn, "r", cq, "2025-02-10", "r1")
    fu = _add_filing(conn, "u", cq, "2025-02-10", "u1")
    for fid, c in [(fr, "CA"), (fr, "CB"), (fu, "CC"), (fr, "COPT")]:
        pc = "Call" if c == "COPT" else None
        conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                     "VALUES (?,?,?,?,?,?)", (fid, c, c, 100, 10, pc))
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES "
                 "('CA','AAA','A'),('CB','BBB','B'),('CC','CCC','C'),('COPT','OPT','O')")
    conn.commit()

    tickers = fundamentals.universe_tickers(conn)
    assert set(tickers) == {"AAA", "BBB"}   # ranked fund only; option excluded; unranked excluded
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_fundamentals.py::test_universe_tickers_current_quarter_ranked_funds -v`
Expected: FAIL (`universe_tickers` not defined).

- [ ] **Step 3: Implement `universe_tickers`**

Add to `pipeline/fundamentals.py`:

```python
def universe_tickers(conn: sqlite3.Connection) -> list[str]:
    """
    Distinct equity tickers held in the most recent quarter by any fund present
    in fund_rankings. Options and unresolved/letterless tickers are excluded.
    """
    cq = adapter.current_quarter_date(conn)
    if cq is None:
        return []
    rows = conn.execute(
        """
        WITH ranked AS (SELECT fund_id FROM fund_rankings),
        latest AS (
            SELECT f.id, f.cik FROM filings f
            JOIN ranked r ON r.fund_id = f.cik
            WHERE f.period_of_report = :cq
              AND f.id = (SELECT f2.id FROM filings f2
                          WHERE f2.cik = f.cik AND f2.period_of_report = :cq
                          ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1)
        )
        SELECT DISTINCT s.ticker
        FROM holdings h
        JOIN latest l ON l.id = h.filing_id
        JOIN securities s ON s.cusip = h.cusip
        WHERE s.ticker IS NOT NULL AND s.ticker <> ''
          AND s.ticker NOT GLOB '*[0-9]*'
          AND (h.put_call IS NULL OR h.put_call = '')
          AND h.value_thousands > 0
        """,
        {"cq": cq},
    ).fetchall()
    return [r[0] for r in rows]
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_fundamentals.py::test_universe_tickers_current_quarter_ranked_funds -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/fundamentals.py tests/test_fundamentals.py
git commit -m "feat(fundamentals): current-quarter stock universe"
```

---

### Task 4: Ingest orchestrator + CLI (`ingest_fundamentals`)

**Files:**
- Modify: `pipeline/fundamentals.py`
- Test: `tests/test_fundamentals.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_fundamentals.py`:

```python
def test_ingest_fundamentals_populates_tables(tmp_path):
    _db_, conn = _db(tmp_path)
    cq = "2024-12-31"
    conn.execute("INSERT INTO filers(cik,name) VALUES ('r','Ranked')")
    conn.execute("INSERT INTO fund_rankings(fund_id,fund_name,rank,final_score,eligible) "
                 "VALUES ('r','Ranked',1,100.0,1)")
    fr = _add_filing(conn, "r", cq, "2025-02-10", "r1")
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?,?,?,?,?,?)", (fr, "CA", "A", 100, 10, None))
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('CA','AAA','A')")
    conn.commit()

    profile = {"sector": "Technology", "market_cap": 2.0e12, "shares_out": 1.0e10}
    metrics = {"pe_ratio": 25.0, "pe_available": 1, "gross_margin_pct": 40.0}
    with patch("pipeline.fundamentals.fetch_profile", return_value=profile), \
         patch("pipeline.fundamentals.fetch_metrics", return_value=metrics):
        stats = fundamentals.ingest_fundamentals(_db_)

    assert stats["tickers"] == 1
    row = conn.execute(
        "SELECT as_of_date, market_cap, shares_out, pe_ratio, pe_available, "
        "gross_margin_pct, source FROM fundamentals WHERE ticker='AAA'").fetchone()
    assert row["as_of_date"] == cq
    assert row["market_cap"] == 2.0e12
    assert row["pe_available"] == 1
    assert row["gross_margin_pct"] == 40.0
    assert row["source"] == "finnhub"
    assert conn.execute("SELECT sector FROM sectors WHERE ticker='AAA'").fetchone()[0] == "Technology"


def test_ingest_fundamentals_skips_sector_when_missing(tmp_path):
    _db_, conn = _db(tmp_path)
    cq = "2024-12-31"
    conn.execute("INSERT INTO filers(cik,name) VALUES ('r','Ranked')")
    conn.execute("INSERT INTO fund_rankings(fund_id,fund_name,rank,final_score,eligible) "
                 "VALUES ('r','Ranked',1,100.0,1)")
    fr = _add_filing(conn, "r", cq, "2025-02-10", "r1")
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?,?,?,?,?,?)", (fr, "CA", "A", 100, 10, None))
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('CA','AAA','A')")
    conn.commit()
    profile = {"sector": None, "market_cap": None, "shares_out": None}
    metrics = {"pe_ratio": 0.0, "pe_available": 0, "gross_margin_pct": None}
    with patch("pipeline.fundamentals.fetch_profile", return_value=profile), \
         patch("pipeline.fundamentals.fetch_metrics", return_value=metrics):
        fundamentals.ingest_fundamentals(_db_)
    # fundamentals row still written; sectors row skipped (sector NOT NULL constraint)
    assert conn.execute("SELECT COUNT(*) FROM fundamentals WHERE ticker='AAA'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM sectors WHERE ticker='AAA'").fetchone()[0] == 0
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_fundamentals.py -k ingest_fundamentals -v`
Expected: FAIL (`ingest_fundamentals` not defined).

- [ ] **Step 3: Implement `ingest_fundamentals` + CLI**

Add to `pipeline/fundamentals.py`:

```python
def ingest_fundamentals(db_path: Path = DB_PATH, limit: int | None = None) -> dict:
    """
    Fetch + store current-quarter fundamentals for the stock universe.
    Returns {tickers, with_sector, failed}.
    """
    conn = get_connection(db_path)
    try:
        adapter.init_schema(conn, db_path)
        cq = adapter.current_quarter_date(conn)
        tickers = universe_tickers(conn)
        if limit:
            tickers = tickers[:limit]
        with_sector = failed = 0
        for ticker in tickers:
            try:
                prof = fetch_profile(ticker)
                met = fetch_metrics(ticker)
            except Exception as exc:                 # noqa: BLE001 — log + continue
                print(f"  [ERROR] {ticker}: {exc}")
                failed += 1
                continue
            conn.execute(
                """
                INSERT INTO fundamentals
                    (ticker, as_of_date, market_cap, shares_out, pe_ratio,
                     pe_available, gross_margin_pct, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'finnhub')
                ON CONFLICT(ticker, as_of_date) DO UPDATE SET
                    market_cap = excluded.market_cap, shares_out = excluded.shares_out,
                    pe_ratio = excluded.pe_ratio, pe_available = excluded.pe_available,
                    gross_margin_pct = excluded.gross_margin_pct, source = excluded.source
                """,
                (ticker, cq, prof["market_cap"], prof["shares_out"],
                 met["pe_ratio"], met["pe_available"], met["gross_margin_pct"]))
            if prof["sector"]:
                conn.execute(
                    "INSERT INTO sectors(ticker, sector) VALUES (?, ?) "
                    "ON CONFLICT(ticker) DO UPDATE SET sector = excluded.sector",
                    (ticker, prof["sector"]))
                with_sector += 1
            conn.commit()
            time.sleep(_RATE_SLEEP)
        return {"tickers": len(tickers), "with_sector": with_sector, "failed": failed}
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent))
    ap = argparse.ArgumentParser(description="Ingest current-quarter fundamentals (Finnhub)")
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--limit", type=int, default=None, help="cap number of tickers")
    args = ap.parse_args()
    if not _api_key():
        print("FINNHUB_API_KEY not set in .env — aborting.")
        sys.exit(1)
    print(ingest_fundamentals(Path(args.db), limit=args.limit))
```

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_fundamentals.py -k ingest_fundamentals -v`
Expected: 2 PASS.

- [ ] **Step 5: Run the full suite**

Run: `python3 -m pytest -q`
Expected: all green (`test_edgar_search`, `test_prices`, `test_adapter`, `test_fund_pipeline`, `test_fundamentals`).

- [ ] **Step 6: Live smoke test (real Finnhub, capped)**

Run: `python3 -m pipeline.fundamentals --limit 3`
Expected: prints `{'tickers': N, 'with_sector': M, 'failed': K}` with N ≤ 3. Then:
`sqlite3 data/13f.db "SELECT ticker, market_cap, pe_ratio, gross_margin_pct FROM fundamentals; SELECT * FROM sectors;"`
Spot-check that at least one row has a real market_cap and a sector. (The real universe is the current-quarter holdings of ranked funds — currently just Baupost's ~11 names, so N may be small. Report actual output.)

> If Finnhub returns 429, the client backs off; if it persists, wait a minute and retry. Do not change providers.

- [ ] **Step 7: Commit**

```bash
git add pipeline/fundamentals.py tests/test_fundamentals.py
git commit -m "feat(fundamentals): ingest orchestrator + CLI"
```

---

## Self-review (completed by plan author)

**Spec coverage (design spec Phase 3, current-quarter scope):**
- `fundamentals` table (market_cap, shares_out, pe_ratio, pe_available, gross_margin_pct, source) → Tasks 1, 4. ✓
- `sectors` table → Tasks 1, 4. ✓
- Finnhub `/stock/profile2` (sector, market cap, shares) → Task 1. ✓
- Finnhub `/stock/metric` (P/E, gross margin), P/E-unavailable rule (0 + dummy) → Task 2. ✓
- Universe = current-quarter holdings of ranked funds, options/unresolved excluded → Task 3. ✓
- Orchestration + CLI + incremental upsert → Task 4. ✓
- **Deferred to P3b (documented):** historical SEC-XBRL fundamentals; 52-week range (Phase 4 computes from `prices`). Not gaps — out of this phase's scope by design.

**Placeholder scan:** none — every step has runnable code/commands.

**Type/signature consistency:** `_finnhub_get(path, params)->dict`; `fetch_profile(ticker)->{sector,market_cap,shares_out}`; `fetch_metrics(ticker)->{pe_ratio,pe_available,gross_margin_pct}`; `universe_tickers(conn)->list[str]`; `ingest_fundamentals(db_path, limit)->{tickers,with_sector,failed}`. Table column names match the Task-1 schema across all INSERTs. `adapter.current_quarter_date`/`adapter.init_schema` reused (already exist from Phase 2). Ticker filter (`NOT GLOB '*[0-9]*'`, options excluded) matches the Phase-1/2 convention. ✓
