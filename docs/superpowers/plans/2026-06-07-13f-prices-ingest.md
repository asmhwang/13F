# 13F Phase 1 — Price + Benchmark Ingest — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest adjusted daily prices for every equity ticker held by a tracked fund, plus the S&P 500 total-return benchmark, into new SQLite tables — scoped, incremental, and reported on for coverage.

**Architecture:** A single `pipeline/prices.py` module fetches from Yahoo Finance's keyless v8 chart endpoint (`query1.finance.yahoo.com/v8/finance/chart/{symbol}`), parses the JSON into `{date, close, adj_close}` rows, and upserts them. New tables live in `pipeline/scoring/schema.sql` (idempotent, grows in later phases). Fetch scope per ticker is `[first holding quarter, min(last holding quarter + 3yr, today)]`; `price_fetch_log` makes re-runs incremental. A coverage report quantifies, by holding value, how much of the most recent quarter is priceable before any scoring runs.

**Tech Stack:** Python 3.13, stdlib `sqlite3`/`datetime`/`urllib`, `requests` (already a dep), `pytest` + `unittest.mock`. **No new dependencies in this phase.**

---

## File structure

| File | Responsibility |
|---|---|
| `pipeline/scoring/__init__.py` | Marks the scoring package (empty). |
| `pipeline/scoring/schema.sql` | DDL for all ranking tables; Phase 1 adds `prices`, `benchmark`, `price_fetch_log`. Idempotent (`IF NOT EXISTS`). |
| `pipeline/prices.py` | Schema init, Yahoo fetch + parse, upsert, ticker-window computation, incremental ingest loop, benchmark ingest, coverage report, CLI. |
| `tests/test_prices.py` | Unit tests for every public function; mocks HTTP, uses temp DBs. |

All work happens on branch `feat/13f-ranking` (already checked out).

---

### Task 1: Scaffold scoring package + price schema

**Files:**
- Create: `pipeline/scoring/__init__.py`
- Create: `pipeline/scoring/schema.sql`
- Create: `pipeline/prices.py`
- Test: `tests/test_prices.py`

- [ ] **Step 1: Create the package marker**

Create `pipeline/scoring/__init__.py` with a single line:

```python
"""Scoring pipelines (fund + stock ranking) and their shared schema."""
```

- [ ] **Step 2: Create the schema file**

Create `pipeline/scoring/schema.sql`:

```sql
-- New tables for the ranking pipelines. Idempotent; every scoring module runs
-- this at startup so required tables always exist.

-- ---- Phase 1: prices + benchmark -------------------------------------------
CREATE TABLE IF NOT EXISTS prices (
    ticker     TEXT NOT NULL,
    date       DATE NOT NULL,
    close      REAL,
    adj_close  REAL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_prices_ticker ON prices(ticker);

CREATE TABLE IF NOT EXISTS benchmark (        -- ^SP500TR total-return series
    date       DATE PRIMARY KEY,
    adj_close  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS price_fetch_log (  -- incremental bookkeeping
    ticker     TEXT PRIMARY KEY,
    first_date DATE,
    last_date  DATE,
    status     TEXT,                          -- 'ok' | 'no_data' | 'error'
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

- [ ] **Step 3: Create `pipeline/prices.py` with the module header and `init_schema`**

```python
"""
Daily price + S&P 500 total-return benchmark ingest for the ranking pipeline.

Source: Yahoo Finance v8 chart endpoint (no API key). Adjusted close included.
Benchmark symbol: ^SP500TR (S&P 500 Total Return index).

Scope: only tickers held by tracked funds (resolved in `securities`), over the
window each ticker is actually needed: [first holding quarter, last holding
quarter + 3 years], capped at today. Incremental — already-covered tickers are
skipped via price_fetch_log.

Run directly:
    python3 -m pipeline.prices              # benchmark + held tickers
    python3 -m pipeline.prices --coverage   # print coverage report only
    python3 -m pipeline.prices --limit 5    # fetch only 5 tickers (smoke test)
"""

import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import requests

from pipeline.database import DB_PATH, get_connection

_SCHEMA_PATH = Path(__file__).parent / "scoring" / "schema.sql"
_CHART_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
_HEADERS = {"User-Agent": "Mozilla/5.0 (13F Research)"}
_RATE_SLEEP = 0.5          # polite gap between Yahoo requests
_MAX_RETRIES = 3
_BENCHMARK_SYMBOL = "^SP500TR"


def init_schema(conn: sqlite3.Connection | None = None, db_path: Path = DB_PATH) -> None:
    """Create the price/benchmark tables if they don't exist (idempotent)."""
    c = conn or get_connection(db_path)
    c.executescript(_SCHEMA_PATH.read_text())
    c.commit()
```

- [ ] **Step 4: Write the failing schema test**

Create `tests/test_prices.py`:

```python
"""Tests for pipeline.prices (Phase 1 price + benchmark ingest)."""
from pathlib import Path

import pytest

from pipeline.database import get_connection
from pipeline import prices


def _tables(conn) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {r[0] for r in rows}


def test_init_schema_creates_tables(tmp_path):
    db = tmp_path / "t.db"
    conn = get_connection(db)
    prices.init_schema(conn, db)
    assert {"prices", "benchmark", "price_fetch_log"} <= _tables(conn)
```

- [ ] **Step 5: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_prices.py::test_init_schema_creates_tables -v`
Expected: PASS (schema applies cleanly).

- [ ] **Step 6: Commit**

```bash
git add pipeline/scoring/__init__.py pipeline/scoring/schema.sql pipeline/prices.py tests/test_prices.py
git commit -m "feat(prices): scaffold scoring package + price schema init"
```

---

### Task 2: Parse Yahoo chart JSON (`parse_chart`)

**Files:**
- Modify: `pipeline/prices.py`
- Test: `tests/test_prices.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_prices.py`:

```python
def _sample_chart_payload():
    # epoch seconds for 2021-01-04 and 2021-01-05 (UTC)
    return {
        "chart": {
            "result": [
                {
                    "timestamp": [1609718400, 1609804800],
                    "indicators": {
                        "quote": [{"close": [100.0, None]}],
                        "adjclose": [{"adjclose": [99.0, 101.0]}],
                    },
                }
            ],
            "error": None,
        }
    }


def test_parse_chart_extracts_rows_and_skips_null_close():
    rows = prices.parse_chart(_sample_chart_payload())
    assert rows == [{"date": "2021-01-04", "close": 100.0, "adj_close": 99.0}]


def test_parse_chart_falls_back_to_close_when_adj_missing():
    payload = {
        "chart": {"result": [{
            "timestamp": [1609718400],
            "indicators": {"quote": [{"close": [50.0]}], "adjclose": [{}]},
        }]}
    }
    rows = prices.parse_chart(payload)
    assert rows == [{"date": "2021-01-04", "close": 50.0, "adj_close": 50.0}]


def test_parse_chart_empty_payload_returns_empty():
    assert prices.parse_chart({"chart": {"result": []}}) == []
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_prices.py -k parse_chart -v`
Expected: FAIL with `AttributeError: module 'pipeline.prices' has no attribute 'parse_chart'`.

- [ ] **Step 3: Implement `parse_chart`**

Add to `pipeline/prices.py`:

```python
def parse_chart(payload: dict) -> list[dict]:
    """
    Turn a Yahoo v8 chart JSON payload into [{date, close, adj_close}, ...].
    Rows with a null close (non-trading gaps) are skipped. When adjclose is
    absent for a row, close is used as the adjusted value.
    """
    results = (payload.get("chart") or {}).get("result") or []
    if not results:
        return []
    res = results[0]
    timestamps = res.get("timestamp") or []
    indicators = res.get("indicators") or {}
    quote_block = (indicators.get("quote") or [{}])[0]
    adj_block = (indicators.get("adjclose") or [{}])[0]
    closes = quote_block.get("close") or []
    adjs = adj_block.get("adjclose") or []
    rows: list[dict] = []
    for i, ts in enumerate(timestamps):
        close = closes[i] if i < len(closes) else None
        if close is None:
            continue
        adj = adjs[i] if i < len(adjs) and adjs[i] is not None else close
        d = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        rows.append({"date": d, "close": close, "adj_close": adj})
    return rows
```

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_prices.py -k parse_chart -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/prices.py tests/test_prices.py
git commit -m "feat(prices): parse Yahoo v8 chart JSON into price rows"
```

---

### Task 3: Build chart URL + fetch (`_chart_url`, `_http_get`, `fetch_prices`)

**Files:**
- Modify: `pipeline/prices.py`
- Test: `tests/test_prices.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_prices.py`:

```python
from unittest.mock import patch, MagicMock


def test_chart_url_encodes_symbol_and_dates():
    url = prices._chart_url("^SP500TR", "2021-01-01", "2021-01-02")
    assert url.startswith("https://query1.finance.yahoo.com/v8/finance/chart/%5ESP500TR")
    assert "interval=1d" in url
    assert "period1=" in url and "period2=" in url


def test_fetch_prices_calls_http_and_parses():
    resp = MagicMock()
    resp.json.return_value = _sample_chart_payload()
    with patch("pipeline.prices._http_get", return_value=resp) as m:
        rows = prices.fetch_prices("AAPL", "2021-01-01", "2021-01-31")
    m.assert_called_once()
    assert rows == [{"date": "2021-01-04", "close": 100.0, "adj_close": 99.0}]
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_prices.py -k "chart_url or fetch_prices" -v`
Expected: FAIL (`_chart_url` / `fetch_prices` not defined).

- [ ] **Step 3: Implement the three functions**

Add to `pipeline/prices.py`:

```python
def _chart_url(symbol: str, start: str, end: str) -> str:
    p1 = int(datetime.strptime(start, "%Y-%m-%d")
             .replace(tzinfo=timezone.utc).timestamp())
    # +1 day so the end date itself is inclusive
    p2 = int((datetime.strptime(end, "%Y-%m-%d")
              .replace(tzinfo=timezone.utc) + timedelta(days=1)).timestamp())
    return (f"{_CHART_BASE}{quote(symbol)}"
            f"?period1={p1}&period2={p2}&interval=1d&events=div%2Csplit")


def _http_get(url: str) -> requests.Response:
    """GET with simple exponential backoff on 429."""
    resp = None
    for attempt in range(_MAX_RETRIES + 1):
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        if resp.status_code == 429:
            wait = 5 * (2 ** attempt)
            print(f"    [429] Yahoo rate limit — waiting {wait}s")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp


def fetch_prices(symbol: str, start: str, end: str) -> list[dict]:
    """Fetch + parse adjusted daily prices for one symbol over [start, end]."""
    resp = _http_get(_chart_url(symbol, start, end))
    return parse_chart(resp.json())
```

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_prices.py -k "chart_url or fetch_prices" -v`
Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/prices.py tests/test_prices.py
git commit -m "feat(prices): Yahoo chart URL builder + rate-limited fetch"
```

---

### Task 4: Store prices idempotently (`store_prices`)

**Files:**
- Modify: `pipeline/prices.py`
- Test: `tests/test_prices.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_prices.py`:

```python
def test_store_prices_inserts_and_upserts(tmp_path):
    db = tmp_path / "t.db"
    conn = get_connection(db)
    prices.init_schema(conn, db)

    n = prices.store_prices(conn, "AAPL", [
        {"date": "2021-01-04", "close": 100.0, "adj_close": 99.0},
        {"date": "2021-01-05", "close": 101.0, "adj_close": 100.0},
    ])
    assert n == 2
    assert conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] == 2

    # Re-store same dates with new values: no duplicates, values updated.
    prices.store_prices(conn, "AAPL", [
        {"date": "2021-01-04", "close": 110.0, "adj_close": 109.0},
    ])
    assert conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] == 2
    row = conn.execute(
        "SELECT adj_close FROM prices WHERE ticker='AAPL' AND date='2021-01-04'"
    ).fetchone()
    assert row[0] == 109.0
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_prices.py::test_store_prices_inserts_and_upserts -v`
Expected: FAIL (`store_prices` not defined).

- [ ] **Step 3: Implement `store_prices`**

Add to `pipeline/prices.py`:

```python
def store_prices(conn: sqlite3.Connection, ticker: str, rows: list[dict]) -> int:
    """Upsert price rows for one ticker. Returns the number of rows written."""
    conn.executemany(
        """
        INSERT INTO prices (ticker, date, close, adj_close)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ticker, date) DO UPDATE SET
            close     = excluded.close,
            adj_close = excluded.adj_close
        """,
        [(ticker, r["date"], r["close"], r["adj_close"]) for r in rows],
    )
    conn.commit()
    return len(rows)
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_prices.py::test_store_prices_inserts_and_upserts -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/prices.py tests/test_prices.py
git commit -m "feat(prices): idempotent price upsert"
```

---

### Task 5: Compute per-ticker fetch windows (`_plus_three_years`, `held_ticker_windows`)

**Files:**
- Modify: `pipeline/prices.py`
- Test: `tests/test_prices.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_prices.py`. The helper seeds a minimal real schema (filers/filings/holdings/securities) so the window query has data:

```python
from pipeline.database import init_db


def _seed_holdings(db):
    """One fund, two quarters, two tickers (one equity, one option-only)."""
    init_db(db)
    conn = get_connection(db)
    conn.execute("INSERT INTO filers(cik, name) VALUES ('111','Fund A')")
    conn.execute("INSERT INTO filings(cik, accession_number, period_of_report, filed_date) "
                 "VALUES ('111','a1','2018-03-31','2018-05-10')")
    conn.execute("INSERT INTO filings(cik, accession_number, period_of_report, filed_date) "
                 "VALUES ('111','a2','2019-03-31','2019-05-10')")
    f1 = conn.execute("SELECT id FROM filings WHERE accession_number='a1'").fetchone()[0]
    f2 = conn.execute("SELECT id FROM filings WHERE accession_number='a2'").fetchone()[0]
    # AAPL equity in both quarters
    for fid in (f1, f2):
        conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                     "VALUES (?, 'C_AAPL','APPLE INC',1000,10,NULL)", (fid,))
    # OPT: present only as an option (put_call set) -> must be excluded
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?, 'C_OPT','OPTONLY CO',500,5,'Call')", (f2,))
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_AAPL','AAPL','Apple')")
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_OPT','OPT','OptOnly')")
    conn.commit()
    return conn


def test_plus_three_years_handles_leap_day():
    assert prices._plus_three_years("2020-02-29") == "2023-02-28"
    assert prices._plus_three_years("2021-03-31") == "2024-03-31"


def test_held_ticker_windows_equity_only_with_3yr_window(tmp_path):
    db = tmp_path / "t.db"
    conn = _seed_holdings(db)
    windows = prices.held_ticker_windows(conn)
    tickers = {w["ticker"] for w in windows}
    assert tickers == {"AAPL"}          # OPT excluded (option-only)
    aapl = next(w for w in windows if w["ticker"] == "AAPL")
    assert aapl["start"] == "2018-03-31"
    assert aapl["end"] == "2022-03-31"  # last quarter 2019-03-31 + 3yr
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_prices.py -k "three_years or held_ticker" -v`
Expected: FAIL (`_plus_three_years` / `held_ticker_windows` not defined).

- [ ] **Step 3: Implement both functions**

Add to `pipeline/prices.py`:

```python
def _plus_three_years(d: str) -> str:
    """Add 3 years to an ISO date string, clamping Feb-29 to Feb-28."""
    y, m, day = (int(x) for x in d.split("-"))
    try:
        return date(y + 3, m, day).isoformat()
    except ValueError:                       # Feb 29 -> Feb 28
        return date(y + 3, m, day - 1).isoformat()


def held_ticker_windows(conn: sqlite3.Connection) -> list[dict]:
    """
    For each equity ticker held by a tracked fund, the date window prices are
    needed: [first holding quarter, min(last holding quarter + 3yr, today)].
    Option-only positions (put_call set) and unresolved CUSIPs are excluded.
    """
    rows = conn.execute(
        """
        SELECT s.ticker                AS ticker,
               MIN(f.period_of_report) AS first_q,
               MAX(f.period_of_report) AS last_q
        FROM holdings h
        JOIN filings f    ON f.id = h.filing_id
        JOIN securities s ON s.cusip = h.cusip
        WHERE s.ticker IS NOT NULL AND s.ticker <> ''
          AND (h.put_call IS NULL OR h.put_call = '')
          AND h.value_thousands > 0
        GROUP BY s.ticker
        """
    ).fetchall()
    today = date.today().isoformat()
    out: list[dict] = []
    for r in rows:
        end = min(_plus_three_years(r["last_q"]), today)
        out.append({"ticker": r["ticker"], "start": r["first_q"], "end": end})
    return out
```

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_prices.py -k "three_years or held_ticker" -v`
Expected: 2 PASS.

> Note: `test_held_ticker_windows_equity_only_with_3yr_window` asserts `end == "2022-03-31"`, which is before today, so the `min(..., today)` clamp does not interfere. Keep the fixture quarters in the past.

- [ ] **Step 5: Commit**

```bash
git add pipeline/prices.py tests/test_prices.py
git commit -m "feat(prices): per-ticker fetch windows from holdings"
```

---

### Task 6: Incremental ingest loop (`_already_covered`, `_log_fetch`, `ingest_prices`)

**Files:**
- Modify: `pipeline/prices.py`
- Test: `tests/test_prices.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_prices.py`:

```python
def test_ingest_prices_fetches_logs_and_is_incremental(tmp_path):
    db = tmp_path / "t.db"
    _seed_holdings(db)

    fake_rows = [
        {"date": "2018-03-29", "close": 10.0, "adj_close": 9.5},
        {"date": "2022-03-31", "close": 20.0, "adj_close": 19.0},
    ]
    with patch("pipeline.prices.fetch_prices", return_value=fake_rows) as m:
        stats = prices.ingest_prices(db)
    assert stats == {"fetched": 1, "skipped": 0, "failed": 0, "total": 1}
    assert m.call_count == 1

    conn = get_connection(db)
    assert conn.execute("SELECT COUNT(*) FROM prices WHERE ticker='AAPL'").fetchone()[0] == 2
    log = conn.execute("SELECT first_date, last_date, status FROM price_fetch_log WHERE ticker='AAPL'").fetchone()
    assert (log[0], log[1], log[2]) == ("2018-03-29", "2022-03-31", "ok")

    # Second run: window already covered -> skipped, no new fetch.
    with patch("pipeline.prices.fetch_prices", return_value=fake_rows) as m2:
        stats2 = prices.ingest_prices(db)
    assert stats2["skipped"] == 1 and stats2["fetched"] == 0
    m2.assert_not_called()


def test_ingest_prices_logs_no_data_when_empty(tmp_path):
    db = tmp_path / "t.db"
    _seed_holdings(db)
    with patch("pipeline.prices.fetch_prices", return_value=[]):
        stats = prices.ingest_prices(db)
    assert stats["fetched"] == 0
    conn = get_connection(db)
    status = conn.execute("SELECT status FROM price_fetch_log WHERE ticker='AAPL'").fetchone()[0]
    assert status == "no_data"
    # no_data tickers are not retried
    with patch("pipeline.prices.fetch_prices") as m:
        prices.ingest_prices(db)
    m.assert_not_called()
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_prices.py -k ingest_prices -v`
Expected: FAIL (`ingest_prices` not defined).

- [ ] **Step 3: Implement the loop**

Add to `pipeline/prices.py`:

```python
def _already_covered(conn: sqlite3.Connection, ticker: str, start: str, end: str) -> bool:
    row = conn.execute(
        "SELECT first_date, last_date, status FROM price_fetch_log WHERE ticker = ?",
        (ticker,),
    ).fetchone()
    if not row:
        return False
    if row["status"] == "no_data":
        return True                          # don't retry dead tickers
    return (row["first_date"] is not None
            and row["last_date"] is not None
            and row["first_date"] <= start
            and row["last_date"] >= end)


def _log_fetch(conn: sqlite3.Connection, ticker: str,
               first_date: str | None, last_date: str | None, status: str) -> None:
    conn.execute(
        """
        INSERT INTO price_fetch_log (ticker, first_date, last_date, status, fetched_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(ticker) DO UPDATE SET
            first_date = excluded.first_date,
            last_date  = excluded.last_date,
            status     = excluded.status,
            fetched_at = CURRENT_TIMESTAMP
        """,
        (ticker, first_date, last_date, status),
    )
    conn.commit()


def ingest_prices(db_path: Path = DB_PATH, force: bool = False,
                  limit: int | None = None) -> dict:
    """
    Fetch + store prices for every held equity ticker over its needed window.
    Incremental: tickers already covered (or marked no_data) are skipped unless
    force=True. Returns {fetched, skipped, failed, total}.
    """
    conn = get_connection(db_path)
    init_schema(conn, db_path)
    windows = held_ticker_windows(conn)
    if limit:
        windows = windows[:limit]
    fetched = skipped = failed = 0
    for w in windows:
        t, start, end = w["ticker"], w["start"], w["end"]
        if not force and _already_covered(conn, t, start, end):
            skipped += 1
            continue
        try:
            rows = fetch_prices(t, start, end)
            if rows:
                store_prices(conn, t, rows)
                _log_fetch(conn, t, rows[0]["date"], rows[-1]["date"], "ok")
                fetched += 1
            else:
                _log_fetch(conn, t, None, None, "no_data")
            time.sleep(_RATE_SLEEP)
        except Exception as exc:                # noqa: BLE001 — log and continue
            print(f"  [ERROR] {t}: {exc}")
            _log_fetch(conn, t, None, None, "error")
            failed += 1
    return {"fetched": fetched, "skipped": skipped, "failed": failed, "total": len(windows)}
```

> Note: `time.sleep` runs in the success path only. Tests patch `fetch_prices` (not `time.sleep`); the 0.5s sleep per ticker is negligible for the 1-ticker fixtures. Do not patch `time.sleep` — keep tests honest about the real code path.

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_prices.py -k ingest_prices -v`
Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/prices.py tests/test_prices.py
git commit -m "feat(prices): incremental ingest loop with fetch log"
```

---

### Task 7: Benchmark ingest (`ingest_benchmark`)

**Files:**
- Modify: `pipeline/prices.py`
- Test: `tests/test_prices.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_prices.py`:

```python
def test_ingest_benchmark_stores_rows_over_filing_span(tmp_path):
    db = tmp_path / "t.db"
    _seed_holdings(db)   # filings span 2018-03-31 .. 2019-03-31

    fake_rows = [
        {"date": "2018-03-29", "close": 2600.0, "adj_close": 2600.0},
        {"date": "2018-04-02", "close": 2580.0, "adj_close": 2580.0},
    ]
    with patch("pipeline.prices.fetch_prices", return_value=fake_rows) as m:
        n = prices.ingest_benchmark(db)

    # Called once for ^SP500TR over [min period, max period + 3yr]
    sym, start, end = m.call_args.args
    assert sym == "^SP500TR"
    assert start == "2018-03-31"
    assert end == "2022-03-31"
    assert n == 2

    conn = get_connection(db)
    assert conn.execute("SELECT COUNT(*) FROM benchmark").fetchone()[0] == 2
    assert conn.execute("SELECT adj_close FROM benchmark WHERE date='2018-03-29'").fetchone()[0] == 2600.0
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_prices.py::test_ingest_benchmark_stores_rows_over_filing_span -v`
Expected: FAIL (`ingest_benchmark` not defined).

- [ ] **Step 3: Implement `ingest_benchmark`**

Add to `pipeline/prices.py`:

```python
def ingest_benchmark(db_path: Path = DB_PATH) -> int:
    """
    Fetch the ^SP500TR total-return series over the full filing span (min period
    .. max period + 3yr, capped today) and upsert into the benchmark table.
    """
    conn = get_connection(db_path)
    init_schema(conn, db_path)
    span = conn.execute(
        "SELECT MIN(period_of_report) AS lo, MAX(period_of_report) AS hi FROM filings"
    ).fetchone()
    if span["lo"] is None:
        return 0
    start = span["lo"]
    end = min(_plus_three_years(span["hi"]), date.today().isoformat())
    rows = fetch_prices(_BENCHMARK_SYMBOL, start, end)
    conn.executemany(
        """
        INSERT INTO benchmark (date, adj_close) VALUES (?, ?)
        ON CONFLICT(date) DO UPDATE SET adj_close = excluded.adj_close
        """,
        [(r["date"], r["adj_close"]) for r in rows],
    )
    conn.commit()
    return len(rows)
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_prices.py::test_ingest_benchmark_stores_rows_over_filing_span -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/prices.py tests/test_prices.py
git commit -m "feat(prices): S&P 500 total-return benchmark ingest"
```

---

### Task 8: Coverage report (`coverage_report`)

**Files:**
- Modify: `pipeline/prices.py`
- Test: `tests/test_prices.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_prices.py`. Seed two equity tickers in the latest quarter, price only one, and assert value-weighted coverage:

```python
def _seed_coverage(db):
    """Latest quarter 2019-03-31 with two equity tickers AAPL(1000) + MSFT(3000)."""
    init_db(db)
    conn = get_connection(db)
    conn.execute("INSERT INTO filers(cik, name) VALUES ('111','Fund A')")
    conn.execute("INSERT INTO filings(cik, accession_number, period_of_report, filed_date) "
                 "VALUES ('111','a1','2019-03-31','2019-05-10')")
    fid = conn.execute("SELECT id FROM filings WHERE accession_number='a1'").fetchone()[0]
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?, 'C_AAPL','APPLE',1000,10,NULL)", (fid,))
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?, 'C_MSFT','MICROSOFT',3000,10,NULL)", (fid,))
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_AAPL','AAPL','Apple')")
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_MSFT','MSFT','Microsoft')")
    prices.init_schema(conn, db)
    # AAPL priced at as-of and forward; MSFT not priced at all.
    prices.store_prices(conn, "AAPL", [
        {"date": "2019-03-29", "close": 50.0, "adj_close": 50.0},   # within 7d of as-of
        {"date": "2022-03-31", "close": 80.0, "adj_close": 80.0},   # forward (as-of+3yr)
    ])
    return conn


def test_coverage_report_value_weighted(tmp_path):
    db = tmp_path / "t.db"
    _seed_coverage(db)
    conn = get_connection(db)
    rep = prices.coverage_report(conn)
    assert rep["quarter"] == "2019-03-31"
    assert rep["total_value_thousands"] == 4000
    # Only AAPL (1000 of 4000) is priced -> 25%
    assert rep["asof_coverage_pct"] == 25.0
    assert rep["forward_coverage_pct"] == 25.0
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_prices.py::test_coverage_report_value_weighted -v`
Expected: FAIL (`coverage_report` not defined).

- [ ] **Step 3: Implement `coverage_report`**

Add to `pipeline/prices.py`:

```python
def coverage_report(conn: sqlite3.Connection) -> dict:
    """
    For the most recent quarter, the fraction of equity holding value (resolved
    tickers) that has a price within 7 calendar days on/before the quarter date
    (as-of) and a price on/before quarter + 3yr (forward).
    """
    latest = conn.execute(
        "SELECT MAX(period_of_report) AS q FROM filings"
    ).fetchone()["q"]
    if latest is None:
        return {"quarter": None, "total_value_thousands": 0,
                "asof_coverage_pct": 0.0, "forward_coverage_pct": 0.0}
    fwd = _plus_three_years(latest)
    row = conn.execute(
        """
        WITH latest_filings AS (
            SELECT f.id FROM filings f
            WHERE f.period_of_report = :q
              AND f.id = (SELECT f2.id FROM filings f2
                          WHERE f2.cik = f.cik AND f2.period_of_report = :q
                          ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1)
        ),
        held AS (
            SELECT s.ticker AS ticker, SUM(h.value_thousands) AS val
            FROM holdings h
            JOIN latest_filings lf ON lf.id = h.filing_id
            JOIN securities s ON s.cusip = h.cusip
            WHERE s.ticker IS NOT NULL AND s.ticker <> ''
              AND (h.put_call IS NULL OR h.put_call = '')
              AND h.value_thousands > 0
            GROUP BY s.ticker
        )
        SELECT
            SUM(val) AS total_val,
            SUM(CASE WHEN EXISTS (
                SELECT 1 FROM prices p WHERE p.ticker = held.ticker
                  AND p.date <= :q AND p.date >= date(:q, '-7 day')
            ) THEN val ELSE 0 END) AS asof_val,
            SUM(CASE WHEN EXISTS (
                SELECT 1 FROM prices p WHERE p.ticker = held.ticker
                  AND p.date <= :fwd
            ) THEN val ELSE 0 END) AS fwd_val
        FROM held
        """,
        {"q": latest, "fwd": fwd},
    ).fetchone()
    total = row["total_val"] or 0
    return {
        "quarter": latest,
        "total_value_thousands": total,
        "asof_coverage_pct": round(100 * (row["asof_val"] or 0) / total, 1) if total else 0.0,
        "forward_coverage_pct": round(100 * (row["fwd_val"] or 0) / total, 1) if total else 0.0,
    }
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_prices.py::test_coverage_report_value_weighted -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/prices.py tests/test_prices.py
git commit -m "feat(prices): value-weighted price coverage report"
```

---

### Task 9: CLI entry point + full-suite green

**Files:**
- Modify: `pipeline/prices.py`

- [ ] **Step 1: Add the CLI block**

Append to the bottom of `pipeline/prices.py`:

```python
if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent))
    ap = argparse.ArgumentParser(description="Ingest prices + benchmark for ranking")
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--coverage", action="store_true", help="print coverage report and exit")
    ap.add_argument("--limit", type=int, default=None, help="cap number of tickers (smoke test)")
    ap.add_argument("--force", action="store_true", help="refetch even if covered")
    ap.add_argument("--no-benchmark", action="store_true", help="skip benchmark fetch")
    args = ap.parse_args()

    db = Path(args.db)
    conn = get_connection(db)
    init_schema(conn, db)

    if args.coverage:
        print(coverage_report(conn))
        sys.exit(0)

    if not args.no_benchmark:
        print(f"benchmark: {ingest_benchmark(db)} rows")
    print(ingest_prices(db, force=args.force, limit=args.limit))
    print(coverage_report(conn))
```

- [ ] **Step 2: Run the full test suite**

Run: `python3 -m pytest tests/test_prices.py -v`
Expected: all Phase-1 tests PASS (init_schema, parse_chart ×3, chart_url, fetch_prices, store_prices, _plus_three_years, held_ticker_windows, ingest_prices ×2, ingest_benchmark, coverage_report).

- [ ] **Step 3: Confirm existing tests still pass**

Run: `python3 -m pytest -q`
Expected: `tests/test_edgar_search.py` + `tests/test_prices.py` all green.

- [ ] **Step 4: Live smoke test (real network, 3 tickers)**

Run: `python3 -m pipeline.prices --limit 3`
Expected: prints `benchmark: <N> rows` (N in the thousands), then `{'fetched': ..., 'skipped': ..., 'failed': ..., 'total': 3}`, then a coverage dict. Spot-check:
`sqlite3 data/13f.db "SELECT COUNT(*) FROM prices; SELECT COUNT(*) FROM benchmark;"` — both > 0.

> If the benchmark line shows 0 rows or the smoke test raises an HTTP error, Yahoo may have rate-limited the IP; wait a minute and retry. Do not switch data sources — the endpoint was validated keyless during design.

- [ ] **Step 5: Commit**

```bash
git add pipeline/prices.py
git commit -m "feat(prices): CLI entry point for price + benchmark ingest"
```

---

## Self-review (completed by plan author)

**Spec coverage (Phase 1 scope only):**
- `prices` table + adjusted close → Tasks 1, 4. ✓
- `benchmark` (^SP500TR total return) → Tasks 1, 7. ✓
- Scope = held tickers, window `[first quarter, last+3yr]` capped today → Task 5. ✓
- Incremental via `price_fetch_log` → Tasks 1, 6. ✓
- Coverage report (value-weighted, as-of + forward) → Task 8. ✓
- Keyless Yahoo v8 source, no new deps → Tasks 2, 3; confirmed in header. ✓
- Options excluded / unresolved CUSIPs excluded → Task 5 query + test. ✓

Out of scope here (later phases, by design): fundamentals (P3), any scoring (P2/P4), website (P5). Not gaps.

**Placeholder scan:** none — every step has runnable code/commands.

**Type/signature consistency:** `init_schema(conn, db_path)`, `parse_chart(payload)->list[dict]`, `_chart_url(symbol,start,end)`, `_http_get(url)`, `fetch_prices(symbol,start,end)->list[dict]`, `store_prices(conn,ticker,rows)->int`, `_plus_three_years(d)->str`, `held_ticker_windows(conn)->list[dict]` (keys `ticker/start/end`), `_already_covered(conn,ticker,start,end)`, `_log_fetch(conn,ticker,first_date,last_date,status)`, `ingest_prices(db_path,force,limit)->dict` (keys `fetched/skipped/failed/total`), `ingest_benchmark(db_path)->int`, `coverage_report(conn)->dict` (keys `quarter/total_value_thousands/asof_coverage_pct/forward_coverage_pct`). Row dicts use `date/close/adj_close` everywhere. Consistent across tasks. ✓
