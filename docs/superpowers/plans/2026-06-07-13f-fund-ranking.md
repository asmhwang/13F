# 13F Phase 2 — Fund Ranking Pipeline (Stages 1-7) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Score and rank eligible funds by long-term stock-selection skill (3-year holding-period excess return vs S&P 500), producing the `fund_rankings` table and its intermediates.

**Architecture:** Two new modules under the existing `pipeline/scoring/` package: `adapter.py` (maps the real filings schema onto the spec's terms + price/benchmark as-of lookups against the Phase-1 `prices`/`benchmark` tables) and `fund_pipeline.py` (one function per spec stage, each reading from and materializing into SQLite result tables, orchestrated by `run_fund_pipeline`). Statistics use Python's stdlib `statistics` — no numpy. Each stage is unit-tested in isolation by seeding its input tables directly; one end-to-end test exercises the orchestrator.

**Tech Stack:** Python 3.13, stdlib `sqlite3` / `statistics` / `datetime`, `pytest`. **No new dependencies.** Reuses `pipeline.prices._plus_three_years` and the Phase-1 `prices`/`benchmark` tables.

---

## Background the implementer needs

The developer spec assumes columns we don't have; the real schema maps as follows (all handled in `adapter.py`):

| Spec term | Real source |
|---|---|
| `fund_id` | `filers.cik` |
| `fund_name` | `filers.name` |
| `quarter_date` | `filings.period_of_report` |
| as-of price date | `filings.filed_date` (the public date — **base price is taken here, not at quarter end**) |
| `position_value_usd` | `holdings.value_thousands * 1000` |
| total portfolio value (AUM) | `SUM(value_thousands*1000)` over a filing's equity rows |
| `ticker` | `securities.ticker` via `holdings.cusip` (NULL/`''`/contains-a-digit ⇒ unresolved) |
| exclude options | `put_call IS NULL OR put_call = ''` |
| 3yr forward prices | Phase-1 `prices.adj_close` (as-of + 3yr); benchmark = `benchmark.adj_close` |

Key rules from the design spec (`docs/superpowers/specs/2026-06-07-13f-ranking-design.md`):
- **As-of = `filed_date`.** Base price = `adj_close` at-or-before `filed_date`; forward = `adj_close` at-or-before `filed_date + 3yr`.
- A quarter is **scoreable** only if `filed_date + 3yr <= today` (3 years have elapsed).
- Weeding constants: any single equity position `> $100M`; `> 30` equity positions; `< 5yr` history; didn't file the current quarter.
- TWS: `λ = 0.85`, needs `>= 6` scoreable quarters, one-hit-wonder discount `× 0.75` when the best quarter contributes `> 50%`.
- Composite: `TWS × turnover_multiplier × 0.70 + consistency × 0.30`, min-max normalized to 0-100.

When a "latest filing for a fund+quarter" is needed, pick the most recent by `filed_date DESC, id DESC` (amendments supersede originals) — the existing `pipeline/queries.py` uses this exact pattern.

---

## File structure

| File | Responsibility |
|---|---|
| `pipeline/scoring/schema.sql` | **Append** the Phase-2 fund tables (idempotent). |
| `pipeline/scoring/adapter.py` | Schema init, current-quarter + latest-filing helpers, `price_asof` / `benchmark_asof` / `three_year_return` / `benchmark_return`. |
| `pipeline/scoring/fund_pipeline.py` | Stages 1-7 (`weed_funds`, `compute_holding_returns`, `compute_qps`, `compute_tws`, `compute_turnover`, `compute_consistency`, `compute_composite`) + `run_fund_pipeline` + CLI. |
| `tests/test_adapter.py` | Tests for adapter helpers. |
| `tests/test_fund_pipeline.py` | Per-stage unit tests + end-to-end orchestrator test. |

All work on branch `feat/13f-ranking` (already checked out; Phase 1 is committed there).

---

### Task 1: Schema additions + adapter helpers

**Files:**
- Modify: `pipeline/scoring/schema.sql`
- Create: `pipeline/scoring/adapter.py`
- Test: `tests/test_adapter.py`

- [ ] **Step 1: Append the fund tables to `pipeline/scoring/schema.sql`**

Add at the end of the file:

```sql

-- ---- Phase 2: fund ranking -------------------------------------------------
CREATE TABLE IF NOT EXISTS fund_eligibility (
    fund_id     TEXT PRIMARY KEY,
    eligible    INTEGER NOT NULL,
    fail_reason TEXT
);

CREATE TABLE IF NOT EXISTS holding_returns (
    fund_id            TEXT NOT NULL,
    quarter_date       DATE NOT NULL,
    ticker             TEXT NOT NULL,   -- resolved ticker, or the cusip when unresolved
    position_value_usd REAL,
    three_yr_return    REAL,            -- NULL when excluded
    data_quality_flag  TEXT,
    PRIMARY KEY (fund_id, quarter_date, ticker)
);

CREATE TABLE IF NOT EXISTS fund_quarterly_scores (
    fund_id                 TEXT NOT NULL,
    quarter_date            DATE NOT NULL,
    qps_raw                 REAL,
    qps_excess              REAL,
    benchmark_return        REAL,
    positions_included      INTEGER,
    positions_excluded_null INTEGER,
    PRIMARY KEY (fund_id, quarter_date)
);

CREATE TABLE IF NOT EXISTS fund_tws (
    fund_id                   TEXT PRIMARY KEY,
    tws                       REAL,
    quarters_scored           INTEGER,
    oldest_quarter_included   DATE,
    one_hit_wonder_flag       INTEGER,
    best_quarter_contribution REAL
);

CREATE TABLE IF NOT EXISTS fund_turnover (
    fund_id                TEXT PRIMARY KEY,
    avg_turnover_rate      REAL,
    turnover_multiplier    REAL,
    quarter_pairs_measured INTEGER
);

CREATE TABLE IF NOT EXISTS fund_consistency (
    fund_id           TEXT PRIMARY KEY,
    qps_stdev         REAL,
    consistency_score REAL
);

CREATE TABLE IF NOT EXISTS fund_rankings (
    fund_id                   TEXT PRIMARY KEY,
    fund_name                 TEXT,
    rank                      INTEGER,
    final_score               REAL,
    tws_raw                   REAL,
    avg_turnover_rate         REAL,
    turnover_multiplier       REAL,
    consistency_score         REAL,
    one_hit_wonder_flag       INTEGER,
    best_quarter_contribution REAL,
    quarters_of_data          INTEGER,
    avg_position_count        REAL,
    avg_aum                   REAL,
    eligible                  INTEGER,
    fail_reason               TEXT
);
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_adapter.py`:

```python
"""Tests for pipeline.scoring.adapter."""
from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return db, conn


def _tables(conn):
    return {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}


def test_init_schema_creates_fund_tables(tmp_path):
    _db, conn = _db(tmp_path)
    assert {"fund_eligibility", "holding_returns", "fund_quarterly_scores",
            "fund_tws", "fund_turnover", "fund_consistency",
            "fund_rankings"} <= _tables(conn)


def test_price_asof_returns_latest_on_or_before(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.executemany(
        "INSERT INTO prices(ticker,date,close,adj_close) VALUES (?,?,?,?)",
        [("AAA", "2020-01-10", 10, 10.0), ("AAA", "2020-01-17", 11, 11.0)],
    )
    conn.commit()
    assert adapter.price_asof(conn, "AAA", "2020-01-20") == ("2020-01-17", 11.0)
    assert adapter.price_asof(conn, "AAA", "2020-01-10") == ("2020-01-10", 10.0)
    assert adapter.price_asof(conn, "AAA", "2020-01-09") is None
    assert adapter.price_asof(conn, "ZZZ", "2020-01-20") is None


def test_three_year_return_clean_and_last_price(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.executemany(
        "INSERT INTO prices(ticker,date,close,adj_close) VALUES (?,?,?,?)",
        [("AAA", "2018-05-10", 50, 50.0), ("AAA", "2021-05-10", 75, 75.0),
         ("BBB", "2018-05-10", 20, 20.0), ("BBB", "2021-01-10", 10, 10.0)],
    )
    conn.commit()
    # AAA: forward price exactly on target date -> clean, +50%
    assert adapter.three_year_return(conn, "AAA", "2018-05-10") == (0.5, "clean")
    # BBB: latest forward price is 4 months stale (delisted) -> last_price, -50%
    ret, flag = adapter.three_year_return(conn, "BBB", "2018-05-10")
    assert round(ret, 4) == -0.5 and flag == "last_price"
    # no prices -> None
    assert adapter.three_year_return(conn, "AAA", "1990-01-01") is None


def test_benchmark_return(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.executemany(
        "INSERT INTO benchmark(date,adj_close) VALUES (?,?)",
        [("2019-05-15", 100.0), ("2022-05-15", 110.0)],
    )
    conn.commit()
    assert round(adapter.benchmark_return(conn, "2019-05-15"), 4) == 0.10
    assert adapter.benchmark_return(conn, "1990-01-01") is None
```

- [ ] **Step 3: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_adapter.py -v`
Expected: FAIL (`pipeline.scoring.adapter` doesn't exist).

- [ ] **Step 4: Create `pipeline/scoring/adapter.py`**

```python
"""
Adapter layer: maps the real filings schema onto the developer spec's terms and
provides price/benchmark lookups against the Phase-1 prices/benchmark tables.

As-of convention: the base price for a holding is taken at its filing's
filed_date (when the position became public), forward price 3 years later.
"""

import sqlite3
from pathlib import Path

from pipeline.database import DB_PATH, get_connection
from pipeline.prices import _plus_three_years

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# Forward price within this many days of the 3yr target counts as "clean";
# anything older means the security stopped trading (delisted/acquired).
_CLEAN_TOLERANCE_DAYS = 7


def init_schema(conn: sqlite3.Connection | None = None, db_path: Path = DB_PATH) -> None:
    c = conn or get_connection(db_path)
    c.executescript(_SCHEMA_PATH.read_text())
    c.commit()


def current_quarter_date(conn: sqlite3.Connection) -> str | None:
    return conn.execute("SELECT MAX(period_of_report) FROM filings").fetchone()[0]


def latest_filing_id(conn: sqlite3.Connection, cik: str, period: str) -> int | None:
    row = conn.execute(
        """
        SELECT id FROM filings
        WHERE cik = ? AND period_of_report = ?
        ORDER BY filed_date DESC, id DESC LIMIT 1
        """,
        (cik, period),
    ).fetchone()
    return row[0] if row else None


def price_asof(conn: sqlite3.Connection, ticker: str, on_date: str) -> tuple[str, float] | None:
    """Latest (date, adj_close) for `ticker` on or before `on_date`, or None."""
    row = conn.execute(
        """
        SELECT date, adj_close FROM prices
        WHERE ticker = ? AND date <= ? AND adj_close IS NOT NULL
        ORDER BY date DESC LIMIT 1
        """,
        (ticker, on_date),
    ).fetchone()
    return (row[0], row[1]) if row else None


def benchmark_asof(conn: sqlite3.Connection, on_date: str) -> tuple[str, float] | None:
    row = conn.execute(
        """
        SELECT date, adj_close FROM benchmark
        WHERE date <= ? AND adj_close IS NOT NULL
        ORDER BY date DESC LIMIT 1
        """,
        (on_date,),
    ).fetchone()
    return (row[0], row[1]) if row else None


def _days_between(a: str, b: str) -> int:
    from datetime import date
    ya, ma, da = (int(x) for x in a.split("-"))
    yb, mb, db = (int(x) for x in b.split("-"))
    return abs((date(yb, mb, db) - date(ya, ma, da)).days)


def three_year_return(conn: sqlite3.Connection, ticker: str, as_of: str
                      ) -> tuple[float, str] | None:
    """
    (return, flag) for a static 3-year hold from `as_of`, or None if no base or
    no forward price exists. flag is 'clean' (forward price near the 3yr target)
    or 'last_price' (forward price is stale -> delisted/acquired).
    """
    base = price_asof(conn, ticker, as_of)
    if base is None:
        return None
    target = _plus_three_years(as_of)
    fwd = price_asof(conn, ticker, target)
    if fwd is None:
        return None
    fwd_date, fwd_px = fwd
    _, base_px = base
    if base_px == 0:
        return None
    ret = (fwd_px - base_px) / base_px
    flag = "clean" if _days_between(fwd_date, target) <= _CLEAN_TOLERANCE_DAYS else "last_price"
    return (ret, flag)


def benchmark_return(conn: sqlite3.Connection, as_of: str) -> float | None:
    """S&P 500 total return over [as_of, as_of+3yr], or None if data missing."""
    base = benchmark_asof(conn, as_of)
    if base is None:
        return None
    fwd = benchmark_asof(conn, _plus_three_years(as_of))
    if fwd is None or base[1] == 0:
        return None
    return (fwd[1] - base[1]) / base[1]
```

- [ ] **Step 5: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_adapter.py -v`
Expected: 4 PASS.

- [ ] **Step 6: Commit**

```bash
git add pipeline/scoring/schema.sql pipeline/scoring/adapter.py tests/test_adapter.py
git commit -m "feat(scoring): fund-ranking schema + adapter price/return helpers"
```

---

### Task 2: Stage 1 — Weeding (`weed_funds`)

**Files:**
- Create: `pipeline/scoring/fund_pipeline.py`
- Test: `tests/test_fund_pipeline.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_fund_pipeline.py`:

```python
"""Per-stage + end-to-end tests for the fund ranking pipeline."""
from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter, fund_pipeline


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return db, conn


def _add_filing(conn, cik, period, filed, acc):
    conn.execute(
        "INSERT INTO filings(cik,accession_number,period_of_report,filed_date) "
        "VALUES (?,?,?,?)", (cik, acc, period, filed))
    return conn.execute("SELECT id FROM filings WHERE accession_number=?", (acc,)).fetchone()[0]


def _add_holding(conn, fid, cusip, value_k, put_call=None):
    conn.execute(
        "INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
        "VALUES (?,?,?,?,?,?)", (fid, cusip, cusip, value_k, 10, put_call))


def test_weed_funds_flags_each_reason(tmp_path):
    _db_, conn = _db(tmp_path)
    cq = "2024-12-31"
    # GOOD: small, concentrated, long history, filed current quarter
    conn.execute("INSERT INTO filers(cik,name) VALUES ('good','Good Fund')")
    _add_filing(conn, "good", "2015-03-31", "2015-05-10", "g_old")
    fg = _add_filing(conn, "good", cq, "2025-02-10", "g_now")
    _add_holding(conn, fg, "CA", 50)       # $50k
    _add_holding(conn, fg, "CB", 60)
    # BIG: a single position over $100M (100000 thousand)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('big','Big Fund')")
    _add_filing(conn, "big", "2015-03-31", "2015-05-10", "b_old")
    fb = _add_filing(conn, "big", cq, "2025-02-10", "b_now")
    _add_holding(conn, fb, "CA", 200000)   # $200M
    # MANY: more than 30 positions
    conn.execute("INSERT INTO filers(cik,name) VALUES ('many','Many Fund')")
    _add_filing(conn, "many", "2015-03-31", "2015-05-10", "m_old")
    fm = _add_filing(conn, "many", cq, "2025-02-10", "m_now")
    for i in range(31):
        _add_holding(conn, fm, f"C{i:02d}", 10)
    # YOUNG: less than 5 years of history
    conn.execute("INSERT INTO filers(cik,name) VALUES ('young','Young Fund')")
    fy = _add_filing(conn, "young", cq, "2025-02-10", "y_now")
    _add_holding(conn, fy, "CA", 10)
    # GONE: did not file the current quarter
    conn.execute("INSERT INTO filers(cik,name) VALUES ('gone','Gone Fund')")
    _add_filing(conn, "gone", "2015-03-31", "2015-05-10", "x_old")
    conn.commit()

    fund_pipeline.weed_funds(conn)

    res = dict(conn.execute(
        "SELECT fund_id, fail_reason FROM fund_eligibility").fetchall())
    assert res["good"] is None
    assert res["big"] == "position_too_large"
    assert res["many"] == "too_many_positions"
    assert res["young"] == "insufficient_history"
    assert res["gone"] == "inactive"
    elig = dict(conn.execute(
        "SELECT fund_id, eligible FROM fund_eligibility").fetchall())
    assert elig["good"] == 1 and elig["big"] == 0
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_weed_funds_flags_each_reason -v`
Expected: FAIL (`fund_pipeline` doesn't exist).

- [ ] **Step 3: Create `pipeline/scoring/fund_pipeline.py` with `weed_funds`**

```python
"""
Fund ranking pipeline (developer spec stages 1-7). Each stage reads from and
writes to SQLite result tables; run_fund_pipeline runs them in order.

All scoring uses the as-of = filed_date convention (see adapter.py).
"""

import sqlite3
import statistics
from datetime import date
from pathlib import Path

from pipeline.database import DB_PATH, get_connection
from pipeline.prices import _plus_three_years
from pipeline.scoring import adapter

_LAMBDA = 0.85
_MIN_SCOREABLE_QUARTERS = 6
_POSITION_LIMIT_THOUSANDS = 100_000      # $100M
_MAX_POSITIONS = 30
_OHW_THRESHOLD = 0.50
_OHW_DISCOUNT = 0.75


def _equity_filter() -> str:
    return "(h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0"


def weed_funds(conn: sqlite3.Connection) -> None:
    """Stage 1 — populate fund_eligibility for every filer."""
    cq = adapter.current_quarter_date(conn)
    five_years_ago = conn.execute("SELECT date('now', '-5 years')").fetchone()[0]
    funds = conn.execute("SELECT cik FROM filers").fetchall()
    for (cik,) in funds:
        span = conn.execute(
            "SELECT MIN(period_of_report), MAX(period_of_report) "
            "FROM filings WHERE cik = ?", (cik,)).fetchone()
        first_q, last_q = span[0], span[1]
        npos = maxval = None
        lf = adapter.latest_filing_id(conn, cik, cq) if cq else None
        if lf is not None:
            agg = conn.execute(
                f"SELECT COUNT(DISTINCT h.cusip), MAX(h.value_thousands) "
                f"FROM holdings h WHERE h.filing_id = ? AND {_equity_filter()}",
                (lf,)).fetchone()
            npos, maxval = agg[0], agg[1]

        reason = None
        if maxval is not None and maxval > _POSITION_LIMIT_THOUSANDS:
            reason = "position_too_large"
        elif npos is not None and npos > _MAX_POSITIONS:
            reason = "too_many_positions"
        elif first_q is None or first_q > five_years_ago:
            reason = "insufficient_history"
        elif last_q is None or cq is None or last_q < cq:
            reason = "inactive"

        conn.execute(
            """
            INSERT INTO fund_eligibility(fund_id, eligible, fail_reason)
            VALUES (?, ?, ?)
            ON CONFLICT(fund_id) DO UPDATE SET
                eligible = excluded.eligible, fail_reason = excluded.fail_reason
            """,
            (cik, 1 if reason is None else 0, reason))
    conn.commit()
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_weed_funds_flags_each_reason -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/fund_pipeline.py tests/test_fund_pipeline.py
git commit -m "feat(scoring): stage 1 fund weeding"
```

---

### Task 3: Stage 2 — Forward return join (`compute_holding_returns`)

**Files:**
- Modify: `pipeline/scoring/fund_pipeline.py`
- Test: `tests/test_fund_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_fund_pipeline.py`:

```python
def _resolve(conn, cusip, ticker):
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES (?,?,?)",
                 (cusip, ticker, cusip))


def test_compute_holding_returns_flags(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('f1','F1')")
    conn.execute("INSERT INTO fund_eligibility(fund_id,eligible,fail_reason) "
                 "VALUES ('f1',1,NULL)")
    fid = _add_filing(conn, "f1", "2018-03-31", "2018-05-10", "f1a")
    _add_holding(conn, fid, "CA", 100)     # AAA  -> clean
    _add_holding(conn, fid, "CB", 20)      # BBB  -> last_price
    _add_holding(conn, fid, "CD", 30)      # DDD  -> resolved but no prices -> null_excluded
    _add_holding(conn, fid, "CU", 50)      # unresolved cusip -> cusip_unresolved
    _add_holding(conn, fid, "CO", 99, put_call="Call")  # option -> ignored
    _resolve(conn, "CA", "AAA"); _resolve(conn, "CB", "BBB"); _resolve(conn, "CD", "DDD")
    conn.executemany(
        "INSERT INTO prices(ticker,date,close,adj_close) VALUES (?,?,?,?)",
        [("AAA", "2018-05-10", 50, 50.0), ("AAA", "2021-05-10", 75, 75.0),
         ("BBB", "2018-05-10", 20, 20.0), ("BBB", "2021-01-10", 10, 10.0)])
    conn.commit()

    fund_pipeline.compute_holding_returns(conn)

    rows = {r["ticker"]: r for r in conn.execute(
        "SELECT ticker, position_value_usd, three_yr_return, data_quality_flag "
        "FROM holding_returns").fetchall()}
    assert set(rows) == {"AAA", "BBB", "DDD", "CU"}        # option excluded
    assert rows["AAA"]["position_value_usd"] == 100000.0
    assert round(rows["AAA"]["three_yr_return"], 4) == 0.5
    assert rows["AAA"]["data_quality_flag"] == "clean"
    assert rows["BBB"]["data_quality_flag"] == "last_price"
    assert rows["DDD"]["three_yr_return"] is None
    assert rows["DDD"]["data_quality_flag"] == "null_excluded"
    assert rows["CU"]["data_quality_flag"] == "cusip_unresolved"


def test_compute_holding_returns_skips_unscoreable_quarter(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('f1','F1')")
    conn.execute("INSERT INTO fund_eligibility(fund_id,eligible,fail_reason) "
                 "VALUES ('f1',1,NULL)")
    # filed_date + 3yr is in the future -> not scoreable yet
    future = date.today().isoformat()
    fid = _add_filing(conn, "f1", future, future, "f1now")
    _add_holding(conn, fid, "CA", 100)
    _resolve(conn, "CA", "AAA")
    conn.commit()
    fund_pipeline.compute_holding_returns(conn)
    assert conn.execute("SELECT COUNT(*) FROM holding_returns").fetchone()[0] == 0
```

(Add `from datetime import date` at the top of the test file.)

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_fund_pipeline.py -k holding_returns -v`
Expected: FAIL (`compute_holding_returns` not defined).

- [ ] **Step 3: Implement `compute_holding_returns`**

Add to `pipeline/scoring/fund_pipeline.py`:

```python
def _is_resolved_ticker(ticker: str | None) -> bool:
    """A usable US equity ticker: non-empty and contains no digit."""
    if not ticker:
        return False
    return not any(ch.isdigit() for ch in ticker)


def compute_holding_returns(conn: sqlite3.Connection) -> None:
    """Stage 2 — per-holding 3yr forward return for eligible funds."""
    today = date.today().isoformat()
    eligible = [r[0] for r in conn.execute(
        "SELECT fund_id FROM fund_eligibility WHERE eligible = 1").fetchall()]
    for cik in eligible:
        filings = conn.execute(
            "SELECT id, period_of_report, filed_date FROM filings WHERE cik = ?",
            (cik,)).fetchall()
        for fid, period, filed in filings:
            if _plus_three_years(filed) > today:
                continue                       # quarter not yet scoreable
            holdings = conn.execute(
                f"""
                SELECT h.cusip, MAX(s.ticker) AS ticker,
                       SUM(h.value_thousands) * 1000.0 AS pos_value
                FROM holdings h
                LEFT JOIN securities s ON s.cusip = h.cusip
                WHERE h.filing_id = ? AND {_equity_filter()}
                GROUP BY h.cusip
                """, (fid,)).fetchall()
            for cusip, ticker, pos_value in holdings:
                if _is_resolved_ticker(ticker):
                    r = adapter.three_year_return(conn, ticker, filed)
                    if r is None:
                        ret, flag, key = None, "null_excluded", ticker
                    else:
                        ret, flag, key = r[0], r[1], ticker
                else:
                    ret, flag, key = None, "cusip_unresolved", cusip
                conn.execute(
                    """
                    INSERT INTO holding_returns
                        (fund_id, quarter_date, ticker, position_value_usd,
                         three_yr_return, data_quality_flag)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fund_id, quarter_date, ticker) DO UPDATE SET
                        position_value_usd = excluded.position_value_usd,
                        three_yr_return    = excluded.three_yr_return,
                        data_quality_flag  = excluded.data_quality_flag
                    """,
                    (cik, period, key, pos_value, ret, flag))
    conn.commit()
```

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_fund_pipeline.py -k holding_returns -v`
Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/fund_pipeline.py tests/test_fund_pipeline.py
git commit -m "feat(scoring): stage 2 forward return join"
```

---

### Task 4: Stage 3 — Quarterly Performance Score (`compute_qps`)

**Files:**
- Modify: `pipeline/scoring/fund_pipeline.py`
- Test: `tests/test_fund_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_fund_pipeline.py`:

```python
def test_compute_qps_value_weighted_excess(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('f1','F1')")
    _add_filing(conn, "f1", "2019-03-31", "2019-05-15", "f1q")
    # holding_returns seeded directly (isolating stage 3)
    conn.executemany(
        "INSERT INTO holding_returns(fund_id,quarter_date,ticker,position_value_usd,"
        "three_yr_return,data_quality_flag) VALUES (?,?,?,?,?,?)",
        [("f1", "2019-03-31", "A", 600.0, 0.10, "clean"),
         ("f1", "2019-03-31", "B", 400.0, -0.05, "clean"),
         ("f1", "2019-03-31", "C", 1000.0, None, "null_excluded")])
    conn.executemany("INSERT INTO benchmark(date,adj_close) VALUES (?,?)",
                     [("2019-05-15", 100.0), ("2022-05-15", 110.0)])
    conn.commit()

    fund_pipeline.compute_qps(conn)

    row = conn.execute(
        "SELECT qps_raw, benchmark_return, qps_excess, positions_included, "
        "positions_excluded_null FROM fund_quarterly_scores "
        "WHERE fund_id='f1' AND quarter_date='2019-03-31'").fetchone()
    assert round(row["qps_raw"], 4) == 0.04        # .6*.10 + .4*(-.05)
    assert round(row["benchmark_return"], 4) == 0.10
    assert round(row["qps_excess"], 4) == -0.06
    assert row["positions_included"] == 2
    assert row["positions_excluded_null"] == 1
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_compute_qps_value_weighted_excess -v`
Expected: FAIL (`compute_qps` not defined).

- [ ] **Step 3: Implement `compute_qps`**

Add to `pipeline/scoring/fund_pipeline.py`:

```python
def _filed_date_for(conn: sqlite3.Connection, cik: str, period: str) -> str | None:
    lf = adapter.latest_filing_id(conn, cik, period)
    if lf is None:
        return None
    return conn.execute("SELECT filed_date FROM filings WHERE id = ?", (lf,)).fetchone()[0]


def compute_qps(conn: sqlite3.Connection) -> None:
    """Stage 3 — value-weighted quarterly performance score vs benchmark."""
    keys = conn.execute(
        "SELECT DISTINCT fund_id, quarter_date FROM holding_returns").fetchall()
    for cik, period in keys:
        rows = conn.execute(
            "SELECT position_value_usd, three_yr_return FROM holding_returns "
            "WHERE fund_id = ? AND quarter_date = ?", (cik, period)).fetchall()
        included = [(v, r) for (v, r) in rows if r is not None]
        excluded_null = len(rows) - len(included)
        if not included:
            continue
        total = sum(v for v, _ in included)
        if total == 0:
            continue
        raw = sum((v / total) * r for v, r in included)
        filed = _filed_date_for(conn, cik, period)
        br = adapter.benchmark_return(conn, filed) if filed else None
        excess = raw - br if br is not None else None
        conn.execute(
            """
            INSERT INTO fund_quarterly_scores
                (fund_id, quarter_date, qps_raw, qps_excess, benchmark_return,
                 positions_included, positions_excluded_null)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fund_id, quarter_date) DO UPDATE SET
                qps_raw = excluded.qps_raw, qps_excess = excluded.qps_excess,
                benchmark_return = excluded.benchmark_return,
                positions_included = excluded.positions_included,
                positions_excluded_null = excluded.positions_excluded_null
            """,
            (cik, period, raw, excess, br, len(included), excluded_null))
    conn.commit()
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_compute_qps_value_weighted_excess -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/fund_pipeline.py tests/test_fund_pipeline.py
git commit -m "feat(scoring): stage 3 quarterly performance score"
```

---

### Task 5: Stage 4 — Time-Weighted Score (`compute_tws`)

**Files:**
- Modify: `pipeline/scoring/fund_pipeline.py`
- Test: `tests/test_fund_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_fund_pipeline.py`:

```python
def _seed_scores(conn, cik, excesses, start_year=2016):
    """Seed fund_quarterly_scores: excesses[0] is the OLDEST quarter."""
    conn.execute(f"INSERT INTO filers(cik,name) VALUES ('{cik}','{cik}')")
    conn.execute(f"INSERT INTO fund_eligibility(fund_id,eligible,fail_reason) "
                 f"VALUES ('{cik}',1,NULL)")
    for i, ex in enumerate(excesses):
        q = f"{start_year + i}-03-31"
        conn.execute(
            "INSERT INTO fund_quarterly_scores(fund_id,quarter_date,qps_raw,"
            "qps_excess,benchmark_return,positions_included,positions_excluded_null)"
            " VALUES (?,?,?,?,?,?,?)", (cik, q, ex, ex, 0.0, 1, 0))


def test_compute_tws_weighted_no_ohw(tmp_path):
    _db_, conn = _db(tmp_path)
    _seed_scores(conn, "f1", [0.10] * 6)     # 6 equal quarters
    conn.commit()
    fund_pipeline.compute_tws(conn)
    row = conn.execute("SELECT tws, quarters_scored, oldest_quarter_included, "
                       "one_hit_wonder_flag, best_quarter_contribution "
                       "FROM fund_tws WHERE fund_id='f1'").fetchone()
    assert round(row["tws"], 6) == 0.10
    assert row["quarters_scored"] == 6
    assert row["oldest_quarter_included"] == "2016-03-31"
    assert row["one_hit_wonder_flag"] == 0
    assert round(row["best_quarter_contribution"], 4) == 0.2409  # 1/sum(0.85^0..5)


def test_compute_tws_one_hit_wonder_discount(tmp_path):
    _db_, conn = _db(tmp_path)
    # one huge quarter dominates -> best contribution > 50% -> ×0.75
    _seed_scores(conn, "f1", [0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
    conn.commit()
    fund_pipeline.compute_tws(conn)
    row = conn.execute("SELECT tws, one_hit_wonder_flag, best_quarter_contribution "
                       "FROM fund_tws WHERE fund_id='f1'").fetchone()
    assert row["one_hit_wonder_flag"] == 1
    assert round(row["best_quarter_contribution"], 4) == 1.0     # only nonzero quarter
    # raw tws = (1.0*1.0)/sum(w) = 1/4.1523 = 0.24084; discounted ×0.75
    assert round(row["tws"], 5) == round(0.24084 * 0.75, 5)


def test_compute_tws_marks_insufficient(tmp_path):
    _db_, conn = _db(tmp_path)
    _seed_scores(conn, "f1", [0.1] * 5)      # only 5 < 6
    conn.commit()
    fund_pipeline.compute_tws(conn)
    assert conn.execute("SELECT COUNT(*) FROM fund_tws WHERE fund_id='f1'").fetchone()[0] == 0
    row = conn.execute("SELECT eligible, fail_reason FROM fund_eligibility "
                       "WHERE fund_id='f1'").fetchone()
    assert row["eligible"] == 0
    assert row["fail_reason"] == "insufficient_scoreable_quarters"
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_fund_pipeline.py -k compute_tws -v`
Expected: FAIL (`compute_tws` not defined).

- [ ] **Step 3: Implement `compute_tws`**

Add to `pipeline/scoring/fund_pipeline.py`:

```python
def compute_tws(conn: sqlite3.Connection) -> None:
    """Stage 4 — time-weighted score with one-hit-wonder discount.

    Funds with fewer than 6 scoreable quarters are demoted to ineligible with
    fail_reason 'insufficient_scoreable_quarters' and get no fund_tws row.
    """
    eligible = [r[0] for r in conn.execute(
        "SELECT fund_id FROM fund_eligibility WHERE eligible = 1").fetchall()]
    for cik in eligible:
        scores = conn.execute(
            "SELECT quarter_date, qps_excess FROM fund_quarterly_scores "
            "WHERE fund_id = ? AND qps_excess IS NOT NULL "
            "ORDER BY quarter_date DESC", (cik,)).fetchall()
        if len(scores) < _MIN_SCOREABLE_QUARTERS:
            conn.execute(
                "UPDATE fund_eligibility SET eligible = 0, "
                "fail_reason = 'insufficient_scoreable_quarters' WHERE fund_id = ?",
                (cik,))
            continue
        # scores[0] is most recent -> weight 1.0; weight decays going back
        weights = [_LAMBDA ** i for i in range(len(scores))]
        contribs = [w * s["qps_excess"] for w, s in zip(weights, scores)]
        wsum = sum(weights)
        csum = sum(contribs)
        tws = csum / wsum
        best = (max(contribs) / csum) if csum != 0 else 0.0
        ohw = best > _OHW_THRESHOLD
        if ohw:
            tws *= _OHW_DISCOUNT
        oldest = scores[-1]["quarter_date"]
        conn.execute(
            """
            INSERT INTO fund_tws(fund_id, tws, quarters_scored,
                oldest_quarter_included, one_hit_wonder_flag, best_quarter_contribution)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(fund_id) DO UPDATE SET
                tws = excluded.tws, quarters_scored = excluded.quarters_scored,
                oldest_quarter_included = excluded.oldest_quarter_included,
                one_hit_wonder_flag = excluded.one_hit_wonder_flag,
                best_quarter_contribution = excluded.best_quarter_contribution
            """,
            (cik, tws, len(scores), oldest, 1 if ohw else 0, best))
    conn.commit()
```

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_fund_pipeline.py -k compute_tws -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/fund_pipeline.py tests/test_fund_pipeline.py
git commit -m "feat(scoring): stage 4 time-weighted score + one-hit-wonder"
```

---

### Task 6: Stage 5 — Turnover (`compute_turnover`)

**Files:**
- Modify: `pipeline/scoring/fund_pipeline.py`
- Test: `tests/test_fund_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_fund_pipeline.py`:

```python
def test_compute_turnover_mean_and_multiplier(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('f1','F1')")
    conn.execute("INSERT INTO fund_tws(fund_id,tws,quarters_scored,"
                 "oldest_quarter_included,one_hit_wonder_flag,best_quarter_contribution)"
                 " VALUES ('f1',0.1,6,'2016-03-31',0,0.2)")
    q1 = _add_filing(conn, "f1", "2020-03-31", "2020-05-10", "q1")
    q2 = _add_filing(conn, "f1", "2020-06-30", "2020-08-10", "q2")
    q3 = _add_filing(conn, "f1", "2020-09-30", "2020-11-10", "q3")
    for c in ("CA", "CB", "CC"):
        _add_holding(conn, q1, c, 10)
    for c in ("CB", "CC", "CD"):            # dropped CA, added CD -> 1/3 turnover
        _add_holding(conn, q2, c, 10)
    for c in ("CB", "CC", "CD"):            # no change -> 0 turnover
        _add_holding(conn, q3, c, 10)
    conn.commit()

    fund_pipeline.compute_turnover(conn)

    row = conn.execute("SELECT avg_turnover_rate, turnover_multiplier, "
                       "quarter_pairs_measured FROM fund_turnover "
                       "WHERE fund_id='f1'").fetchone()
    assert round(row["avg_turnover_rate"], 4) == 0.1667    # mean(1/3, 0)
    assert round(row["turnover_multiplier"], 4) == 0.9167  # 1 - 0.1667*0.5
    assert row["quarter_pairs_measured"] == 2
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_compute_turnover_mean_and_multiplier -v`
Expected: FAIL (`compute_turnover` not defined).

- [ ] **Step 3: Implement `compute_turnover`**

Add to `pipeline/scoring/fund_pipeline.py`:

```python
def _quarter_cusips(conn: sqlite3.Connection, cik: str, period: str) -> set[str]:
    lf = adapter.latest_filing_id(conn, cik, period)
    if lf is None:
        return set()
    rows = conn.execute(
        f"SELECT DISTINCT h.cusip FROM holdings h "
        f"WHERE h.filing_id = ? AND {_equity_filter()}", (lf,)).fetchall()
    return {r[0] for r in rows}


def compute_turnover(conn: sqlite3.Connection) -> None:
    """Stage 5 — average position turnover and its score multiplier.

    Computed for funds that have a fund_tws row (fully scored funds).
    """
    funds = [r[0] for r in conn.execute(
        "SELECT fund_id FROM fund_tws").fetchall()]
    for cik in funds:
        periods = [r[0] for r in conn.execute(
            "SELECT DISTINCT period_of_report FROM filings WHERE cik = ? "
            "ORDER BY period_of_report", (cik,)).fetchall()]
        rates: list[float] = []
        prev = _quarter_cusips(conn, cik, periods[0]) if periods else set()
        for period in periods[1:]:
            cur = _quarter_cusips(conn, cik, period)
            if prev:
                dropped = len(prev - cur)
                rates.append(dropped / len(prev))
            prev = cur
        avg = sum(rates) / len(rates) if rates else 0.0
        mult = max(0.5, min(1.0, 1 - avg * 0.5))
        conn.execute(
            """
            INSERT INTO fund_turnover(fund_id, avg_turnover_rate,
                turnover_multiplier, quarter_pairs_measured)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(fund_id) DO UPDATE SET
                avg_turnover_rate = excluded.avg_turnover_rate,
                turnover_multiplier = excluded.turnover_multiplier,
                quarter_pairs_measured = excluded.quarter_pairs_measured
            """,
            (cik, avg, mult, len(rates)))
    conn.commit()
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_compute_turnover_mean_and_multiplier -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/fund_pipeline.py tests/test_fund_pipeline.py
git commit -m "feat(scoring): stage 5 turnover rate + multiplier"
```

---

### Task 7: Stage 6 — Consistency (`compute_consistency`)

**Files:**
- Modify: `pipeline/scoring/fund_pipeline.py`
- Test: `tests/test_fund_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_fund_pipeline.py`:

```python
def test_compute_consistency_percentile(tmp_path):
    _db_, conn = _db(tmp_path)
    # three funds with distinct excess-QPS stdevs: 0, 0.1, 0.2
    for cik, exc in [("f1", [0.1, 0.1, 0.1]),
                     ("f2", [0.0, 0.1, 0.2]),
                     ("f3", [0.0, 0.2, 0.4])]:
        conn.execute(f"INSERT INTO filers(cik,name) VALUES ('{cik}','{cik}')")
        conn.execute(f"INSERT INTO fund_tws(fund_id,tws,quarters_scored,"
                     f"oldest_quarter_included,one_hit_wonder_flag,"
                     f"best_quarter_contribution) VALUES ('{cik}',0.1,3,"
                     f"'2018-03-31',0,0.2)")
        for i, e in enumerate(exc):
            conn.execute(
                "INSERT INTO fund_quarterly_scores(fund_id,quarter_date,qps_raw,"
                "qps_excess,benchmark_return,positions_included,"
                "positions_excluded_null) VALUES (?,?,?,?,?,?,?)",
                (cik, f"{2018+i}-03-31", e, e, 0.0, 1, 0))
    conn.commit()

    fund_pipeline.compute_consistency(conn)

    res = {r["fund_id"]: r for r in conn.execute(
        "SELECT fund_id, qps_stdev, consistency_score FROM fund_consistency").fetchall()}
    assert round(res["f1"]["qps_stdev"], 4) == 0.0
    assert round(res["f2"]["qps_stdev"], 4) == 0.1
    assert round(res["f3"]["qps_stdev"], 4) == 0.2
    # lowest stdev -> most consistent -> 1.0; highest -> 0.0; middle -> 0.5
    assert round(res["f1"]["consistency_score"], 4) == 1.0
    assert round(res["f2"]["consistency_score"], 4) == 0.5
    assert round(res["f3"]["consistency_score"], 4) == 0.0
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_compute_consistency_percentile -v`
Expected: FAIL (`compute_consistency` not defined).

- [ ] **Step 3: Implement `compute_consistency`**

Add to `pipeline/scoring/fund_pipeline.py`:

```python
def compute_consistency(conn: sqlite3.Connection) -> None:
    """Stage 6 — relative consistency: 1 - percent_rank(stdev of excess QPS).

    Lower stdev = more consistent = higher score. Percentile is across all
    funds that have a fund_tws row, so it must run after all of them are scored.
    """
    funds = [r[0] for r in conn.execute("SELECT fund_id FROM fund_tws").fetchall()]
    stdevs: dict[str, float] = {}
    for cik in funds:
        vals = [r[0] for r in conn.execute(
            "SELECT qps_excess FROM fund_quarterly_scores "
            "WHERE fund_id = ? AND qps_excess IS NOT NULL", (cik,)).fetchall()]
        stdevs[cik] = statistics.stdev(vals) if len(vals) > 1 else 0.0

    n = len(stdevs)
    ordered = sorted(stdevs.values())
    for cik, sd in stdevs.items():
        if n <= 1:
            consistency = 1.0
        else:
            # PERCENT_RANK with ascending stdev: rank = #strictly-less + 1
            rank = sum(1 for v in ordered if v < sd) + 1
            percent_rank = (rank - 1) / (n - 1)
            consistency = 1.0 - percent_rank
        conn.execute(
            """
            INSERT INTO fund_consistency(fund_id, qps_stdev, consistency_score)
            VALUES (?, ?, ?)
            ON CONFLICT(fund_id) DO UPDATE SET
                qps_stdev = excluded.qps_stdev,
                consistency_score = excluded.consistency_score
            """,
            (cik, sd, consistency))
    conn.commit()
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_compute_consistency_percentile -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/fund_pipeline.py tests/test_fund_pipeline.py
git commit -m "feat(scoring): stage 6 consistency percentile"
```

---

### Task 8: Stage 7 — Composite score + `fund_rankings` (`compute_composite`)

**Files:**
- Modify: `pipeline/scoring/fund_pipeline.py`
- Test: `tests/test_fund_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_fund_pipeline.py`:

```python
def test_compute_composite_normalizes_and_ranks(tmp_path):
    _db_, conn = _db(tmp_path)
    for cik, tws, mult, cons in [("f1", 0.10, 0.9, 1.0), ("f2", 0.20, 0.8, 0.5)]:
        conn.execute(f"INSERT INTO filers(cik,name) VALUES ('{cik}','Fund {cik}')")
        conn.execute(f"INSERT INTO fund_eligibility(fund_id,eligible,fail_reason) "
                     f"VALUES ('{cik}',1,NULL)")
        conn.execute("INSERT INTO fund_tws(fund_id,tws,quarters_scored,"
                     "oldest_quarter_included,one_hit_wonder_flag,"
                     "best_quarter_contribution) VALUES (?,?,?,?,?,?)",
                     (cik, tws, 8, "2014-03-31", 0, 0.2))
        conn.execute("INSERT INTO fund_turnover(fund_id,avg_turnover_rate,"
                     "turnover_multiplier,quarter_pairs_measured) VALUES (?,?,?,?)",
                     (cik, (1 - mult) * 2, mult, 5))
        conn.execute("INSERT INTO fund_consistency(fund_id,qps_stdev,"
                     "consistency_score) VALUES (?,?,?)", (cik, 0.05, cons))
        fid = _add_filing(conn, cik, "2020-03-31", "2020-05-10", f"{cik}f")
        _add_holding(conn, fid, "CA", 100)
        _add_holding(conn, fid, "CB", 100)
    conn.commit()

    fund_pipeline.compute_composite(conn)

    rows = {r["fund_id"]: r for r in conn.execute(
        "SELECT fund_id, rank, final_score, tws_raw, fund_name, "
        "quarters_of_data, eligible FROM fund_rankings").fetchall()}
    # raw f1 = .1*.9*.7 + 1.0*.3 = .363 ; raw f2 = .2*.8*.7 + .5*.3 = .262
    assert rows["f1"]["rank"] == 1 and round(rows["f1"]["final_score"], 1) == 100.0
    assert rows["f2"]["rank"] == 2 and round(rows["f2"]["final_score"], 1) == 0.0
    assert rows["f1"]["fund_name"] == "Fund f1"
    assert rows["f1"]["quarters_of_data"] == 8
    assert rows["f1"]["eligible"] == 1
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_compute_composite_normalizes_and_ranks -v`
Expected: FAIL (`compute_composite` not defined).

- [ ] **Step 3: Implement `compute_composite`**

Add to `pipeline/scoring/fund_pipeline.py`:

```python
def _fund_aum_and_positions(conn: sqlite3.Connection, cik: str) -> tuple[float, float]:
    """Average AUM (USD) and average equity position count across filed quarters."""
    periods = [r[0] for r in conn.execute(
        "SELECT DISTINCT period_of_report FROM filings WHERE cik = ?", (cik,)).fetchall()]
    aums: list[float] = []
    counts: list[int] = []
    for period in periods:
        lf = adapter.latest_filing_id(conn, cik, period)
        if lf is None:
            continue
        agg = conn.execute(
            f"SELECT COUNT(DISTINCT h.cusip), SUM(h.value_thousands) * 1000.0 "
            f"FROM holdings h WHERE h.filing_id = ? AND {_equity_filter()}",
            (lf,)).fetchone()
        if agg[0]:
            counts.append(agg[0])
            aums.append(agg[1] or 0.0)
    avg_aum = sum(aums) / len(aums) if aums else 0.0
    avg_pos = sum(counts) / len(counts) if counts else 0.0
    return avg_aum, avg_pos


def compute_composite(conn: sqlite3.Connection) -> None:
    """Stage 7 — composite score, 0-100 normalization, ranking, fund_rankings."""
    funds = conn.execute(
        """
        SELECT t.fund_id, t.tws, t.quarters_scored, t.one_hit_wonder_flag,
               t.best_quarter_contribution,
               tr.avg_turnover_rate, tr.turnover_multiplier,
               c.consistency_score, f.name
        FROM fund_tws t
        JOIN fund_turnover tr   ON tr.fund_id = t.fund_id
        JOIN fund_consistency c ON c.fund_id = t.fund_id
        JOIN filers f           ON f.cik = t.fund_id
        """).fetchall()
    if not funds:
        return
    raw = {}
    for r in funds:
        raw[r["fund_id"]] = (r["tws"] * r["turnover_multiplier"] * 0.70
                             + r["consistency_score"] * 0.30)
    lo, hi = min(raw.values()), max(raw.values())
    span = hi - lo

    ranked = sorted(funds, key=lambda r: raw[r["fund_id"]], reverse=True)
    for rank, r in enumerate(ranked, start=1):
        final = 100.0 if span == 0 else (raw[r["fund_id"]] - lo) / span * 100.0
        avg_aum, avg_pos = _fund_aum_and_positions(conn, r["fund_id"])
        conn.execute(
            """
            INSERT INTO fund_rankings
                (fund_id, fund_name, rank, final_score, tws_raw,
                 avg_turnover_rate, turnover_multiplier, consistency_score,
                 one_hit_wonder_flag, best_quarter_contribution, quarters_of_data,
                 avg_position_count, avg_aum, eligible, fail_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL)
            ON CONFLICT(fund_id) DO UPDATE SET
                fund_name = excluded.fund_name, rank = excluded.rank,
                final_score = excluded.final_score, tws_raw = excluded.tws_raw,
                avg_turnover_rate = excluded.avg_turnover_rate,
                turnover_multiplier = excluded.turnover_multiplier,
                consistency_score = excluded.consistency_score,
                one_hit_wonder_flag = excluded.one_hit_wonder_flag,
                best_quarter_contribution = excluded.best_quarter_contribution,
                quarters_of_data = excluded.quarters_of_data,
                avg_position_count = excluded.avg_position_count,
                avg_aum = excluded.avg_aum, eligible = 1, fail_reason = NULL
            """,
            (r["fund_id"], r["name"], rank, final, r["tws"],
             r["avg_turnover_rate"], r["turnover_multiplier"], r["consistency_score"],
             r["one_hit_wonder_flag"], r["best_quarter_contribution"],
             r["quarters_scored"], avg_pos, avg_aum))
    conn.commit()
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_compute_composite_normalizes_and_ranks -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/fund_pipeline.py tests/test_fund_pipeline.py
git commit -m "feat(scoring): stage 7 composite score + fund_rankings"
```

---

### Task 9: Orchestrator + CLI + end-to-end test

**Files:**
- Modify: `pipeline/scoring/fund_pipeline.py`
- Test: `tests/test_fund_pipeline.py`

- [ ] **Step 1: Write the failing end-to-end test**

Add to `tests/test_fund_pipeline.py`:

```python
def test_run_fund_pipeline_end_to_end(tmp_path):
    _db_, conn = _db(tmp_path)
    # One eligible fund 'win': 6 scoreable quarters, small + concentrated.
    conn.execute("INSERT INTO filers(cik,name) VALUES ('win','Winner Fund')")
    conn.execute("INSERT INTO filers(cik,name) VALUES ('big','Big Fund')")
    # 'big' fails weeding: a single $200M position in the current quarter.
    cq = adapter.current_quarter_date(conn) or "2025-12-31"

    # Winner files 7 historical March quarters 2016..2022 (filed +40 days),
    # each holding one equity 'AAA' worth $50k; AAA doubles over each 3yr window.
    _resolve(conn, "CA", "AAA")
    prices = []
    for yr in range(2016, 2023):
        period = f"{yr}-03-31"
        filed = f"{yr}-05-10"
        fid = _add_filing(conn, "win", period, filed, f"w{yr}")
        _add_holding(conn, fid, "CA", 50)
        prices.append((filed, 100.0))
        prices.append((_three_years_later(filed), 130.0))  # +30% each window
    # Winner must also have filed the current quarter to pass 'inactive'.
    fnow = _add_filing(conn, "win", cq, "2026-02-10", "wnow")
    _add_holding(conn, fnow, "CA", 50)
    conn.executemany("INSERT OR IGNORE INTO prices(ticker,date,close,adj_close) "
                     "VALUES ('AAA',?,?,?)", [(d, p, p) for d, p in prices])
    # Benchmark: flat 100 the whole time -> 0% benchmark return -> excess = +30%
    bench_dates = sorted({d for d, _ in prices})
    conn.executemany("INSERT OR IGNORE INTO benchmark(date,adj_close) VALUES (?,100.0)",
                     [(d,) for d in bench_dates])

    # Big fund: long history + filed current quarter + one $200M position.
    _add_filing(conn, "big", "2015-03-31", "2015-05-10", "bold")
    fb = _add_filing(conn, "big", cq, "2026-02-10", "bnow")
    _add_holding(conn, fb, "CA", 200000)
    conn.commit()

    summary = fund_pipeline.run_fund_pipeline(_db_)

    # Winner is eligible and ranked #1; big fund is weeded out.
    win = conn.execute("SELECT rank, final_score, eligible, quarters_of_data "
                       "FROM fund_rankings WHERE fund_id='win'").fetchone()
    assert win is not None and win["rank"] == 1 and win["eligible"] == 1
    assert win["quarters_of_data"] >= 6
    assert conn.execute("SELECT COUNT(*) FROM fund_rankings "
                        "WHERE fund_id='big'").fetchone()[0] == 0
    big = conn.execute("SELECT fail_reason FROM fund_eligibility "
                       "WHERE fund_id='big'").fetchone()
    assert big["fail_reason"] == "position_too_large"
    assert summary["ranked"] >= 1
```

Add this helper near the top of the test file (next to `_db`):

```python
def _three_years_later(d: str) -> str:
    from pipeline.prices import _plus_three_years
    return _plus_three_years(d)
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_run_fund_pipeline_end_to_end -v`
Expected: FAIL (`run_fund_pipeline` not defined).

- [ ] **Step 3: Implement `run_fund_pipeline` + CLI**

Add to `pipeline/scoring/fund_pipeline.py`:

```python
def run_fund_pipeline(db_path: Path = DB_PATH) -> dict:
    """Run stages 1-7 in order. Returns a small summary dict."""
    conn = get_connection(db_path)
    try:
        adapter.init_schema(conn, db_path)
        weed_funds(conn)
        compute_holding_returns(conn)
        compute_qps(conn)
        compute_tws(conn)
        compute_turnover(conn)
        compute_consistency(conn)
        compute_composite(conn)
        ranked = conn.execute("SELECT COUNT(*) FROM fund_rankings").fetchone()[0]
        eligible = conn.execute(
            "SELECT COUNT(*) FROM fund_eligibility WHERE eligible = 1").fetchone()[0]
        return {"eligible": eligible, "ranked": ranked}
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    ap = argparse.ArgumentParser(description="Run the fund ranking pipeline")
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()
    print(run_fund_pipeline(Path(args.db)))
```

- [ ] **Step 4: Run the end-to-end test, expect PASS**

Run: `python3 -m pytest tests/test_fund_pipeline.py::test_run_fund_pipeline_end_to_end -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `python3 -m pytest -q`
Expected: all green — `tests/test_edgar_search.py`, `tests/test_prices.py`, `tests/test_adapter.py`, `tests/test_fund_pipeline.py`.

- [ ] **Step 6: Smoke test against the real DB**

Run: `python3 -m pipeline.scoring.fund_pipeline`
Expected: prints `{'eligible': N, 'ranked': M}`. Then inspect:
`sqlite3 data/13f.db "SELECT fund_id, fund_name, rank, final_score, quarters_of_data FROM fund_rankings ORDER BY rank LIMIT 10;"`
and `sqlite3 data/13f.db "SELECT fail_reason, COUNT(*) FROM fund_eligibility GROUP BY fail_reason;"`

> Reality note: with the current 31 large seed filers and only ~20 tickers priced so far, `ranked` may be small or 0, and many funds will show `position_too_large`. That is correct behavior, not a bug — the pipeline reflects the data. A fuller ranking needs the full price backfill (Phase 1 operational step) and smaller filers. Report the actual numbers either way.

- [ ] **Step 7: Commit**

```bash
git add pipeline/scoring/fund_pipeline.py tests/test_fund_pipeline.py
git commit -m "feat(scoring): fund pipeline orchestrator + CLI"
```

---

## Self-review (completed by plan author)

**Spec coverage (design spec Phase 2):**
- Stage 1 weeding, all 5 fail reasons → Task 2. ✓ (`insufficient_scoreable_quarters` → Task 5.)
- Stage 2 forward return join, as-of=`filed_date`, last-price rule, null/unresolved flags, 3yr-elapsed gate → Tasks 1 (helpers) + 3. ✓ (Spinoff legs intentionally deferred to v2 per design spec — not implemented; `holding_returns` flag column still supports the values.)
- Stage 3 QPS, weight renormalization over non-null, excess vs benchmark → Task 4. ✓
- Stage 4 TWS, λ=0.85, ≥6 quarters gate, one-hit-wonder ×0.75 → Task 5. ✓
- Stage 5 turnover + clamp(0.5,1.0) → Task 6. ✓
- Stage 6 consistency = 1−percent_rank(stdev), cross-fund → Task 7. ✓
- Stage 7 composite 0.70/0.30, 0-100 normalize, rank, all `fund_rankings` columns → Task 8. ✓
- Orchestration + refresh-ready CLI → Task 9. ✓ (Wiring into `refresh.sh`/`.bat` and the Streamlit page is Phase 5, per design.)

**Placeholder scan:** none — every step has runnable code/commands.

**Type/signature consistency:** stage functions all take `conn: sqlite3.Connection` and return `None`; `run_fund_pipeline(db_path)->dict{eligible,ranked}`; adapter `price_asof`/`benchmark_asof`→`tuple[str,float]|None`, `three_year_return`/`benchmark_return` use them; result-table column names match the schema in Task 1 across all INSERTs; `_equity_filter()`, `_is_resolved_ticker()`, `latest_filing_id()` reused consistently. Helper `_three_years_later` in the test delegates to `pipeline.prices._plus_three_years` (same function the implementation uses), so fixture forward dates line up exactly with the lookup. ✓

**Decisions surfaced:** `_CLEAN_TOLERANCE_DAYS=7` defines clean-vs-last_price; consistency uses sample stdev (`statistics.stdev`, 0.0 for n≤1); composite normalization returns 100.0 when all funds tie (`span==0`); turnover/consistency/composite operate on funds with a `fund_tws` row (fully scored), matching the spec's "after all funds complete stages 2-6" ordering.
