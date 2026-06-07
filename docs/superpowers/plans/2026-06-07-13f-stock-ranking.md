# 13F Phase 4 — Stock Ranking Pipeline (Stages 1-6) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rank the stocks held by top-ranked funds — aggregate per-stock conviction signals, score them via a weighted regression on 3-year forward return, attach a composite confidence grade, and emit raw + filtered ranking tables.

**Architecture:** One new module `pipeline/scoring/stock_pipeline.py`. It builds per-fund quarter→holdings histories once, derives per-stock signals (reusable for any quarter), fits an OLS model (`numpy.linalg.lstsq`) on historically-consistent features with a fund_conviction fallback for tiny/singular training sets, computes a 5-component confidence score, and materializes `stock_signals`, `stock_confidence`, `stock_rankings_raw`, `stock_rankings_filtered`. Reuses `pipeline.scoring.adapter` and the Phase 1-3 tables (`prices`, `fund_rankings`, `holding_returns`, `fundamentals`, `sectors`).

**Tech Stack:** Python 3.13, stdlib `sqlite3`/`math`, **`numpy`** (new dep — first phase to need it), `pytest`. Reuses `pipeline.prices._plus_three_years`.

---

## Scope & decisions (read first)

The eligible universe is currently 1 fund / 22 stocks with only ~11 priced historical returns, so the regression is structurally exercised but degenerate until the full price backfill + more filers land. Decisions baked into this plan:

1. **Qualifying funds** = `fund_rankings` rows with `rank <= ceil(total/2)` (so a lone ranked fund still qualifies).
2. **Regression features** (consistent at train and predict time): `holder_count`, `fund_conviction`, `avg_relative_size`, `52wk_range_position` (0.5 when NULL), and one-hot **sector dummies**. Target = mean `3yr_return` across qualifying holders of that (stock, quarter). Fit with `numpy.linalg.lstsq` (handles rank-deficiency via least-norm). **Fallback:** when the training set has fewer rows than `_MIN_TRAIN_ROWS` (=8), set `raw_score = fund_conviction` for every universe stock and skip the fit. `sector_adjusted_score = raw_score − sector_mean(raw_score)`.
3. **Display/filter-only fundamentals**: `market_cap`, `pe_ratio`, `pe_available`, `gross_margin_pct` come from the `fundamentals` table (current quarter) — used in `stock_rankings_raw` columns and the filtered-output market-cap band, **not** as regression features (no historical values exist until P3b). `net_change_pct` and `avg_tenure` are full current-quarter signals (display + confidence) but are not regression features.
4. **Confidence** (5 components, normalized across the universe) and **filtered output** follow the design spec exactly.

---

## File structure

| File | Responsibility |
|---|---|
| `pipeline/scoring/schema.sql` | **Append** Phase-4 tables: `stock_signals`, `stock_confidence`, `stock_rankings_raw`, `stock_rankings_filtered`. |
| `pipeline/scoring/stock_pipeline.py` | Qualifying funds, fund histories, per-period signals, 52-week range, regression, confidence, output assembly, `run_stock_pipeline`, CLI. |
| `requirements.txt` | Add `numpy`. |
| `tests/test_stock_pipeline.py` | Per-stage unit tests + end-to-end test. |

All work on branch `feat/13f-ranking`.

---

### Task 1: Schema + numpy dep + qualifying funds + fund histories + universe

**Files:**
- Modify: `pipeline/scoring/schema.sql`, `requirements.txt`
- Create: `pipeline/scoring/stock_pipeline.py`
- Test: `tests/test_stock_pipeline.py`

- [ ] **Step 1: Append Phase-4 tables to `pipeline/scoring/schema.sql`**

```sql

-- ---- Phase 4: stock ranking ------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_signals (
    ticker            TEXT NOT NULL,
    as_of_date        DATE NOT NULL,
    fund_conviction   REAL,
    holder_count      INTEGER,
    net_change_pct    REAL,
    avg_relative_size REAL,
    avg_tenure        REAL,
    PRIMARY KEY (ticker, as_of_date)
);

CREATE TABLE IF NOT EXISTS stock_confidence (
    ticker                TEXT PRIMARY KEY,
    confidence_flag       TEXT,
    confidence_raw        REAL,
    weighted_holder_score REAL,
    avg_tenure_score      REAL,
    avg_relative_size     REAL,
    direction_agreement   REAL,
    data_quality_score    REAL,
    confidence_percentile REAL
);

CREATE TABLE IF NOT EXISTS stock_rankings_raw (
    ticker                TEXT PRIMARY KEY,
    company_name          TEXT,
    sector                TEXT,
    rank                  INTEGER,
    raw_score             REAL,
    sector_adjusted_score REAL,
    confidence_flag       TEXT,
    confidence_raw        REAL,
    holder_count          INTEGER,
    fund_conviction       REAL,
    net_change_pct        REAL,
    avg_relative_size     REAL,
    avg_tenure            REAL,
    market_cap            REAL,
    range_position        REAL,    -- 52-week range position
    partial               INTEGER, -- 52-week partial flag (0/1)
    pe_ratio              REAL,
    pe_available          INTEGER,
    gross_margin_pct      REAL
);

CREATE TABLE IF NOT EXISTS stock_rankings_filtered (
    ticker                TEXT PRIMARY KEY,
    rank                  INTEGER,
    company_name          TEXT,
    sector                TEXT,
    sector_adjusted_score REAL,
    confidence_flag       TEXT,
    market_cap            REAL,
    range_position        REAL,
    holder_count          INTEGER
);
```

- [ ] **Step 2: Add numpy to `requirements.txt`**

Append one line to `requirements.txt`:

```
numpy>=1.26
```

- [ ] **Step 3: Write the failing test**

Create `tests/test_stock_pipeline.py`:

```python
"""Per-stage + end-to-end tests for the stock ranking pipeline."""
from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter, stock_pipeline


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return db, conn


def _add_filing(conn, cik, period, filed, acc):
    conn.execute("INSERT INTO filings(cik,accession_number,period_of_report,filed_date) "
                 "VALUES (?,?,?,?)", (cik, acc, period, filed))
    return conn.execute("SELECT id FROM filings WHERE accession_number=?", (acc,)).fetchone()[0]


def _hold(conn, fid, cusip, value_k, put_call=None):
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?,?,?,?,?,?)", (fid, cusip, cusip, value_k, 10, put_call))


def _rank(conn, cik, name, rank, score):
    conn.execute("INSERT INTO filers(cik,name) VALUES (?,?)", (cik, name))
    conn.execute("INSERT INTO fund_rankings(fund_id,fund_name,rank,final_score,eligible) "
                 "VALUES (?,?,?,?,1)", (cik, name, rank, score))


def test_qualifying_funds_ceil_cutoff(tmp_path):
    _db_, conn = _db(tmp_path)
    _rank(conn, "a", "A", 1, 100.0)
    _rank(conn, "b", "B", 2, 50.0)
    _rank(conn, "c", "C", 3, 10.0)
    conn.commit()
    q = stock_pipeline.qualifying_funds(conn)   # ceil(3/2)=2 -> ranks 1,2
    assert set(q) == {"a", "b"}
    assert q["a"] == 100.0


def test_qualifying_funds_single(tmp_path):
    _db_, conn = _db(tmp_path)
    _rank(conn, "a", "A", 1, 100.0)
    conn.commit()
    assert set(stock_pipeline.qualifying_funds(conn)) == {"a"}   # ceil(1/2)=1
```

- [ ] **Step 4: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_stock_pipeline.py -k qualifying -v`
Expected: FAIL (module missing).

- [ ] **Step 5: Create `pipeline/scoring/stock_pipeline.py`**

```python
"""
Stock ranking pipeline (developer spec stages 1-6). Aggregates per-stock
conviction signals across qualifying funds, scores stocks via an OLS regression
on 3-year forward return (with a fund_conviction fallback for tiny training
sets), grades confidence, and materializes raw + filtered ranking tables.
"""

import math
import sqlite3
from pathlib import Path

import numpy as np

from pipeline.database import DB_PATH, get_connection
from pipeline.prices import _plus_three_years
from pipeline.scoring import adapter

_MIN_TRAIN_ROWS = 8          # below this, skip the fit and fall back to fund_conviction
_TRADING_DAYS_52W = 252


def qualifying_funds(conn: sqlite3.Connection) -> dict[str, float]:
    """{fund_id: final_score} for funds in the top half of fund_rankings
    (rank <= ceil(n/2), so a single ranked fund still qualifies)."""
    rows = conn.execute("SELECT fund_id, final_score, rank FROM fund_rankings").fetchall()
    n = len(rows)
    if n == 0:
        return {}
    cutoff = (n + 1) // 2
    return {r["fund_id"]: r["final_score"] for r in rows if r["rank"] <= cutoff}


def _equity_holdings(conn: sqlite3.Connection, cik: str, period: str) -> dict[str, float]:
    """{ticker: position_value_usd} for the latest filing of cik at period
    (equity, resolved tickers only)."""
    lf = adapter.latest_filing_id(conn, cik, period)
    if lf is None:
        return {}
    rows = conn.execute(
        """
        SELECT s.ticker AS ticker, SUM(h.value_thousands) * 1000.0 AS v
        FROM holdings h JOIN securities s ON s.cusip = h.cusip
        WHERE h.filing_id = ? AND s.ticker IS NOT NULL AND s.ticker <> ''
          AND s.ticker NOT GLOB '*[0-9]*'
          AND (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
        GROUP BY s.ticker
        """, (lf,)).fetchall()
    return {r["ticker"]: r["v"] for r in rows}


def fund_histories(conn: sqlite3.Connection, qualifying: dict[str, float]
                   ) -> dict[str, dict[str, dict[str, float]]]:
    """Per qualifying fund: {period: {ticker: value}} across all its filed
    quarters. Built once so signal/tenure computation needs no further holdings
    queries."""
    hist: dict[str, dict[str, dict[str, float]]] = {}
    for cik in qualifying:
        periods = [r[0] for r in conn.execute(
            "SELECT DISTINCT period_of_report FROM filings WHERE cik = ? "
            "ORDER BY period_of_report", (cik,)).fetchall()]
        hist[cik] = {p: _equity_holdings(conn, cik, p) for p in periods}
    return hist
```

- [ ] **Step 6: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_stock_pipeline.py -k qualifying -v`
Expected: 2 PASS.

- [ ] **Step 7: Commit**

```bash
git add pipeline/scoring/schema.sql requirements.txt pipeline/scoring/stock_pipeline.py tests/test_stock_pipeline.py
git commit -m "feat(stock): schema + numpy dep + qualifying funds + fund histories"
```

---

### Task 2: Per-period signals (`signals_for_period`)

**Files:**
- Modify: `pipeline/scoring/stock_pipeline.py`
- Test: `tests/test_stock_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_stock_pipeline.py`:

```python
def test_signals_for_period(tmp_path):
    _db_, conn = _db(tmp_path)
    # two qualifying funds A (score 100) and B (score 50)
    _rank(conn, "a", "A", 1, 100.0)
    _rank(conn, "b", "B", 2, 50.0)
    # prior quarter 2024-09-30
    fa0 = _add_filing(conn, "a", "2024-09-30", "2024-11-10", "a0")
    _hold(conn, fa0, "CX", 100)            # A held X last quarter ($100k)
    # current quarter 2024-12-31
    fa1 = _add_filing(conn, "a", "2024-12-31", "2025-02-10", "a1")
    _hold(conn, fa1, "CX", 300)            # A increased X to $300k
    _hold(conn, fa1, "CY", 100)            # A new Y
    fb1 = _add_filing(conn, "b", "2024-12-31", "2025-02-10", "b1")
    _hold(conn, fb1, "CX", 100)            # B new X
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES "
                 "('CX','XXX','X'),('CY','YYY','Y')")
    conn.commit()

    q = stock_pipeline.qualifying_funds(conn)
    hist = stock_pipeline.fund_histories(conn, q)
    sig = stock_pipeline.signals_for_period(conn, "2024-12-31", q, hist)

    # X held by both funds -> holder_count 2; Y by A only -> 1
    assert sig["XXX"]["holder_count"] == 2
    assert sig["YYY"]["holder_count"] == 1
    # A portfolio = 300+100=400 -> X weight .75 ; B portfolio = 100 -> X weight 1.0
    # fund_conviction(X) = (100*.75 + 50*1.0)/(100+50) = 125/150 = .8333
    assert round(sig["XXX"]["fund_conviction"], 4) == 0.8333
    # avg_relative_size(X) = mean(.75, 1.0) = .875
    assert round(sig["XXX"]["avg_relative_size"], 4) == 0.875
    # tenure: A held X for 2 consecutive quarters, B for 1 -> mean 1.5
    assert round(sig["XXX"]["avg_tenure"], 4) == 1.5
    # net_change(X): A +200 (100->300), B +100 (new) = +300 ; universe AUM=400+100=500
    # net_change_pct = 300/500 = 0.6
    assert round(sig["XXX"]["net_change_pct"], 4) == 0.6
    # direction: both funds buyers of X -> buyers=2 sellers=0
    assert sig["XXX"]["buyers"] == 2 and sig["XXX"]["sellers"] == 0
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_stock_pipeline.py::test_signals_for_period -v`
Expected: FAIL (`signals_for_period` not defined).

- [ ] **Step 3: Implement `signals_for_period`**

Add to `pipeline/scoring/stock_pipeline.py`:

```python
def _prior_period(conn: sqlite3.Connection, cik: str, period: str) -> str | None:
    r = conn.execute(
        "SELECT MAX(period_of_report) FROM filings WHERE cik = ? AND period_of_report < ?",
        (cik, period)).fetchone()
    return r[0]


def _tenure(periods_desc: list[str], holdings_by_period: dict[str, dict[str, float]],
            ticker: str, period: str) -> int:
    """Consecutive quarters (the fund's own filed quarters, going back from
    `period`) that the fund held `ticker`, stopping at the first gap."""
    count = 0
    for p in periods_desc:
        if p > period:
            continue
        if ticker in holdings_by_period.get(p, {}):
            count += 1
        else:
            break
    return count


def signals_for_period(conn: sqlite3.Connection, period: str,
                       qualifying: dict[str, float],
                       hist: dict[str, dict[str, dict[str, float]]]) -> dict[str, dict]:
    """
    Per-ticker conviction signals aggregated over qualifying funds holding the
    ticker at `period`. Returns {ticker: {fund_conviction, holder_count,
    net_change_pct, avg_relative_size, avg_tenure, buyers, sellers}}.
    """
    # Per-fund current holdings + portfolio value + prior holdings.
    cur: dict[str, dict[str, float]] = {}
    prior: dict[str, dict[str, float]] = {}
    portfolio: dict[str, float] = {}
    periods_desc: dict[str, list[str]] = {}
    for cik in qualifying:
        cur[cik] = hist[cik].get(period, {})
        portfolio[cik] = sum(cur[cik].values())
        pp = _prior_period(conn, cik, period)
        prior[cik] = hist[cik].get(pp, {}) if pp else {}
        periods_desc[cik] = sorted(hist[cik].keys(), reverse=True)

    universe_aum = sum(portfolio.values())
    tickers = {t for cik in qualifying for t in cur[cik]}
    out: dict[str, dict] = {}
    for ticker in tickers:
        holders = [cik for cik in qualifying if ticker in cur[cik] and portfolio[cik] > 0]
        if not holders:
            continue
        weights = [cur[cik][ticker] / portfolio[cik] for cik in holders]
        scores = [qualifying[cik] for cik in holders]
        score_sum = sum(scores)
        fund_conviction = (sum(s * w for s, w in zip(scores, weights)) / score_sum
                           if score_sum > 0 else 0.0)
        avg_relative_size = sum(weights) / len(weights)
        tenures = [_tenure(periods_desc[cik], hist[cik], ticker, period) for cik in holders]
        avg_tenure = sum(tenures) / len(tenures)
        # net change across ALL qualifying funds (a fund that exited still counts)
        net_change = 0.0
        buyers = sellers = 0
        for cik in qualifying:
            now = cur[cik].get(ticker, 0.0)
            was = prior[cik].get(ticker, 0.0)
            delta = now - was
            if now > 0 or was > 0:
                net_change += delta
                if delta > 0:
                    buyers += 1
                elif delta < 0:
                    sellers += 1
        net_change_pct = net_change / universe_aum if universe_aum > 0 else 0.0
        out[ticker] = {
            "fund_conviction": fund_conviction,
            "holder_count": len(holders),
            "net_change_pct": net_change_pct,
            "avg_relative_size": avg_relative_size,
            "avg_tenure": avg_tenure,
            "buyers": buyers,
            "sellers": sellers,
        }
    return out
```

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_stock_pipeline.py::test_signals_for_period -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/stock_pipeline.py tests/test_stock_pipeline.py
git commit -m "feat(stock): per-period conviction signals"
```

---

### Task 3: 52-week range position (`range_position_52w`)

**Files:**
- Modify: `pipeline/scoring/stock_pipeline.py`
- Test: `tests/test_stock_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_stock_pipeline.py`:

```python
def test_range_position_52w(tmp_path):
    _db_, conn = _db(tmp_path)
    # 25 daily points within the trailing year: low 10, high 30, last (as-of) = 25.
    # >= 20 points but < 252 -> usable range, partial=1.
    pts = ([("2024-12-02", 10.0)]
           + [(f"2024-12-{d:02d}", 20.0) for d in range(3, 26)]
           + [("2024-12-26", 30.0), ("2024-12-27", 25.0)])
    conn.executemany("INSERT INTO prices(ticker,date,close,adj_close) VALUES ('ZZZ',?,?,?)",
                     [(d, p, p) for d, p in pts])
    conn.commit()
    pos, partial = stock_pipeline.range_position_52w(conn, "ZZZ", "2024-12-31")
    assert round(pos, 4) == 0.75            # (25-10)/(30-10)
    assert partial == 1                      # >=20 but < 252 trading days

    # fewer than 20 trading days of data -> NULL position, partial=1
    conn.execute("INSERT INTO prices(ticker,date,close,adj_close) VALUES ('FEW','2024-12-28',5,5.0)")
    conn.commit()
    pos2, partial2 = stock_pipeline.range_position_52w(conn, "FEW", "2024-12-31")
    assert pos2 is None and partial2 == 1

    # no prices -> NULL, partial 1
    pos3, partial3 = stock_pipeline.range_position_52w(conn, "NONE", "2024-12-31")
    assert pos3 is None and partial3 == 1
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_stock_pipeline.py::test_range_position_52w -v`
Expected: FAIL (`range_position_52w` not defined).

- [ ] **Step 3: Implement `range_position_52w`**

Add to `pipeline/scoring/stock_pipeline.py`:

```python
def _minus_one_year(d: str) -> str:
    y, m, day = (int(x) for x in d.split("-"))
    try:
        from datetime import date
        return date(y - 1, m, day).isoformat()
    except ValueError:
        from datetime import date
        return date(y - 1, m, day - 1).isoformat()


def range_position_52w(conn: sqlite3.Connection, ticker: str, as_of: str
                       ) -> tuple[float | None, int]:
    """
    (range_position, partial) over the trailing 52 weeks ending at `as_of`.
    range_position = (price - low) / (high - low). Rules:
      - distinct trading days >= ~52 weeks (>= _TRADING_DAYS_52W) -> partial=0
      - 4+ weeks but < 52 weeks -> use available history, partial=1
      - < 4 weeks (< 20 trading days) of data -> position NULL, partial=1
      - no on/before price -> NULL, partial=1
    """
    start = _minus_one_year(as_of)
    rows = conn.execute(
        "SELECT date, adj_close FROM prices "
        "WHERE ticker = ? AND date >= ? AND date <= ? AND adj_close IS NOT NULL "
        "ORDER BY date", (ticker, start, as_of)).fetchall()
    n = len(rows)
    if n < 20:
        return (None, 1)
    lo = min(r["adj_close"] for r in rows)
    hi = max(r["adj_close"] for r in rows)
    price = rows[-1]["adj_close"]
    partial = 0 if n >= _TRADING_DAYS_52W else 1
    if hi == lo:
        return (0.5, partial)
    return ((price - lo) / (hi - lo), partial)
```

> Note: `n < 20` returns `(None, 1)` (under ~4 weeks). The `< 20 → None` and `< _TRADING_DAYS_52W → partial=1` thresholds implement the spec's "4+ weeks: use available, partial=1; under 4 weeks: NULL, partial=1; full 52 weeks: partial=0" rule.

- [ ] **Step 4: Run the test, expect PASS**

Run: `python3 -m pytest tests/test_stock_pipeline.py::test_range_position_52w -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/stock_pipeline.py tests/test_stock_pipeline.py
git commit -m "feat(stock): 52-week range position"
```

---

### Task 4: Regression scoring (`build_training_set`, `regress_scores`)

**Files:**
- Modify: `pipeline/scoring/stock_pipeline.py`
- Test: `tests/test_stock_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_stock_pipeline.py`:

```python
import numpy as np


def test_regress_scores_recovers_linear_signal():
    # y = 2*x1 + 3 ; one feature, perfect line -> predictions equal targets
    feature_names = ["x1"]
    train_X = [[1.0], [2.0], [3.0], [4.0], [5.0], [6.0], [7.0], [8.0]]
    train_y = [5.0, 7.0, 9.0, 11.0, 13.0, 15.0, 17.0, 19.0]
    pred_rows = {"AAA": [1.0], "BBB": [10.0]}
    scores = stock_pipeline.regress_scores(feature_names, train_X, train_y, pred_rows)
    assert round(scores["AAA"], 3) == 5.0
    assert round(scores["BBB"], 3) == 23.0


def test_regress_scores_fallback_when_too_few_rows():
    # fewer than _MIN_TRAIN_ROWS -> returns the provided fallback per ticker
    scores = stock_pipeline.regress_scores(
        ["x1"], [[1.0], [2.0]], [1.0, 2.0],
        {"AAA": [9.0], "BBB": [9.0]},
        fallback={"AAA": 0.7, "BBB": 0.2})
    assert scores == {"AAA": 0.7, "BBB": 0.2}


def test_sector_adjust():
    raw = {"AAA": 1.0, "BBB": 3.0, "CCC": 5.0}
    sector = {"AAA": "Tech", "BBB": "Tech", "CCC": "Energy"}
    adj = stock_pipeline.sector_adjust(raw, sector)
    # Tech mean = 2.0 ; Energy mean = 5.0
    assert round(adj["AAA"], 4) == -1.0 and round(adj["BBB"], 4) == 1.0
    assert round(adj["CCC"], 4) == 0.0
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_stock_pipeline.py -k "regress or sector_adjust" -v`
Expected: FAIL (functions not defined).

- [ ] **Step 3: Implement `regress_scores` and `sector_adjust`**

Add to `pipeline/scoring/stock_pipeline.py`:

```python
def regress_scores(feature_names: list[str], train_X: list[list[float]],
                   train_y: list[float], pred_rows: dict[str, list[float]],
                   fallback: dict[str, float] | None = None) -> dict[str, float]:
    """
    Fit OLS (with intercept) via least-squares and predict a raw score for each
    ticker in pred_rows. If the training set has fewer than _MIN_TRAIN_ROWS rows,
    return `fallback` unchanged (caller supplies fund_conviction as the fallback).
    """
    if len(train_X) < _MIN_TRAIN_ROWS:
        return dict(fallback) if fallback else {t: 0.0 for t in pred_rows}
    A = np.array([[1.0, *row] for row in train_X], dtype=float)
    y = np.array(train_y, dtype=float)
    coef, *_ = np.linalg.lstsq(A, y, rcond=None)
    scores: dict[str, float] = {}
    for ticker, row in pred_rows.items():
        x = np.array([1.0, *row], dtype=float)
        scores[ticker] = float(x @ coef)
    return scores


def sector_adjust(raw: dict[str, float], sector: dict[str, str]) -> dict[str, float]:
    """sector_adjusted_score = raw_score - mean(raw_score within the same sector)."""
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    for ticker, score in raw.items():
        s = sector.get(ticker, "Unknown")
        sums[s] = sums.get(s, 0.0) + score
        counts[s] = counts.get(s, 0) + 1
    return {ticker: score - sums[sector.get(ticker, "Unknown")] / counts[sector.get(ticker, "Unknown")]
            for ticker, score in raw.items()}


def build_training_set(conn: sqlite3.Connection, qualifying: dict[str, float],
                       hist: dict[str, dict[str, dict[str, float]]],
                       sector: dict[str, str]) -> tuple[list[str], list[list[float]], list[float]]:
    """
    Assemble (feature_names, X, y) from historical (stock, quarter) observations
    that have a non-null 3yr return. Features: holder_count, fund_conviction,
    avg_relative_size, 52wk_range_position (0.5 if NULL), + one-hot sector.
    Target: mean 3yr_return across qualifying holders of that (stock, quarter).
    """
    sector_names = sorted(set(sector.values()))
    feature_names = ["holder_count", "fund_conviction", "avg_relative_size",
                     "range_position"] + [f"sector_{s}" for s in sector_names]
    # target: mean return per (ticker, quarter) among qualifying funds
    rows = conn.execute(
        """
        SELECT hr.ticker, hr.quarter_date, AVG(hr.three_yr_return) AS ret
        FROM holding_returns hr
        JOIN fund_rankings fr ON fr.fund_id = hr.fund_id
        WHERE hr.three_yr_return IS NOT NULL
        GROUP BY hr.ticker, hr.quarter_date
        """).fetchall()
    by_period: dict[str, dict[str, float]] = {}
    for r in rows:
        by_period.setdefault(r["quarter_date"], {})[r["ticker"]] = r["ret"]

    X: list[list[float]] = []
    y: list[float] = []
    for period, ticker_ret in by_period.items():
        sig = signals_for_period(conn, period, qualifying, hist)
        for ticker, ret in ticker_ret.items():
            if ticker not in sig:
                continue
            rp, _ = range_position_52w(conn, ticker, period)
            rp = 0.5 if rp is None else rp
            base = [float(sig[ticker]["holder_count"]), sig[ticker]["fund_conviction"],
                    sig[ticker]["avg_relative_size"], rp]
            onehot = [1.0 if sector.get(ticker, "Unknown") == s else 0.0 for s in sector_names]
            X.append(base + onehot)
            y.append(ret)
    return feature_names, X, y
```

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_stock_pipeline.py -k "regress or sector_adjust" -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/stock_pipeline.py tests/test_stock_pipeline.py
git commit -m "feat(stock): OLS regression scoring + sector adjustment"
```

---

### Task 5: Confidence score (`compute_confidence`)

**Files:**
- Modify: `pipeline/scoring/stock_pipeline.py`
- Test: `tests/test_stock_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_stock_pipeline.py`:

```python
def test_normalize_unit_range():
    assert stock_pipeline._normalize({"a": 10.0, "b": 20.0, "c": 30.0}) == {"a": 0.0, "b": 0.5, "c": 1.0}
    assert stock_pipeline._normalize({"a": 5.0}) == {"a": 1.0}     # single -> 1.0


def test_confidence_buckets_by_thirds():
    # three stocks with strictly increasing confidence inputs -> Low/Med/High
    universe = {
        "LO":  {"weighted_holder_score": 1.0, "avg_tenure_score": 1.0,
                "avg_relative_size": 0.0, "direction_agreement": 0.0, "data_quality_score": 0.0},
        "MID": {"weighted_holder_score": 2.0, "avg_tenure_score": 2.0,
                "avg_relative_size": 0.5, "direction_agreement": 0.5, "data_quality_score": 0.5},
        "HI":  {"weighted_holder_score": 3.0, "avg_tenure_score": 3.0,
                "avg_relative_size": 1.0, "direction_agreement": 1.0, "data_quality_score": 1.0},
    }
    flags = stock_pipeline.confidence_flags(universe)
    assert flags["HI"] == "High" and flags["MID"] == "Medium" and flags["LO"] == "Low"
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `python3 -m pytest tests/test_stock_pipeline.py -k "normalize or confidence_buckets" -v`
Expected: FAIL.

- [ ] **Step 3: Implement `_normalize` and `confidence_flags`**

Add to `pipeline/scoring/stock_pipeline.py`:

```python
def _normalize(values: dict[str, float]) -> dict[str, float]:
    """Min-max to 0-1 across the universe; a single value maps to 1.0."""
    if not values:
        return {}
    if len(values) == 1:
        return {k: 1.0 for k in values}
    lo, hi = min(values.values()), max(values.values())
    if hi == lo:
        return {k: 1.0 for k in values}
    return {k: (v - lo) / (hi - lo) for k, v in values.items()}


def confidence_flags(universe: dict[str, dict]) -> dict[str, str]:
    """
    Composite confidence -> percentile bucket per the design spec.
    Each input dict has weighted_holder_score, avg_tenure_score,
    avg_relative_size (these three get normalized across the universe), plus
    direction_agreement and data_quality_score (already 0-1).
    Returns {ticker: 'High'|'Medium'|'Low'}.
    """
    whs = _normalize({t: v["weighted_holder_score"] for t, v in universe.items()})
    ats = _normalize({t: v["avg_tenure_score"] for t, v in universe.items()})
    ars = _normalize({t: v["avg_relative_size"] for t, v in universe.items()})
    raw = {}
    for t, v in universe.items():
        raw[t] = (whs[t] * 0.30 + ats[t] * 0.25 + ars[t] * 0.20
                  + v["direction_agreement"] * 0.15 + v["data_quality_score"] * 0.10)
    # percentile bucket (recomputed over this universe)
    ordered = sorted(raw.values())
    n = len(ordered)
    flags = {}
    for t, r in raw.items():
        pr = (sum(1 for x in ordered if x < r)) / (n - 1) if n > 1 else 1.0
        flags[t] = "High" if pr >= 0.6667 else ("Medium" if pr >= 0.3333 else "Low")
    return flags
```

> Note: `confidence_flags` is the pure bucketing core. The full `compute_confidence` (Task 6) assembles the component dict from signals + holding_returns and writes `stock_confidence` with all columns; this task delivers and tests the normalization + bucketing math in isolation.

- [ ] **Step 4: Run the tests, expect PASS**

Run: `python3 -m pytest tests/test_stock_pipeline.py -k "normalize or confidence_buckets" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/scoring/stock_pipeline.py tests/test_stock_pipeline.py
git commit -m "feat(stock): confidence normalization + percentile bucketing"
```

---

### Task 6: Output assembly + orchestrator (`run_stock_pipeline`) + CLI

**Files:**
- Modify: `pipeline/scoring/stock_pipeline.py`
- Test: `tests/test_stock_pipeline.py`

- [ ] **Step 1: Write the failing end-to-end test**

Add to `tests/test_stock_pipeline.py`:

```python
def test_run_stock_pipeline_end_to_end(tmp_path):
    _db_, conn = _db(tmp_path)
    _rank(conn, "a", "Fund A", 1, 100.0)
    fa = _add_filing(conn, "a", "2024-12-31", "2025-02-10", "a1")
    _hold(conn, fa, "CX", 300); _hold(conn, fa, "CY", 100)
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('CX','XXX','X co'),('CY','YYY','Y co')")
    # 52wk prices for both (>=20 pts)
    for tk in ("XXX", "YYY"):
        pts = [(f"2024-12-{d:02d}", 20.0) for d in range(1, 26)]
        conn.executemany(f"INSERT INTO prices(ticker,date,close,adj_close) VALUES ('{tk}',?,?,?)",
                         [(d, p, p) for d, p in pts])
    # fundamentals (current quarter) + sectors
    conn.executemany("INSERT INTO fundamentals(ticker,as_of_date,market_cap,shares_out,pe_ratio,"
                     "pe_available,gross_margin_pct,source) VALUES (?,?,?,?,?,?,?,'finnhub')",
                     [("XXX", "2024-12-31", 1.0e9, 1e7, 20.0, 1, 40.0),
                      ("YYY", "2024-12-31", 2.0e9, 1e7, 15.0, 1, 30.0)])
    conn.execute("INSERT INTO sectors(ticker,sector) VALUES ('XXX','Tech'),('YYY','Energy')")
    # a couple of historical returns (below _MIN_TRAIN_ROWS -> fallback path)
    conn.executemany("INSERT INTO holding_returns(fund_id,quarter_date,ticker,position_value_usd,"
                     "three_yr_return,data_quality_flag) VALUES (?,?,?,?,?,?)",
                     [("a", "2020-12-31", "XXX", 100.0, 0.5, "clean")])
    conn.commit()

    summary = stock_pipeline.run_stock_pipeline(_db_)

    assert summary["universe"] == 2
    raw = {r["ticker"]: r for r in conn.execute(
        "SELECT ticker, rank, raw_score, sector_adjusted_score, confidence_flag, "
        "market_cap, holder_count, range_position, partial FROM stock_rankings_raw").fetchall()}
    assert set(raw) == {"XXX", "YYY"}
    assert raw["XXX"]["holder_count"] == 1
    assert raw["XXX"]["market_cap"] == 1.0e9
    assert {raw["XXX"]["rank"], raw["YYY"]["rank"]} == {1, 2}
    # confidence flag present and valid
    assert raw["XXX"]["confidence_flag"] in {"High", "Medium", "Low"}
    # stock_confidence populated
    assert conn.execute("SELECT COUNT(*) FROM stock_confidence").fetchone()[0] == 2
```

- [ ] **Step 2: Run the test, expect FAIL**

Run: `python3 -m pytest tests/test_stock_pipeline.py::test_run_stock_pipeline_end_to_end -v`
Expected: FAIL (`run_stock_pipeline` not defined).

- [ ] **Step 3: Implement assembly + orchestrator + CLI**

Add to `pipeline/scoring/stock_pipeline.py`:

```python
def _current_company_names(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, str]:
    out = {}
    for t in tickers:
        r = conn.execute(
            "SELECT name FROM securities WHERE ticker = ? AND name IS NOT NULL LIMIT 1", (t,)).fetchone()
        out[t] = r[0] if r else t
    return out


def _data_quality_for(conn: sqlite3.Connection, ticker: str, period: str,
                      qualifying: dict[str, float]) -> float:
    """Fraction of current-quarter holding_returns rows for this ticker (across
    qualifying funds) flagged 'clean'."""
    qs = ",".join("?" * len(qualifying))
    rows = conn.execute(
        f"SELECT data_quality_flag FROM holding_returns "
        f"WHERE ticker = ? AND quarter_date = ? AND fund_id IN ({qs})",
        (ticker, period, *qualifying.keys())).fetchall()
    if not rows:
        return 0.0
    clean = sum(1 for r in rows if r["data_quality_flag"] == "clean")
    return clean / len(rows)


def _truncate(conn: sqlite3.Connection) -> None:
    for t in ("stock_signals", "stock_confidence", "stock_rankings_raw",
              "stock_rankings_filtered"):
        conn.execute(f"DELETE FROM {t}")
    conn.commit()


def run_stock_pipeline(db_path: Path = DB_PATH) -> dict:
    """Run stages 1-6 and materialize the stock ranking tables (idempotent)."""
    conn = get_connection(db_path)
    try:
        adapter.init_schema(conn, db_path)
        _truncate(conn)
        cq = adapter.current_quarter_date(conn)
        qualifying = qualifying_funds(conn)
        if cq is None or not qualifying:
            return {"universe": 0, "ranked": 0}
        hist = fund_histories(conn, qualifying)
        sig = signals_for_period(conn, cq, qualifying, hist)
        universe = list(sig.keys())
        sector = {t: (conn.execute("SELECT sector FROM sectors WHERE ticker = ?", (t,)).fetchone() or ["Unknown"])[0]
                  for t in universe}

        # persist signals
        for t in universe:
            s = sig[t]
            conn.execute(
                "INSERT INTO stock_signals(ticker,as_of_date,fund_conviction,holder_count,"
                "net_change_pct,avg_relative_size,avg_tenure) VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(ticker,as_of_date) DO UPDATE SET "
                "fund_conviction=excluded.fund_conviction, holder_count=excluded.holder_count, "
                "net_change_pct=excluded.net_change_pct, avg_relative_size=excluded.avg_relative_size, "
                "avg_tenure=excluded.avg_tenure",
                (t, cq, s["fund_conviction"], s["holder_count"], s["net_change_pct"],
                 s["avg_relative_size"], s["avg_tenure"]))

        # regression (with fund_conviction fallback)
        feature_names, X, y = build_training_set(conn, qualifying, hist, sector)
        sector_names = [f.removeprefix("sector_") for f in feature_names if f.startswith("sector_")]
        pred_rows = {}
        for t in universe:
            rp, _ = range_position_52w(conn, t, cq)
            rp = 0.5 if rp is None else rp
            base = [float(sig[t]["holder_count"]), sig[t]["fund_conviction"],
                    sig[t]["avg_relative_size"], rp]
            onehot = [1.0 if sector[t] == s else 0.0 for s in sector_names]
            pred_rows[t] = base + onehot
        fallback = {t: sig[t]["fund_conviction"] for t in universe}
        raw_scores = regress_scores(feature_names, X, y, pred_rows, fallback=fallback)
        adj_scores = sector_adjust(raw_scores, sector)

        # confidence components
        comp = {}
        for t in universe:
            holders = sig[t]["holder_count"]
            whs = sum(qualifying[c] for c in qualifying
                      if t in hist[c].get(cq, {}))
            comp[t] = {
                "weighted_holder_score": whs,
                "avg_tenure_score": sig[t]["avg_tenure"],
                "avg_relative_size": sig[t]["avg_relative_size"],
                "direction_agreement": (abs(sig[t]["buyers"] - sig[t]["sellers"]) / holders
                                        if holders else 0.0),
                "data_quality_score": _data_quality_for(conn, t, cq, qualifying),
            }
        flags = confidence_flags(comp)
        whs_n = _normalize({t: comp[t]["weighted_holder_score"] for t in universe})
        ats_n = _normalize({t: comp[t]["avg_tenure_score"] for t in universe})
        ars_n = _normalize({t: comp[t]["avg_relative_size"] for t in universe})
        for t in universe:
            craw = (whs_n[t] * 0.30 + ats_n[t] * 0.25 + ars_n[t] * 0.20
                    + comp[t]["direction_agreement"] * 0.15 + comp[t]["data_quality_score"] * 0.10)
            conn.execute(
                "INSERT INTO stock_confidence(ticker,confidence_flag,confidence_raw,"
                "weighted_holder_score,avg_tenure_score,avg_relative_size,direction_agreement,"
                "data_quality_score,confidence_percentile) VALUES (?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(ticker) DO UPDATE SET confidence_flag=excluded.confidence_flag, "
                "confidence_raw=excluded.confidence_raw",
                (t, flags[t], craw, whs_n[t], ats_n[t], ars_n[t],
                 comp[t]["direction_agreement"], comp[t]["data_quality_score"], craw))

        # fundamentals + 52wk + assemble raw output, ranked by sector_adjusted_score desc
        names = _current_company_names(conn, universe)
        ranked = sorted(universe, key=lambda t: adj_scores[t], reverse=True)
        for rank, t in enumerate(ranked, start=1):
            f = conn.execute(
                "SELECT market_cap, pe_ratio, pe_available, gross_margin_pct "
                "FROM fundamentals WHERE ticker = ? AND as_of_date = ?", (t, cq)).fetchone()
            mc = f["market_cap"] if f else None
            rp, partial = range_position_52w(conn, t, cq)
            conn.execute(
                "INSERT INTO stock_rankings_raw(ticker,company_name,sector,rank,raw_score,"
                "sector_adjusted_score,confidence_flag,confidence_raw,holder_count,fund_conviction,"
                "net_change_pct,avg_relative_size,avg_tenure,market_cap,range_position,partial,"
                "pe_ratio,pe_available,gross_margin_pct) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(ticker) DO UPDATE SET rank=excluded.rank",
                (t, names[t], sector[t], rank, raw_scores[t], adj_scores[t], flags[t],
                 None, sig[t]["holder_count"], sig[t]["fund_conviction"], sig[t]["net_change_pct"],
                 sig[t]["avg_relative_size"], sig[t]["avg_tenure"], mc, rp, partial,
                 (f["pe_ratio"] if f else None), (f["pe_available"] if f else None),
                 (f["gross_margin_pct"] if f else None)))

        # filtered output: confidence != Low, 300M<=mktcap<=4B, 0.1<=range<=0.9, holders>=3
        frank = 0
        for t in ranked:
            row = conn.execute(
                "SELECT market_cap, range_position, holder_count, confidence_flag, sector, company_name, "
                "sector_adjusted_score FROM stock_rankings_raw WHERE ticker = ?", (t,)).fetchone()
            mc, rp = row["market_cap"], row["range_position"]
            if (row["confidence_flag"] != "Low" and mc is not None
                    and 300_000_000 <= mc <= 4_000_000_000
                    and rp is not None and 0.1 <= rp <= 0.9
                    and row["holder_count"] >= 3):
                frank += 1
                conn.execute(
                    "INSERT INTO stock_rankings_filtered(ticker,rank,company_name,sector,"
                    "sector_adjusted_score,confidence_flag,market_cap,range_position,holder_count) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (t, frank, row["company_name"], row["sector"], row["sector_adjusted_score"],
                     row["confidence_flag"], mc, rp, row["holder_count"]))
        conn.commit()
        return {"universe": len(universe), "ranked": len(universe), "filtered": frank}
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    ap = argparse.ArgumentParser(description="Run the stock ranking pipeline")
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()
    print(run_stock_pipeline(Path(args.db)))
```

- [ ] **Step 4: Run the end-to-end test, expect PASS**

Run: `python3 -m pytest tests/test_stock_pipeline.py::test_run_stock_pipeline_end_to_end -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `python3 -m pytest -q`
Expected: all green (edgar, prices, adapter, fund_pipeline, fundamentals, stock_pipeline).

- [ ] **Step 6: Smoke test against the real DB**

Run: `python3 -m pipeline.scoring.stock_pipeline`
Expected: prints `{'universe': N, 'ranked': N, 'filtered': K}` (N≈22 for Baupost's current holdings; `filtered` likely 0 until the price backfill fills 52wk ranges and more mid-caps qualify). Then:
`sqlite3 data/13f.db "SELECT ticker, rank, confidence_flag, holder_count, market_cap FROM stock_rankings_raw ORDER BY rank LIMIT 10;"`

> Reality note: with one qualifying fund and ~11 priced returns, the regression hits the fallback (`raw_score = fund_conviction`) and `filtered` may be empty (52wk ranges need the price backfill). That is correct behavior; report the actual numbers.

- [ ] **Step 7: Commit**

```bash
git add pipeline/scoring/stock_pipeline.py tests/test_stock_pipeline.py
git commit -m "feat(stock): output assembly + confidence + orchestrator + CLI"
```

---

## Self-review (completed by plan author)

**Spec coverage (design spec Phase 4):**
- Stage 1 universe (top-50% funds, ceil cutoff) → Task 1. ✓
- Stage 2 signals (fund_conviction, holder_count, net_change_pct, avg_relative_size, avg_tenure) → Task 2. ✓
- Stage 3 fundamental vars: 52wk_range_position + partial → Task 3; market_cap/PE/margin/sector pulled from Phase-3 tables in Task 6. ✓
- Stage 4 regression on 3yr_return + sector adjustment → Task 4 (features scoped to historically-consistent set + fallback; documented under Scope). ✓
- Stage 5 confidence (5 components, normalized, percentile-bucketed) → Tasks 5, 6. ✓
- Stage 6 outputs raw + filtered (all filter conditions) → Task 6. ✓
- Orchestration + CLI + truncate-rebuild idempotency → Task 6. ✓
- **Documented scope cut:** market_cap/PE/margin excluded from regression features (display/filter only) pending historical fundamentals (P3b); net_change/avg_tenure are signals/confidence inputs, not regression features. Recorded under Scope & decisions.

**Placeholder scan:** none — every step has runnable code/commands. (Task 3 Step 1 includes a corrected ZZZ seeding block to use; the superseded 3-point block is explicitly replaced before running.)

**Type/signature consistency:** `qualifying_funds(conn)->dict[str,float]`; `fund_histories(conn,qualifying)`; `signals_for_period(conn,period,qualifying,hist)->dict[ticker->{...,buyers,sellers}]`; `range_position_52w(conn,ticker,as_of)->(float|None,int)`; `regress_scores(feature_names,X,y,pred_rows,fallback)->dict`; `sector_adjust(raw,sector)->dict`; `build_training_set(...)->(names,X,y)`; `_normalize(dict)->dict`; `confidence_flags(universe)->dict`; `run_stock_pipeline(db_path)->{universe,ranked,filtered}`. Table column names match the Task-1 schema across all INSERTs. Reuses `adapter.latest_filing_id`/`current_quarter_date`/`init_schema` and `prices._plus_three_years` (existing). ✓
