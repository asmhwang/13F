# 13F Rankings Website (P5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two Apple-flavored pages — Fund Rankings and Stock Rankings — to the existing Streamlit app, reading the P1–P4 result tables, with Emil-Kowalski-style CSS motion and graceful thin-data states.

**Architecture:** New isolated `webui/` package. `data.py` holds pure SQL→DataFrame query functions (optional `conn` param, like `pipeline/queries.py`) plus thin `@st.cache_data` wrappers. `components.py` holds pure formatting/filter helpers + Streamlit render helpers. `theme.py` injects scoped `.rk-*` CSS + motion. `fund_rankings.py` / `stock_rankings.py` compose the pages. `app.py` gains only two `st.radio` options + dispatch + one theme inject.

**Tech Stack:** Python, Streamlit ≥1.35 (`st.dialog`, `st.tabs`), pandas, Plotly, SQLite. Tests: pytest against a fixture DB built with `pipeline.database.init_db` + `pipeline.scoring.adapter.init_schema`.

**Spec:** `docs/superpowers/specs/2026-06-07-13f-rankings-website-design.md`

**Conventions (match existing repo):**
- Query functions live in `webui/data.py`, take `conn: sqlite3.Connection | None = None`, use a local `_conn` helper (mirror `pipeline/queries.py:15`).
- `@st.cache_data(ttl=300)` wrappers open `get_connection()`, call the pure fn, close, return (mirror `app.py:649`).
- `fund_id == cik`. Ticker comes from `securities.ticker` joined on `holdings.cusip`. Dollar value = `holdings.value_thousands * 1000`. Exclude options: `WHERE h.put_call IS NULL`.
- Tests build the DB with the fixture helpers shown in Task 1; reuse them across test files.

---

## File Structure

| File | Responsibility |
|---|---|
| `webui/__init__.py` | Package marker (empty). |
| `webui/data.py` | Pure query functions + `@st.cache_data` wrappers + meta. |
| `webui/components.py` | Pure helpers (format/color/filter/sort) + render helpers (hero, kpi_strip, score_bar, badge, ranking_list, open_modal). |
| `webui/theme.py` | `inject()` — scoped `.rk-*` CSS + motion keyframes. |
| `webui/fund_rankings.py` | `render_fund_rankings()`. |
| `webui/stock_rankings.py` | `render_stock_rankings()`. |
| `app.py` (modify) | Two `st.radio` options, `theme.inject()`, dispatch. |
| `refresh.sh` / `refresh.bat` (modify) | Append recalc chain. |
| `tests/test_webui_data.py` | Query-function tests. |
| `tests/test_webui_components.py` | Pure-helper tests. |

---

## Task 1: Package + first query (`fund_rankings`)

**Files:**
- Create: `webui/__init__.py`
- Create: `webui/data.py`
- Test: `tests/test_webui_data.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_webui_data.py`:

```python
"""Tests for the webui data layer (pure SQL->DataFrame query functions)."""
from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter
from webui import data


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return conn


def _rank(conn, cik, name, rank, score, **kw):
    conn.execute("INSERT OR IGNORE INTO filers(cik,name) VALUES (?,?)", (cik, name))
    cols = dict(fund_id=cik, fund_name=name, rank=rank, final_score=score, eligible=1)
    cols.update(kw)
    keys = ",".join(cols)
    qs = ",".join("?" * len(cols))
    conn.execute(f"INSERT INTO fund_rankings({keys}) VALUES ({qs})", tuple(cols.values()))


def test_fund_rankings_ordered_by_rank(tmp_path):
    conn = _db(tmp_path)
    _rank(conn, "b", "Bravo", 2, 80.0)
    _rank(conn, "a", "Alpha", 1, 100.0)
    conn.commit()
    df = data.fund_rankings(conn)
    assert list(df["fund_name"]) == ["Alpha", "Bravo"]
    assert df.iloc[0]["final_score"] == 100.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_webui_data.py::test_fund_rankings_ordered_by_rank -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'webui'`

- [ ] **Step 3: Create the package + implementation**

Create `webui/__init__.py` (empty file).

Create `webui/data.py`:

```python
"""Data layer for the rankings website.

Pure query functions take an optional connection (testable against a fixture DB,
mirroring pipeline/queries.py). The @st.cache_data wrappers below them are what
the Streamlit pages call.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from pipeline.database import DB_PATH, get_connection


def _conn(conn: sqlite3.Connection | None, db_path: Path = DB_PATH) -> sqlite3.Connection:
    return conn or get_connection(db_path)


# ----------------------------- pure query functions -----------------------------

def fund_rankings(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """All ranked funds, best first."""
    c = _conn(conn)
    return pd.read_sql(
        "SELECT * FROM fund_rankings WHERE eligible = 1 ORDER BY rank ASC", c
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_webui_data.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add webui/__init__.py webui/data.py tests/test_webui_data.py
git commit -m "feat(webui): data package + fund_rankings query"
```

---

## Task 2: Fund detail queries + rankings meta

**Files:**
- Modify: `webui/data.py`
- Test: `tests/test_webui_data.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_webui_data.py`:

```python
def _filing(conn, cik, period, filed, acc):
    conn.execute(
        "INSERT INTO filings(cik,accession_number,period_of_report,filed_date) VALUES (?,?,?,?)",
        (cik, acc, period, filed),
    )
    return conn.execute("SELECT id FROM filings WHERE accession_number=?", (acc,)).fetchone()[0]


def test_fund_quarterly_scores_time_ordered(tmp_path):
    conn = _db(tmp_path)
    conn.execute("INSERT INTO fund_quarterly_scores(fund_id,quarter_date,qps_raw,qps_excess,benchmark_return) "
                 "VALUES ('a','2021-03-31',0.2,0.05,0.15)")
    conn.execute("INSERT INTO fund_quarterly_scores(fund_id,quarter_date,qps_raw,qps_excess,benchmark_return) "
                 "VALUES ('a','2020-12-31',0.1,0.02,0.08)")
    conn.commit()
    df = data.fund_quarterly_scores("a", conn)
    assert list(df["quarter_date"]) == ["2020-12-31", "2021-03-31"]


def test_rankings_meta_latest_quarter(tmp_path):
    conn = _db(tmp_path)
    _filing(conn, "a", "2024-09-30", "2024-11-10", "x1")
    _filing(conn, "a", "2024-12-31", "2025-02-10", "x2")
    _rank(conn, "a", "Alpha", 1, 100.0)
    conn.commit()
    meta = data.rankings_meta(conn)
    assert meta["latest_quarter"] == "2024-12-31"
    assert meta["fund_count"] == 1
```

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m pytest tests/test_webui_data.py -k "quarterly_scores or rankings_meta" -v`
Expected: FAIL — `AttributeError: module 'webui.data' has no attribute 'fund_quarterly_scores'`

- [ ] **Step 3: Implement**

Append to `webui/data.py` (after `fund_rankings`):

```python
def fund_quarterly_scores(fund_id: str, conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """QPS time series for one fund, oldest quarter first (for the detail chart)."""
    c = _conn(conn)
    return pd.read_sql(
        "SELECT quarter_date, qps_raw, qps_excess, benchmark_return "
        "FROM fund_quarterly_scores WHERE fund_id = ? ORDER BY quarter_date ASC",
        c, params=(fund_id,),
    )


def fund_turnover(fund_id: str, conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """Turnover summary row for one fund."""
    c = _conn(conn)
    return pd.read_sql(
        "SELECT avg_turnover_rate, turnover_multiplier, quarter_pairs_measured "
        "FROM fund_turnover WHERE fund_id = ?",
        c, params=(fund_id,),
    )


def rankings_meta(conn: sqlite3.Connection | None = None) -> dict:
    """Latest filing quarter + headline counts for staleness labels."""
    c = _conn(conn)
    latest = c.execute("SELECT MAX(period_of_report) FROM filings").fetchone()[0]
    fund_count = c.execute(
        "SELECT COUNT(*) FROM fund_rankings WHERE eligible = 1"
    ).fetchone()[0]
    return {"latest_quarter": latest, "fund_count": fund_count}
```

- [ ] **Step 4: Run to verify pass**

Run: `python3 -m pytest tests/test_webui_data.py -v`
Expected: PASS (all)

- [ ] **Step 5: Commit**

```bash
git add webui/data.py tests/test_webui_data.py
git commit -m "feat(webui): fund detail queries + rankings meta"
```

---

## Task 3: Stock queries (`stock_rankings`, `stock_holders`)

**Files:**
- Modify: `webui/data.py`
- Test: `tests/test_webui_data.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_webui_data.py`:

```python
def _security(conn, cusip, ticker, name):
    conn.execute("INSERT OR IGNORE INTO securities(cusip,ticker,name) VALUES (?,?,?)",
                 (cusip, ticker, name))


def _hold(conn, filing_id, cusip, value_k):
    conn.execute(
        "INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
        "VALUES (?,?,?,?,?,NULL)",
        (filing_id, cusip, cusip, value_k, 100),
    )


def test_stock_rankings_raw_and_filtered(tmp_path):
    conn = _db(tmp_path)
    conn.execute("INSERT INTO stock_rankings_raw(ticker,company_name,sector,rank,raw_score,"
                 "sector_adjusted_score,confidence_flag,confidence_raw,holder_count,fund_conviction,"
                 "net_change_pct,avg_relative_size,avg_tenure,market_cap,range_position,partial,"
                 "pe_ratio,pe_available,gross_margin_pct) VALUES "
                 "('AAA','Alpha Co','Tech',1,0.9,0.4,'High',0.8,3,0.5,0.1,0.2,4,5e9,0.5,0,20,1,0.3)")
    conn.execute("INSERT INTO stock_rankings_filtered(ticker,rank,company_name,sector,"
                 "sector_adjusted_score,confidence_flag,market_cap,range_position,holder_count) "
                 "VALUES ('BBB',1,'Bravo Co','Health',0.6,'Medium',1e9,0.4,3)")
    conn.commit()
    raw = data.stock_rankings("raw", conn)
    filt = data.stock_rankings("filtered", conn)
    assert list(raw["ticker"]) == ["AAA"]
    assert list(filt["ticker"]) == ["BBB"]


def test_stock_holders_weight_and_quarters(tmp_path):
    conn = _db(tmp_path)
    _rank(conn, "a", "Alpha Fund", 1, 100.0)
    _security(conn, "C1", "AAA", "Alpha Co")
    _security(conn, "C2", "ZZZ", "Other Co")
    f1 = _filing(conn, "a", "2024-09-30", "2024-11-10", "a1")
    f2 = _filing(conn, "a", "2024-12-31", "2025-02-10", "a2")
    _hold(conn, f1, "C1", 250); _hold(conn, f1, "C2", 750)   # AAA = 25% of fund
    _hold(conn, f2, "C1", 250); _hold(conn, f2, "C2", 750)
    conn.commit()
    df = data.stock_holders("AAA", conn)
    assert list(df["fund_name"]) == ["Alpha Fund"]
    assert abs(df.iloc[0]["weight"] - 0.25) < 1e-6
    assert df.iloc[0]["quarters_held"] == 2
```

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m pytest tests/test_webui_data.py -k "stock_rankings or stock_holders" -v`
Expected: FAIL — `AttributeError: module 'webui.data' has no attribute 'stock_rankings'`

- [ ] **Step 3: Implement**

Append to `webui/data.py`:

```python
def stock_rankings(kind: str = "raw", conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """Raw or filtered stock rankings, best first. kind in {'raw','filtered'}."""
    table = "stock_rankings_filtered" if kind == "filtered" else "stock_rankings_raw"
    c = _conn(conn)
    return pd.read_sql(f"SELECT * FROM {table} ORDER BY rank ASC", c)


def stock_holders(ticker: str, conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """Ranked funds holding `ticker`: latest-quarter weight + quarters held.

    weight = position_value / fund's total portfolio value in the latest quarter.
    quarters_held = distinct quarters the fund reported this ticker (simplified tenure).
    """
    c = _conn(conn)
    return pd.read_sql(
        """
        WITH latest AS (
            SELECT f.cik, MAX(f.period_of_report) AS period
            FROM filings f GROUP BY f.cik
        ),
        latest_filing AS (
            SELECT f.id, f.cik FROM filings f
            JOIN latest l ON l.cik = f.cik AND l.period = f.period_of_report
        ),
        fund_total AS (
            SELECT lf.cik, SUM(h.value_thousands) AS total_k
            FROM holdings h JOIN latest_filing lf ON lf.id = h.filing_id
            WHERE h.put_call IS NULL GROUP BY lf.cik
        ),
        pos AS (
            SELECT lf.cik, SUM(h.value_thousands) AS pos_k
            FROM holdings h
            JOIN latest_filing lf ON lf.id = h.filing_id
            JOIN securities s ON s.cusip = h.cusip
            WHERE s.ticker = ? AND h.put_call IS NULL
            GROUP BY lf.cik
        ),
        held AS (
            SELECT f.cik, COUNT(DISTINCT f.period_of_report) AS quarters_held
            FROM filings f
            JOIN holdings h ON h.filing_id = f.id
            JOIN securities s ON s.cusip = h.cusip
            WHERE s.ticker = ? AND h.put_call IS NULL
            GROUP BY f.cik
        )
        SELECT fr.fund_name, fr.rank, fr.final_score,
               (pos.pos_k * 1.0 / ft.total_k) AS weight,
               held.quarters_held
        FROM pos
        JOIN fund_rankings fr ON fr.fund_id = pos.cik AND fr.eligible = 1
        JOIN fund_total ft ON ft.cik = pos.cik
        JOIN held ON held.cik = pos.cik
        ORDER BY fr.rank ASC
        """,
        c, params=(ticker, ticker),
    )
```

- [ ] **Step 4: Run to verify pass**

Run: `python3 -m pytest tests/test_webui_data.py -v`
Expected: PASS (all)

- [ ] **Step 5: Commit**

```bash
git add webui/data.py tests/test_webui_data.py
git commit -m "feat(webui): stock rankings + stock holders queries"
```

---

## Task 4: Pure presentation helpers + `@st.cache_data` wrappers

**Files:**
- Create: `webui/components.py`
- Modify: `webui/data.py`
- Test: `tests/test_webui_components.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_webui_components.py`:

```python
"""Tests for pure presentation helpers (no Streamlit calls)."""
import pandas as pd

from webui import components as c


def test_fmt_money_scales():
    assert c.fmt_money(5_300_000_000) == "$5.3B"
    assert c.fmt_money(420_000_000) == "$420.0M"
    assert c.fmt_money(0) == "$0"
    assert c.fmt_money(None) == "—"


def test_fmt_pct_sign():
    assert c.fmt_pct(0.123) == "+12.3%"
    assert c.fmt_pct(-0.05) == "-5.0%"
    assert c.fmt_pct(None) == "—"


def test_net_change_color():
    assert c.net_change_color(0.1) == c.BUY_GREEN
    assert c.net_change_color(-0.1) == c.SELL_RED
    assert c.net_change_color(0) == c.INK_SECONDARY


def test_confidence_color():
    assert c.confidence_color("High") == c.CONF_HIGH
    assert c.confidence_color("Low") == c.CONF_LOW
    assert c.confidence_color("anything-else") == c.INK_SECONDARY


def test_apply_filters_sort():
    df = pd.DataFrame({"sector": ["Tech", "Health", "Tech"],
                       "score": [3.0, 9.0, 5.0]})
    out = c.apply_filters_sort(df, {"sector": ["Tech"]}, sort_col="score", ascending=False)
    assert list(out["score"]) == [5.0, 3.0]
    assert set(out["sector"]) == {"Tech"}
```

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m pytest tests/test_webui_components.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'webui.components'`

- [ ] **Step 3: Implement the pure helpers**

Create `webui/components.py`:

```python
"""Presentation layer: pure formatting/color/filter helpers (top half, fully
testable) and Streamlit render helpers (bottom half, added in Task 6)."""
from __future__ import annotations

import pandas as pd

# --- palette (kept in sync with theme.py) ---
INK             = "#1d1d1f"
INK_SECONDARY   = "#6e6e73"
ACCENT          = "#0071e3"
BUY_GREEN       = "#34c759"
SELL_RED        = "#ff3b30"
CONF_HIGH       = "#34c759"
CONF_MEDIUM     = "#ff9f0a"
CONF_LOW        = "#8e8e93"


def fmt_money(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    v = float(v)
    if v == 0:
        return "$0"
    if abs(v) >= 1e9:
        return f"${v / 1e9:.1f}B"
    if abs(v) >= 1e6:
        return f"${v / 1e6:.1f}M"
    if abs(v) >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:.0f}"


def fmt_pct(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v * 100:+.1f}%"


def net_change_color(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == 0:
        return INK_SECONDARY
    return BUY_GREEN if v > 0 else SELL_RED


def confidence_color(flag: str) -> str:
    return {"High": CONF_HIGH, "Medium": CONF_MEDIUM, "Low": CONF_LOW}.get(
        flag, INK_SECONDARY
    )


def apply_filters_sort(df: pd.DataFrame, filters: dict, sort_col: str | None = None,
                       ascending: bool = False) -> pd.DataFrame:
    """Client-side filter + sort for ranking tables.

    filters: {column: [allowed values]} for categorical, or
             {column: (lo, hi)} tuple for numeric ranges.
    """
    out = df.copy()
    for col, cond in filters.items():
        if col not in out.columns or cond is None:
            continue
        if isinstance(cond, tuple) and len(cond) == 2:
            lo, hi = cond
            out = out[(out[col] >= lo) & (out[col] <= hi)]
        elif isinstance(cond, (list, set)) and len(cond) > 0:
            out = out[out[col].isin(list(cond))]
    if sort_col and sort_col in out.columns:
        out = out.sort_values(sort_col, ascending=ascending)
    return out.reset_index(drop=True)
```

- [ ] **Step 4: Run to verify pass**

Run: `python3 -m pytest tests/test_webui_components.py -v`
Expected: PASS

- [ ] **Step 5: Add the cached wrappers to data.py**

Append to `webui/data.py` (these are not unit-tested — they wrap the tested pure fns):

```python
# ----------------------------- streamlit cache wrappers -----------------------------

@st.cache_data(ttl=300)
def load_fund_rankings() -> pd.DataFrame:
    conn = get_connection()
    try:
        return fund_rankings(conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_fund_quarterly_scores(fund_id: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return fund_quarterly_scores(fund_id, conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_fund_turnover(fund_id: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return fund_turnover(fund_id, conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_stock_rankings(kind: str = "raw") -> pd.DataFrame:
    conn = get_connection()
    try:
        return stock_rankings(kind, conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_stock_holders(ticker: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return stock_holders(ticker, conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_rankings_meta() -> dict:
    conn = get_connection()
    try:
        return rankings_meta(conn)
    finally:
        conn.close()
```

- [ ] **Step 6: Run the full suite**

Run: `python3 -m pytest -q`
Expected: PASS (existing 55 + new webui tests)

- [ ] **Step 7: Commit**

```bash
git add webui/components.py webui/data.py tests/test_webui_components.py
git commit -m "feat(webui): pure presentation helpers + cached data wrappers"
```

---

## Task 5: Theme — Apple-flavored CSS + motion

**Files:**
- Create: `webui/theme.py`

This is presentation-only (no unit test). Verify by running the app (Step 3).

- [ ] **Step 1: Implement `webui/theme.py`**

```python
"""Apple-flavored design system + Emil-Kowalski-style motion for the rankings
pages. All classes namespaced `.rk-` so the existing views are untouched.
Injected once per run from app.py."""
import streamlit as st

_CSS = """
<style>
:root {
  --rk-bg:#f5f5f7; --rk-card:#fff; --rk-ink:#1d1d1f; --rk-ink2:#6e6e73;
  --rk-accent:#0071e3; --rk-buy:#34c759; --rk-sell:#ff3b30;
  --rk-hi:#34c759; --rk-med:#ff9f0a; --rk-lo:#8e8e93;
  --rk-radius:18px; --rk-ease:cubic-bezier(.16,1,.3,1);
  --rk-shadow:0 2px 14px rgba(0,0,0,.06); --rk-shadow-h:0 8px 28px rgba(0,0,0,.10);
}
.rk-wrap{font-family:-apple-system,"SF Pro Display",system-ui,sans-serif;color:var(--rk-ink);}
.rk-hero{padding:8px 0 4px;}
.rk-hero h1{font-size:56px;font-weight:700;letter-spacing:-.02em;margin:0;line-height:1.05;}
.rk-hero .sub{font-size:20px;color:var(--rk-ink2);margin:8px 0 0;font-weight:400;}
.rk-stale{font-size:13px;color:var(--rk-ink2);margin-top:10px;letter-spacing:.01em;}
.rk-kpis{display:flex;gap:16px;margin:28px 0 8px;flex-wrap:wrap;}
.rk-kpi{background:var(--rk-card);border-radius:var(--rk-radius);padding:20px 24px;
  min-width:160px;flex:1;box-shadow:var(--rk-shadow);transition:transform .15s var(--rk-ease),box-shadow .15s var(--rk-ease);}
.rk-kpi:hover{transform:translateY(-2px);box-shadow:var(--rk-shadow-h);}
.rk-kpi .v{font-size:30px;font-weight:600;letter-spacing:-.01em;}
.rk-kpi .l{font-size:13px;color:var(--rk-ink2);margin-top:4px;}
.rk-row{display:grid;align-items:center;background:var(--rk-card);border-radius:14px;
  padding:16px 22px;margin:8px 0;box-shadow:var(--rk-shadow);
  transition:transform .15s var(--rk-ease),box-shadow .15s var(--rk-ease);
  opacity:0;animation:rk-fade .5s var(--rk-ease) forwards;}
.rk-row:hover{transform:translateY(-2px);box-shadow:var(--rk-shadow-h);}
.rk-rank{font-size:22px;font-weight:700;color:var(--rk-ink2);width:44px;}
.rk-name{font-size:17px;font-weight:600;}
.rk-sub{font-size:13px;color:var(--rk-ink2);}
.rk-bar{height:6px;border-radius:3px;background:#e8e8ed;overflow:hidden;}
.rk-bar > i{display:block;height:100%;background:var(--rk-accent);border-radius:3px;
  width:0;animation:rk-grow .7s var(--rk-ease) forwards;}
.rk-badge{display:inline-block;font-size:12px;font-weight:600;padding:3px 10px;
  border-radius:999px;color:#fff;animation:rk-pop .3s var(--rk-ease);}
.rk-chip{display:inline-block;font-size:11px;font-weight:600;padding:2px 8px;
  border-radius:6px;background:#f0e7ff;color:#7a3cff;margin-left:8px;}
.rk-empty{background:var(--rk-card);border-radius:var(--rk-radius);padding:40px;
  text-align:center;color:var(--rk-ink2);box-shadow:var(--rk-shadow);}
@keyframes rk-fade{from{opacity:0;transform:translateY(8px);}to{opacity:1;transform:none;}}
@keyframes rk-grow{from{width:0;}}
@keyframes rk-pop{from{opacity:0;transform:scale(.96);}to{opacity:1;transform:none;}}
@media (prefers-reduced-motion: reduce){
  .rk-row,.rk-badge{animation:none !important;opacity:1 !important;}
  .rk-bar > i{animation:none !important;}
  .rk-kpi:hover,.rk-row:hover{transform:none;}
}
</style>
"""


def inject() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
```

- [ ] **Step 2: Smoke-import**

Run: `python3 -c "import webui.theme; print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add webui/theme.py
git commit -m "feat(webui): Apple-flavored theme + motion CSS"
```

---

## Task 6: Shared render helpers

**Files:**
- Modify: `webui/components.py`

Render helpers call `st.*` (not unit-tested). Verified when pages render in Task 9.

- [ ] **Step 1: Append render helpers to `webui/components.py`**

```python
# ----------------------------- streamlit render helpers -----------------------------
import html as _html

import streamlit as st


def hero(title: str, subtitle: str, staleness: str = "") -> None:
    stale = f'<div class="rk-stale">{_html.escape(staleness)}</div>' if staleness else ""
    st.markdown(
        f'<div class="rk-wrap"><div class="rk-hero"><h1>{_html.escape(title)}</h1>'
        f'<p class="sub">{_html.escape(subtitle)}</p>{stale}</div></div>',
        unsafe_allow_html=True,
    )


def kpi_strip(cards: list[tuple[str, str]]) -> None:
    """cards = [(value, label), ...]"""
    inner = "".join(
        f'<div class="rk-kpi"><div class="v">{_html.escape(str(v))}</div>'
        f'<div class="l">{_html.escape(str(l))}</div></div>'
        for v, l in cards
    )
    st.markdown(f'<div class="rk-wrap"><div class="rk-kpis">{inner}</div></div>',
                unsafe_allow_html=True)


def score_bar_html(score: float, max_score: float = 100.0) -> str:
    pct = max(0.0, min(100.0, (score / max_score) * 100.0)) if max_score else 0.0
    return (f'<div class="rk-bar"><i style="width:{pct:.0f}%"></i></div>')


def badge_html(text: str, color: str) -> str:
    return f'<span class="rk-badge" style="background:{color}">{_html.escape(text)}</span>'


def ranking_list(rows_html: list[str], stagger_ms: int = 50) -> None:
    """Render pre-built row HTML with staggered fade-in."""
    parts = []
    for i, r in enumerate(rows_html):
        delay = i * stagger_ms
        parts.append(r.replace("<div class=\"rk-row\"",
                               f'<div class="rk-row" style="animation-delay:{delay}ms"', 1))
    st.markdown(f'<div class="rk-wrap">{"".join(parts)}</div>', unsafe_allow_html=True)


def empty_card(message: str) -> None:
    st.markdown(f'<div class="rk-wrap"><div class="rk-empty">{_html.escape(message)}</div></div>',
                unsafe_allow_html=True)
```

- [ ] **Step 2: Smoke-import**

Run: `python3 -c "import webui.components; print('ok')"`
Expected: `ok`

- [ ] **Step 3: Run the full suite (helpers must not break pure-fn tests)**

Run: `python3 -m pytest tests/test_webui_components.py -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add webui/components.py
git commit -m "feat(webui): shared Streamlit render helpers (hero, kpi, list, badge)"
```

---

## Task 7: Fund Rankings page

**Files:**
- Create: `webui/fund_rankings.py`

- [ ] **Step 1: Implement `webui/fund_rankings.py`**

```python
"""Fund Rankings page: ranked funds + click-through detail (holdings, QPS chart)."""
from __future__ import annotations

import html as _html

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from webui import components as c
from webui import data


def _fund_row_html(r: pd.Series) -> str:
    score = float(r.get("final_score") or 0)
    chip = '<span class="rk-chip">one-hit wonder</span>' if r.get("one_hit_wonder_flag") else ""
    return (
        '<div class="rk-row" style="grid-template-columns:44px 2.4fr 1.4fr 1fr 1fr 1fr 1fr">'
        f'<div class="rk-rank">{int(r["rank"])}</div>'
        f'<div><div class="rk-name">{_html.escape(str(r["fund_name"]))}{chip}</div>'
        f'<div class="rk-sub">{c.fmt_money((r.get("avg_aum") or 0) * 1000)} avg AUM</div></div>'
        f'<div><div class="rk-name">{score:.0f}</div>{c.score_bar_html(score)}</div>'
        f'<div><div class="rk-sub">Positions</div><div>{int(r.get("avg_position_count") or 0)}</div></div>'
        f'<div><div class="rk-sub">Quarters</div><div>{int(r.get("quarters_of_data") or 0)}</div></div>'
        f'<div><div class="rk-sub">Turnover</div><div>{c.fmt_pct(r.get("avg_turnover_rate"))}</div></div>'
        f'<div><div class="rk-sub">TWS</div><div>{c.fmt_pct(r.get("tws_raw"))}</div></div>'
        '</div>'
    )


@st.dialog("Fund detail", width="large")
def _fund_detail(fund_id: str, fund_name: str) -> None:
    st.subheader(fund_name)
    qps = data.load_fund_quarterly_scores(fund_id)
    if not qps.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=qps["quarter_date"], y=qps["qps_excess"],
                                 mode="lines+markers", line=dict(color="#0071e3", width=2),
                                 name="Excess QPS"))
        fig.update_layout(height=260, margin=dict(l=10, r=10, t=10, b=10),
                          yaxis_tickformat=".0%", paper_bgcolor="rgba(0,0,0,0)",
                          plot_bgcolor="rgba(0,0,0,0)")
        st.caption("Historical excess QPS (3yr forward, vs S&P 500 TR)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("No scoreable quarters yet.")
    to = data.load_fund_turnover(fund_id)
    if not to.empty:
        st.caption(f"Avg turnover {c.fmt_pct(to.iloc[0]['avg_turnover_rate'])} · "
                   f"multiplier {to.iloc[0]['turnover_multiplier']:.2f} · "
                   f"{int(to.iloc[0]['quarter_pairs_measured'])} quarter pairs")


def render_fund_rankings() -> None:
    df = data.load_fund_rankings()
    meta = data.load_rankings_meta()
    stale = ""
    if meta.get("latest_quarter"):
        stale = f"Most recent quarter end: {meta['latest_quarter']} · {meta['fund_count']} eligible fund(s)"
    c.hero("Fund Rankings",
           "Small, concentrated funds ranked by long-term selection skill.", stale)

    if df.empty:
        c.empty_card("No eligible funds yet. Run the fund pipeline after ingesting more small filers.")
        return

    median = df["final_score"].median()
    top = df.iloc[0]["fund_name"]
    c.kpi_strip([
        (str(len(df)), "Eligible funds"),
        (top, "Top fund"),
        (f"{median:.0f}", "Median score"),
        (str(int(df["quarters_of_data"].max() or 0)), "Max quarters"),
    ])

    with st.container():
        col1, col2 = st.columns([3, 1])
        with col2:
            sort_col = st.selectbox("Sort by",
                                    ["rank", "final_score", "avg_aum", "avg_position_count"],
                                    index=0, key="fund_sort")
        with col1:
            score_rng = st.slider("Score range", 0, 100, (0, 100), key="fund_score_rng")
    view = c.apply_filters_sort(
        df, {"final_score": (score_rng[0], score_rng[1])},
        sort_col=sort_col, ascending=(sort_col == "rank"),
    )

    c.ranking_list([_fund_row_html(r) for _, r in view.iterrows()])

    options = {f'{int(r["rank"])} · {r["fund_name"]}': r["fund_id"] for _, r in view.iterrows()}
    pick = st.selectbox("Inspect a fund", ["—"] + list(options), key="fund_inspect")
    if pick != "—":
        _fund_detail(options[pick], pick.split(" · ", 1)[1])
```

- [ ] **Step 2: Smoke-import**

Run: `python3 -c "import webui.fund_rankings; print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add webui/fund_rankings.py
git commit -m "feat(webui): Fund Rankings page (list + detail modal + QPS chart)"
```

---

## Task 8: Stock Rankings page

**Files:**
- Create: `webui/stock_rankings.py`

- [ ] **Step 1: Implement `webui/stock_rankings.py`**

```python
"""Stock Rankings page: Raw/Filtered tabs + click-through detail (holders, fundamentals)."""
from __future__ import annotations

import html as _html

import pandas as pd
import streamlit as st

from webui import components as c
from webui import data


def _stock_row_html(r: pd.Series) -> str:
    flag = str(r.get("confidence_flag") or "")
    badge = c.badge_html(flag, c.confidence_color(flag)) if flag else ""
    nc = r.get("net_change_pct")
    nc_color = c.net_change_color(nc)
    arrow = "▲" if (nc or 0) > 0 else ("▼" if (nc or 0) < 0 else "·")
    score = r.get("sector_adjusted_score")
    return (
        '<div class="rk-row" style="grid-template-columns:44px 1.4fr 2fr 1.2fr 1fr 1fr 1.1fr 1fr">'
        f'<div class="rk-rank">{int(r["rank"])}</div>'
        f'<div class="rk-name">{_html.escape(str(r["ticker"]))}</div>'
        f'<div><div class="rk-sub">{_html.escape(str(r.get("company_name") or ""))}</div>'
        f'<div class="rk-sub">{_html.escape(str(r.get("sector") or ""))}</div></div>'
        f'<div>{badge}</div>'
        f'<div><div class="rk-sub">Score</div><div>{("—" if score is None else f"{score:.2f}")}</div></div>'
        f'<div><div class="rk-sub">Funds</div><div>{int(r.get("holder_count") or 0)}</div></div>'
        f'<div><div class="rk-sub">Net change</div>'
        f'<div style="color:{nc_color}">{arrow} {c.fmt_pct(nc)}</div></div>'
        f'<div><div class="rk-sub">Avg tenure</div>'
        f'<div>{("—" if r.get("avg_tenure") is None else f"{float(r.get("avg_tenure")):.1f}q")}</div></div>'
        '</div>'
    )


@st.dialog("Stock detail", width="large")
def _stock_detail(ticker: str, row: pd.Series) -> None:
    st.subheader(ticker)
    cols = st.columns(4)
    cols[0].metric("Market cap", c.fmt_money(row.get("market_cap")))
    cols[1].metric("P/E", "—" if not row.get("pe_available") else f'{float(row.get("pe_ratio") or 0):.1f}')
    rp = row.get("range_position")
    cols[2].metric("52wk range", "—" if rp is None else f"{float(rp) * 100:.0f}%")
    gm = row.get("gross_margin_pct")
    cols[3].metric("Gross margin", "—" if gm is None else f"{float(gm) * 100:.0f}%")
    st.markdown("**Held by qualifying funds**")
    holders = data.load_stock_holders(ticker)
    if holders.empty:
        st.caption("No qualifying-fund holdings recorded.")
    else:
        show = holders.assign(
            weight=lambda d: (d["weight"] * 100).round(1).astype(str) + "%",
        )[["rank", "fund_name", "weight", "quarters_held"]]
        st.dataframe(show, use_container_width=True, hide_index=True)


def _render_tab(df: pd.DataFrame, kind: str) -> None:
    if df.empty:
        if kind == "filtered":
            c.empty_card("No stocks meet the filtered criteria yet "
                         "(needs ≥3 holders + $300M–$4B market cap + populated fundamentals). "
                         "See the Raw tab.")
        else:
            c.empty_card("No ranked stocks yet. Run the stock pipeline after the fund pipeline.")
        return

    sectors = sorted([s for s in df.get("sector", pd.Series()).dropna().unique()])
    col1, col2 = st.columns(2)
    with col1:
        pick_sectors = st.multiselect("Sector", sectors, default=sectors, key=f"{kind}_sectors")
    with col2:
        confs = [x for x in ["High", "Medium", "Low"] if x in set(df.get("confidence_flag", []))]
        pick_conf = st.multiselect("Confidence", confs, default=confs, key=f"{kind}_conf")

    view = c.apply_filters_sort(
        df, {"sector": pick_sectors, "confidence_flag": pick_conf},
        sort_col="rank", ascending=True,
    )
    if view.empty:
        c.empty_card("No stocks match the current filters.")
        return

    c.ranking_list([_stock_row_html(r) for _, r in view.iterrows()])

    options = {f'{int(r["rank"])} · {r["ticker"]}': i for i, (_, r) in enumerate(view.iterrows())}
    pick = st.selectbox("Inspect a stock", ["—"] + list(options), key=f"{kind}_inspect")
    if pick != "—":
        row = view.iloc[options[pick]]
        _stock_detail(row["ticker"], row)


def render_stock_rankings() -> None:
    meta = data.load_rankings_meta()
    stale = "Positions reflect holdings at quarter end. Holdings may have changed since filing."
    if meta.get("latest_quarter"):
        stale = f"Most recent quarter end: {meta['latest_quarter']} · " + stale
    c.hero("Stock Rankings",
           "Stocks the top-ranked funds are most convicted on right now.", stale)

    raw = data.load_stock_rankings("raw")
    if not raw.empty:
        high = int((raw["confidence_flag"] == "High").sum())
        med_score = raw["sector_adjusted_score"].median()
        n_sectors = raw["sector"].nunique()
        c.kpi_strip([
            (str(len(raw)), "Universe size"),
            (str(high), "High confidence"),
            ("—" if pd.isna(med_score) else f"{med_score:.2f}", "Median score"),
            (str(n_sectors), "Sectors"),
        ])

    tab_raw, tab_filt = st.tabs(["Raw Rankings", "Filtered Rankings"])
    with tab_raw:
        _render_tab(raw, "raw")
    with tab_filt:
        _render_tab(data.load_stock_rankings("filtered"), "filtered")
```

- [ ] **Step 2: Smoke-import**

Run: `python3 -c "import webui.stock_rankings; print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add webui/stock_rankings.py
git commit -m "feat(webui): Stock Rankings page (raw/filtered tabs + detail modal)"
```

---

## Task 9: Wire pages into `app.py`

**Files:**
- Modify: `app.py` (imports near top; `st.radio` options ~line 791; theme inject; dispatch after the existing view branches)

- [ ] **Step 1: Add imports**

Find the existing pipeline imports block (around `app.py:22`) and add after it:

```python
from webui import theme as rk_theme
from webui.fund_rankings import render_fund_rankings
from webui.stock_rankings import render_stock_rankings
```

- [ ] **Step 2: Inject the theme once**

Find the existing `inject_css()` call (search for `inject_css()` invoked at module top, near `st.set_page_config`). Immediately after it, add:

```python
rk_theme.inject()
```

- [ ] **Step 3: Add the two views to the sidebar radio**

Modify `app.py:791-796` from:

```python
    view = st.radio(
        "view",
        ["Single Filer", "Cross-Filer Overview", "Conviction Scores"],
        index=0,
        label_visibility="collapsed",
    )
```

to:

```python
    view = st.radio(
        "view",
        ["Single Filer", "Cross-Filer Overview", "Conviction Scores",
         "Fund Rankings", "Stock Rankings"],
        index=0,
        label_visibility="collapsed",
    )
```

- [ ] **Step 4: Guard the sidebar filters for the new views**

The sidebar block at `app.py:798-819` builds period/filer selectors used by the
existing views. The ranking views need none of them. Wrap that filter block so it
only runs for the existing views. Change the start of the Filters block
(`app.py:798`, `st.markdown('<div class="sb-sec">Filters</div>'...)`) to:

```python
    RANKING_VIEWS = {"Fund Rankings", "Stock Rankings"}
    if view not in RANKING_VIEWS:
        st.markdown('<div class="sb-sec">Filters</div>', unsafe_allow_html=True)
```

and indent the existing `if view == "Single Filer": ... else: selected_period = ...`
block (through the `min_filers_filter` slider) one level so it sits inside that
`if view not in RANKING_VIEWS:` guard. (Leave the `Data` / `Add New Filer` sidebar
sections below it unchanged.)

- [ ] **Step 5: Add dispatch in the main body**

Find where the main body dispatches on `view` (search for `if view == "Single Filer"`
in the body, **not** the sidebar). At the end of that if/elif chain, add:

```python
elif view == "Fund Rankings":
    render_fund_rankings()
elif view == "Stock Rankings":
    render_stock_rankings()
```

- [ ] **Step 6: Verify the app boots**

Run: `python3 -c "import ast; ast.parse(open('app.py').read()); print('syntax ok')"`
Expected: `syntax ok`

Then launch and click both new views (manual):
Run: `streamlit run app.py` (then in the sidebar select Fund Rankings, then Stock Rankings)
Expected: Both pages render — Fund Rankings shows Baupost; Stock Rankings shows ~22 raw stocks, Filtered tab shows the empty-state card. No exceptions in the terminal.

- [ ] **Step 7: Commit**

```bash
git add app.py
git commit -m "feat(webui): wire Fund + Stock Rankings into app nav"
```

---

## Task 10: Refresh wiring

**Files:**
- Modify: `refresh.sh`, `refresh.bat`
- Modify: `app.py` (the `_run_refresh` function around line 602)

- [ ] **Step 1: Inspect the current refresh scripts and `_run_refresh`**

Run: `sed -n '602,630p' app.py; echo '--- sh ---'; cat refresh.sh; echo '--- bat ---'; cat refresh.bat`
Expected: see the current ingest/CUSIP steps so the new steps append in the same style.

- [ ] **Step 2: Append the recalc chain to `refresh.sh`**

After the existing CUSIP-resolution line in `refresh.sh`, append (match existing
`python3 -m ...` invocation style; keep `set -e` semantics if present):

```bash
python3 -m pipeline.prices
python3 -m pipeline.fundamentals
python3 -m pipeline.scoring.fund_pipeline
python3 -m pipeline.scoring.stock_pipeline
```

- [ ] **Step 3: Append the same chain to `refresh.bat`**

After the existing CUSIP-resolution line in `refresh.bat`:

```bat
python -m pipeline.prices
python -m pipeline.fundamentals
python -m pipeline.scoring.fund_pipeline
python -m pipeline.scoring.stock_pipeline
```

- [ ] **Step 4: Mirror the chain in the in-app `_run_refresh`**

In `app.py`'s `_run_refresh` (around line 602), after the existing ingest + CUSIP
subprocess calls, add the same four module runs in order using the same
`subprocess.run([sys.executable, "-m", "<module>"], ...)` pattern already used there:

```python
    for mod in ("pipeline.prices", "pipeline.fundamentals",
                "pipeline.scoring.fund_pipeline", "pipeline.scoring.stock_pipeline"):
        subprocess.run([sys.executable, "-m", mod], check=True)
```

(Place it inside the existing try/except that sets `_refresh_status["error"]` so a
failure surfaces in the sidebar, matching the current behavior.)

- [ ] **Step 5: Syntax check**

Run: `python3 -c "import ast; ast.parse(open('app.py').read()); print('ok')"`
Expected: `ok`

- [ ] **Step 6: Commit**

```bash
git add refresh.sh refresh.bat app.py
git commit -m "feat(webui): wire ranking recalc chain into refresh"
```

---

## Task 11: Polish pass (design skills) + final verification

**Files:**
- Modify: `webui/theme.py`, `webui/components.py` (as polish dictates)

- [ ] **Step 1: Run the suite green**

Run: `python3 -m pytest -q`
Expected: PASS (55 existing + new webui tests).

- [ ] **Step 2: Apply the design skills**

Invoke, in order, on the rendered pages (screenshot the running app for each):
1. `emil-design-eng` — verify motion is purposeful, fast, eased; tune durations/stagger.
2. `impeccable` — verify spacing rhythm, hierarchy, alignment; adjust tokens.
3. `taste-skill` — pick the strongest direction; apply final refinements.

Record concrete adjustments as small edits to `theme.py` / `components.py`. Keep all
classes `.rk-` scoped; do not touch existing views.

- [ ] **Step 3: Accessibility / reduced-motion check**

Confirm the `prefers-reduced-motion` block disables transforms/animations (toggle OS
setting or DevTools emulation). Confirm text contrast on `#f5f5f7` meets AA.

- [ ] **Step 4: Manual screenshot verification**

Run: `streamlit run app.py`
Verify (screenshot each):
- Fund Rankings: hero + staleness, KPI strip, Baupost row with animated score bar, hover lift, Inspect → modal with QPS chart.
- Stock Rankings: hero, KPI strip, Raw tab ~22 rows with confidence badges + net-change color, Filtered tab empty-state card, Inspect → modal with holders + fundamentals.

- [ ] **Step 5: Final commit + push**

```bash
git add webui/theme.py webui/components.py
git commit -m "polish(webui): motion + spacing pass (emil/impeccable/taste)"
git push origin main
```

---

## Self-Review (against the spec)

**Spec coverage:**
- Architecture / `webui/` package → Tasks 1,4,5,6,7,8.
- Apple design system + scoped CSS → Task 5.
- Emil motion (stagger, hover, score bar, reduced-motion) → Task 5, tuned Task 11.
- Fund page (hero, staleness, KPI, filters, list, score tooltip, detail modal, QPS chart) → Task 7. *(Score tooltip = the per-component sub-labels shown in the detail; if a hover tooltip on the score is wanted it can be added as a `title=` attr in `_fund_row_html`.)*
- Stock page (hero, disclaimer, raw/filtered tabs, KPI, filters, badges, net-change color, detail modal, holders, fundamentals, filtered empty state) → Task 8.
- Interaction pattern (HTML list + selectbox → `st.dialog`) → Tasks 7,8.
- Data layer (all loaders + `stock_holders`) → Tasks 1–4.
- Refresh wiring → Task 10.
- Thin-data/empty states → Tasks 7,8 (empty_card).
- Testing (data + pure helpers, suite green) → Tasks 1–4,6,11.

**Placeholder scan:** none — every code step is complete.

**Type consistency:** `data.fund_rankings/fund_quarterly_scores/fund_turnover/stock_rankings/stock_holders/rankings_meta` names match between Tasks 1–4 and their callers in Tasks 7–8. `components` palette constants + `fmt_money/fmt_pct/net_change_color/confidence_color/apply_filters_sort/hero/kpi_strip/score_bar_html/badge_html/ranking_list/empty_card` names match between Tasks 4/6 and Tasks 7/8.

**Note for executor:** exact `app.py` line numbers (radio at ~791, dispatch chain, `_run_refresh` ~602) may drift — search for the quoted anchor strings rather than trusting the numbers.
