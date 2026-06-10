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


def effective_filing_ids(conn: sqlite3.Connection, cik: str, period: str) -> list[int]:
    """Filing ids that together represent this (cik, period)'s holdings:
    the base filing plus any NEW HOLDINGS amendments (see
    database.rebuild_effective_filings). Falls back to the latest filing when
    effective_filings hasn't been built (e.g. minimal test fixtures)."""
    try:
        rows = conn.execute(
            "SELECT filing_id FROM effective_filings "
            "WHERE cik = ? AND period_of_report = ?",
            (cik, period),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    if rows:
        return [r[0] for r in rows]
    lf = latest_filing_id(conn, cik, period)
    return [lf] if lf is not None else []


def original_filed_date(conn: sqlite3.Connection, cik: str, period: str) -> str | None:
    """First public disclosure date for a (cik, period): the original 13F-HR's
    filed_date — NOT the latest amendment's, which can be years later and would
    shift the forward-return window to the wrong era."""
    try:
        row = conn.execute(
            "SELECT original_filed_date FROM effective_filings "
            "WHERE cik = ? AND period_of_report = ? LIMIT 1",
            (cik, period),
        ).fetchone()
        if row:
            return row[0]
    except sqlite3.OperationalError:
        pass
    row = conn.execute(
        """
        SELECT COALESCE(
            (SELECT MIN(filed_date) FROM filings
             WHERE cik = :cik AND period_of_report = :p
               AND report_type NOT LIKE '%/A%'),
            (SELECT MIN(filed_date) FROM filings
             WHERE cik = :cik AND period_of_report = :p)
        )
        """,
        {"cik": cik, "p": period},
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
