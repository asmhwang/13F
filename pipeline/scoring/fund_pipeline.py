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
