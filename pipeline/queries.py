"""
Pre-built analytical queries against the 13F database.

All functions accept an optional sqlite3.Connection; if omitted they open
their own connection using the default DB_PATH.
"""

import sqlite3
from pathlib import Path
from typing import Any

from pipeline.database import DB_PATH, get_connection


def _conn(conn: sqlite3.Connection | None, db_path: Path = DB_PATH) -> sqlite3.Connection:
    return conn or get_connection(db_path)


# ---------------------------------------------------------------------------
# Holdings snapshot
# ---------------------------------------------------------------------------

def top_holdings(
    period: str,
    top_n: int = 25,
    conn: sqlite3.Connection | None = None,
) -> list[sqlite3.Row]:
    """
    Largest positions (by aggregate market value) held across all filers
    in a given period (e.g. '2024-09-30').
    """
    c = _conn(conn)
    return c.execute(
        """
        SELECT
            h.cusip,
            h.name_of_issuer,
            COUNT(DISTINCT f.cik)                    AS num_filers,
            SUM(h.value_thousands)                   AS total_value_thousands,
            SUM(COALESCE(h.shares, 0))               AS total_shares
        FROM holdings h
        JOIN filings f ON f.id = h.filing_id
        WHERE f.period_of_report = ?
          AND (h.put_call IS NULL OR h.put_call = '')
          AND h.value_thousands > 0
          AND f.id = (
              SELECT f2.id FROM filings f2
              WHERE f2.cik = f.cik AND f2.period_of_report = f.period_of_report
              ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1
          )
        GROUP BY h.cusip, h.name_of_issuer
        ORDER BY total_value_thousands DESC
        LIMIT ?
        """,
        (period, top_n),
    ).fetchall()


# ---------------------------------------------------------------------------
# Position changes (quarter-over-quarter)
# ---------------------------------------------------------------------------

def position_changes(
    cik: str,
    period_new: str,
    period_old: str,
    conn: sqlite3.Connection | None = None,
) -> list[sqlite3.Row]:
    """
    Compare a filer's holdings between two periods.
    Returns rows with: cusip, name, old_value, new_value, pct_change, status
    status ∈ {increased, decreased, new, closed}
    """
    c = _conn(conn)
    return c.execute(
        """
        WITH old_h AS (
            SELECT h.cusip, h.name_of_issuer, h.value_thousands, h.shares
            FROM holdings h
            JOIN filings f ON f.id = h.filing_id
            WHERE f.cik = ? AND f.period_of_report = ?
              AND (h.put_call IS NULL OR h.put_call = '')
              AND h.value_thousands > 0
              AND f.id = (
                  SELECT f2.id FROM filings f2
                  WHERE f2.cik = f.cik AND f2.period_of_report = f.period_of_report
                  ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1
              )
        ),
        new_h AS (
            SELECT h.cusip, h.name_of_issuer, h.value_thousands, h.shares
            FROM holdings h
            JOIN filings f ON f.id = h.filing_id
            WHERE f.cik = ? AND f.period_of_report = ?
              AND (h.put_call IS NULL OR h.put_call = '')
              AND h.value_thousands > 0
              AND f.id = (
                  SELECT f2.id FROM filings f2
                  WHERE f2.cik = f.cik AND f2.period_of_report = f.period_of_report
                  ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1
              )
        )
        SELECT
            COALESCE(n.cusip, o.cusip)            AS cusip,
            COALESCE(n.name_of_issuer, o.name_of_issuer) AS name_of_issuer,
            o.value_thousands                     AS old_value_thousands,
            n.value_thousands                     AS new_value_thousands,
            CASE
                WHEN o.value_thousands > 0
                THEN ROUND(
                    (CAST(n.value_thousands AS REAL) - o.value_thousands)
                    / o.value_thousands * 100, 2)
                ELSE NULL
            END                                   AS pct_change,
            CASE
                WHEN o.cusip IS NULL              THEN 'new'
                WHEN n.cusip IS NULL              THEN 'closed'
                WHEN n.value_thousands > o.value_thousands THEN 'increased'
                ELSE 'decreased'
            END                                   AS status
        FROM new_h n
        FULL OUTER JOIN old_h o ON o.cusip = n.cusip
        ORDER BY ABS(COALESCE(n.value_thousands, 0) - COALESCE(o.value_thousands, 0)) DESC
        """,
        (cik, period_old, cik, period_new),
    ).fetchall()


# ---------------------------------------------------------------------------
# Conviction score
# ---------------------------------------------------------------------------

def conviction_scores(
    period: str,
    min_filers: int = 3,
    conn: sqlite3.Connection | None = None,
) -> list[sqlite3.Row]:
    """
    Compute a simple conviction score for each security in a given period.

    Score formula:
        conviction = num_filers_holding
                   * LOG(1 + avg_portfolio_weight_pct)
                   * (1 + net_buyer_ratio)

    Where:
        avg_portfolio_weight_pct = avg of (position_value / filer_total_aum * 100)
        net_buyer_ratio          = (# filers who increased/opened) / num_filers  [0-1]
          (requires prior period data; defaults to 0.5 if not available)
    """
    c = _conn(conn)
    return c.execute(
        """
        WITH latest_filings AS (
            SELECT f.id, f.cik FROM filings f
            WHERE f.period_of_report = ?
              AND f.id = (
                  SELECT f2.id FROM filings f2
                  WHERE f2.cik = f.cik AND f2.period_of_report = f.period_of_report
                  ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1
              )
        ),
        filer_aum AS (
            SELECT lf.cik, SUM(h.value_thousands) AS total_aum
            FROM holdings h JOIN latest_filings lf ON lf.id = h.filing_id
            WHERE (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
            GROUP BY lf.cik
        ),
        position_weights AS (
            SELECT
                h.cusip,
                h.name_of_issuer,
                lf.cik,
                h.value_thousands,
                CAST(h.value_thousands AS REAL) / NULLIF(fa.total_aum, 0) * 100
                    AS portfolio_weight_pct
            FROM holdings h
            JOIN latest_filings lf ON lf.id = h.filing_id
            JOIN filer_aum      fa ON fa.cik = lf.cik
            WHERE (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
        )
        SELECT
            cusip,
            name_of_issuer,
            COUNT(DISTINCT cik)                              AS num_filers,
            SUM(value_thousands)                             AS total_value_thousands,
            ROUND(AVG(portfolio_weight_pct), 4)              AS avg_weight_pct,
            ROUND(
                COUNT(DISTINCT cik)
                * LOG(1 + AVG(portfolio_weight_pct))
                * 1.0,   -- net_buyer_ratio placeholder = 1.0
            4)                                               AS conviction_score
        FROM position_weights
        GROUP BY cusip, name_of_issuer
        HAVING num_filers >= ?
        ORDER BY conviction_score DESC
        """,
        (period, min_filers),
    ).fetchall()


# ---------------------------------------------------------------------------
# Filer summary
# ---------------------------------------------------------------------------

def filer_summary(
    cik: str,
    period: str,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """Return high-level stats for a filer in a given period."""
    c = _conn(conn)
    row = c.execute(
        """
        SELECT
            COUNT(*)                   AS num_positions,
            SUM(value_thousands)       AS total_aum_thousands,
            MAX(value_thousands)       AS largest_position_thousands,
            COUNT(DISTINCT h.cusip)    AS unique_cusips
        FROM holdings h
        JOIN filings f ON f.id = h.filing_id
        WHERE f.cik = ? AND f.period_of_report = ?
          AND (h.put_call IS NULL OR h.put_call = '')
          AND h.value_thousands > 0
          AND f.id = (
              SELECT f2.id FROM filings f2
              WHERE f2.cik = f.cik AND f2.period_of_report = f.period_of_report
              ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1
          )
        """,
        (cik, period),
    ).fetchone()
    return dict(row) if row else {}


def available_periods(conn: sqlite3.Connection | None = None) -> list[str]:
    """List all period_of_report values in the database, newest first."""
    c = _conn(conn)
    rows = c.execute(
        "SELECT DISTINCT period_of_report FROM filings ORDER BY period_of_report DESC"
    ).fetchall()
    return [r[0] for r in rows]
