"""
Pre-built analytical queries against the 13F database.

All functions accept an optional sqlite3.Connection; if omitted they open
their own connection using the default DB_PATH.
"""

import math
import sqlite3
from pathlib import Path
from typing import Any

from pipeline.database import DB_PATH, get_connection


def _conn(conn: sqlite3.Connection | None, db_path: Path = DB_PATH) -> sqlite3.Connection:
    c = conn or get_connection(db_path)
    # Natural log used by conviction_scores. get_connection registers it, but a
    # caller-supplied raw sqlite3 connection would otherwise error (or silently
    # use SQLite's built-in base-10 log on 3.35+). Registration is idempotent.
    c.create_function("LOG", 1, math.log)
    return c


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
        WITH latest_filings AS (
            -- effective set per (cik, period): base filing + NEW HOLDINGS
            -- amendments (RESTATEMENT amendments replace the base)
            SELECT ef.filing_id AS id, ef.cik
            FROM effective_filings ef
            WHERE ef.period_of_report = ?
        )
        SELECT
            h.cusip,
            MAX(h.name_of_issuer)                    AS name_of_issuer,
            COUNT(DISTINCT lf.cik)                   AS num_filers,
            SUM(h.value_thousands)                   AS total_value_thousands,
            SUM(COALESCE(h.shares, 0))               AS total_shares
        FROM holdings h
        JOIN latest_filings lf ON lf.id = h.filing_id
        WHERE (h.put_call IS NULL OR h.put_call = '')
          AND h.value_thousands > 0
        GROUP BY h.cusip
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
        WITH old_latest AS (
            SELECT filing_id AS id FROM effective_filings
            WHERE cik = ? AND period_of_report = ?
        ),
        new_latest AS (
            SELECT filing_id AS id FROM effective_filings
            WHERE cik = ? AND period_of_report = ?
        ),
        old_h AS (
            SELECT h.cusip, MAX(h.name_of_issuer) AS name_of_issuer,
                   SUM(h.value_thousands) AS value_thousands, SUM(h.shares) AS shares
            FROM holdings h
            JOIN old_latest l ON h.filing_id = l.id
            WHERE (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
            GROUP BY h.cusip
        ),
        new_h AS (
            SELECT h.cusip, MAX(h.name_of_issuer) AS name_of_issuer,
                   SUM(h.value_thousands) AS value_thousands, SUM(h.shares) AS shares
            FROM holdings h
            JOIN new_latest l ON h.filing_id = l.id
            WHERE (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
            GROUP BY h.cusip
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
                WHEN n.value_thousands < o.value_thousands THEN 'decreased'
                ELSE 'unchanged'
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
            SELECT ef.filing_id AS id, ef.cik
            FROM effective_filings ef
            WHERE ef.period_of_report = ?
        ),
        -- Each filer's OWN most recent prior period. A single global prior
        -- period would make every position of a filer that skipped that exact
        -- period look freshly opened, inflating its buyer ratio.
        filer_prior AS (
            SELECT cik, MAX(period_of_report) AS period
            FROM effective_filings
            WHERE period_of_report < ?
            GROUP BY cik
        ),
        prior_filings AS (
            SELECT ef.filing_id AS id, ef.cik
            FROM effective_filings ef
            JOIN filer_prior fp ON fp.cik = ef.cik AND fp.period = ef.period_of_report
        ),
        filer_aum AS (
            SELECT lf.cik, SUM(h.value_thousands) AS total_aum
            FROM holdings h JOIN latest_filings lf ON lf.id = h.filing_id
            WHERE (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
            GROUP BY lf.cik
        ),
        prior_holdings AS (
            SELECT h.cusip, pf.cik, SUM(h.value_thousands) AS prior_value
            FROM holdings h
            JOIN prior_filings pf ON pf.id = h.filing_id
            WHERE (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
            GROUP BY h.cusip, pf.cik
        ),
        position_weights AS (
            SELECT
                h.cusip,
                COALESCE(s.name,   h.name_of_issuer) AS name_of_issuer,
                COALESCE(s.ticker, h.name_of_issuer) AS ticker,
                lf.cik,
                h.value_thousands,
                CAST(h.value_thousands AS REAL) / NULLIF(fa.total_aum, 0) * 100
                    AS portfolio_weight_pct
            FROM holdings h
            JOIN latest_filings lf ON lf.id = h.filing_id
            JOIN filer_aum      fa ON fa.cik = lf.cik
            LEFT JOIN securities s ON s.cusip = h.cusip
            WHERE (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
        ),
        buyer_flags AS (
            SELECT pw.cusip, pw.cik,
                -- NULL when this filer has no prior filing → COALESCE to 0.5
                CASE
                    WHEN fp.cik IS NULL                      THEN NULL
                    WHEN ph.prior_value IS NULL              THEN 1   -- new position
                    WHEN pw.value_thousands > ph.prior_value THEN 1   -- increased
                    ELSE 0
                END AS is_buyer
            FROM position_weights pw
            LEFT JOIN filer_prior fp ON fp.cik = pw.cik
            LEFT JOIN prior_holdings ph ON ph.cusip = pw.cusip AND ph.cik = pw.cik
        )
        SELECT
            pw.cusip,
            MAX(pw.ticker)                                       AS ticker,
            MAX(pw.name_of_issuer)                               AS name_of_issuer,
            COUNT(DISTINCT pw.cik)                               AS num_filers,
            SUM(pw.value_thousands)                              AS total_value_thousands,
            ROUND(AVG(pw.portfolio_weight_pct), 4)               AS avg_weight_pct,
            ROUND(AVG(COALESCE(bf.is_buyer, 0.5)), 4)            AS net_buyer_ratio,
            ROUND(
                COUNT(DISTINCT pw.cik)
                * LOG(1 + AVG(pw.portfolio_weight_pct))
                * (1 + AVG(COALESCE(bf.is_buyer, 0.5))),
            4)                                                   AS conviction_score
        FROM position_weights pw
        LEFT JOIN buyer_flags bf ON bf.cusip = pw.cusip AND bf.cik = pw.cik
        GROUP BY pw.cusip
        HAVING num_filers >= ?
        ORDER BY conviction_score DESC
        """,
        (period, period, min_filers),
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
        WITH latest AS (
            SELECT filing_id AS id FROM effective_filings
            WHERE cik = ? AND period_of_report = ?
        )
        SELECT
            COUNT(*)                   AS num_positions,
            SUM(v)                     AS total_aum_thousands,
            MAX(v)                     AS largest_position_thousands,
            COUNT(*)                   AS unique_cusips
        FROM (
            -- aggregate per CUSIP so SOLE/SHARED split rows count as one position
            SELECT SUM(h.value_thousands) AS v
            FROM holdings h
            JOIN latest l ON h.filing_id = l.id
            WHERE (h.put_call IS NULL OR h.put_call = '')
              AND h.value_thousands > 0
            GROUP BY h.cusip
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
