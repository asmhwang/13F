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
