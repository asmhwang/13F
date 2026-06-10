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


def stock_rankings(kind: str = "raw", conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """Raw or filtered stock rankings, best first. kind in {'raw','filtered'}."""
    assert kind in {"raw", "filtered"}, f"unknown rankings kind: {kind!r}"
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
            -- the effective filing set per (cik, latest period): base filing
            -- plus NEW HOLDINGS amendments; RESTATEMENT amendments replace the
            -- base (see database.rebuild_effective_filings).
            SELECT ef.filing_id AS id, ef.cik FROM effective_filings ef
            JOIN latest l ON l.cik = ef.cik AND l.period = ef.period_of_report
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


# ----------------------------- v2 query functions -----------------------------

def fund_rankings_v2(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """All v2-ranked funds, best first."""
    c = _conn(conn)
    return pd.read_sql(
        "SELECT * FROM fund_rankings_v2 WHERE eligible = 1 ORDER BY rank ASC", c
    )


def fund_clone_windows_v2(fund_id: str, conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """Valid clone windows for one fund, oldest first (for the detail chart)."""
    c = _conn(conn)
    return pd.read_sql(
        "SELECT start_period, end_period, clone_return, benchmark_return, "
        "excess_return, coverage FROM fund_clone_windows_v2 "
        "WHERE fund_id = ? AND valid = 1 ORDER BY start_period ASC",
        c, params=(fund_id,),
    )


def stock_rankings_v2(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """V2 stock rankings, best first."""
    c = _conn(conn)
    return pd.read_sql("SELECT * FROM stock_rankings_v2 ORDER BY rank ASC", c)


def stock_holders_v2(ticker: str, conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """V2-ranked funds holding `ticker`: latest-quarter weight + quarters held."""
    c = _conn(conn)
    return pd.read_sql(
        """
        WITH latest AS (
            SELECT f.cik, MAX(f.period_of_report) AS period
            FROM filings f GROUP BY f.cik
        ),
        latest_filing AS (
            SELECT ef.filing_id AS id, ef.cik FROM effective_filings ef
            JOIN latest l ON l.cik = ef.cik AND l.period = ef.period_of_report
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
        SELECT fr.fund_name, fr.rank, fr.score, fr.shrunk_ir_annual,
               (pos.pos_k * 1.0 / ft.total_k) AS weight,
               held.quarters_held
        FROM pos
        JOIN fund_rankings_v2 fr ON fr.fund_id = pos.cik AND fr.eligible = 1
        JOIN fund_total ft ON ft.cik = pos.cik
        JOIN held ON held.cik = pos.cik
        ORDER BY fr.rank ASC
        """,
        c, params=(ticker, ticker),
    )


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


@st.cache_data(ttl=300)
def load_fund_rankings_v2() -> pd.DataFrame:
    conn = get_connection()
    try:
        return fund_rankings_v2(conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_fund_clone_windows_v2(fund_id: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return fund_clone_windows_v2(fund_id, conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_stock_rankings_v2() -> pd.DataFrame:
    conn = get_connection()
    try:
        return stock_rankings_v2(conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_stock_holders_v2(ticker: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return stock_holders_v2(ticker, conn)
    finally:
        conn.close()
