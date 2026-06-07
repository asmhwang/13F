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
