"""Tests for pipeline.prices (Phase 1 price + benchmark ingest)."""
from pathlib import Path

import pytest

from pipeline.database import get_connection
from pipeline import prices


def _tables(conn) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {r[0] for r in rows}


def test_init_schema_creates_tables(tmp_path):
    db = tmp_path / "t.db"
    conn = get_connection(db)
    prices.init_schema(conn, db)
    assert {"prices", "benchmark", "price_fetch_log"} <= _tables(conn)
