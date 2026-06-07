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


def _sample_chart_payload():
    # epoch seconds for 2021-01-04 and 2021-01-05 (UTC)
    return {
        "chart": {
            "result": [
                {
                    "timestamp": [1609718400, 1609804800],
                    "indicators": {
                        "quote": [{"close": [100.0, None]}],
                        "adjclose": [{"adjclose": [99.0, 101.0]}],
                    },
                }
            ],
            "error": None,
        }
    }


def test_parse_chart_extracts_rows_and_skips_null_close():
    rows = prices.parse_chart(_sample_chart_payload())
    assert rows == [{"date": "2021-01-04", "close": 100.0, "adj_close": 99.0}]


def test_parse_chart_falls_back_to_close_when_adj_missing():
    payload = {
        "chart": {"result": [{
            "timestamp": [1609718400],
            "indicators": {"quote": [{"close": [50.0]}], "adjclose": [{}]},
        }]}
    }
    rows = prices.parse_chart(payload)
    assert rows == [{"date": "2021-01-04", "close": 50.0, "adj_close": 50.0}]


def test_parse_chart_empty_payload_returns_empty():
    assert prices.parse_chart({"chart": {"result": []}}) == []
