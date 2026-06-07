"""Tests for pipeline.fundamentals (Phase 3 current-quarter fundamentals)."""
from unittest.mock import patch

from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter
from pipeline import fundamentals


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return db, conn


def test_fetch_profile_converts_millions(tmp_path):
    payload = {"name": "Apple Inc", "finnhubIndustry": "Technology",
               "marketCapitalization": 4514012.29, "shareOutstanding": 14687.36}
    with patch("pipeline.fundamentals._finnhub_get", return_value=payload):
        prof = fundamentals.fetch_profile("AAPL")
    assert prof["sector"] == "Technology"
    assert round(prof["market_cap"]) == round(4514012.29 * 1_000_000)
    assert round(prof["shares_out"]) == round(14687.36 * 1_000_000)


def test_fetch_profile_missing_fields(tmp_path):
    with patch("pipeline.fundamentals._finnhub_get", return_value={}):
        prof = fundamentals.fetch_profile("ZZZ")
    assert prof["sector"] is None
    assert prof["market_cap"] is None
    assert prof["shares_out"] is None


def test_fetch_metrics_pe_and_margin():
    payload = {"metric": {"peTTM": 36.83, "grossMarginTTM": 47.86}}
    with patch("pipeline.fundamentals._finnhub_get", return_value=payload):
        m = fundamentals.fetch_metrics("AAPL")
    assert round(m["pe_ratio"], 2) == 36.83
    assert m["pe_available"] == 1
    assert round(m["gross_margin_pct"], 2) == 47.86


def test_fetch_metrics_missing_pe_uses_zero():
    payload = {"metric": {"peTTM": None, "grossMarginTTM": None}}
    with patch("pipeline.fundamentals._finnhub_get", return_value=payload):
        m = fundamentals.fetch_metrics("ZZZ")
    assert m["pe_ratio"] == 0.0
    assert m["pe_available"] == 0
    assert m["gross_margin_pct"] is None


def test_fetch_metrics_negative_pe_unavailable():
    payload = {"metric": {"peTTM": -12.0, "grossMarginTTM": 10.0}}
    with patch("pipeline.fundamentals._finnhub_get", return_value=payload):
        m = fundamentals.fetch_metrics("LOSS")
    assert m["pe_ratio"] == 0.0
    assert m["pe_available"] == 0
