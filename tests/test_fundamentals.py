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


def _add_filing(conn, cik, period, filed, acc):
    conn.execute("INSERT INTO filings(cik,accession_number,period_of_report,filed_date) "
                 "VALUES (?,?,?,?)", (cik, acc, period, filed))
    return conn.execute("SELECT id FROM filings WHERE accession_number=?", (acc,)).fetchone()[0]


def test_universe_tickers_current_quarter_ranked_funds(tmp_path):
    _db_, conn = _db(tmp_path)
    cq = adapter.current_quarter_date(conn)  # None when empty -> set explicitly below
    cq = "2024-12-31"
    # ranked fund 'r' holds AAA + BBB this quarter; unranked fund 'u' holds CCC
    conn.execute("INSERT INTO filers(cik,name) VALUES ('r','Ranked'),('u','Unranked')")
    conn.execute("INSERT INTO fund_rankings(fund_id,fund_name,rank,final_score,eligible) "
                 "VALUES ('r','Ranked',1,100.0,1)")
    fr = _add_filing(conn, "r", cq, "2025-02-10", "r1")
    fu = _add_filing(conn, "u", cq, "2025-02-10", "u1")
    for fid, c in [(fr, "CA"), (fr, "CB"), (fu, "CC"), (fr, "COPT")]:
        pc = "Call" if c == "COPT" else None
        conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                     "VALUES (?,?,?,?,?,?)", (fid, c, c, 100, 10, pc))
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES "
                 "('CA','AAA','A'),('CB','BBB','B'),('CC','CCC','C'),('COPT','OPT','O')")
    conn.commit()

    tickers = fundamentals.universe_tickers(conn)
    assert set(tickers) == {"AAA", "BBB"}   # ranked fund only; option excluded; unranked excluded
