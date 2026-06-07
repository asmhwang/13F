"""Per-stage + end-to-end tests for the fund ranking pipeline."""
from datetime import date

from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter, fund_pipeline


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return db, conn


def _three_years_later(d: str) -> str:
    from pipeline.prices import _plus_three_years
    return _plus_three_years(d)


def _add_filing(conn, cik, period, filed, acc):
    conn.execute(
        "INSERT INTO filings(cik,accession_number,period_of_report,filed_date) "
        "VALUES (?,?,?,?)", (cik, acc, period, filed))
    return conn.execute("SELECT id FROM filings WHERE accession_number=?", (acc,)).fetchone()[0]


def _add_holding(conn, fid, cusip, value_k, put_call=None):
    conn.execute(
        "INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
        "VALUES (?,?,?,?,?,?)", (fid, cusip, cusip, value_k, 10, put_call))


def test_weed_funds_flags_each_reason(tmp_path):
    _db_, conn = _db(tmp_path)
    cq = "2024-12-31"
    # GOOD: small, concentrated, long history, filed current quarter
    conn.execute("INSERT INTO filers(cik,name) VALUES ('good','Good Fund')")
    _add_filing(conn, "good", "2015-03-31", "2015-05-10", "g_old")
    fg = _add_filing(conn, "good", cq, "2025-02-10", "g_now")
    _add_holding(conn, fg, "CA", 50)       # $50k
    _add_holding(conn, fg, "CB", 60)
    # BIG: a single position over $100M (100000 thousand)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('big','Big Fund')")
    _add_filing(conn, "big", "2015-03-31", "2015-05-10", "b_old")
    fb = _add_filing(conn, "big", cq, "2025-02-10", "b_now")
    _add_holding(conn, fb, "CA", 200000)   # $200M
    # MANY: more than 30 positions
    conn.execute("INSERT INTO filers(cik,name) VALUES ('many','Many Fund')")
    _add_filing(conn, "many", "2015-03-31", "2015-05-10", "m_old")
    fm = _add_filing(conn, "many", cq, "2025-02-10", "m_now")
    for i in range(31):
        _add_holding(conn, fm, f"C{i:02d}", 10)
    # YOUNG: less than 5 years of history
    conn.execute("INSERT INTO filers(cik,name) VALUES ('young','Young Fund')")
    fy = _add_filing(conn, "young", cq, "2025-02-10", "y_now")
    _add_holding(conn, fy, "CA", 10)
    # GONE: did not file the current quarter
    conn.execute("INSERT INTO filers(cik,name) VALUES ('gone','Gone Fund')")
    _add_filing(conn, "gone", "2015-03-31", "2015-05-10", "x_old")
    conn.commit()

    fund_pipeline.weed_funds(conn)

    res = dict(conn.execute(
        "SELECT fund_id, fail_reason FROM fund_eligibility").fetchall())
    assert res["good"] is None
    assert res["big"] == "position_too_large"
    assert res["many"] == "too_many_positions"
    assert res["young"] == "insufficient_history"
    assert res["gone"] == "inactive"
    elig = dict(conn.execute(
        "SELECT fund_id, eligible FROM fund_eligibility").fetchall())
    assert elig["good"] == 1 and elig["big"] == 0
