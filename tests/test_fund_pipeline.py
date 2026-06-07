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


def _resolve(conn, cusip, ticker):
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES (?,?,?)",
                 (cusip, ticker, cusip))


def test_compute_holding_returns_flags(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('f1','F1')")
    conn.execute("INSERT INTO fund_eligibility(fund_id,eligible,fail_reason) "
                 "VALUES ('f1',1,NULL)")
    fid = _add_filing(conn, "f1", "2018-03-31", "2018-05-10", "f1a")
    _add_holding(conn, fid, "CA", 100)     # AAA  -> clean
    _add_holding(conn, fid, "CB", 20)      # BBB  -> last_price
    _add_holding(conn, fid, "CD", 30)      # DDD  -> resolved but no prices -> null_excluded
    _add_holding(conn, fid, "CU", 50)      # unresolved cusip -> cusip_unresolved
    _add_holding(conn, fid, "CO", 99, put_call="Call")  # option -> ignored
    _resolve(conn, "CA", "AAA"); _resolve(conn, "CB", "BBB"); _resolve(conn, "CD", "DDD")
    conn.executemany(
        "INSERT INTO prices(ticker,date,close,adj_close) VALUES (?,?,?,?)",
        [("AAA", "2018-05-10", 50, 50.0), ("AAA", "2021-05-10", 75, 75.0),
         ("BBB", "2018-05-10", 20, 20.0), ("BBB", "2021-01-10", 10, 10.0)])
    conn.commit()

    fund_pipeline.compute_holding_returns(conn)

    rows = {r["ticker"]: r for r in conn.execute(
        "SELECT ticker, position_value_usd, three_yr_return, data_quality_flag "
        "FROM holding_returns").fetchall()}
    assert set(rows) == {"AAA", "BBB", "DDD", "CU"}        # option excluded
    assert rows["AAA"]["position_value_usd"] == 100000.0
    assert round(rows["AAA"]["three_yr_return"], 4) == 0.5
    assert rows["AAA"]["data_quality_flag"] == "clean"
    assert rows["BBB"]["data_quality_flag"] == "last_price"
    assert rows["DDD"]["three_yr_return"] is None
    assert rows["DDD"]["data_quality_flag"] == "null_excluded"
    assert rows["CU"]["data_quality_flag"] == "cusip_unresolved"


def test_compute_holding_returns_skips_unscoreable_quarter(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('f1','F1')")
    conn.execute("INSERT INTO fund_eligibility(fund_id,eligible,fail_reason) "
                 "VALUES ('f1',1,NULL)")
    # filed_date + 3yr is in the future -> not scoreable yet
    future = date.today().isoformat()
    fid = _add_filing(conn, "f1", future, future, "f1now")
    _add_holding(conn, fid, "CA", 100)
    _resolve(conn, "CA", "AAA")
    conn.commit()
    fund_pipeline.compute_holding_returns(conn)
    assert conn.execute("SELECT COUNT(*) FROM holding_returns").fetchone()[0] == 0


def test_compute_qps_value_weighted_excess(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('f1','F1')")
    _add_filing(conn, "f1", "2019-03-31", "2019-05-15", "f1q")
    # holding_returns seeded directly (isolating stage 3)
    conn.executemany(
        "INSERT INTO holding_returns(fund_id,quarter_date,ticker,position_value_usd,"
        "three_yr_return,data_quality_flag) VALUES (?,?,?,?,?,?)",
        [("f1", "2019-03-31", "A", 600.0, 0.10, "clean"),
         ("f1", "2019-03-31", "B", 400.0, -0.05, "clean"),
         ("f1", "2019-03-31", "C", 1000.0, None, "null_excluded")])
    conn.executemany("INSERT INTO benchmark(date,adj_close) VALUES (?,?)",
                     [("2019-05-15", 100.0), ("2022-05-15", 110.0)])
    conn.commit()

    fund_pipeline.compute_qps(conn)

    row = conn.execute(
        "SELECT qps_raw, benchmark_return, qps_excess, positions_included, "
        "positions_excluded_null FROM fund_quarterly_scores "
        "WHERE fund_id='f1' AND quarter_date='2019-03-31'").fetchone()
    assert round(row["qps_raw"], 4) == 0.04        # .6*.10 + .4*(-.05)
    assert round(row["benchmark_return"], 4) == 0.10
    assert round(row["qps_excess"], 4) == -0.06
    assert row["positions_included"] == 2
    assert row["positions_excluded_null"] == 1


def _seed_scores(conn, cik, excesses, start_year=2016):
    """Seed fund_quarterly_scores: excesses[0] is the OLDEST quarter."""
    conn.execute(f"INSERT INTO filers(cik,name) VALUES ('{cik}','{cik}')")
    conn.execute(f"INSERT INTO fund_eligibility(fund_id,eligible,fail_reason) "
                 f"VALUES ('{cik}',1,NULL)")
    for i, ex in enumerate(excesses):
        q = f"{start_year + i}-03-31"
        conn.execute(
            "INSERT INTO fund_quarterly_scores(fund_id,quarter_date,qps_raw,"
            "qps_excess,benchmark_return,positions_included,positions_excluded_null)"
            " VALUES (?,?,?,?,?,?,?)", (cik, q, ex, ex, 0.0, 1, 0))


def test_compute_tws_weighted_no_ohw(tmp_path):
    _db_, conn = _db(tmp_path)
    _seed_scores(conn, "f1", [0.10] * 6)     # 6 equal quarters
    conn.commit()
    fund_pipeline.compute_tws(conn)
    row = conn.execute("SELECT tws, quarters_scored, oldest_quarter_included, "
                       "one_hit_wonder_flag, best_quarter_contribution "
                       "FROM fund_tws WHERE fund_id='f1'").fetchone()
    assert round(row["tws"], 6) == 0.10
    assert row["quarters_scored"] == 6
    assert row["oldest_quarter_included"] == "2016-03-31"
    assert row["one_hit_wonder_flag"] == 0
    assert round(row["best_quarter_contribution"], 4) == 0.2408  # 1/sum(0.85^0..5)


def test_compute_tws_one_hit_wonder_discount(tmp_path):
    _db_, conn = _db(tmp_path)
    # one huge quarter dominates -> best contribution > 50% -> ×0.75
    _seed_scores(conn, "f1", [0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
    conn.commit()
    fund_pipeline.compute_tws(conn)
    row = conn.execute("SELECT tws, one_hit_wonder_flag, best_quarter_contribution "
                       "FROM fund_tws WHERE fund_id='f1'").fetchone()
    assert row["one_hit_wonder_flag"] == 1
    assert round(row["best_quarter_contribution"], 4) == 1.0     # only nonzero quarter
    # raw tws = (1.0*1.0)/sum(w) = 1/4.1498 ≈ 0.24083; discounted ×0.75
    assert round(row["tws"], 5) == round(0.24083 * 0.75, 5)


def test_compute_tws_marks_insufficient(tmp_path):
    _db_, conn = _db(tmp_path)
    _seed_scores(conn, "f1", [0.1] * 5)      # only 5 < 6
    conn.commit()
    fund_pipeline.compute_tws(conn)
    assert conn.execute("SELECT COUNT(*) FROM fund_tws WHERE fund_id='f1'").fetchone()[0] == 0
    row = conn.execute("SELECT eligible, fail_reason FROM fund_eligibility "
                       "WHERE fund_id='f1'").fetchone()
    assert row["eligible"] == 0
    assert row["fail_reason"] == "insufficient_scoreable_quarters"


def test_compute_turnover_mean_and_multiplier(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.execute("INSERT INTO filers(cik,name) VALUES ('f1','F1')")
    conn.execute("INSERT INTO fund_tws(fund_id,tws,quarters_scored,"
                 "oldest_quarter_included,one_hit_wonder_flag,best_quarter_contribution)"
                 " VALUES ('f1',0.1,6,'2016-03-31',0,0.2)")
    q1 = _add_filing(conn, "f1", "2020-03-31", "2020-05-10", "q1")
    q2 = _add_filing(conn, "f1", "2020-06-30", "2020-08-10", "q2")
    q3 = _add_filing(conn, "f1", "2020-09-30", "2020-11-10", "q3")
    for c in ("CA", "CB", "CC"):
        _add_holding(conn, q1, c, 10)
    for c in ("CB", "CC", "CD"):            # dropped CA, added CD -> 1/3 turnover
        _add_holding(conn, q2, c, 10)
    for c in ("CB", "CC", "CD"):            # no change -> 0 turnover
        _add_holding(conn, q3, c, 10)
    conn.commit()

    fund_pipeline.compute_turnover(conn)

    row = conn.execute("SELECT avg_turnover_rate, turnover_multiplier, "
                       "quarter_pairs_measured FROM fund_turnover "
                       "WHERE fund_id='f1'").fetchone()
    assert round(row["avg_turnover_rate"], 4) == 0.1667    # mean(1/3, 0)
    assert round(row["turnover_multiplier"], 4) == 0.9167  # 1 - 0.1667*0.5
    assert row["quarter_pairs_measured"] == 2
