"""Per-stage + end-to-end tests for the stock ranking pipeline."""
from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter, stock_pipeline


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return db, conn


def _add_filing(conn, cik, period, filed, acc):
    conn.execute("INSERT INTO filings(cik,accession_number,period_of_report,filed_date) "
                 "VALUES (?,?,?,?)", (cik, acc, period, filed))
    return conn.execute("SELECT id FROM filings WHERE accession_number=?", (acc,)).fetchone()[0]


def _hold(conn, fid, cusip, value_k, put_call=None):
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?,?,?,?,?,?)", (fid, cusip, cusip, value_k, 10, put_call))


def _rank(conn, cik, name, rank, score):
    conn.execute("INSERT INTO filers(cik,name) VALUES (?,?)", (cik, name))
    conn.execute("INSERT INTO fund_rankings(fund_id,fund_name,rank,final_score,eligible) "
                 "VALUES (?,?,?,?,1)", (cik, name, rank, score))


def test_qualifying_funds_ceil_cutoff(tmp_path):
    _db_, conn = _db(tmp_path)
    _rank(conn, "a", "A", 1, 100.0)
    _rank(conn, "b", "B", 2, 50.0)
    _rank(conn, "c", "C", 3, 10.0)
    conn.commit()
    q = stock_pipeline.qualifying_funds(conn)   # ceil(3/2)=2 -> ranks 1,2
    assert set(q) == {"a", "b"}
    assert q["a"] == 100.0


def test_qualifying_funds_single(tmp_path):
    _db_, conn = _db(tmp_path)
    _rank(conn, "a", "A", 1, 100.0)
    conn.commit()
    assert set(stock_pipeline.qualifying_funds(conn)) == {"a"}   # ceil(1/2)=1


def test_signals_for_period(tmp_path):
    _db_, conn = _db(tmp_path)
    # two qualifying funds A (score 100) and B (score 50); C (rank 3) does not qualify
    # ceil(3/2)=2 -> funds A and B qualify
    _rank(conn, "a", "A", 1, 100.0)
    _rank(conn, "b", "B", 2, 50.0)
    _rank(conn, "c", "C", 3, 10.0)
    # prior quarter 2024-09-30
    fa0 = _add_filing(conn, "a", "2024-09-30", "2024-11-10", "a0")
    _hold(conn, fa0, "CX", 100)            # A held X last quarter ($100k)
    # current quarter 2024-12-31
    fa1 = _add_filing(conn, "a", "2024-12-31", "2025-02-10", "a1")
    _hold(conn, fa1, "CX", 300)            # A increased X to $300k
    _hold(conn, fa1, "CY", 100)            # A new Y
    fb1 = _add_filing(conn, "b", "2024-12-31", "2025-02-10", "b1")
    _hold(conn, fb1, "CX", 100)            # B new X
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES "
                 "('CX','XXX','X'),('CY','YYY','Y')")
    conn.commit()

    q = stock_pipeline.qualifying_funds(conn)
    hist = stock_pipeline.fund_histories(conn, q)
    sig = stock_pipeline.signals_for_period(conn, "2024-12-31", q, hist)

    # X held by both funds -> holder_count 2; Y by A only -> 1
    assert sig["XXX"]["holder_count"] == 2
    assert sig["YYY"]["holder_count"] == 1
    # A portfolio = 300+100=400 -> X weight .75 ; B portfolio = 100 -> X weight 1.0
    # fund_conviction(X) = (100*.75 + 50*1.0)/(100+50) = 125/150 = .8333
    assert round(sig["XXX"]["fund_conviction"], 4) == 0.8333
    # avg_relative_size(X) = mean(.75, 1.0) = .875
    assert round(sig["XXX"]["avg_relative_size"], 4) == 0.875
    # tenure: A held X for 2 consecutive quarters, B for 1 -> mean 1.5
    assert round(sig["XXX"]["avg_tenure"], 4) == 1.5
    # net_change(X): A +200 (100->300), B +100 (new) = +300 ; universe AUM=400+100=500
    # net_change_pct = 300/500 = 0.6
    assert round(sig["XXX"]["net_change_pct"], 4) == 0.6
    # direction: both funds buyers of X -> buyers=2 sellers=0
    assert sig["XXX"]["buyers"] == 2 and sig["XXX"]["sellers"] == 0


def test_range_position_52w(tmp_path):
    _db_, conn = _db(tmp_path)
    # 25 daily points within the trailing year: low 10, high 30, last (as-of) = 25.
    # >= 20 points but < 252 -> usable range, partial=1.
    pts = ([("2024-12-02", 10.0)]
           + [(f"2024-12-{d:02d}", 20.0) for d in range(3, 26)]
           + [("2024-12-26", 30.0), ("2024-12-27", 25.0)])
    conn.executemany("INSERT INTO prices(ticker,date,close,adj_close) VALUES ('ZZZ',?,?,?)",
                     [(d, p, p) for d, p in pts])
    conn.commit()
    pos, partial = stock_pipeline.range_position_52w(conn, "ZZZ", "2024-12-31")
    assert round(pos, 4) == 0.75            # (25-10)/(30-10)
    assert partial == 1                      # >=20 but < 252 trading days

    # fewer than 20 trading days of data -> NULL position, partial=1
    conn.execute("INSERT INTO prices(ticker,date,close,adj_close) VALUES ('FEW','2024-12-28',5,5.0)")
    conn.commit()
    pos2, partial2 = stock_pipeline.range_position_52w(conn, "FEW", "2024-12-31")
    assert pos2 is None and partial2 == 1

    # no prices -> NULL, partial 1
    pos3, partial3 = stock_pipeline.range_position_52w(conn, "NONE", "2024-12-31")
    assert pos3 is None and partial3 == 1


import numpy as np


def test_regress_scores_recovers_linear_signal():
    # y = 2*x1 + 3 ; one feature, perfect line -> predictions equal targets
    feature_names = ["x1"]
    train_X = [[1.0], [2.0], [3.0], [4.0], [5.0], [6.0], [7.0], [8.0]]
    train_y = [5.0, 7.0, 9.0, 11.0, 13.0, 15.0, 17.0, 19.0]
    pred_rows = {"AAA": [1.0], "BBB": [10.0]}
    scores = stock_pipeline.regress_scores(feature_names, train_X, train_y, pred_rows)
    assert round(scores["AAA"], 3) == 5.0
    assert round(scores["BBB"], 3) == 23.0


def test_regress_scores_fallback_when_too_few_rows():
    # fewer than _MIN_TRAIN_ROWS -> returns the provided fallback per ticker
    scores = stock_pipeline.regress_scores(
        ["x1"], [[1.0], [2.0]], [1.0, 2.0],
        {"AAA": [9.0], "BBB": [9.0]},
        fallback={"AAA": 0.7, "BBB": 0.2})
    assert scores == {"AAA": 0.7, "BBB": 0.2}


def test_sector_adjust():
    raw = {"AAA": 1.0, "BBB": 3.0, "CCC": 5.0}
    sector = {"AAA": "Tech", "BBB": "Tech", "CCC": "Energy"}
    adj = stock_pipeline.sector_adjust(raw, sector)
    # Tech mean = 2.0 ; Energy mean = 5.0
    assert round(adj["AAA"], 4) == -1.0 and round(adj["BBB"], 4) == 1.0
    assert round(adj["CCC"], 4) == 0.0


def test_normalize_unit_range():
    assert stock_pipeline._normalize({"a": 10.0, "b": 20.0, "c": 30.0}) == {"a": 0.0, "b": 0.5, "c": 1.0}
    assert stock_pipeline._normalize({"a": 5.0}) == {"a": 1.0}     # single -> 1.0


def test_confidence_buckets_by_thirds():
    # three stocks with strictly increasing confidence inputs -> Low/Med/High
    universe = {
        "LO":  {"weighted_holder_score": 1.0, "avg_tenure_score": 1.0,
                "avg_relative_size": 0.0, "direction_agreement": 0.0, "data_quality_score": 0.0},
        "MID": {"weighted_holder_score": 2.0, "avg_tenure_score": 2.0,
                "avg_relative_size": 0.5, "direction_agreement": 0.5, "data_quality_score": 0.5},
        "HI":  {"weighted_holder_score": 3.0, "avg_tenure_score": 3.0,
                "avg_relative_size": 1.0, "direction_agreement": 1.0, "data_quality_score": 1.0},
    }
    flags = stock_pipeline.confidence_flags(universe)
    assert flags["HI"] == "High" and flags["MID"] == "Medium" and flags["LO"] == "Low"


def test_passes_filtered_gate():
    g = stock_pipeline.passes_filtered_gate
    # holder_count 1 small/mid-cap, valid range, non-Low -> passes at the
    # default threshold of 1 (concentrated funds don't co-hold small/mid-caps,
    # so >=2 consensus is structurally empty; see _MIN_FILTERED_HOLDERS).
    assert g(market_cap=1.0e9, range_position=0.5, holder_count=1, confidence_flag="Medium")
    # Low confidence is excluded
    assert not g(market_cap=1.0e9, range_position=0.5, holder_count=1, confidence_flag="Low")
    # market cap above the 4B cap is excluded
    assert not g(market_cap=5.0e9, range_position=0.5, holder_count=1, confidence_flag="High")
    # market cap below the 300M floor is excluded
    assert not g(market_cap=2.0e8, range_position=0.5, holder_count=1, confidence_flag="High")
    # range position outside 0.1-0.9 is excluded
    assert not g(market_cap=1.0e9, range_position=0.95, holder_count=1, confidence_flag="High")
    # None market_cap / range excluded
    assert not g(market_cap=None, range_position=0.5, holder_count=1, confidence_flag="High")
    assert not g(market_cap=1.0e9, range_position=None, holder_count=1, confidence_flag="High")
    # the holder threshold is honored when overridden
    assert not g(market_cap=1.0e9, range_position=0.5, holder_count=1,
                 confidence_flag="High", min_holders=3)


def test_run_stock_pipeline_end_to_end(tmp_path):
    _db_, conn = _db(tmp_path)
    _rank(conn, "a", "Fund A", 1, 100.0)
    fa = _add_filing(conn, "a", "2024-12-31", "2025-02-10", "a1")
    _hold(conn, fa, "CX", 300); _hold(conn, fa, "CY", 100)
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('CX','XXX','X co'),('CY','YYY','Y co')")
    # 52wk prices for both (>=20 pts)
    for tk in ("XXX", "YYY"):
        pts = [(f"2024-12-{d:02d}", 20.0) for d in range(1, 26)]
        conn.executemany(f"INSERT INTO prices(ticker,date,close,adj_close) VALUES ('{tk}',?,?,?)",
                         [(d, p, p) for d, p in pts])
    # fundamentals (current quarter) + sectors
    conn.executemany("INSERT INTO fundamentals(ticker,as_of_date,market_cap,shares_out,pe_ratio,"
                     "pe_available,gross_margin_pct,source) VALUES (?,?,?,?,?,?,?,'finnhub')",
                     [("XXX", "2024-12-31", 1.0e9, 1e7, 20.0, 1, 40.0),
                      ("YYY", "2024-12-31", 2.0e9, 1e7, 15.0, 1, 30.0)])
    conn.execute("INSERT INTO sectors(ticker,sector) VALUES ('XXX','Tech'),('YYY','Energy')")
    # a couple of historical returns (below _MIN_TRAIN_ROWS -> fallback path)
    conn.executemany("INSERT INTO holding_returns(fund_id,quarter_date,ticker,position_value_usd,"
                     "three_yr_return,data_quality_flag) VALUES (?,?,?,?,?,?)",
                     [("a", "2020-12-31", "XXX", 100.0, 0.5, "clean")])
    conn.commit()

    summary = stock_pipeline.run_stock_pipeline(_db_)

    assert summary["universe"] == 2
    raw = {r["ticker"]: r for r in conn.execute(
        "SELECT ticker, rank, raw_score, sector_adjusted_score, confidence_flag, "
        "market_cap, holder_count, range_position, partial FROM stock_rankings_raw").fetchall()}
    assert set(raw) == {"XXX", "YYY"}
    assert raw["XXX"]["holder_count"] == 1
    assert raw["XXX"]["market_cap"] == 1.0e9
    assert {raw["XXX"]["rank"], raw["YYY"]["rank"]} == {1, 2}
    # confidence flag present and valid
    assert raw["XXX"]["confidence_flag"] in {"High", "Medium", "Low"}
    # stock_confidence populated
    assert conn.execute("SELECT COUNT(*) FROM stock_confidence").fetchone()[0] == 2
    # confidence_raw now populated in the raw table (not NULL)
    assert raw["XXX"]["confidence_flag"] in {"High", "Medium", "Low"}
    cr = conn.execute("SELECT confidence_raw FROM stock_rankings_raw WHERE ticker='XXX'").fetchone()[0]
    assert cr is not None
