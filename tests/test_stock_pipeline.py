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
