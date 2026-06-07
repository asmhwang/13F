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
