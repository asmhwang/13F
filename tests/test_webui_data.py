"""Tests for the webui data layer (pure SQL->DataFrame query functions)."""
from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter
from webui import data


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return conn


def _rank(conn, cik, name, rank, score, **kw):
    conn.execute("INSERT OR IGNORE INTO filers(cik,name) VALUES (?,?)", (cik, name))
    cols = dict(fund_id=cik, fund_name=name, rank=rank, final_score=score, eligible=1)
    cols.update(kw)
    keys = ",".join(cols)
    qs = ",".join("?" * len(cols))
    conn.execute(f"INSERT INTO fund_rankings({keys}) VALUES ({qs})", tuple(cols.values()))


def test_fund_rankings_ordered_by_rank(tmp_path):
    conn = _db(tmp_path)
    _rank(conn, "b", "Bravo", 2, 80.0)
    _rank(conn, "a", "Alpha", 1, 100.0)
    conn.commit()
    df = data.fund_rankings(conn)
    assert list(df["fund_name"]) == ["Alpha", "Bravo"]
    assert df.iloc[0]["final_score"] == 100.0


def _filing(conn, cik, period, filed, acc):
    conn.execute("INSERT OR IGNORE INTO filers(cik,name) VALUES (?,?)", (cik, cik))
    conn.execute(
        "INSERT INTO filings(cik,accession_number,period_of_report,filed_date) VALUES (?,?,?,?)",
        (cik, acc, period, filed),
    )
    return conn.execute("SELECT id FROM filings WHERE accession_number=?", (acc,)).fetchone()[0]


def test_fund_quarterly_scores_time_ordered(tmp_path):
    conn = _db(tmp_path)
    conn.execute("INSERT INTO fund_quarterly_scores(fund_id,quarter_date,qps_raw,qps_excess,benchmark_return) "
                 "VALUES ('a','2021-03-31',0.2,0.05,0.15)")
    conn.execute("INSERT INTO fund_quarterly_scores(fund_id,quarter_date,qps_raw,qps_excess,benchmark_return) "
                 "VALUES ('a','2020-12-31',0.1,0.02,0.08)")
    conn.commit()
    df = data.fund_quarterly_scores("a", conn)
    assert list(df["quarter_date"]) == ["2020-12-31", "2021-03-31"]


def test_rankings_meta_latest_quarter(tmp_path):
    conn = _db(tmp_path)
    _filing(conn, "a", "2024-09-30", "2024-11-10", "x1")
    _filing(conn, "a", "2024-12-31", "2025-02-10", "x2")
    _rank(conn, "a", "Alpha", 1, 100.0)
    conn.commit()
    meta = data.rankings_meta(conn)
    assert meta["latest_quarter"] == "2024-12-31"
    assert meta["fund_count"] == 1
