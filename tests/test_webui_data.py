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
