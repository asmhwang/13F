"""Tests for pipeline.scoring.adapter."""
from pipeline.database import get_connection, init_db
from pipeline.scoring import adapter


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    adapter.init_schema(conn, db)
    return db, conn


def _tables(conn):
    return {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}


def test_init_schema_creates_fund_tables(tmp_path):
    _db_, conn = _db(tmp_path)
    assert {"fund_eligibility", "holding_returns", "fund_quarterly_scores",
            "fund_tws", "fund_turnover", "fund_consistency",
            "fund_rankings"} <= _tables(conn)


def test_price_asof_returns_latest_on_or_before(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.executemany(
        "INSERT INTO prices(ticker,date,close,adj_close) VALUES (?,?,?,?)",
        [("AAA", "2020-01-10", 10, 10.0), ("AAA", "2020-01-17", 11, 11.0)],
    )
    conn.commit()
    assert adapter.price_asof(conn, "AAA", "2020-01-20") == ("2020-01-17", 11.0)
    assert adapter.price_asof(conn, "AAA", "2020-01-10") == ("2020-01-10", 10.0)
    assert adapter.price_asof(conn, "AAA", "2020-01-09") is None
    assert adapter.price_asof(conn, "ZZZ", "2020-01-20") is None


def test_three_year_return_clean_and_last_price(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.executemany(
        "INSERT INTO prices(ticker,date,close,adj_close) VALUES (?,?,?,?)",
        [("AAA", "2018-05-10", 50, 50.0), ("AAA", "2021-05-10", 75, 75.0),
         ("BBB", "2018-05-10", 20, 20.0), ("BBB", "2021-01-10", 10, 10.0)],
    )
    conn.commit()
    # AAA: forward price exactly on target date -> clean, +50%
    assert adapter.three_year_return(conn, "AAA", "2018-05-10") == (0.5, "clean")
    # BBB: latest forward price is 4 months stale (delisted) -> last_price, -50%
    ret, flag = adapter.three_year_return(conn, "BBB", "2018-05-10")
    assert round(ret, 4) == -0.5 and flag == "last_price"
    # no prices -> None
    assert adapter.three_year_return(conn, "AAA", "1990-01-01") is None


def test_benchmark_return(tmp_path):
    _db_, conn = _db(tmp_path)
    conn.executemany(
        "INSERT INTO benchmark(date,adj_close) VALUES (?,?)",
        [("2019-05-15", 100.0), ("2022-05-15", 110.0)],
    )
    conn.commit()
    assert round(adapter.benchmark_return(conn, "2019-05-15"), 4) == 0.10
    assert adapter.benchmark_return(conn, "1990-01-01") is None
