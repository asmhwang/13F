"""Tests for pipeline.prices (Phase 1 price + benchmark ingest)."""
from pathlib import Path

from pipeline.database import get_connection
from pipeline import prices


def _tables(conn) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {r[0] for r in rows}


def test_init_schema_creates_tables(tmp_path):
    db = tmp_path / "t.db"
    conn = get_connection(db)
    prices.init_schema(conn, db)
    assert {"prices", "benchmark", "price_fetch_log"} <= _tables(conn)


def _sample_chart_payload():
    # epoch seconds for 2021-01-04 and 2021-01-05 (UTC)
    return {
        "chart": {
            "result": [
                {
                    "timestamp": [1609718400, 1609804800],
                    "indicators": {
                        "quote": [{"close": [100.0, None]}],
                        "adjclose": [{"adjclose": [99.0, 101.0]}],
                    },
                }
            ],
            "error": None,
        }
    }


def test_parse_chart_extracts_rows_and_skips_null_close():
    rows = prices.parse_chart(_sample_chart_payload())
    assert rows == [{"date": "2021-01-04", "close": 100.0, "adj_close": 99.0}]


def test_parse_chart_falls_back_to_close_when_adj_missing():
    payload = {
        "chart": {"result": [{
            "timestamp": [1609718400],
            "indicators": {"quote": [{"close": [50.0]}], "adjclose": [{}]},
        }]}
    }
    rows = prices.parse_chart(payload)
    assert rows == [{"date": "2021-01-04", "close": 50.0, "adj_close": 50.0}]


def test_parse_chart_empty_payload_returns_empty():
    assert prices.parse_chart({"chart": {"result": []}}) == []


from unittest.mock import patch, MagicMock


def test_chart_url_encodes_symbol_and_dates():
    url = prices._chart_url("^SP500TR", "2021-01-01", "2021-01-02")
    assert url.startswith("https://query1.finance.yahoo.com/v8/finance/chart/%5ESP500TR")
    assert "interval=1d" in url
    assert "period1=" in url and "period2=" in url


def test_fetch_prices_calls_http_and_parses():
    resp = MagicMock()
    resp.json.return_value = _sample_chart_payload()
    with patch("pipeline.prices._http_get", return_value=resp) as m:
        rows = prices.fetch_prices("AAPL", "2021-01-01", "2021-01-31")
    m.assert_called_once()
    assert rows == [{"date": "2021-01-04", "close": 100.0, "adj_close": 99.0}]


from pipeline.database import init_db


def _seed_holdings(db):
    """One fund, two quarters, two tickers (one equity, one option-only)."""
    init_db(db)
    conn = get_connection(db)
    conn.execute("INSERT INTO filers(cik, name) VALUES ('111','Fund A')")
    conn.execute("INSERT INTO filings(cik, accession_number, period_of_report, filed_date) "
                 "VALUES ('111','a1','2018-03-31','2018-05-10')")
    conn.execute("INSERT INTO filings(cik, accession_number, period_of_report, filed_date) "
                 "VALUES ('111','a2','2019-03-31','2019-05-10')")
    f1 = conn.execute("SELECT id FROM filings WHERE accession_number='a1'").fetchone()[0]
    f2 = conn.execute("SELECT id FROM filings WHERE accession_number='a2'").fetchone()[0]
    # AAPL equity in both quarters
    for fid in (f1, f2):
        conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                     "VALUES (?, 'C_AAPL','APPLE INC',1000,10,NULL)", (fid,))
    # OPT: present only as an option (put_call set) -> must be excluded
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?, 'C_OPT','OPTONLY CO',500,5,'Call')", (f2,))
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_AAPL','AAPL','Apple')")
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_OPT','OPT','OptOnly')")
    conn.commit()
    return conn


def test_plus_three_years_handles_leap_day():
    assert prices._plus_three_years("2020-02-29") == "2023-02-28"
    assert prices._plus_three_years("2021-03-31") == "2024-03-31"


def test_held_ticker_windows_equity_only_with_3yr_window(tmp_path):
    db = tmp_path / "t.db"
    conn = _seed_holdings(db)
    windows = prices.held_ticker_windows(conn)
    tickers = {w["ticker"] for w in windows}
    assert tickers == {"AAPL"}          # OPT excluded (option-only)
    aapl = next(w for w in windows if w["ticker"] == "AAPL")
    assert aapl["start"] == "2018-03-31"
    assert aapl["end"] == "2022-03-31"  # last quarter 2019-03-31 + 3yr


def _seed_coverage(db):
    """Latest quarter 2019-03-31 with two equity tickers AAPL(1000) + MSFT(3000)."""
    init_db(db)
    conn = get_connection(db)
    conn.execute("INSERT INTO filers(cik, name) VALUES ('111','Fund A')")
    conn.execute("INSERT INTO filings(cik, accession_number, period_of_report, filed_date) "
                 "VALUES ('111','a1','2019-03-31','2019-05-10')")
    fid = conn.execute("SELECT id FROM filings WHERE accession_number='a1'").fetchone()[0]
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?, 'C_AAPL','APPLE',1000,10,NULL)", (fid,))
    conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                 "VALUES (?, 'C_MSFT','MICROSOFT',3000,10,NULL)", (fid,))
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_AAPL','AAPL','Apple')")
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_MSFT','MSFT','Microsoft')")
    prices.init_schema(conn, db)
    # AAPL priced at as-of and forward; MSFT not priced at all.
    prices.store_prices(conn, "AAPL", [
        {"date": "2019-03-29", "close": 50.0, "adj_close": 50.0},   # within 7d of as-of
        {"date": "2022-03-31", "close": 80.0, "adj_close": 80.0},   # forward (as-of+3yr)
    ])
    return conn


def test_coverage_report_value_weighted(tmp_path):
    db = tmp_path / "t.db"
    _seed_coverage(db)
    conn = get_connection(db)
    rep = prices.coverage_report(conn)
    assert rep["quarter"] == "2019-03-31"
    assert rep["total_value_thousands"] == 4000
    # Only AAPL (1000 of 4000) is priced -> 25%
    assert rep["asof_coverage_pct"] == 25.0
    assert rep["forward_coverage_pct"] == 25.0


def test_ingest_benchmark_stores_rows_over_filing_span(tmp_path):
    db = tmp_path / "t.db"
    _seed_holdings(db)   # filings span 2018-03-31 .. 2019-03-31

    fake_rows = [
        {"date": "2018-03-29", "close": 2600.0, "adj_close": 2600.0},
        {"date": "2018-04-02", "close": 2580.0, "adj_close": 2580.0},
    ]
    with patch("pipeline.prices.fetch_prices", return_value=fake_rows) as m:
        n = prices.ingest_benchmark(db)

    # Called once for ^SP500TR over [min period, max period + 3yr]
    sym, start, end = m.call_args.args
    assert sym == "^SP500TR"
    assert start == "2018-03-31"
    assert end == "2022-03-31"
    assert n == 2

    conn = get_connection(db)
    assert conn.execute("SELECT COUNT(*) FROM benchmark").fetchone()[0] == 2
    assert conn.execute("SELECT adj_close FROM benchmark WHERE date='2018-03-29'").fetchone()[0] == 2600.0


def test_ingest_prices_fetches_logs_and_is_incremental(tmp_path):
    db = tmp_path / "t.db"
    _seed_holdings(db)

    fake_rows = [
        {"date": "2018-03-29", "close": 10.0, "adj_close": 9.5},
        {"date": "2022-03-31", "close": 20.0, "adj_close": 19.0},
    ]
    with patch("pipeline.prices.fetch_prices", return_value=fake_rows) as m:
        stats = prices.ingest_prices(db)
    assert stats == {"fetched": 1, "skipped": 0, "failed": 0, "total": 1}
    assert m.call_count == 1

    conn = get_connection(db)
    assert conn.execute("SELECT COUNT(*) FROM prices WHERE ticker='AAPL'").fetchone()[0] == 2
    log = conn.execute("SELECT first_date, last_date, status FROM price_fetch_log WHERE ticker='AAPL'").fetchone()
    assert (log[0], log[1], log[2]) == ("2018-03-29", "2022-03-31", "ok")

    # Second run: window already covered -> skipped, no new fetch.
    with patch("pipeline.prices.fetch_prices", return_value=fake_rows) as m2:
        stats2 = prices.ingest_prices(db)
    assert stats2["skipped"] == 1 and stats2["fetched"] == 0
    m2.assert_not_called()


def test_ingest_prices_logs_no_data_when_empty(tmp_path):
    db = tmp_path / "t.db"
    _seed_holdings(db)
    with patch("pipeline.prices.fetch_prices", return_value=[]):
        stats = prices.ingest_prices(db)
    assert stats["fetched"] == 0
    conn = get_connection(db)
    status = conn.execute("SELECT status FROM price_fetch_log WHERE ticker='AAPL'").fetchone()[0]
    assert status == "no_data"
    # no_data tickers are not retried
    with patch("pipeline.prices.fetch_prices") as m:
        prices.ingest_prices(db)
    m.assert_not_called()


def test_held_ticker_windows_excludes_digit_tickers(tmp_path):
    """Junk tickers containing any digit (all-digit '016' or mixed '02Z0') are
    filtered out; real letter-only US tickers are kept."""
    db = tmp_path / "t.db"
    init_db(db)
    conn = get_connection(db)
    conn.execute("INSERT INTO filers(cik, name) VALUES ('222','Fund B')")
    conn.execute("INSERT INTO filings(cik, accession_number, period_of_report, filed_date) "
                 "VALUES ('222','b1','2020-03-31','2020-05-10')")
    fid = conn.execute("SELECT id FROM filings WHERE accession_number='b1'").fetchone()[0]
    # Real ticker
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_MSFT','MSFT','Microsoft')")
    # Junk: all digits, no letters
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_JUNK','016','Junk CUSIP')")
    # Junk: letters + digits (FIGI/SEDOL-style code)
    conn.execute("INSERT INTO securities(cusip,ticker,name) VALUES ('C_JNK2','02Z0','Junk FIGI')")
    for cusip in ("C_MSFT", "C_JUNK", "C_JNK2"):
        conn.execute("INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares,put_call) "
                     "VALUES (?, ?, 'X', 1000, 10, NULL)", (fid, cusip))
    conn.commit()
    prices.init_schema(conn, db)
    windows = prices.held_ticker_windows(conn)
    tickers = {w["ticker"] for w in windows}
    assert "016" not in tickers
    assert "02Z0" not in tickers
    assert "MSFT" in tickers


def test_store_prices_inserts_and_upserts(tmp_path):
    db = tmp_path / "t.db"
    conn = get_connection(db)
    prices.init_schema(conn, db)

    n = prices.store_prices(conn, "AAPL", [
        {"date": "2021-01-04", "close": 100.0, "adj_close": 99.0},
        {"date": "2021-01-05", "close": 101.0, "adj_close": 100.0},
    ])
    assert n == 2
    assert conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] == 2

    # Re-store same dates with new values: no duplicates, values updated.
    prices.store_prices(conn, "AAPL", [
        {"date": "2021-01-04", "close": 110.0, "adj_close": 109.0},
    ])
    assert conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] == 2
    row = conn.execute(
        "SELECT adj_close FROM prices WHERE ticker='AAPL' AND date='2021-01-04'"
    ).fetchone()
    assert row[0] == 109.0
