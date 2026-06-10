"""Tests for amendment-aware effective-filing resolution and its consumers."""
from pipeline import database
from pipeline.database import get_connection, init_db
from pipeline.edgar import parse_amendment_type
from pipeline.scoring import adapter


def _db(tmp_path):
    db = tmp_path / "t.db"
    init_db(db)
    return get_connection(db)


def _filing(conn, cik, period, filed, acc, rtype="13F-HR", atype=None, n_holdings=0):
    conn.execute("INSERT OR IGNORE INTO filers(cik,name) VALUES (?,?)", (cik, cik))
    fid = database.insert_filing(
        conn, cik=cik, accession_number=acc, period_of_report=period,
        filed_date=filed, report_type=rtype, amendment_type=atype)
    for i in range(n_holdings):
        conn.execute(
            "INSERT INTO holdings(filing_id,cusip,name_of_issuer,value_thousands,shares) "
            "VALUES (?,?,?,?,?)", (fid, f"CUSIP{i:04d}X", f"Issuer {i}", 100, 10))
    return fid


def test_restatement_replaces_original(tmp_path):
    conn = _db(tmp_path)
    orig = _filing(conn, "1", "2024-12-31", "2025-02-10", "a1", n_holdings=10)
    rest = _filing(conn, "1", "2024-12-31", "2025-03-01", "a2",
                   rtype="13F-HR/A", atype="RESTATEMENT", n_holdings=9)
    database.rebuild_effective_filings(conn)
    assert adapter.effective_filing_ids(conn, "1", "2024-12-31") == [rest]


def test_new_holdings_amendment_unions_with_original(tmp_path):
    conn = _db(tmp_path)
    orig = _filing(conn, "1", "2024-12-31", "2025-02-10", "a1", n_holdings=10)
    adds = _filing(conn, "1", "2024-12-31", "2025-08-14", "a2",
                   rtype="13F-HR/A", atype="NEW HOLDINGS", n_holdings=4)
    database.rebuild_effective_filings(conn)
    ids = adapter.effective_filing_ids(conn, "1", "2024-12-31")
    assert sorted(ids) == sorted([orig, adds])


def test_unknown_amendment_classified_by_size(tmp_path):
    conn = _db(tmp_path)
    # Tiny unknown /A (< 50% of original) is treated as additive…
    orig = _filing(conn, "1", "2024-12-31", "2025-02-10", "a1", n_holdings=20)
    tiny = _filing(conn, "1", "2024-12-31", "2025-08-14", "a2",
                   rtype="13F-HR/A", n_holdings=2)
    # …while a near-full-size unknown /A replaces.
    orig2 = _filing(conn, "2", "2024-12-31", "2025-02-10", "b1", n_holdings=20)
    full = _filing(conn, "2", "2024-12-31", "2025-03-01", "b2",
                   rtype="13F-HR/A", n_holdings=19)
    database.rebuild_effective_filings(conn)
    assert sorted(adapter.effective_filing_ids(conn, "1", "2024-12-31")) == sorted([orig, tiny])
    assert orig2 not in adapter.effective_filing_ids(conn, "2", "2024-12-31")
    assert adapter.effective_filing_ids(conn, "2", "2024-12-31") == [full]


def test_tiny_labeled_restatement_treated_as_additive(tmp_path):
    """Pre-XML confidential-treatment releases are labeled RESTATEMENT but
    contain only the previously-omitted positions — they must union, not wipe
    the quarter (e.g. Berkshire 2003-12-31: 32-holding HR + 1-holding 'RESTATEMENT')."""
    conn = _db(tmp_path)
    orig = _filing(conn, "1", "2003-12-31", "2004-02-17", "a1", n_holdings=32)
    conf = _filing(conn, "1", "2003-12-31", "2004-08-25", "a2",
                   rtype="13F-HR/A", atype="RESTATEMENT", n_holdings=1)
    database.rebuild_effective_filings(conn)
    assert sorted(adapter.effective_filing_ids(conn, "1", "2003-12-31")) == sorted([orig, conf])


def test_original_filed_date_ignores_late_amendment(tmp_path):
    conn = _db(tmp_path)
    _filing(conn, "1", "2008-12-31", "2009-02-13", "a1", n_holdings=5)
    _filing(conn, "1", "2008-12-31", "2012-03-01", "a2",
            rtype="13F-HR/A", atype="RESTATEMENT", n_holdings=5)
    database.rebuild_effective_filings(conn)
    # The return window must anchor at first public disclosure, not the /A.
    assert adapter.original_filed_date(conn, "1", "2008-12-31") == "2009-02-13"


def test_effective_ids_fallback_without_rebuild(tmp_path):
    conn = _db(tmp_path)
    fid = _filing(conn, "1", "2024-12-31", "2025-02-10", "a1", n_holdings=3)
    # No rebuild_effective_filings call — falls back to the latest filing.
    assert adapter.effective_filing_ids(conn, "1", "2024-12-31") == [fid]


def test_parse_amendment_type():
    assert parse_amendment_type(
        "<amendmentInfo><amendmentType>RESTATEMENT</amendmentType></amendmentInfo>"
    ) == "RESTATEMENT"
    assert parse_amendment_type(
        "<ns1:amendmentType>NEW HOLDINGS</ns1:amendmentType>") == "NEW HOLDINGS"
    assert parse_amendment_type("<coverPage>nothing here</coverPage>") is None
