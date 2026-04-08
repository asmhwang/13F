"""
SQLite schema and database helpers for 13F holdings pipeline.
"""

import math
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "13f.db"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.create_function("LOG", 1, math.log)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path = DB_PATH) -> None:
    with get_connection(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS filers (
                cik         TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- One row per 13F-HR (or 13F-HR/A amendment) filing
            CREATE TABLE IF NOT EXISTS filings (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                cik              TEXT    NOT NULL REFERENCES filers(cik),
                accession_number TEXT    UNIQUE NOT NULL,
                period_of_report DATE    NOT NULL,   -- e.g. 2024-09-30
                filed_date       DATE    NOT NULL,
                report_type      TEXT    NOT NULL DEFAULT '13F-HR',
                raw_url          TEXT,               -- EDGAR document URL
                ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_filings_cik        ON filings(cik);
            CREATE INDEX IF NOT EXISTS idx_filings_period     ON filings(period_of_report);

            -- Individual equity positions inside a filing
            CREATE TABLE IF NOT EXISTS holdings (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id            INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
                cusip                TEXT    NOT NULL,
                name_of_issuer       TEXT    NOT NULL,
                title_of_class       TEXT,
                -- SEC reports value in thousands of USD
                value_thousands      INTEGER NOT NULL,
                shares               INTEGER,         -- NULL when type = PRN
                principal_amount     REAL,            -- NULL when type = SH
                share_type           TEXT,            -- 'SH' | 'PRN'
                investment_discretion TEXT,           -- 'SOLE' | 'SHARED' | 'OTHER'
                put_call             TEXT,            -- 'Put' | 'Call' | NULL
                voting_sole          INTEGER,
                voting_shared        INTEGER,
                voting_none          INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_holdings_filing  ON holdings(filing_id);
            CREATE INDEX IF NOT EXISTS idx_holdings_cusip   ON holdings(cusip);

            -- Optional: map CUSIP -> ticker for convenience
            CREATE TABLE IF NOT EXISTS securities (
                cusip       TEXT PRIMARY KEY,
                ticker      TEXT,
                name        TEXT,
                exchange    TEXT,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    print(f"Database initialised at {db_path}")


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def upsert_filer(conn: sqlite3.Connection, cik: str, name: str) -> None:
    conn.execute(
        """
        INSERT INTO filers(cik, name, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(cik) DO UPDATE SET
            name       = excluded.name,
            updated_at = CURRENT_TIMESTAMP
        """,
        (cik, name),
    )


def insert_filing(
    conn: sqlite3.Connection,
    cik: str,
    accession_number: str,
    period_of_report: str,
    filed_date: str,
    report_type: str = "13F-HR",
    raw_url: str | None = None,
) -> int:
    """Insert filing, return its row id. Skip if already exists."""
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO filings
            (cik, accession_number, period_of_report, filed_date, report_type, raw_url)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (cik, accession_number, period_of_report, filed_date, report_type, raw_url),
    )
    if cur.lastrowid and cur.rowcount:
        return cur.lastrowid
    # Already existed — fetch its id
    row = conn.execute(
        "SELECT id FROM filings WHERE accession_number = ?", (accession_number,)
    ).fetchone()
    return row["id"]


def insert_holdings(conn: sqlite3.Connection, filing_id: int, holdings: list[dict]) -> None:
    conn.executemany(
        """
        INSERT INTO holdings
            (filing_id, cusip, name_of_issuer, title_of_class,
             value_thousands, shares, principal_amount, share_type,
             investment_discretion, put_call,
             voting_sole, voting_shared, voting_none)
        VALUES
            (:filing_id, :cusip, :name_of_issuer, :title_of_class,
             :value_thousands, :shares, :principal_amount, :share_type,
             :investment_discretion, :put_call,
             :voting_sole, :voting_shared, :voting_none)
        """,
        [{"filing_id": filing_id, **h} for h in holdings],
    )


def upsert_security(conn: sqlite3.Connection, cusip: str, ticker: str | None, name: str | None, exchange: str | None = None) -> None:
    conn.execute(
        """
        INSERT INTO securities(cusip, ticker, name, exchange, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(cusip) DO UPDATE SET
            ticker     = COALESCE(excluded.ticker, ticker),
            name       = COALESCE(excluded.name, name),
            exchange   = COALESCE(excluded.exchange, exchange),
            updated_at = CURRENT_TIMESTAMP
        """,
        (cusip, ticker, name, exchange),
    )
