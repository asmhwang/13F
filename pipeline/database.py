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
                -- 13F-HR/A cover page <amendmentType>: 'RESTATEMENT' replaces the
                -- original; 'NEW HOLDINGS' only adds positions. NULL = original
                -- filing or unknown (legacy text amendments).
                amendment_type   TEXT,
                raw_url          TEXT,               -- EDGAR document URL
                ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_filings_cik           ON filings(cik);
            CREATE INDEX IF NOT EXISTS idx_filings_period        ON filings(period_of_report);
            CREATE INDEX IF NOT EXISTS idx_filings_cik_period    ON filings(cik, period_of_report);

            -- Which filings actually represent each (cik, period)'s holdings.
            -- A RESTATEMENT amendment replaces the original; a NEW HOLDINGS
            -- amendment is unioned with it. Rebuilt by rebuild_effective_filings
            -- after every ingest. original_filed_date is the first public
            -- disclosure (the original 13F-HR's filed_date) — the correct as-of
            -- for forward-return windows.
            CREATE TABLE IF NOT EXISTS effective_filings (
                cik                 TEXT    NOT NULL,
                period_of_report    DATE    NOT NULL,
                filing_id           INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
                original_filed_date DATE    NOT NULL,
                PRIMARY KEY (cik, period_of_report, filing_id)
            );
            CREATE INDEX IF NOT EXISTS idx_effective_filing_id ON effective_filings(filing_id);

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
        _migrate(conn)
    print(f"Database initialised at {db_path}")


def _migrate(conn: sqlite3.Connection) -> None:
    """Bring an existing database up to the current schema (idempotent)."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "amendment_type" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN amendment_type TEXT")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS effective_filings (
            cik                 TEXT    NOT NULL,
            period_of_report    DATE    NOT NULL,
            filing_id           INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
            original_filed_date DATE    NOT NULL,
            PRIMARY KEY (cik, period_of_report, filing_id)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_effective_filing_id ON effective_filings(filing_id)")


def rebuild_effective_filings(conn: sqlite3.Connection, cik: str | None = None) -> None:
    """Resolve which filing(s) represent each (cik, period)'s holdings.

    SEC 13F-HR/A amendments come in two kinds: RESTATEMENT (replaces the whole
    information table) and NEW HOLDINGS (contains only added positions, e.g.
    after confidential treatment expires). Taking "the newest filing" therefore
    silently shrinks a quarter to the handful of added positions — e.g.
    Berkshire 2025-03-31 resolving to a 4-holding /A instead of the 110-holding
    original. Per (cik, period):

      base      = the latest original 13F-HR or RESTATEMENT amendment
      effective = base + every NEW HOLDINGS amendment filed after it

    Amendments without a parsed amendment_type (legacy text filings) are
    classified by size: an /A carrying >= 50% of the largest earlier filing's
    holdings is treated as a restatement, smaller ones as additive.
    """
    _migrate(conn)
    if cik is not None:
        where, params = "WHERE cik = ?", (cik,)
        conn.execute("DELETE FROM effective_filings WHERE cik = ?", (cik,))
    else:
        where, params = "", ()
        conn.execute("DELETE FROM effective_filings")

    rows = conn.execute(
        f"""
        SELECT f.id, f.cik, f.period_of_report, f.filed_date, f.report_type,
               f.amendment_type,
               (SELECT COUNT(*) FROM holdings h WHERE h.filing_id = f.id) AS n_holdings
        FROM filings f {where}
        ORDER BY f.cik, f.period_of_report,
                 f.filed_date, (f.report_type LIKE '%/A%'), f.id
        """,
        params,
    ).fetchall()

    groups: dict[tuple[str, str], list] = {}
    for r in rows:
        groups.setdefault((r["cik"], r["period_of_report"]), []).append(r)

    inserts: list[tuple] = []
    for (g_cik, period), filings in groups.items():
        original_filed = next(
            (f["filed_date"] for f in filings if "/A" not in f["report_type"]),
            filings[0]["filed_date"],
        )
        max_seen = 0
        base_idx = 0
        additive: set[int] = set()
        for i, f in enumerate(filings):
            is_amendment = "/A" in f["report_type"]
            atype = (f["amendment_type"] or "").upper()
            if not is_amendment or atype == "RESTATEMENT":
                kind = "base"
            elif atype == "NEW HOLDINGS":
                kind = "add"
            else:  # unknown legacy /A — classify by relative size
                kind = "base" if (max_seen == 0 or f["n_holdings"] >= 0.5 * max_seen) else "add"
            if kind == "base":
                base_idx = i
            else:
                additive.add(i)
            max_seen = max(max_seen, f["n_holdings"])

        effective = [filings[base_idx]["id"]] + [
            f["id"] for i, f in enumerate(filings)
            if i in additive and i > base_idx and f["n_holdings"] > 0
        ]
        inserts.extend((g_cik, period, fid, original_filed) for fid in effective)

    conn.executemany(
        "INSERT OR REPLACE INTO effective_filings "
        "(cik, period_of_report, filing_id, original_filed_date) VALUES (?, ?, ?, ?)",
        inserts,
    )
    conn.commit()


def ensure_effective_filings(conn: sqlite3.Connection) -> None:
    """Rebuild effective_filings if it is missing or empty while filings exist
    (cheap guard for databases ingested before the table was introduced)."""
    try:
        n = conn.execute("SELECT COUNT(*) FROM effective_filings").fetchone()[0]
    except sqlite3.OperationalError:
        n = 0
    if n == 0 and conn.execute("SELECT EXISTS(SELECT 1 FROM filings)").fetchone()[0]:
        rebuild_effective_filings(conn)


def ensure_indexes(db_path: Path = DB_PATH) -> None:
    """Idempotently create indexes on an existing database (silent, no schema changes)."""
    conn = get_connection(db_path)
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_filings_cik           ON filings(cik);
        CREATE INDEX IF NOT EXISTS idx_filings_period        ON filings(period_of_report);
        CREATE INDEX IF NOT EXISTS idx_filings_cik_period    ON filings(cik, period_of_report);
        CREATE INDEX IF NOT EXISTS idx_holdings_filing       ON holdings(filing_id);
        CREATE INDEX IF NOT EXISTS idx_holdings_cusip        ON holdings(cusip);
    """)


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
    amendment_type: str | None = None,
) -> int:
    """Insert filing, return its row id. Re-ingest updates metadata in place
    (so a --force run can backfill amendment_type on existing rows)."""
    cur = conn.execute(
        """
        INSERT INTO filings
            (cik, accession_number, period_of_report, filed_date, report_type,
             raw_url, amendment_type)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(accession_number) DO UPDATE SET
            period_of_report = excluded.period_of_report,
            filed_date       = excluded.filed_date,
            report_type      = excluded.report_type,
            raw_url          = excluded.raw_url,
            amendment_type   = COALESCE(excluded.amendment_type, amendment_type)
        """,
        (cik, accession_number, period_of_report, filed_date, report_type,
         raw_url, amendment_type),
    )
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
