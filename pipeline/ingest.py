"""
Main ingestion script.

Usage:
    # Ingest all seed filers (most recent filing only):
    python -m pipeline.ingest --seed --latest-only

    # Ingest a specific filer (all available filings):
    python -m pipeline.ingest --cik 0001067983

    # Ingest a specific filer, only filings since a date:
    python -m pipeline.ingest --cik 0001067983 --since 2022-01-01

    # Ingest all seed filers, all history:
    python -m pipeline.ingest --seed
"""

import argparse
import sys
from pathlib import Path

# Allow running as `python pipeline/ingest.py` from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import database, edgar, parser


def ingest_filer(
    cik: str,
    *,
    latest_only: bool = False,
    since: str | None = None,
    db_path: Path = database.DB_PATH,
) -> None:
    print(f"\n{'='*60}")
    print(f"Fetching submissions for CIK {cik}...")

    # ---- filer metadata ----
    submissions = edgar.get_filer_submissions(cik)
    filer_name = submissions.get("name", "Unknown")
    print(f"  Filer: {filer_name}")

    with database.get_connection(db_path) as conn:
        database.upsert_filer(conn, cik.lstrip("0") or "0", filer_name)

    # ---- filing list ----
    filings = edgar.get_13f_filings_for_filer(cik)
    print(f"  Found {len(filings)} 13F-HR filing(s)")

    if not filings:
        print("  Nothing to ingest.")
        return

    # Filter by date
    if since:
        filings = [f for f in filings if f["filed_date"] >= since]
        print(f"  After filtering by since={since}: {len(filings)} filing(s)")

    if latest_only:
        filings = filings[:1]

    cik_bare = cik.lstrip("0") or "0"

    for filing_meta in filings:
        acc   = filing_meta["accession_number"]
        period = filing_meta["period_of_report"]
        filed  = filing_meta["filed_date"]
        rtype  = filing_meta["report_type"]

        print(f"\n  Filing {acc}  period={period}  filed={filed}")

        # Find information table XML
        xml_url = edgar.get_information_table_url(cik_bare, acc)
        if not xml_url:
            print("    [SKIP] could not locate information table document")
            continue

        print(f"    URL: {xml_url}")

        # Fetch XML
        try:
            xml_text = edgar.fetch_document(xml_url)
        except Exception as exc:
            print(f"    [ERROR] fetch failed: {exc}")
            continue

        # Parse holdings
        try:
            holdings = parser.parse_information_table(xml_text)
        except ValueError as exc:
            print(f"    [ERROR] parse failed: {exc}")
            continue

        if not holdings:
            print("    [SKIP] no holdings parsed")
            continue

        print(f"    Parsed {len(holdings)} holdings")

        # Persist
        with database.get_connection(db_path) as conn:
            filing_id = database.insert_filing(
                conn,
                cik=cik_bare,
                accession_number=acc,
                period_of_report=period,
                filed_date=filed,
                report_type=rtype,
                raw_url=xml_url,
            )
            # Remove old holdings for this filing before re-inserting
            conn.execute("DELETE FROM holdings WHERE filing_id = ?", (filing_id,))
            database.insert_holdings(conn, filing_id, holdings)

        total_value = sum(h["value_thousands"] for h in holdings)
        print(f"    Stored filing_id={filing_id}  AUM≈${total_value/1_000:,.0f}M")


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest SEC 13F filings into SQLite")
    ap.add_argument("--cik",         help="Single filer CIK to ingest")
    ap.add_argument("--seed",        action="store_true", help="Ingest all seed filers")
    ap.add_argument("--latest-only", action="store_true", help="Only ingest most recent filing per filer")
    ap.add_argument("--since",       help="Only ingest filings filed on/after this date (YYYY-MM-DD)")
    ap.add_argument("--db",          help="Path to SQLite database", default=str(database.DB_PATH))
    args = ap.parse_args()

    db_path = Path(args.db)
    database.init_db(db_path)

    filers_to_ingest: list[str] = []

    if args.cik:
        filers_to_ingest.append(args.cik)

    if args.seed:
        filers_to_ingest.extend(cik for cik, _ in edgar.SEED_FILERS)

    if not filers_to_ingest:
        print("Specify --cik <CIK> or --seed. Run with -h for help.")
        sys.exit(1)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_filers = []
    for c in filers_to_ingest:
        if c not in seen:
            seen.add(c)
            unique_filers.append(c)

    for cik in unique_filers:
        try:
            ingest_filer(
                cik,
                latest_only=args.latest_only,
                since=args.since,
                db_path=db_path,
            )
        except Exception as exc:
            print(f"  [ERROR] CIK {cik}: {exc}")

    print("\nDone.")


if __name__ == "__main__":
    main()
