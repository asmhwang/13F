"""
Offline CUSIP resolution passes — no network required.

OpenFIGI/Polygon map *current* CUSIPs; a historical 13F database is full of
dead ones (pre-split Citigroup, old GE, Priceline, acquired names), which is
why ~26% of holding value was flagged cusip_unresolved. Three local passes
recover the recoverable ones from data already in the database:

  Pass 1 — prefix-8 chain: same issuer + issue code, different check digit.
           Essentially the same security; map to the resolved sibling's ticker.
  Pass 2 — prefix-6 chain: same issuer, different issue. Mapped only when the
           issuer resolves to exactly ONE distinct clean ticker (avoids
           guessing between share classes).
  Pass 3 — name match: normalize holdings' name_of_issuer text (legacy filings
           embed table fragments like 'SOLE 69259 0 144457 GENERAL ELECTRIC
           COMPANY COMM') and map names that resolve to exactly one ticker.

Every mapping is recorded in cusip_resolution_log with its pass + evidence so
it can be audited or reverted. Run directly:

    python3 -m pipeline.cusip_local            # apply all passes
    python3 -m pipeline.cusip_local --dry-run  # report only
"""

import re
import sqlite3
from pathlib import Path

from pipeline.database import DB_PATH, get_connection, upsert_security

# Tokens stripped during name normalization (suffixes/noise, conservative).
_NOISE_TOKENS = {
    "COM", "COMM", "COMMON", "STK", "STOCK", "SHS", "SHARES", "SH",
    "CL", "CLASS", "NEW", "DEL", "ADR", "ADS", "SP", "SPONSORED",
    "INC", "CORP", "CORPORATION", "CO", "COMPANY", "PLC", "LTD", "LLC",
    "LP", "THE", "HLDGS", "HOLDINGS", "GRP", "USD",
}
# Investment-discretion keywords leaked into legacy name fields.
_DISCRETION_TOKENS = {"SOLE", "SHARED", "DFND", "DEFINED", "NONE", "OTR", "OTHER"}

_MIN_NAME_KEY_LEN = 6      # don't index tiny ambiguous keys
_MIN_NAME_HIT_LEN = 8      # require at least one strong key to map a cusip

# Real US equity ticker shape: 1-5 letters, optional class suffix (BRK/B,
# BF.B). Rejects currency-suffixed junk from earlier resolver runs
# (BVSNEUR, ARQLUSD) and FIGI-style codes.
_TICKER_RE = re.compile(r"^[A-Z]{1,5}([./-][A-Z])?$")

# Non-common-equity markers in issuer names: a CUSIP whose names mention any
# of these is a preferred/note/warrant/unit — mapping it to the issuer's
# common ticker would price the wrong security.
_NON_COMMON_RE = re.compile(
    r"PFD|PREF|%|\bNT\b|\bNTS\b|NOTE|DEBENT|\bDEB\b|BOND|\bBD\b|"
    r"\bWT\b|\bWTS\b|WARR|\bUNIT\b|\bUTS\b|RIGHT|\bRT\b|\bRTS\b|CONV\b")


def _is_clean_ticker(t: str | None) -> bool:
    return bool(t) and bool(_TICKER_RE.match(t))


def _is_equity_cusip(cusip: str) -> bool:
    """Equity issue codes (chars 7-8) are numeric; bonds use letters (e.g.
    126349AB5 = CSG Systems debt, not stock)."""
    return len(cusip) >= 8 and cusip[6].isdigit() and cusip[7].isdigit()


def _looks_non_common(conn: sqlite3.Connection, cusip: str) -> bool:
    """True if any filer's name for this CUSIP marks it preferred/debt/etc."""
    rows = conn.execute(
        "SELECT DISTINCT name_of_issuer FROM holdings WHERE cusip = ? "
        "AND name_of_issuer IS NOT NULL", (cusip,)).fetchall()
    return any(_NON_COMMON_RE.search(r[0].upper()) for r in rows)


def normalize_issuer_name(raw: str | None) -> str | None:
    """Clean a holdings name_of_issuer into a match key, or None if hopeless.

    Handles legacy text-format leakage: drops pure-number tokens and leading
    discretion keywords, strips punctuation, removes suffix noise tokens.
    """
    if not raw:
        return None
    s = re.sub(r"[^A-Za-z0-9& ]+", " ", raw.upper())
    tokens = [t for t in s.split() if not t.isdigit()]
    while tokens and tokens[0] in _DISCRETION_TOKENS:
        tokens.pop(0)
    tokens = [t for t in tokens if t not in _NOISE_TOKENS]
    key = " ".join(tokens)
    if len(key) < _MIN_NAME_KEY_LEN:
        return None
    return key


def _init_log(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cusip_resolution_log (
            cusip       TEXT PRIMARY KEY,
            ticker      TEXT NOT NULL,
            method      TEXT NOT NULL,     -- 'prefix8' | 'prefix6' | 'name'
            evidence    TEXT,              -- sibling cusip or matched name key
            resolved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
    conn.commit()


def _unresolved_cusips(conn: sqlite3.Connection) -> list[str]:
    """Equity-issue CUSIPs present in holdings with no valid ticker, skipping
    preferred/debt/warrant rows that must not map to a common ticker."""
    rows = conn.execute(
        """
        SELECT DISTINCT h.cusip
        FROM holdings h
        LEFT JOIN securities s ON s.cusip = h.cusip
        WHERE h.cusip IS NOT NULL AND LENGTH(h.cusip) >= 8
          AND (s.ticker IS NULL OR s.ticker = '' OR s.ticker GLOB '*[0-9]*')
        """).fetchall()
    out = []
    for (cusip,) in rows:
        if not _is_equity_cusip(cusip):
            continue
        if _looks_non_common(conn, cusip):
            continue
        out.append(cusip)
    return out


def _resolved_by_prefix(conn: sqlite3.Connection, plen: int) -> dict[str, dict]:
    """{prefix: {'tickers': set, 'example': (cusip, ticker, name)}} for clean
    resolved securities."""
    rows = conn.execute(
        """
        SELECT cusip, ticker, name FROM securities
        WHERE ticker IS NOT NULL AND ticker <> '' AND LENGTH(cusip) >= 8
        """).fetchall()
    out: dict[str, dict] = {}
    for cusip, ticker, name in rows:
        if not _is_clean_ticker(ticker) or not _is_equity_cusip(cusip):
            continue
        p = cusip[:plen]
        slot = out.setdefault(p, {"tickers": set(), "example": None})
        slot["tickers"].add(ticker)
        if slot["example"] is None or slot["example"][1] != ticker:
            slot["example"] = (cusip, ticker, name)
    return out


def pass_prefix(conn: sqlite3.Connection, unresolved: list[str], plen: int,
                method: str, dry_run: bool) -> dict[str, str]:
    """Map unresolved CUSIPs whose prefix matches exactly one resolved ticker."""
    index = _resolved_by_prefix(conn, plen)
    mapped: dict[str, str] = {}
    for cusip in unresolved:
        slot = index.get(cusip[:plen])
        if not slot or len(slot["tickers"]) != 1:
            continue
        sib_cusip, ticker, name = slot["example"]
        mapped[cusip] = ticker
        if not dry_run:
            upsert_security(conn, cusip, ticker=ticker, name=name, exchange=None)
            conn.execute(
                "INSERT INTO cusip_resolution_log(cusip, ticker, method, evidence) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(cusip) DO UPDATE SET "
                "ticker = excluded.ticker, method = excluded.method, "
                "evidence = excluded.evidence, resolved_at = CURRENT_TIMESTAMP",
                (cusip, ticker, method, sib_cusip))
    if not dry_run:
        conn.commit()
    return mapped


def _name_index(conn: sqlite3.Connection) -> dict[str, set[str]]:
    """normalized name -> set of clean tickers, built from (a) securities
    names and (b) holdings names of already-resolved CUSIPs."""
    index: dict[str, set[str]] = {}
    for name, ticker in conn.execute(
            "SELECT name, ticker FROM securities "
            "WHERE ticker IS NOT NULL AND ticker <> '' AND name IS NOT NULL"):
        key = normalize_issuer_name(name)
        if key and _is_clean_ticker(ticker):
            index.setdefault(key, set()).add(ticker)
    for name, ticker in conn.execute(
            """
            SELECT DISTINCT h.name_of_issuer, s.ticker
            FROM holdings h JOIN securities s ON s.cusip = h.cusip
            WHERE s.ticker IS NOT NULL AND s.ticker <> ''
              AND h.name_of_issuer IS NOT NULL
            """):
        key = normalize_issuer_name(name)
        if key and _is_clean_ticker(ticker):
            index.setdefault(key, set()).add(ticker)
    return index


def pass_name(conn: sqlite3.Connection, unresolved: list[str],
              dry_run: bool) -> dict[str, str]:
    """Map unresolved CUSIPs whose normalized issuer name matches exactly one
    resolved ticker."""
    index = _name_index(conn)
    mapped: dict[str, str] = {}
    for cusip in unresolved:
        names = [r[0] for r in conn.execute(
            "SELECT DISTINCT name_of_issuer FROM holdings WHERE cusip = ? "
            "AND name_of_issuer IS NOT NULL", (cusip,)).fetchall()]
        tickers: set[str] = set()
        hit_key = None
        best_hit_len = 0
        for raw in names:
            key = normalize_issuer_name(raw)
            if key and key in index:
                tickers |= index[key]
                hit_key = key
                best_hit_len = max(best_hit_len, len(key))
        # Require agreement on exactly one ticker AND at least one strong
        # (long) matching key — short keys collide too easily.
        if len(tickers) != 1 or best_hit_len < _MIN_NAME_HIT_LEN:
            continue
        ticker = next(iter(tickers))
        mapped[cusip] = ticker
        if not dry_run:
            upsert_security(conn, cusip, ticker=ticker,
                            name=hit_key, exchange=None)
            conn.execute(
                "INSERT INTO cusip_resolution_log(cusip, ticker, method, evidence) "
                "VALUES (?, ?, 'name', ?) ON CONFLICT(cusip) DO UPDATE SET "
                "ticker = excluded.ticker, method = excluded.method, "
                "evidence = excluded.evidence, resolved_at = CURRENT_TIMESTAMP",
                (cusip, ticker, hit_key))
    if not dry_run:
        conn.commit()
    return mapped


def resolve_local(db_path: Path = DB_PATH, dry_run: bool = False) -> dict:
    """Run all passes. Returns counts per pass."""
    conn = get_connection(db_path)
    try:
        _init_log(conn)
        unresolved = _unresolved_cusips(conn)
        total0 = len(unresolved)

        m8 = pass_prefix(conn, unresolved, 8, "prefix8", dry_run)
        unresolved = [c for c in unresolved if c not in m8]
        m6 = pass_prefix(conn, unresolved, 6, "prefix6", dry_run)
        unresolved = [c for c in unresolved if c not in m6]
        mn = pass_name(conn, unresolved, dry_run)
        unresolved = [c for c in unresolved if c not in mn]

        return {
            "unresolved_before": total0,
            "prefix8": len(m8),
            "prefix6": len(m6),
            "name": len(mn),
            "unresolved_after": len(unresolved),
            "dry_run": dry_run,
        }
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent))
    ap = argparse.ArgumentParser(description="Offline CUSIP resolution passes")
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    print(resolve_local(Path(args.db), dry_run=args.dry_run))
