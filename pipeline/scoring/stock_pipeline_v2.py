"""
Stock ranking pipeline v2 — "best ideas of skilled managers".

V1 tried to find consensus among top funds and score it with an OLS regression.
At this data scale (most universe stocks held by exactly one fund) consensus is
structurally absent, and the regression was circular (funds qualified on the
same forward returns it was trained to predict). V2 drops both: a stock's score
is the sum, over positive-skill ranked funds holding it, of

    skill(fund) x conviction(fund, stock) x recency_mult x tenure_mult

where skill = max(shrunk_ir_annual, 0) from fund_rankings_v2, conviction is the
position's weight relative to the fund's median position weight (how emphasized
it is within that book, capped), recency rewards fresh buys/adds and discounts
trims, and tenure adds a small boost for positions held many quarters.
Transparent, defensible with a single holder, no fitted coefficients.

V1 tables are untouched; v2 writes stock_rankings_v2.
"""

import sqlite3
import statistics
from pathlib import Path

from pipeline.database import DB_PATH, ensure_effective_filings, get_connection
from pipeline.scoring import adapter
from pipeline.scoring.fund_pipeline_v2 import percentile_scores

_SCHEMA_V2 = Path(__file__).parent / "schema_v2.sql"

CONVICTION_CAP = 8.0      # max weight / median-weight multiple
MAX_BACKER_POSITIONS = 100  # median positions; index-scale filers (FMR, TROW)
                            # have skill scores but their books aren't "ideas"
NEW_BUY_MULT = 1.25
ADD_MULT = 1.10
TRIM_MULT = 0.85
ADD_TRIM_THRESHOLD = 0.20  # >=20% value change counts as add/trim
TENURE_BOOST = 0.02        # per quarter held, capped at TENURE_CAP quarters
TENURE_CAP = 10
PRICE_FRESH_DAYS = 7


# ---------------------------------------------------------------------------
# Pure scoring math (unit-testable, no DB)
# ---------------------------------------------------------------------------

def conviction_multiple(weight: float, median_weight: float,
                        cap: float = CONVICTION_CAP) -> float:
    """How emphasized a position is within its own book: weight / median
    position weight, capped. 1.0 = a typical position for this fund."""
    if median_weight <= 0:
        return 0.0
    return min(weight / median_weight, cap)


def recency_multiplier(now_value: float, prior_value: float | None,
                       has_prior_filing: bool) -> float:
    """NEW position > add > hold > trim. No prior filing -> neutral."""
    if not has_prior_filing:
        return 1.0
    if prior_value is None or prior_value == 0:
        return NEW_BUY_MULT
    change = (now_value - prior_value) / prior_value
    if change >= ADD_TRIM_THRESHOLD:
        return ADD_MULT
    if change <= -ADD_TRIM_THRESHOLD:
        return TRIM_MULT
    return 1.0


def tenure_multiplier(quarters_held: int) -> float:
    return 1.0 + TENURE_BOOST * min(quarters_held, TENURE_CAP)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def backer_funds(conn: sqlite3.Connection) -> dict[str, dict]:
    """Ranked v2 funds with positive skill whose books are concentrated enough
    that a position reflects an idea: {fund_id: {name, skill}}."""
    rows = conn.execute(
        "SELECT fund_id, fund_name, shrunk_ir_annual FROM fund_rankings_v2 "
        "WHERE eligible = 1 AND shrunk_ir_annual > 0 "
        "AND median_positions <= ?", (MAX_BACKER_POSITIONS,)).fetchall()
    return {r["fund_id"]: {"name": r["fund_name"], "skill": r["shrunk_ir_annual"]}
            for r in rows}


def _equity_holdings(conn: sqlite3.Connection, cik: str, period: str) -> dict[str, float]:
    """{ticker: value_usd} for the effective filing set (resolved tickers only)."""
    ids = adapter.effective_filing_ids(conn, cik, period)
    if not ids:
        return {}
    ph = ",".join("?" * len(ids))
    rows = conn.execute(
        f"""
        SELECT s.ticker AS ticker, SUM(h.value_thousands) * 1000.0 AS v
        FROM holdings h JOIN securities s ON s.cusip = h.cusip
        WHERE h.filing_id IN ({ph})
          AND s.ticker IS NOT NULL AND s.ticker <> ''
          AND s.ticker NOT GLOB '*[0-9]*'
          AND (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
        GROUP BY s.ticker
        """, ids).fetchall()
    return {r["ticker"]: r["v"] for r in rows}


def _fund_periods(conn: sqlite3.Connection, cik: str) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT period_of_report FROM filings WHERE cik = ? "
        "ORDER BY period_of_report DESC", (cik,)).fetchall()]


def _tenure(periods_desc: list[str], holdings_by_period: dict[str, dict[str, float]],
            ticker: str, period: str) -> int:
    """Consecutive filed quarters holding `ticker`, counting back from `period`."""
    count = 0
    for p in periods_desc:
        if p > period:
            continue
        if ticker in holdings_by_period.get(p, {}):
            count += 1
        else:
            break
    return count


def _price_fresh(conn: sqlite3.Connection, ticker: str, as_of: str) -> int:
    row = conn.execute(
        "SELECT MAX(date) FROM prices WHERE ticker = ? AND date <= ? "
        "AND adj_close IS NOT NULL", (ticker, as_of)).fetchone()
    if not row or row[0] is None:
        return 0
    gap = conn.execute(
        "SELECT julianday(?) - julianday(?)", (as_of, row[0])).fetchone()[0]
    return 1 if gap <= PRICE_FRESH_DAYS else 0


def _company_name(conn: sqlite3.Connection, ticker: str) -> str:
    r = conn.execute(
        "SELECT name FROM securities WHERE ticker = ? AND name IS NOT NULL LIMIT 1",
        (ticker,)).fetchone()
    return r[0] if r else ticker


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_stock_pipeline_v2(db_path: Path = DB_PATH) -> dict:
    conn = get_connection(db_path)
    try:
        conn.executescript(_SCHEMA_V2.read_text())
        ensure_effective_filings(conn)
        conn.execute("DELETE FROM stock_rankings_v2")
        conn.commit()

        cq = adapter.current_quarter_date(conn)
        backers = backer_funds(conn)
        if cq is None or not backers:
            return {"universe": 0}

        # Per-backer holdings history (for tenure + prior-quarter comparison).
        hist: dict[str, dict[str, dict[str, float]]] = {}
        periods_desc: dict[str, list[str]] = {}
        for cik in backers:
            periods_desc[cik] = _fund_periods(conn, cik)
            hist[cik] = {p: _equity_holdings(conn, cik, p) for p in periods_desc[cik]}

        # Aggregate per-stock contributions.
        stocks: dict[str, dict] = {}
        for cik, info in backers.items():
            cur = hist[cik].get(cq, {})
            total = sum(cur.values())
            if not cur or total <= 0:
                continue                      # fund has not filed for cq
            weights = {t: v / total for t, v in cur.items()}
            med_w = statistics.median(weights.values())
            prior_p = next((p for p in periods_desc[cik] if p < cq), None)
            prior = hist[cik].get(prior_p, {}) if prior_p else {}
            for ticker, w in weights.items():
                conv = conviction_multiple(w, med_w)
                rec = recency_multiplier(cur[ticker], prior.get(ticker),
                                         has_prior_filing=bool(prior_p))
                ten_q = _tenure(periods_desc[cik], hist[cik], ticker, cq)
                contrib = info["skill"] * conv * rec * tenure_multiplier(ten_q)
                s = stocks.setdefault(ticker, {
                    "raw_score": 0.0, "n_backers": 0, "weights": [],
                    "tenures": [], "new_buys": 0, "adds": 0, "trims": 0,
                    "top_fund_id": None, "top_fund_name": None, "top_fund_skill": -1.0,
                })
                s["raw_score"] += contrib
                s["n_backers"] += 1
                s["weights"].append(w)
                s["tenures"].append(ten_q)
                if rec == NEW_BUY_MULT:
                    s["new_buys"] += 1
                elif rec == ADD_MULT:
                    s["adds"] += 1
                elif rec == TRIM_MULT:
                    s["trims"] += 1
                if info["skill"] > s["top_fund_skill"]:
                    s["top_fund_id"] = cik
                    s["top_fund_name"] = info["name"]
                    s["top_fund_skill"] = info["skill"]

        pct = percentile_scores({t: s["raw_score"] for t, s in stocks.items()})
        ranked = sorted(stocks, key=lambda t: stocks[t]["raw_score"], reverse=True)
        for rank, ticker in enumerate(ranked, start=1):
            s = stocks[ticker]
            f = conn.execute(
                "SELECT market_cap, pe_ratio, pe_available FROM fundamentals "
                "WHERE ticker = ? AND as_of_date = ?", (ticker, cq)).fetchone()
            sector = (conn.execute(
                "SELECT sector FROM sectors WHERE ticker = ?", (ticker,)).fetchone()
                or ["Unknown"])[0]
            conn.execute(
                """
                INSERT INTO stock_rankings_v2
                    (ticker, company_name, sector, rank, score, raw_score,
                     n_backers, top_fund_id, top_fund_name, top_fund_skill,
                     max_weight, avg_weight, new_buys, adds, trims, avg_tenure,
                     market_cap, pe_ratio, pe_available, price_fresh, as_of_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ticker, _company_name(conn, ticker), sector, rank, pct[ticker],
                 s["raw_score"], s["n_backers"], s["top_fund_id"],
                 s["top_fund_name"], s["top_fund_skill"], max(s["weights"]),
                 statistics.fmean(s["weights"]), s["new_buys"], s["adds"],
                 s["trims"], statistics.fmean(s["tenures"]),
                 (f["market_cap"] if f else None), (f["pe_ratio"] if f else None),
                 (f["pe_available"] if f else None),
                 _price_fresh(conn, ticker, cq), cq))
        conn.commit()
        return {"universe": len(stocks), "backers": len(backers)}
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    ap = argparse.ArgumentParser(description="Run the v2 stock ranking pipeline")
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()
    print(run_stock_pipeline_v2(Path(args.db)))
