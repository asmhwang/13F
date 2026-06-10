"""
Fund ranking pipeline v2 — "coattail clone" methodology.

For every pair of consecutive quarterly filings, simulate buying the fund's
filed portfolio at its disclosure date (original filed_date) and holding it
until the next disclosure. Each window's value-weighted return minus the
S&P 500 TR over the same dates is one NON-OVERLAPPING observation of
stock-selection skill. This replaces v1's overlapping 3-year windows, whose
autocorrelation broke the consistency / one-hit-wonder / lambda-decay logic.

Skill score = shrunk annualized information ratio:

    mean_shrunk = mean_excess * n / (n + K)        (empirical-Bayes toward 0)
    shrunk_ir_annual = mean_shrunk / stdev_excess * sqrt(4)

Shrinkage handles short histories without arbitrary cliffs; the IR form makes
"consistency" intrinsic (volatile excess -> lower score) instead of a bolted-on
percentile. Final 0-100 score is a percentile rank, not min-max, so one outlier
fund cannot compress everyone else.

Soft weeding: no absolute position-size gate (v1's $200M cap expelled funds for
growing). Funds are ranked whenever they have >= MIN_WINDOWS valid windows;
size/concentration are descriptive columns the UI can filter on.

V1 tables are untouched; v2 writes fund_clone_windows_v2 / fund_rankings_v2.
"""

import sqlite3
import statistics
from pathlib import Path

from pipeline.database import DB_PATH, ensure_effective_filings, get_connection
from pipeline.scoring import adapter

_SCHEMA_V2 = Path(__file__).parent / "schema_v2.sql"

MIN_WINDOWS = 12          # ~3 years of quarterly observations to be ranked
SHRINK_K = 8              # pseudo-observations of zero skill (empirical Bayes)
MIN_COVERAGE = 0.6        # priced value / total equity value for a valid window
MAX_PERIOD_GAP_DAYS = 100 # consecutive calendar quarters only (no skipped qtrs)
MAX_BASE_STALENESS = 10   # base price must be within N days of window start
ANNUALIZE = 2.0           # sqrt(4 quarters)


# ---------------------------------------------------------------------------
# Pure scoring math (unit-testable, no DB)
# ---------------------------------------------------------------------------

def shrunk_ir(excesses: list[float], k: int = SHRINK_K,
              annualize: float = ANNUALIZE) -> dict | None:
    """Skill stats from a list of per-window excess returns.

    Returns None when there is no dispersion (stdev 0) or < 2 observations.
    """
    n = len(excesses)
    if n < 2:
        return None
    mean = statistics.fmean(excesses)
    sd = statistics.stdev(excesses)
    if sd == 0:
        return None
    mean_shrunk = mean * n / (n + k)
    return {
        "mean": mean,
        "stdev": sd,
        "n": n,
        "t_stat": mean / (sd / n ** 0.5),
        "ir_annual": mean / sd * annualize,
        "shrunk_ir_annual": mean_shrunk / sd * annualize,
        "win_rate": sum(1 for e in excesses if e > 0) / n,
    }


def percentile_scores(values: dict[str, float]) -> dict[str, float]:
    """0-100 percentile rank per key (100 = best). Single entry -> 100."""
    n = len(values)
    if n == 0:
        return {}
    if n == 1:
        return {k: 100.0 for k in values}
    ordered = sorted(values.values())
    return {k: 100.0 * sum(1 for x in ordered if x < v) / (n - 1)
            for k, v in values.items()}


def _days_between(a: str, b: str) -> int:
    from datetime import date
    ya, ma, da = (int(x) for x in a.split("-"))
    yb, mb, db = (int(x) for x in b.split("-"))
    return (date(yb, mb, db) - date(ya, ma, da)).days


def _is_resolved_ticker(ticker: str | None) -> bool:
    if not ticker:
        return False
    return not any(ch in "0123456789" for ch in ticker)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _equity_positions(conn: sqlite3.Connection, cik: str, period: str
                      ) -> list[tuple[str, bool, float]]:
    """[(ticker_or_cusip, resolved, value_usd)] aggregated per resolved ticker."""
    ids = adapter.effective_filing_ids(conn, cik, period)
    if not ids:
        return []
    ph = ",".join("?" * len(ids))
    rows = conn.execute(
        f"""
        SELECT h.cusip, MAX(s.ticker) AS ticker,
               SUM(h.value_thousands) * 1000.0 AS v
        FROM holdings h
        LEFT JOIN securities s ON s.cusip = h.cusip
        WHERE h.filing_id IN ({ph})
          AND (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
        GROUP BY h.cusip
        """, ids).fetchall()
    agg: dict[str, list] = {}
    for cusip, ticker, v in rows:
        resolved = _is_resolved_ticker(ticker)
        key = ticker if resolved else cusip
        slot = agg.setdefault(key, [resolved, 0.0])
        slot[1] += v
    return [(k, s[0], s[1]) for k, s in agg.items()]


def _window_return(conn: sqlite3.Connection, cik: str, period: str,
                   start: str, end: str) -> dict:
    """Clone return for one window. Positions with no usable base price are
    dropped and weights renormalized; coverage records how much survived."""
    positions = _equity_positions(conn, cik, period)
    total_value = sum(v for _, _, v in positions)
    priced: list[tuple[float, float]] = []     # (value, return)
    for key, resolved, value in positions:
        if not resolved:
            continue
        base = adapter.price_asof(conn, key, start)
        if base is None or base[1] == 0:
            continue
        if _days_between(base[0], start) > MAX_BASE_STALENESS:
            continue                            # not trading at window start
        fwd = adapter.price_asof(conn, key, end)
        if fwd is None:
            continue
        # fwd falls back to the last traded price — delistings/acquisitions
        # resolve to their final price naturally.
        priced.append((value, (fwd[1] - base[1]) / base[1]))
    priced_value = sum(v for v, _ in priced)
    coverage = priced_value / total_value if total_value > 0 else 0.0
    clone = (sum(v * r for v, r in priced) / priced_value
             if priced_value > 0 else None)
    return {
        "clone": clone,
        "coverage": coverage,
        "positions_priced": len(priced),
        "positions_total": len(positions),
    }


def _benchmark_window(conn: sqlite3.Connection, start: str, end: str) -> float | None:
    a = adapter.benchmark_asof(conn, start)
    b = adapter.benchmark_asof(conn, end)
    if a is None or b is None or a[1] == 0:
        return None
    return (b[1] - a[1]) / a[1]


def _descriptives(conn: sqlite3.Connection, cik: str, periods: list[str]) -> dict:
    """Median position count / AUM / top-10 concentration across filed quarters."""
    counts: list[int] = []
    aums: list[float] = []
    top10: list[float] = []
    for period in periods:
        positions = _equity_positions(conn, cik, period)
        if not positions:
            continue
        values = sorted((v for _, _, v in positions), reverse=True)
        total = sum(values)
        counts.append(len(values))
        aums.append(total)
        if total > 0:
            top10.append(sum(values[:10]) / total)
    return {
        "median_positions": statistics.median(counts) if counts else None,
        "median_aum": statistics.median(aums) if aums else None,
        "top10_weight_med": statistics.median(top10) if top10 else None,
    }


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def compute_clone_windows(conn: sqlite3.Connection) -> None:
    """Populate fund_clone_windows_v2 for every fund."""
    funds = [r[0] for r in conn.execute("SELECT cik FROM filers").fetchall()]
    for cik in funds:
        periods = [r[0] for r in conn.execute(
            "SELECT DISTINCT period_of_report FROM filings WHERE cik = ? "
            "ORDER BY period_of_report", (cik,)).fetchall()]
        for p_start, p_end in zip(periods, periods[1:]):
            if _days_between(p_start, p_end) > MAX_PERIOD_GAP_DAYS:
                continue                        # skipped quarter -> no window
            start = adapter.original_filed_date(conn, cik, p_start)
            end = adapter.original_filed_date(conn, cik, p_end)
            valid, reason = 1, None
            clone = bench = excess = None
            coverage = 0.0
            n_priced = n_total = 0
            if start is None or end is None or end <= start:
                valid, reason = 0, "bad_window"
            else:
                w = _window_return(conn, cik, p_start, start, end)
                clone, coverage = w["clone"], w["coverage"]
                n_priced, n_total = w["positions_priced"], w["positions_total"]
                bench = _benchmark_window(conn, start, end)
                if clone is None or coverage < MIN_COVERAGE:
                    valid, reason = 0, "low_coverage"
                elif bench is None:
                    valid, reason = 0, "no_benchmark"
                else:
                    excess = clone - bench
            conn.execute(
                """
                INSERT INTO fund_clone_windows_v2
                    (fund_id, start_period, end_period, start_date, end_date,
                     clone_return, benchmark_return, excess_return, coverage,
                     positions_priced, positions_total, valid, invalid_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fund_id, start_period) DO UPDATE SET
                    end_period = excluded.end_period,
                    start_date = excluded.start_date, end_date = excluded.end_date,
                    clone_return = excluded.clone_return,
                    benchmark_return = excluded.benchmark_return,
                    excess_return = excluded.excess_return,
                    coverage = excluded.coverage,
                    positions_priced = excluded.positions_priced,
                    positions_total = excluded.positions_total,
                    valid = excluded.valid, invalid_reason = excluded.invalid_reason
                """,
                (cik, p_start, p_end, start, end, clone, bench, excess,
                 coverage, n_priced, n_total, valid, reason))
    conn.commit()


def compute_rankings(conn: sqlite3.Connection) -> None:
    """Score every fund from its valid windows; rank by shrunk IR percentile."""
    funds = conn.execute("SELECT cik, name FROM filers").fetchall()
    scored: dict[str, dict] = {}
    unscored: dict[str, str] = {}
    for cik, name in funds:
        rows = conn.execute(
            "SELECT start_period, excess_return, coverage FROM fund_clone_windows_v2 "
            "WHERE fund_id = ? AND valid = 1 ORDER BY start_period", (cik,)).fetchall()
        excesses = [r["excess_return"] for r in rows]
        if len(excesses) < MIN_WINDOWS:
            unscored[cik] = "insufficient_windows"
            continue
        stats = shrunk_ir(excesses)
        if stats is None:
            unscored[cik] = "no_dispersion"
            continue
        periods = [r[0] for r in conn.execute(
            "SELECT DISTINCT period_of_report FROM filings WHERE cik = ? "
            "ORDER BY period_of_report", (cik,)).fetchall()]
        desc = _descriptives(conn, cik, periods)
        scored[cik] = {
            "name": name, **stats, **desc,
            "recent_4q_excess": sum(excesses[-4:]),
            "avg_coverage": statistics.fmean(r["coverage"] for r in rows),
            "first_window": rows[0]["start_period"],
            "last_window": rows[-1]["start_period"],
        }

    pct = percentile_scores({c: s["shrunk_ir_annual"] for c, s in scored.items()})
    ranked = sorted(scored, key=lambda c: scored[c]["shrunk_ir_annual"], reverse=True)
    for rank, cik in enumerate(ranked, start=1):
        s = scored[cik]
        conn.execute(
            """
            INSERT INTO fund_rankings_v2
                (fund_id, fund_name, rank, score, shrunk_ir_annual, ir_annual,
                 t_stat, mean_excess_q, stdev_excess_q, n_windows, win_rate,
                 recent_4q_excess, avg_coverage, median_positions, median_aum,
                 top10_weight_med, first_window, last_window, eligible, fail_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL)
            """,
            (cik, s["name"], rank, pct[cik], s["shrunk_ir_annual"], s["ir_annual"],
             s["t_stat"], s["mean"], s["stdev"], s["n"], s["win_rate"],
             s["recent_4q_excess"], s["avg_coverage"], s["median_positions"],
             s["median_aum"], s["top10_weight_med"], s["first_window"],
             s["last_window"]))
    for cik, reason in unscored.items():
        name = next(n for c, n in funds if c == cik)
        conn.execute(
            "INSERT INTO fund_rankings_v2 (fund_id, fund_name, eligible, fail_reason) "
            "VALUES (?, ?, 0, ?)", (cik, name, reason))
    conn.commit()


def run_fund_pipeline_v2(db_path: Path = DB_PATH) -> dict:
    conn = get_connection(db_path)
    try:
        conn.executescript(_SCHEMA_V2.read_text())
        ensure_effective_filings(conn)
        for t in ("fund_clone_windows_v2", "fund_rankings_v2"):
            conn.execute(f"DELETE FROM {t}")
        conn.commit()
        compute_clone_windows(conn)
        compute_rankings(conn)
        ranked = conn.execute(
            "SELECT COUNT(*) FROM fund_rankings_v2 WHERE eligible = 1").fetchone()[0]
        windows = conn.execute(
            "SELECT COUNT(*) FROM fund_clone_windows_v2 WHERE valid = 1").fetchone()[0]
        return {"ranked": ranked, "valid_windows": windows}
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    ap = argparse.ArgumentParser(description="Run the v2 fund ranking pipeline")
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()
    print(run_fund_pipeline_v2(Path(args.db)))
