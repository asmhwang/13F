"""
Fund ranking pipeline (developer spec stages 1-7). Each stage reads from and
writes to SQLite result tables; run_fund_pipeline runs them in order.

All scoring uses the as-of = filed_date convention (see adapter.py).
"""

import sqlite3
import statistics
from datetime import date
from pathlib import Path

from pipeline.database import DB_PATH, get_connection
from pipeline.prices import _plus_three_years
from pipeline.scoring import adapter

_LAMBDA = 0.85
_MIN_SCOREABLE_QUARTERS = 6
_POSITION_LIMIT_THOUSANDS = 100_000      # $100M
_MAX_POSITIONS = 30
_OHW_THRESHOLD = 0.50
_OHW_DISCOUNT = 0.75


def _equity_filter() -> str:
    return "(h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0"


def weed_funds(conn: sqlite3.Connection) -> None:
    """Stage 1 — populate fund_eligibility for every filer."""
    cq = adapter.current_quarter_date(conn)
    five_years_ago = conn.execute("SELECT date('now', '-5 years')").fetchone()[0]
    funds = conn.execute("SELECT cik FROM filers").fetchall()
    for (cik,) in funds:
        span = conn.execute(
            "SELECT MIN(period_of_report), MAX(period_of_report) "
            "FROM filings WHERE cik = ?", (cik,)).fetchone()
        first_q, last_q = span[0], span[1]
        npos = maxval = None
        lf = adapter.latest_filing_id(conn, cik, cq) if cq else None
        if lf is not None:
            agg = conn.execute(
                f"SELECT COUNT(DISTINCT h.cusip), MAX(h.value_thousands) "
                f"FROM holdings h WHERE h.filing_id = ? AND {_equity_filter()}",
                (lf,)).fetchone()
            npos, maxval = agg[0], agg[1]

        reason = None
        if maxval is not None and maxval > _POSITION_LIMIT_THOUSANDS:
            reason = "position_too_large"
        elif npos is not None and npos > _MAX_POSITIONS:
            reason = "too_many_positions"
        elif first_q is None or first_q > five_years_ago:
            reason = "insufficient_history"
        elif last_q is None or cq is None or last_q < cq:
            reason = "inactive"

        conn.execute(
            """
            INSERT INTO fund_eligibility(fund_id, eligible, fail_reason)
            VALUES (?, ?, ?)
            ON CONFLICT(fund_id) DO UPDATE SET
                eligible = excluded.eligible, fail_reason = excluded.fail_reason
            """,
            (cik, 1 if reason is None else 0, reason))
    conn.commit()


def _is_resolved_ticker(ticker: str | None) -> bool:
    """A usable US equity ticker: non-empty and contains no digit.

    ASCII-only digit check so this matches the SQL GLOB '*[0-9]*' filter exactly.
    """
    if not ticker:
        return False
    return not any(ch in "0123456789" for ch in ticker)


def compute_holding_returns(conn: sqlite3.Connection) -> None:
    """Stage 2 — per-holding 3yr forward return for eligible funds.

    Drives off the latest (superseding) filing per period so that amendments
    replace originals rather than racing on last-write-wins order.
    """
    today = date.today().isoformat()
    eligible = [r[0] for r in conn.execute(
        "SELECT fund_id FROM fund_eligibility WHERE eligible = 1").fetchall()]
    for cik in eligible:
        periods = conn.execute(
            "SELECT DISTINCT period_of_report FROM filings WHERE cik = ?", (cik,)).fetchall()
        for (period,) in periods:
            fid = adapter.latest_filing_id(conn, cik, period)
            if fid is None:
                continue
            filed = conn.execute(
                "SELECT filed_date FROM filings WHERE id = ?", (fid,)).fetchone()[0]
            if _plus_three_years(filed) > today:
                continue                       # quarter not yet scoreable
            holdings = conn.execute(
                f"""
                SELECT h.cusip, MAX(s.ticker) AS ticker,
                       SUM(h.value_thousands) * 1000.0 AS pos_value
                FROM holdings h
                LEFT JOIN securities s ON s.cusip = h.cusip
                WHERE h.filing_id = ? AND {_equity_filter()}
                GROUP BY h.cusip
                """, (fid,)).fetchall()
            for cusip, ticker, pos_value in holdings:
                if _is_resolved_ticker(ticker):
                    r = adapter.three_year_return(conn, ticker, filed)
                    if r is None:
                        ret, flag, key = None, "null_excluded", ticker
                    else:
                        ret, flag, key = r[0], r[1], ticker
                else:
                    ret, flag, key = None, "cusip_unresolved", cusip
                conn.execute(
                    """
                    INSERT INTO holding_returns
                        (fund_id, quarter_date, ticker, position_value_usd,
                         three_yr_return, data_quality_flag)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fund_id, quarter_date, ticker) DO UPDATE SET
                        position_value_usd = excluded.position_value_usd,
                        three_yr_return    = excluded.three_yr_return,
                        data_quality_flag  = excluded.data_quality_flag
                    """,
                    (cik, period, key, pos_value, ret, flag))
    conn.commit()


def _filed_date_for(conn: sqlite3.Connection, cik: str, period: str) -> str | None:
    lf = adapter.latest_filing_id(conn, cik, period)
    if lf is None:
        return None
    return conn.execute("SELECT filed_date FROM filings WHERE id = ?", (lf,)).fetchone()[0]


def compute_qps(conn: sqlite3.Connection) -> None:
    """Stage 3 — value-weighted quarterly performance score vs benchmark."""
    keys = conn.execute(
        "SELECT DISTINCT fund_id, quarter_date FROM holding_returns").fetchall()
    for cik, period in keys:
        rows = conn.execute(
            "SELECT position_value_usd, three_yr_return FROM holding_returns "
            "WHERE fund_id = ? AND quarter_date = ?", (cik, period)).fetchall()
        included = [(v, r) for (v, r) in rows if r is not None]
        excluded_null = len(rows) - len(included)
        if not included:
            continue
        total = sum(v for v, _ in included)
        if total == 0:
            continue
        raw = sum((v / total) * r for v, r in included)
        filed = _filed_date_for(conn, cik, period)
        br = adapter.benchmark_return(conn, filed) if filed else None
        excess = raw - br if br is not None else None
        conn.execute(
            """
            INSERT INTO fund_quarterly_scores
                (fund_id, quarter_date, qps_raw, qps_excess, benchmark_return,
                 positions_included, positions_excluded_null)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fund_id, quarter_date) DO UPDATE SET
                qps_raw = excluded.qps_raw, qps_excess = excluded.qps_excess,
                benchmark_return = excluded.benchmark_return,
                positions_included = excluded.positions_included,
                positions_excluded_null = excluded.positions_excluded_null
            """,
            (cik, period, raw, excess, br, len(included), excluded_null))
    conn.commit()


def compute_tws(conn: sqlite3.Connection) -> None:
    """Stage 4 — time-weighted score with one-hit-wonder discount.

    Funds with fewer than 6 scoreable quarters are demoted to ineligible with
    fail_reason 'insufficient_scoreable_quarters' and get no fund_tws row.
    """
    eligible = [r[0] for r in conn.execute(
        "SELECT fund_id FROM fund_eligibility WHERE eligible = 1").fetchall()]
    for cik in eligible:
        scores = conn.execute(
            "SELECT quarter_date, qps_excess FROM fund_quarterly_scores "
            "WHERE fund_id = ? AND qps_excess IS NOT NULL "
            "ORDER BY quarter_date DESC", (cik,)).fetchall()
        if len(scores) < _MIN_SCOREABLE_QUARTERS:
            conn.execute(
                "UPDATE fund_eligibility SET eligible = 0, "
                "fail_reason = 'insufficient_scoreable_quarters' WHERE fund_id = ?",
                (cik,))
            continue
        # scores[0] is most recent -> weight 1.0; weight decays going back
        weights = [_LAMBDA ** i for i in range(len(scores))]
        contribs = [w * s["qps_excess"] for w, s in zip(weights, scores)]
        wsum = sum(weights)
        csum = sum(contribs)
        tws = csum / wsum
        # Best-quarter-contribution / one-hit-wonder only applies when the fund's
        # cumulative weighted excess is positive; avoid dividing into a negative csum.
        best = (max(contribs) / csum) if csum > 0 else 0.0
        ohw = best > _OHW_THRESHOLD
        if ohw:
            tws *= _OHW_DISCOUNT
        oldest = scores[-1]["quarter_date"]
        conn.execute(
            """
            INSERT INTO fund_tws(fund_id, tws, quarters_scored,
                oldest_quarter_included, one_hit_wonder_flag, best_quarter_contribution)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(fund_id) DO UPDATE SET
                tws = excluded.tws, quarters_scored = excluded.quarters_scored,
                oldest_quarter_included = excluded.oldest_quarter_included,
                one_hit_wonder_flag = excluded.one_hit_wonder_flag,
                best_quarter_contribution = excluded.best_quarter_contribution
            """,
            (cik, tws, len(scores), oldest, 1 if ohw else 0, best))
    conn.commit()


def _quarter_cusips(conn: sqlite3.Connection, cik: str, period: str) -> set[str]:
    lf = adapter.latest_filing_id(conn, cik, period)
    if lf is None:
        return set()
    rows = conn.execute(
        f"SELECT DISTINCT h.cusip FROM holdings h "
        f"WHERE h.filing_id = ? AND {_equity_filter()}", (lf,)).fetchall()
    return {r[0] for r in rows}


def compute_turnover(conn: sqlite3.Connection) -> None:
    """Stage 5 — average position turnover and its score multiplier.

    Computed for funds that have a fund_tws row (fully scored funds).
    """
    funds = [r[0] for r in conn.execute(
        "SELECT fund_id FROM fund_tws").fetchall()]
    for cik in funds:
        periods = [r[0] for r in conn.execute(
            "SELECT DISTINCT period_of_report FROM filings WHERE cik = ? "
            "ORDER BY period_of_report", (cik,)).fetchall()]
        rates: list[float] = []
        prev = _quarter_cusips(conn, cik, periods[0]) if periods else set()
        for period in periods[1:]:
            cur = _quarter_cusips(conn, cik, period)
            if prev:
                dropped = len(prev - cur)
                rates.append(dropped / len(prev))
            prev = cur
        avg = sum(rates) / len(rates) if rates else 0.0
        mult = max(0.5, min(1.0, 1 - avg * 0.5))
        conn.execute(
            """
            INSERT INTO fund_turnover(fund_id, avg_turnover_rate,
                turnover_multiplier, quarter_pairs_measured)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(fund_id) DO UPDATE SET
                avg_turnover_rate = excluded.avg_turnover_rate,
                turnover_multiplier = excluded.turnover_multiplier,
                quarter_pairs_measured = excluded.quarter_pairs_measured
            """,
            (cik, avg, mult, len(rates)))
    conn.commit()


def compute_consistency(conn: sqlite3.Connection) -> None:
    """Stage 6 — relative consistency: 1 - percent_rank(stdev of excess QPS).

    Lower stdev = more consistent = higher score. Percentile is across all
    funds that have a fund_tws row, so it must run after all of them are scored.
    """
    funds = [r[0] for r in conn.execute("SELECT fund_id FROM fund_tws").fetchall()]
    stdevs: dict[str, float] = {}
    for cik in funds:
        vals = [r[0] for r in conn.execute(
            "SELECT qps_excess FROM fund_quarterly_scores "
            "WHERE fund_id = ? AND qps_excess IS NOT NULL", (cik,)).fetchall()]
        stdevs[cik] = statistics.stdev(vals) if len(vals) > 1 else 0.0

    n = len(stdevs)
    ordered = sorted(stdevs.values())
    for cik, sd in stdevs.items():
        if n <= 1:
            consistency = 1.0
        else:
            # PERCENT_RANK with ascending stdev: rank = #strictly-less + 1
            rank = sum(1 for v in ordered if v < sd) + 1
            percent_rank = (rank - 1) / (n - 1)
            consistency = 1.0 - percent_rank
        conn.execute(
            """
            INSERT INTO fund_consistency(fund_id, qps_stdev, consistency_score)
            VALUES (?, ?, ?)
            ON CONFLICT(fund_id) DO UPDATE SET
                qps_stdev = excluded.qps_stdev,
                consistency_score = excluded.consistency_score
            """,
            (cik, sd, consistency))
    conn.commit()


def _fund_aum_and_positions(conn: sqlite3.Connection, cik: str) -> tuple[float, float]:
    """Average AUM (USD) and average equity position count across filed quarters."""
    periods = [r[0] for r in conn.execute(
        "SELECT DISTINCT period_of_report FROM filings WHERE cik = ?", (cik,)).fetchall()]
    aums: list[float] = []
    counts: list[int] = []
    for period in periods:
        lf = adapter.latest_filing_id(conn, cik, period)
        if lf is None:
            continue
        agg = conn.execute(
            f"SELECT COUNT(DISTINCT h.cusip), SUM(h.value_thousands) * 1000.0 "
            f"FROM holdings h WHERE h.filing_id = ? AND {_equity_filter()}",
            (lf,)).fetchone()
        if agg[0]:
            counts.append(agg[0])
            aums.append(agg[1] or 0.0)
    avg_aum = sum(aums) / len(aums) if aums else 0.0
    avg_pos = sum(counts) / len(counts) if counts else 0.0
    return avg_aum, avg_pos


def compute_composite(conn: sqlite3.Connection) -> None:
    """Stage 7 — composite score, 0-100 normalization, ranking, fund_rankings."""
    funds = conn.execute(
        """
        SELECT t.fund_id, t.tws, t.quarters_scored, t.one_hit_wonder_flag,
               t.best_quarter_contribution,
               tr.avg_turnover_rate, tr.turnover_multiplier,
               c.consistency_score, f.name
        FROM fund_tws t
        JOIN fund_turnover tr   ON tr.fund_id = t.fund_id
        JOIN fund_consistency c ON c.fund_id = t.fund_id
        JOIN filers f           ON f.cik = t.fund_id
        """).fetchall()
    if not funds:
        return
    raw = {}
    for r in funds:
        raw[r["fund_id"]] = (r["tws"] * r["turnover_multiplier"] * 0.70
                             + r["consistency_score"] * 0.30)
    lo, hi = min(raw.values()), max(raw.values())
    span = hi - lo

    ranked = sorted(funds, key=lambda r: raw[r["fund_id"]], reverse=True)
    for rank, r in enumerate(ranked, start=1):
        final = 100.0 if span == 0 else (raw[r["fund_id"]] - lo) / span * 100.0
        avg_aum, avg_pos = _fund_aum_and_positions(conn, r["fund_id"])
        conn.execute(
            """
            INSERT INTO fund_rankings
                (fund_id, fund_name, rank, final_score, tws_raw,
                 avg_turnover_rate, turnover_multiplier, consistency_score,
                 one_hit_wonder_flag, best_quarter_contribution, quarters_of_data,
                 avg_position_count, avg_aum, eligible, fail_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL)
            ON CONFLICT(fund_id) DO UPDATE SET
                fund_name = excluded.fund_name, rank = excluded.rank,
                final_score = excluded.final_score, tws_raw = excluded.tws_raw,
                avg_turnover_rate = excluded.avg_turnover_rate,
                turnover_multiplier = excluded.turnover_multiplier,
                consistency_score = excluded.consistency_score,
                one_hit_wonder_flag = excluded.one_hit_wonder_flag,
                best_quarter_contribution = excluded.best_quarter_contribution,
                quarters_of_data = excluded.quarters_of_data,
                avg_position_count = excluded.avg_position_count,
                avg_aum = excluded.avg_aum, eligible = 1, fail_reason = NULL
            """,
            (r["fund_id"], r["name"], rank, final, r["tws"],
             r["avg_turnover_rate"], r["turnover_multiplier"], r["consistency_score"],
             r["one_hit_wonder_flag"], r["best_quarter_contribution"],
             r["quarters_scored"], avg_pos, avg_aum))
    conn.commit()


def run_fund_pipeline(db_path: Path = DB_PATH) -> dict:
    """Run stages 1-7 in order. Returns a small summary dict."""
    conn = get_connection(db_path)
    try:
        adapter.init_schema(conn, db_path)
        # Truncate all result tables so each run is a clean rebuild; this ensures
        # funds that become ineligible between runs are not left as stale rows.
        for t in ("fund_eligibility", "holding_returns", "fund_quarterly_scores",
                  "fund_tws", "fund_turnover", "fund_consistency", "fund_rankings"):
            conn.execute(f"DELETE FROM {t}")
        conn.commit()
        weed_funds(conn)
        compute_holding_returns(conn)
        compute_qps(conn)
        compute_tws(conn)
        compute_turnover(conn)
        compute_consistency(conn)
        compute_composite(conn)
        ranked = conn.execute("SELECT COUNT(*) FROM fund_rankings").fetchone()[0]
        eligible = conn.execute(
            "SELECT COUNT(*) FROM fund_eligibility WHERE eligible = 1").fetchone()[0]
        return {"eligible": eligible, "ranked": ranked}
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    ap = argparse.ArgumentParser(description="Run the fund ranking pipeline")
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()
    print(run_fund_pipeline(Path(args.db)))
