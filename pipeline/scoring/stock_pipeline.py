"""
Stock ranking pipeline (developer spec stages 1-6). Aggregates per-stock
conviction signals across qualifying funds, scores stocks via an OLS regression
on 3-year forward return (with a fund_conviction fallback for tiny training
sets), grades confidence, and materializes raw + filtered ranking tables.
"""

import sqlite3
from pathlib import Path

import numpy as np

from pipeline.database import DB_PATH, get_connection
from pipeline.scoring import adapter

_MIN_TRAIN_ROWS = 8          # below this, skip the fit and fall back to fund_conviction
_TRADING_DAYS_52W = 252


def qualifying_funds(conn: sqlite3.Connection) -> dict[str, float]:
    """{fund_id: final_score} for funds in the top half of fund_rankings
    (rank <= ceil(n/2), so a single ranked fund still qualifies)."""
    rows = conn.execute("SELECT fund_id, final_score, rank FROM fund_rankings").fetchall()
    n = len(rows)
    if n == 0:
        return {}
    cutoff = (n + 1) // 2
    return {r["fund_id"]: r["final_score"] for r in rows if r["rank"] <= cutoff}


def _equity_holdings(conn: sqlite3.Connection, cik: str, period: str) -> dict[str, float]:
    """{ticker: position_value_usd} for the latest filing of cik at period
    (equity, resolved tickers only)."""
    lf = adapter.latest_filing_id(conn, cik, period)
    if lf is None:
        return {}
    rows = conn.execute(
        """
        SELECT s.ticker AS ticker, SUM(h.value_thousands) * 1000.0 AS v
        FROM holdings h JOIN securities s ON s.cusip = h.cusip
        WHERE h.filing_id = ? AND s.ticker IS NOT NULL AND s.ticker <> ''
          AND s.ticker NOT GLOB '*[0-9]*'
          AND (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
        GROUP BY s.ticker
        """, (lf,)).fetchall()
    return {r["ticker"]: r["v"] for r in rows}


def fund_histories(conn: sqlite3.Connection, qualifying: dict[str, float]
                   ) -> dict[str, dict[str, dict[str, float]]]:
    """Per qualifying fund: {period: {ticker: value}} across all its filed
    quarters. Built once so signal/tenure computation needs no further holdings
    queries."""
    hist: dict[str, dict[str, dict[str, float]]] = {}
    for cik in qualifying:
        periods = [r[0] for r in conn.execute(
            "SELECT DISTINCT period_of_report FROM filings WHERE cik = ? "
            "ORDER BY period_of_report", (cik,)).fetchall()]
        hist[cik] = {p: _equity_holdings(conn, cik, p) for p in periods}
    return hist


def _prior_period(conn: sqlite3.Connection, cik: str, period: str) -> str | None:
    r = conn.execute(
        "SELECT MAX(period_of_report) FROM filings WHERE cik = ? AND period_of_report < ?",
        (cik, period)).fetchone()
    return r[0]


def _tenure(periods_desc: list[str], holdings_by_period: dict[str, dict[str, float]],
            ticker: str, period: str) -> int:
    """Consecutive quarters (the fund's own filed quarters, going back from
    `period`) that the fund held `ticker`, stopping at the first gap."""
    count = 0
    for p in periods_desc:
        if p > period:
            continue
        if ticker in holdings_by_period.get(p, {}):
            count += 1
        else:
            break
    return count


def signals_for_period(conn: sqlite3.Connection, period: str,
                       qualifying: dict[str, float],
                       hist: dict[str, dict[str, dict[str, float]]]) -> dict[str, dict]:
    """
    Per-ticker conviction signals aggregated over qualifying funds holding the
    ticker at `period`. Returns {ticker: {fund_conviction, holder_count,
    net_change_pct, avg_relative_size, avg_tenure, buyers, sellers}}.
    """
    # Per-fund current holdings + portfolio value + prior holdings.
    cur: dict[str, dict[str, float]] = {}
    prior: dict[str, dict[str, float]] = {}
    portfolio: dict[str, float] = {}
    periods_desc: dict[str, list[str]] = {}
    for cik in qualifying:
        cur[cik] = hist[cik].get(period, {})
        portfolio[cik] = sum(cur[cik].values())
        pp = _prior_period(conn, cik, period)
        prior[cik] = hist[cik].get(pp, {}) if pp else {}
        periods_desc[cik] = sorted(hist[cik].keys(), reverse=True)

    universe_aum = sum(portfolio.values())
    tickers = {t for cik in qualifying for t in cur[cik]}
    out: dict[str, dict] = {}
    for ticker in tickers:
        holders = [cik for cik in qualifying if ticker in cur[cik] and portfolio[cik] > 0]
        if not holders:
            continue
        weights = [cur[cik][ticker] / portfolio[cik] for cik in holders]
        scores = [qualifying[cik] for cik in holders]
        score_sum = sum(scores)
        fund_conviction = (sum(s * w for s, w in zip(scores, weights)) / score_sum
                           if score_sum > 0 else 0.0)
        avg_relative_size = sum(weights) / len(weights)
        tenures = [_tenure(periods_desc[cik], hist[cik], ticker, period) for cik in holders]
        avg_tenure = sum(tenures) / len(tenures)
        # net change across ALL qualifying funds (a fund that exited still counts)
        net_change = 0.0
        buyers = sellers = 0
        for cik in qualifying:
            now = cur[cik].get(ticker, 0.0)
            was = prior[cik].get(ticker, 0.0)
            delta = now - was
            if now > 0 or was > 0:
                net_change += delta
                if delta > 0:
                    buyers += 1
                elif delta < 0:
                    sellers += 1
        net_change_pct = net_change / universe_aum if universe_aum > 0 else 0.0
        out[ticker] = {
            "fund_conviction": fund_conviction,
            "holder_count": len(holders),
            "net_change_pct": net_change_pct,
            "avg_relative_size": avg_relative_size,
            "avg_tenure": avg_tenure,
            "buyers": buyers,
            "sellers": sellers,
        }
    return out


def _minus_one_year(d: str) -> str:
    y, m, day = (int(x) for x in d.split("-"))
    try:
        from datetime import date
        return date(y - 1, m, day).isoformat()
    except ValueError:
        from datetime import date
        return date(y - 1, m, day - 1).isoformat()


def range_position_52w(conn: sqlite3.Connection, ticker: str, as_of: str
                       ) -> tuple[float | None, int]:
    """
    (range_position, partial) over the trailing 52 weeks ending at `as_of`.
    range_position = (price - low) / (high - low). Rules:
      - distinct trading days >= ~52 weeks (>= _TRADING_DAYS_52W) -> partial=0
      - 4+ weeks but < 52 weeks -> use available history, partial=1
      - < 4 weeks (< 20 trading days) of data -> position NULL, partial=1
      - no on/before price -> NULL, partial=1
    """
    start = _minus_one_year(as_of)
    rows = conn.execute(
        "SELECT date, adj_close FROM prices "
        "WHERE ticker = ? AND date >= ? AND date <= ? AND adj_close IS NOT NULL "
        "ORDER BY date", (ticker, start, as_of)).fetchall()
    n = len(rows)
    if n < 20:
        return (None, 1)
    lo = min(r["adj_close"] for r in rows)
    hi = max(r["adj_close"] for r in rows)
    price = rows[-1]["adj_close"]
    partial = 0 if n >= _TRADING_DAYS_52W else 1
    if hi == lo:
        return (0.5, partial)
    return ((price - lo) / (hi - lo), partial)


def regress_scores(feature_names: list[str], train_X: list[list[float]],
                   train_y: list[float], pred_rows: dict[str, list[float]],
                   fallback: dict[str, float] | None = None) -> dict[str, float]:
    """
    Fit OLS (with intercept) via least-squares and predict a raw score for each
    ticker in pred_rows. If the training set has fewer than _MIN_TRAIN_ROWS rows,
    return `fallback` unchanged (caller supplies fund_conviction as the fallback).
    """
    if len(train_X) < _MIN_TRAIN_ROWS:
        return dict(fallback) if fallback else {t: 0.0 for t in pred_rows}
    A = np.array([[1.0, *row] for row in train_X], dtype=float)
    y = np.array(train_y, dtype=float)
    coef, *_ = np.linalg.lstsq(A, y, rcond=None)
    scores: dict[str, float] = {}
    for ticker, row in pred_rows.items():
        x = np.array([1.0, *row], dtype=float)
        scores[ticker] = float(x @ coef)
    return scores


def sector_adjust(raw: dict[str, float], sector: dict[str, str]) -> dict[str, float]:
    """sector_adjusted_score = raw_score - mean(raw_score within the same sector)."""
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    for ticker, score in raw.items():
        s = sector.get(ticker, "Unknown")
        sums[s] = sums.get(s, 0.0) + score
        counts[s] = counts.get(s, 0) + 1
    return {ticker: score - sums[sector.get(ticker, "Unknown")] / counts[sector.get(ticker, "Unknown")]
            for ticker, score in raw.items()}


def build_training_set(conn: sqlite3.Connection, qualifying: dict[str, float],
                       hist: dict[str, dict[str, dict[str, float]]],
                       sector: dict[str, str]) -> tuple[list[str], list[list[float]], list[float]]:
    """
    Assemble (feature_names, X, y) from historical (stock, quarter) observations
    that have a non-null 3yr return. Features: holder_count, fund_conviction,
    avg_relative_size, 52wk_range_position (0.5 if NULL), + one-hot sector.
    Target: mean 3yr_return across qualifying holders of that (stock, quarter).
    """
    sector_names = sorted(set(sector.values()))
    feature_names = ["holder_count", "fund_conviction", "avg_relative_size",
                     "range_position"] + [f"sector_{s}" for s in sector_names]
    # target: mean return per (ticker, quarter) among qualifying funds only
    qs = ",".join("?" * len(qualifying))
    rows = conn.execute(
        f"""
        SELECT hr.ticker, hr.quarter_date, AVG(hr.three_yr_return) AS ret
        FROM holding_returns hr
        WHERE hr.three_yr_return IS NOT NULL
          AND hr.fund_id IN ({qs})
        GROUP BY hr.ticker, hr.quarter_date
        """, tuple(qualifying.keys())).fetchall()
    by_period: dict[str, dict[str, float]] = {}
    for r in rows:
        by_period.setdefault(r["quarter_date"], {})[r["ticker"]] = r["ret"]

    X: list[list[float]] = []
    y: list[float] = []
    for period, ticker_ret in by_period.items():
        sig = signals_for_period(conn, period, qualifying, hist)
        for ticker, ret in ticker_ret.items():
            if ticker not in sig:
                continue
            rp, _ = range_position_52w(conn, ticker, period)
            rp = 0.5 if rp is None else rp
            base = [float(sig[ticker]["holder_count"]), sig[ticker]["fund_conviction"],
                    sig[ticker]["avg_relative_size"], rp]
            onehot = [1.0 if sector.get(ticker, "Unknown") == s else 0.0 for s in sector_names]
            X.append(base + onehot)
            y.append(ret)
    return feature_names, X, y


def _normalize(values: dict[str, float]) -> dict[str, float]:
    """Min-max to 0-1 across the universe; a single value maps to 1.0."""
    if not values:
        return {}
    if len(values) == 1:
        return {k: 1.0 for k in values}
    lo, hi = min(values.values()), max(values.values())
    if hi == lo:
        return {k: 1.0 for k in values}
    return {k: (v - lo) / (hi - lo) for k, v in values.items()}


def compute_confidence(universe: dict[str, dict]) -> dict[str, dict]:
    """
    Composite confidence per stock. `universe[ticker]` has weighted_holder_score,
    avg_tenure_score, avg_relative_size (normalized across the universe here),
    plus direction_agreement and data_quality_score (already 0-1).
    Returns {ticker: {confidence_flag, confidence_raw, confidence_percentile,
    weighted_holder_score, avg_tenure_score, avg_relative_size,
    direction_agreement, data_quality_score}} where the three *_score/size values
    are the normalized 0-1 components. A single-stock universe normalizes to 1.0
    and buckets to 'High' (no relative ranking possible).
    """
    if not universe:
        return {}
    whs = _normalize({t: v["weighted_holder_score"] for t, v in universe.items()})
    ats = _normalize({t: v["avg_tenure_score"] for t, v in universe.items()})
    ars = _normalize({t: v["avg_relative_size"] for t, v in universe.items()})
    raw = {t: (whs[t] * 0.30 + ats[t] * 0.25 + ars[t] * 0.20
               + v["direction_agreement"] * 0.15 + v["data_quality_score"] * 0.10)
           for t, v in universe.items()}
    ordered = sorted(raw.values())
    n = len(ordered)
    out = {}
    for t, v in universe.items():
        pr = (sum(1 for x in ordered if x < raw[t]) / (n - 1)) if n > 1 else 1.0
        flag = "High" if pr >= 0.6667 else ("Medium" if pr >= 0.3333 else "Low")
        out[t] = {
            "confidence_flag": flag,
            "confidence_raw": raw[t],
            "confidence_percentile": pr,
            "weighted_holder_score": whs[t],
            "avg_tenure_score": ats[t],
            "avg_relative_size": ars[t],
            "direction_agreement": v["direction_agreement"],
            "data_quality_score": v["data_quality_score"],
        }
    return out


def confidence_flags(universe: dict[str, dict]) -> dict[str, str]:
    return {t: v["confidence_flag"] for t, v in compute_confidence(universe).items()}


def _current_company_names(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, str]:
    out = {}
    for t in tickers:
        r = conn.execute(
            "SELECT name FROM securities WHERE ticker = ? AND name IS NOT NULL LIMIT 1", (t,)).fetchone()
        out[t] = r[0] if r else t
    return out


def _data_quality_for(conn: sqlite3.Connection, ticker: str, period: str,
                      qualifying: dict[str, float]) -> float:
    """Fraction of current-quarter holding_returns rows for this ticker (across
    qualifying funds) flagged 'clean'."""
    qs = ",".join("?" * len(qualifying))
    rows = conn.execute(
        f"SELECT data_quality_flag FROM holding_returns "
        f"WHERE ticker = ? AND quarter_date = ? AND fund_id IN ({qs})",
        (ticker, period, *qualifying.keys())).fetchall()
    if not rows:
        return 0.0
    clean = sum(1 for r in rows if r["data_quality_flag"] == "clean")
    return clean / len(rows)


def _truncate(conn: sqlite3.Connection) -> None:
    for t in ("stock_signals", "stock_confidence", "stock_rankings_raw",
              "stock_rankings_filtered"):
        conn.execute(f"DELETE FROM {t}")
    conn.commit()


def run_stock_pipeline(db_path: Path = DB_PATH) -> dict:
    """Run stages 1-6 and materialize the stock ranking tables (idempotent)."""
    conn = get_connection(db_path)
    try:
        adapter.init_schema(conn, db_path)
        _truncate(conn)
        cq = adapter.current_quarter_date(conn)
        qualifying = qualifying_funds(conn)
        if cq is None or not qualifying:
            return {"universe": 0, "ranked": 0}
        hist = fund_histories(conn, qualifying)
        sig = signals_for_period(conn, cq, qualifying, hist)
        universe = list(sig.keys())
        sector = {t: (conn.execute("SELECT sector FROM sectors WHERE ticker = ?", (t,)).fetchone() or ["Unknown"])[0]
                  for t in universe}

        # persist signals
        for t in universe:
            s = sig[t]
            conn.execute(
                "INSERT INTO stock_signals(ticker,as_of_date,fund_conviction,holder_count,"
                "net_change_pct,avg_relative_size,avg_tenure) VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(ticker,as_of_date) DO UPDATE SET "
                "fund_conviction=excluded.fund_conviction, holder_count=excluded.holder_count, "
                "net_change_pct=excluded.net_change_pct, avg_relative_size=excluded.avg_relative_size, "
                "avg_tenure=excluded.avg_tenure",
                (t, cq, s["fund_conviction"], s["holder_count"], s["net_change_pct"],
                 s["avg_relative_size"], s["avg_tenure"]))

        # regression (with fund_conviction fallback)
        feature_names, X, y = build_training_set(conn, qualifying, hist, sector)
        sector_names = [f.removeprefix("sector_") for f in feature_names if f.startswith("sector_")]
        pred_rows = {}
        for t in universe:
            rp, _ = range_position_52w(conn, t, cq)
            rp = 0.5 if rp is None else rp
            base = [float(sig[t]["holder_count"]), sig[t]["fund_conviction"],
                    sig[t]["avg_relative_size"], rp]
            onehot = [1.0 if sector[t] == s else 0.0 for s in sector_names]
            pred_rows[t] = base + onehot
        fallback = {t: sig[t]["fund_conviction"] for t in universe}
        raw_scores = regress_scores(feature_names, X, y, pred_rows, fallback=fallback)
        adj_scores = sector_adjust(raw_scores, sector)

        # confidence components
        comp = {}
        for t in universe:
            holders = sig[t]["holder_count"]
            whs = sum(qualifying[c] for c in qualifying
                      if t in hist[c].get(cq, {}))
            comp[t] = {
                "weighted_holder_score": whs,
                "avg_tenure_score": sig[t]["avg_tenure"],
                "avg_relative_size": sig[t]["avg_relative_size"],
                "direction_agreement": (abs(sig[t]["buyers"] - sig[t]["sellers"]) / holders
                                        if holders else 0.0),
                "data_quality_score": _data_quality_for(conn, t, cq, qualifying),
            }
        conf = compute_confidence(comp)
        for t in universe:
            c = conf[t]
            conn.execute(
                "INSERT INTO stock_confidence(ticker,confidence_flag,confidence_raw,"
                "weighted_holder_score,avg_tenure_score,avg_relative_size,direction_agreement,"
                "data_quality_score,confidence_percentile) VALUES (?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(ticker) DO UPDATE SET confidence_flag=excluded.confidence_flag, "
                "confidence_raw=excluded.confidence_raw",
                (t, c["confidence_flag"], c["confidence_raw"], c["weighted_holder_score"],
                 c["avg_tenure_score"], c["avg_relative_size"], c["direction_agreement"],
                 c["data_quality_score"], c["confidence_percentile"]))
        flags = {t: conf[t]["confidence_flag"] for t in universe}

        # fundamentals + 52wk + assemble raw output, ranked by sector_adjusted_score desc
        names = _current_company_names(conn, universe)
        ranked = sorted(universe, key=lambda t: adj_scores[t], reverse=True)
        for rank, t in enumerate(ranked, start=1):
            f = conn.execute(
                "SELECT market_cap, pe_ratio, pe_available, gross_margin_pct "
                "FROM fundamentals WHERE ticker = ? AND as_of_date = ?", (t, cq)).fetchone()
            mc = f["market_cap"] if f else None
            rp, partial = range_position_52w(conn, t, cq)
            conn.execute(
                "INSERT INTO stock_rankings_raw(ticker,company_name,sector,rank,raw_score,"
                "sector_adjusted_score,confidence_flag,confidence_raw,holder_count,fund_conviction,"
                "net_change_pct,avg_relative_size,avg_tenure,market_cap,range_position,partial,"
                "pe_ratio,pe_available,gross_margin_pct) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(ticker) DO UPDATE SET rank=excluded.rank",
                (t, names[t], sector[t], rank, raw_scores[t], adj_scores[t], flags[t],
                 conf[t]["confidence_raw"], sig[t]["holder_count"], sig[t]["fund_conviction"], sig[t]["net_change_pct"],
                 sig[t]["avg_relative_size"], sig[t]["avg_tenure"], mc, rp, partial,
                 (f["pe_ratio"] if f else None), (f["pe_available"] if f else None),
                 (f["gross_margin_pct"] if f else None)))

        # filtered output: confidence != Low, 300M<=mktcap<=4B, 0.1<=range<=0.9, holders>=3
        frank = 0
        for t in ranked:
            row = conn.execute(
                "SELECT market_cap, range_position, holder_count, confidence_flag, sector, company_name, "
                "sector_adjusted_score FROM stock_rankings_raw WHERE ticker = ?", (t,)).fetchone()
            mc, rp = row["market_cap"], row["range_position"]
            if (row["confidence_flag"] != "Low" and mc is not None
                    and 300_000_000 <= mc <= 4_000_000_000
                    and rp is not None and 0.1 <= rp <= 0.9
                    and row["holder_count"] >= 3):
                frank += 1
                conn.execute(
                    "INSERT INTO stock_rankings_filtered(ticker,rank,company_name,sector,"
                    "sector_adjusted_score,confidence_flag,market_cap,range_position,holder_count) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (t, frank, row["company_name"], row["sector"], row["sector_adjusted_score"],
                     row["confidence_flag"], mc, rp, row["holder_count"]))
        conn.commit()
        return {"universe": len(universe), "ranked": len(universe), "filtered": frank}
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    ap = argparse.ArgumentParser(description="Run the stock ranking pipeline")
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()
    print(run_stock_pipeline(Path(args.db)))
