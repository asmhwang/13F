"""
Walk-forward backtest: v1 vs v2 stock rankings, point-in-time.

At each historical quarter T the harness rebuilds what each methodology would
have published using ONLY information available then (filings with period <=
T, prices/benchmark <= T+50d), takes each method's top-N stock list, "buys" it
equal-weight at the trade date (T + 50 days — after the 13F deadline, so the
holdings were actually public), holds to the next epoch's trade date, and
records the portfolio return minus the S&P 500 TR over the same window.

Strategies compared per epoch:
    v1_top      — top N of v1 stock_rankings_raw (full v1 pipeline run with
                  as_of = trade date; regression, sector adjust, etc.)
    v2_top      — top N of v2 best-ideas scores (shrunk-IR skill from clone
                  windows closed by the trade date)
    backer_univ — equal weight ALL stocks held by v2 backers at T (control:
                  does v2's *selection* add value beyond its universe?)

Shared caveat (applies equally to all strategies): CUSIP->ticker mapping is
today's knowledge applied historically, and stocks with no price data at entry
(typically delisted, not yet backfilled) are dropped from all portfolios —
n_priced/n_stocks is reported so coverage is visible.

The harness copies the needed tables once into a slim local SQLite file
(working over the mounted 2.3 GB production DB is I/O-bound) and never writes
to the production database.

Run:
    python3 -m pipeline.backtest --start 2019-12-31 --end 2025-12-31
    python3 -m pipeline.backtest --skip-v1          # v2-only quick pass
"""

import argparse
import csv
import sqlite3
import statistics
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from pipeline.database import DB_PATH, get_connection
from pipeline.scoring import adapter
from pipeline.scoring.fund_pipeline import run_fund_pipeline
from pipeline.scoring.fund_pipeline_v2 import MIN_WINDOWS, shrunk_ir
from pipeline.scoring.stock_pipeline import run_stock_pipeline
from pipeline.scoring.stock_pipeline_v2 import (
    MAX_BACKER_POSITIONS,
    _equity_holdings,
    _tenure,
    conviction_multiple,
    recency_multiplier,
    tenure_multiplier,
)

TRADE_LAG_DAYS = 50          # 13F deadline is 45 days; trade 50 days after T
ENTRY_STALENESS_DAYS = 10    # entry price must be within N days of trade date
TOP_N = 20

_SLIM_TABLES_FULL = ["filers", "filings", "securities", "sectors",
                     "fundamentals", "benchmark", "effective_filings",
                     "fund_clone_windows_v2"]


# ---------------------------------------------------------------------------
# Slim local database
# ---------------------------------------------------------------------------

def build_slim_db(src: Path, dst: Path) -> None:
    """Copy only the tables/columns the backtest needs into a fast local DB."""
    if dst.exists():
        dst.unlink()
    conn = sqlite3.connect(dst, isolation_level=None)   # autocommit: a late
    # error (e.g. DETACH lock) must not roll back the bulk inserts
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute(f"ATTACH DATABASE '{src}' AS prod")
    for t in _SLIM_TABLES_FULL:
        conn.execute(f"CREATE TABLE {t} AS SELECT * FROM prod.{t}")
    # holdings: only the columns the pipelines reference
    conn.execute(
        "CREATE TABLE holdings AS "
        "SELECT filing_id, cusip, value_thousands, put_call FROM prod.holdings")
    # prices: WITHOUT ROWID keeps (ticker,date) lookups index-organized
    conn.execute(
        "CREATE TABLE prices (ticker TEXT NOT NULL, date DATE NOT NULL, "
        "close REAL, adj_close REAL, PRIMARY KEY (ticker, date)) WITHOUT ROWID")
    conn.execute(
        "INSERT OR IGNORE INTO prices "
        "SELECT ticker, date, NULL, adj_close FROM prod.prices "
        "WHERE adj_close IS NOT NULL")
    try:
        conn.execute("DETACH DATABASE prod")
    except sqlite3.OperationalError:
        # A concurrent writer on prod can block DETACH; the copies above are
        # already committed (DDL autocommits), so just drop the connection.
        pass
    conn.close()
    conn = sqlite3.connect(dst)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_h_filing ON holdings(filing_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_f_cik ON filings(cik, period_of_report)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ef ON effective_filings(cik, period_of_report)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sec_cusip ON securities(cusip)")
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Point-in-time v2 scoring
# ---------------------------------------------------------------------------

class V2Pit:
    """Point-in-time v2 scorer with cross-epoch caches."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        # all valid clone windows, per fund, ordered
        self.windows: dict[str, list[tuple[str, str, float]]] = {}
        for fid, sp, ed, ex in conn.execute(
                "SELECT fund_id, start_period, end_date, excess_return "
                "FROM fund_clone_windows_v2 WHERE valid = 1 ORDER BY start_period"):
            self.windows.setdefault(fid, []).append((sp, ed, ex))
        # equity position counts per (fund, period) for the concentration gate
        self.pos_counts: dict[str, list[tuple[str, int]]] = {}
        for cik, period, n in conn.execute(
                """
                SELECT ef.cik, ef.period_of_report, COUNT(DISTINCT h.cusip)
                FROM effective_filings ef
                JOIN holdings h ON h.filing_id = ef.filing_id
                WHERE (h.put_call IS NULL OR h.put_call = '')
                  AND h.value_thousands > 0
                GROUP BY ef.cik, ef.period_of_report
                ORDER BY ef.period_of_report
                """):
            self.pos_counts.setdefault(cik, []).append((period, n))
        self.names = dict(conn.execute("SELECT cik, name FROM filers"))
        self._hist: dict[str, dict[str, dict[str, float]]] = {}

    def _median_positions(self, cik: str, as_of_period: str) -> float | None:
        counts = [n for p, n in self.pos_counts.get(cik, []) if p <= as_of_period]
        return statistics.median(counts) if counts else None

    def _fund_hist(self, cik: str) -> dict[str, dict[str, float]]:
        if cik not in self._hist:
            periods = [p for p, _ in self.pos_counts.get(cik, [])]
            self._hist[cik] = {p: _equity_holdings(self.conn, cik, p)
                               for p in periods}
        return self._hist[cik]

    def backers(self, as_of_period: str, cutoff: str) -> dict[str, float]:
        """{fund_id: skill} of PIT-positive-skill concentrated funds."""
        out: dict[str, float] = {}
        for cik, wins in self.windows.items():
            ex = [e for _, ed, e in wins if ed is not None and ed <= cutoff]
            if len(ex) < MIN_WINDOWS:
                continue
            stats = shrunk_ir(ex)
            if stats is None or stats["shrunk_ir_annual"] <= 0:
                continue
            med = self._median_positions(cik, as_of_period)
            if med is None or med > MAX_BACKER_POSITIONS:
                continue
            out[cik] = stats["shrunk_ir_annual"]
        return out

    def stock_scores(self, as_of_period: str, cutoff: str
                     ) -> tuple[dict[str, float], set[str]]:
        """({ticker: raw_score}, universe_of_backer_holdings) at as_of_period."""
        backers = self.backers(as_of_period, cutoff)
        scores: dict[str, float] = {}
        universe: set[str] = set()
        for cik, skill in backers.items():
            hist = self._fund_hist(cik)
            cur = hist.get(as_of_period, {})
            total = sum(cur.values())
            if not cur or total <= 0:
                continue
            periods_desc = sorted([p for p in hist if p <= as_of_period],
                                  reverse=True)
            weights = {t: v / total for t, v in cur.items()}
            med_w = statistics.median(weights.values())
            prior_p = periods_desc[1] if len(periods_desc) > 1 else None
            prior = hist.get(prior_p, {}) if prior_p else {}
            for ticker, w in weights.items():
                universe.add(ticker)
                conv = conviction_multiple(w, med_w)
                rec = recency_multiplier(cur[ticker], prior.get(ticker),
                                         has_prior_filing=bool(prior_p))
                ten = _tenure(periods_desc, hist, ticker, as_of_period)
                scores[ticker] = scores.get(ticker, 0.0) + (
                    skill * conv * rec * tenure_multiplier(ten))
        return scores, universe


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def _add_days(d: str, n: int) -> str:
    y, m, dd = (int(x) for x in d.split("-"))
    return (date(y, m, dd) + timedelta(days=n)).isoformat()


def portfolio_return(conn: sqlite3.Connection, tickers: list[str],
                     entry: str, exit_: str) -> tuple[float | None, int]:
    """Equal-weight return of `tickers` from entry to exit. Positions with no
    fresh entry price are dropped. Returns (mean_return, n_priced)."""
    rets = []
    for t in tickers:
        base = adapter.price_asof(conn, t, entry)
        if base is None or base[1] == 0:
            continue
        if (date.fromisoformat(entry) - date.fromisoformat(base[0])).days \
                > ENTRY_STALENESS_DAYS:
            continue
        fwd = adapter.price_asof(conn, t, exit_)
        if fwd is None:
            continue
        rets.append((fwd[1] - base[1]) / base[1])
    if not rets:
        return None, 0
    return statistics.fmean(rets), len(rets)


def benchmark_return(conn: sqlite3.Connection, entry: str, exit_: str) -> float | None:
    a = adapter.benchmark_asof(conn, entry)
    b = adapter.benchmark_asof(conn, exit_)
    if a is None or b is None or a[1] == 0:
        return None
    return (b[1] - a[1]) / a[1]


def summarize(rows: list[dict], strategy: str) -> dict | None:
    ex = [r[f"{strategy}_excess"] for r in rows
          if r.get(f"{strategy}_excess") is not None]
    if len(ex) < 2:
        return None
    rs = [r[f"{strategy}_ret"] for r in rows
          if r.get(f"{strategy}_ret") is not None]
    cum = 1.0
    for r in rs:
        cum *= (1 + r)
    mean, sd = statistics.fmean(ex), statistics.stdev(ex)
    return {
        "strategy": strategy,
        "epochs": len(ex),
        "mean_excess_q": mean,
        "t_stat": mean / (sd / len(ex) ** 0.5) if sd > 0 else None,
        "hit_rate": sum(1 for e in ex if e > 0) / len(ex),
        "cum_return": cum - 1,
        "worst_q_excess": min(ex),
        "best_q_excess": max(ex),
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run_backtest(slim_db: Path, start: str, end: str, top_n: int = TOP_N,
                 step: int = 1, skip_v1: bool = False,
                 out_csv: Path | None = None) -> list[dict]:
    conn = get_connection(slim_db)
    periods = [r[0] for r in conn.execute(
        "SELECT DISTINCT period_of_report FROM filings "
        "WHERE period_of_report BETWEEN ? AND ? ORDER BY period_of_report",
        (start, end)).fetchall()]
    periods = periods[::step]
    pit = V2Pit(conn)
    rows: list[dict] = []

    for i, T in enumerate(periods):
        t0 = time.time()
        entry = _add_days(T, TRADE_LAG_DAYS)
        # exit at the next epoch's trade date (last epoch: skip eval)
        if i + 1 >= len(periods):
            break
        exit_ = _add_days(periods[i + 1], TRADE_LAG_DAYS)
        row: dict = {"epoch": T, "entry": entry, "exit": exit_}
        bench = benchmark_return(conn, entry, exit_)
        row["bench_ret"] = bench

        # ---- v2 ----
        scores, universe = pit.stock_scores(T, entry)
        v2_list = [t for t, _ in sorted(scores.items(), key=lambda kv: -kv[1])[:top_n]]
        ret, n = portfolio_return(conn, v2_list, entry, exit_)
        row.update(v2_top_n_stocks=len(v2_list), v2_top_n_priced=n, v2_top_ret=ret,
                   v2_top_excess=(ret - bench) if (ret is not None and bench is not None) else None)
        # control: whole backer universe
        ret_u, n_u = portfolio_return(conn, sorted(universe), entry, exit_)
        row.update(backer_univ_n_stocks=len(universe), backer_univ_n_priced=n_u,
                   backer_univ_ret=ret_u,
                   backer_univ_excess=(ret_u - bench) if (ret_u is not None and bench is not None) else None)

        # ---- v1 (full pipeline, point-in-time) ----
        if not skip_v1:
            run_fund_pipeline(slim_db, as_of=entry)
            run_stock_pipeline(slim_db, as_of=entry)
            v1_list = [r[0] for r in conn.execute(
                "SELECT ticker FROM stock_rankings_raw ORDER BY rank LIMIT ?",
                (top_n,)).fetchall()]
            ret1, n1 = portfolio_return(conn, v1_list, entry, exit_)
            row.update(v1_top_n_stocks=len(v1_list), v1_top_n_priced=n1, v1_top_ret=ret1,
                       v1_top_excess=(ret1 - bench) if (ret1 is not None and bench is not None) else None)

        rows.append(row)
        print(f"[{i + 1}/{len(periods) - 1}] {T}  "
              f"v2={row.get('v2_top_ret')} v1={row.get('v1_top_ret')} "
              f"bench={bench}  ({time.time() - t0:.0f}s)", flush=True)
        if out_csv:
            _write_csv(rows, out_csv)

    conn.close()
    return rows


def _write_csv(rows: list[dict], path: Path) -> None:
    keys: list[str] = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Walk-forward backtest v1 vs v2")
    ap.add_argument("--db-src", default=str(DB_PATH))
    ap.add_argument("--slim-db", default="/tmp/13f_backtest.db")
    ap.add_argument("--rebuild-slim", action="store_true")
    ap.add_argument("--start", default="2019-12-31")
    ap.add_argument("--end", default="2025-12-31")
    ap.add_argument("--top", type=int, default=TOP_N)
    ap.add_argument("--step", type=int, default=1, help="1=quarterly, 2=semiannual")
    ap.add_argument("--skip-v1", action="store_true")
    ap.add_argument("--out", default=None, help="CSV output path")
    args = ap.parse_args()

    slim = Path(args.slim_db)
    if args.rebuild_slim or not slim.exists():
        print("building slim DB...", flush=True)
        t0 = time.time()
        build_slim_db(Path(args.db_src), slim)
        print(f"slim DB built in {time.time() - t0:.0f}s "
              f"({slim.stat().st_size / 1e9:.2f} GB)", flush=True)

    out_csv = Path(args.out) if args.out else None
    rows = run_backtest(slim, args.start, args.end, top_n=args.top,
                        step=args.step, skip_v1=args.skip_v1, out_csv=out_csv)

    print("\n==== SUMMARY (quarterly excess vs S&P 500 TR) ====")
    for strat in ("v1_top", "v2_top", "backer_univ"):
        s = summarize(rows, strat)
        if s:
            print(f"{strat:12s} epochs={s['epochs']:2d} "
                  f"mean_excess={s['mean_excess_q']:+.3%} t={s['t_stat']:.2f} "
                  f"hit={s['hit_rate']:.0%} cum={s['cum_return']:+.1%} "
                  f"worst={s['worst_q_excess']:+.1%}")
    bench_rets = [r["bench_ret"] for r in rows if r["bench_ret"] is not None]
    cum_b = 1.0
    for b in bench_rets:
        cum_b *= (1 + b)
    print(f"{'benchmark':12s} epochs={len(bench_rets):2d} cum={cum_b - 1:+.1%}")


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent))
    main()
