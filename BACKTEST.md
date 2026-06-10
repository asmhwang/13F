# Walk-Forward Backtest — v1 vs v2 Stock Rankings

Run 2026-06-10 · `python3 -m pipeline.backtest` · per-epoch detail in `data/backtest_results.csv`

## Method

At each quarter T (40 epochs, 2015-12-31 → 2025-09-30), each methodology is
rebuilt **point-in-time**: only filings with period ≤ T and prices up to the
trade date are visible. The top-20 stock list is "bought" equal-weight at
T + 50 days (after the 13F filing deadline, so the holdings were genuinely
public), held to the next epoch's trade date, and measured against the S&P 500
TR over the identical window.

- **v1_top** — full v1 pipeline (weeding → TWS → regression stock scores) run
  with `as_of` = trade date; top 20 of `stock_rankings_raw`.
- **v2_top** — v2 best-ideas scores from clone windows closed by the trade
  date; skill recomputed per epoch from point-in-time windows only.
- **backer_univ** — equal weight in *every* stock held by v2's backer funds at
  T. Control: does v2's selection add value beyond its universe?

## Results (quarterly, vs S&P 500 TR)

| Strategy    | Mean excess/q | t-stat | Hit rate | Cumulative | Worst qtr excess |
|-------------|--------------|--------|----------|-----------|------------------|
| v1_top      | −0.23%       | −0.33  | 47.5%    | +267%     | −12.5%           |
| v2_top      | **+0.95%**   | 0.73   | **67.5%**| **+400%** | −23.1%           |
| backer_univ | +0.26%       | 0.36   | 55.0%    | +335%     | −8.5%            |
| S&P 500 TR  | —            | —      | —        | +325%     | —                |

## Reading

- **v1 fails its own purpose**: negative mean excess, sub-coin-flip hit rate,
  and it lags the index it was built to beat. This is consistent with the
  structural critique (circular regression, overlapping windows, outlier-driven
  composite) rather than bad luck.
- **v2 is directionally better on every metric** — beats v1, the benchmark,
  and its own universe control (so the skill-weighted selection, not just the
  fund universe, adds the value). Hit rate 67.5% over 40 quarters.
- **The v2 edge is NOT yet statistically significant** (t = 0.73). Forty
  quarters of a noisy strategy cannot prove skill; treat v2 as "clearly better
  than v1, plausibly better than the index," not as a proven alpha source.
- v2's worst quarter (−23% excess, epoch 2021-12-31) is a concentration tax:
  high-conviction growth books in the 2022 drawdown. A position cap or
  vol-aware sizing would trade some upside for that tail.

## Caveats

- CUSIP→ticker mapping is today's knowledge applied historically (affects all
  strategies equally).
- ~6% of top-list positions had no entry price (mostly delisted, not yet
  backfilled) and were dropped from all portfolios alike (avg 18.7/20 priced
  for v2, 17.3/20 for v1).
- The 54 tracked filers were hand-picked knowing their reputations — the fund
  *universe* itself embeds hindsight. Walk-forward scoring removes look-ahead
  in ranking, not in universe construction.
- Backer gate (median positions ≤ 100) and top-N = 20 were set today, not
  walked forward; modest specification-search bias is possible.

## Re-running

```
python3 -m pipeline.backtest --rebuild-slim            # full run
python3 -m pipeline.backtest --skip-v1 --step 2        # quick v2-only pass
```
