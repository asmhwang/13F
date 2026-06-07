# 13F Intelligence Platform — Developer Spec
## Fund Ranking + Stock Ranking Pipeline

---

## Overview

Transform ingested 13F SEC filing data into two ranked outputs:
1. **Fund Rankings** — which small, concentrated funds have the best long-term track records
2. **Stock Rankings** — which stocks those top funds are most convicted on right now

Both outputs are displayed on the website: a raw version and a filtered/curated version.

Scoring methodology is framed publicly as "Holding Period Return Simulation" — it measures
whether a fund's stock picks at each filing date appreciated over 3 years on a static hold
basis. This isolates selection skill from trading skill and is not a measure of actual
fund performance.

---

## Data Assumptions (What We Have)

The following data is already ingested and available:

| Table | Fields |
|---|---|
| `holdings` | fund_id, quarter_date, ticker, cusip, position_value_usd, share_count |
| `funds` | fund_id, fund_name, first_filing_date |
| `portfolios` | fund_id, quarter_date, total_portfolio_value_usd |
| `prices` | ticker, date, close_price, adjusted_close_price |

**Notes for developer:**
- Some identifiers in holdings are CUSIPs not tickers — need a CUSIP→ticker mapping table or resolution step before scoring
- Options/derivatives positions exist in the data — exclude these from all calculations (equity positions only)
- Quarter dates follow 13F filing cadence: March 31, June 30, September 30, December 31

---

## PIPELINE 1: FUND RANKING

### Stage 1 — Weeding

Define a single global constant at the top of the pipeline:

```
current_quarter_date = most recent 13F quarter end date (e.g. 2024-12-31)
```

This value is applied identically to every fund. Do not use each fund's
own most recent quarter — all funds must be evaluated against the same period.

```
FILTER OUT if ANY of:
  MAX(position_value_usd) > 100,000,000     -- any single position over $100M
  COUNT(ticker) > 30                         -- more than 30 equity positions
  (TODAY - first_filing_date) < 5 years      -- less than 5 years of filing history
  last_filing_date < current_quarter_date    -- did not file in the most recent quarter
```

Store result as `fund_eligibility` table:
```
fund_id | eligible (bool) | fail_reason (string or null)
```

fail_reason values:
```
"position_too_large"    -- single position exceeded $100M
"too_many_positions"    -- more than 30 equity positions
"insufficient_history"  -- less than 5 years of filing history
"inactive"              -- did not file in most recent quarter
null                    -- fund passed all filters
```

All subsequent pipeline stages only process eligible funds.

---

### Stage 2 — Forward Return Join

For each holding in `holdings` (eligible funds only):

```
3yr_return(ticker, quarter_date) =
  (adjusted_close_price(ticker, quarter_date + 3 years) - adjusted_close_price(ticker, quarter_date))
  / adjusted_close_price(ticker, quarter_date)
```

IMPORTANT — Look-ahead bias rule:
When scoring any historical quarter, only price data within that quarter's
exact 3-year window may be used. No prices from after the forward date
may be used in that quarter's calculation.

  Example: Q3 2021 scoring uses prices between 2021-09-30 and 2024-09-30 only.

If the 3-year forward date price is unavailable, use the last available
adjusted price before that date instead. This handles delistings,
take-privates, and acquisitions in one rule — the market price at exit
naturally reflects the outcome (near zero for bankruptcy, at premium
for acquisitions).

Edge cases — handle explicitly:

| Situation | Treatment |
|---|---|
| Ticker delisted before 3yr mark | Use last available adjusted price as forward price. Bankruptcy approaches zero naturally. Acquisition premium reflected naturally in final traded price. |
| Price data missing entirely | Flag as NULL, exclude from that quarter's QPS, renormalize remaining position weights to 100% |
| Corporate action (merger/acquisition) | Covered by last-price rule — acquisition price will be the final traded price before delisting |
| Spinoff | Record return on the original ticker up to the spinoff date. Then track both successor tickers weighted by their relative value at spinoff. Combined return = weighted average of both legs. If successor ticker data is unavailable, flag NULL and exclude. |
| CUSIP not resolved to ticker | Flag as unresolvable, exclude from scoring |

Store as `holding_returns`:
```
fund_id | quarter_date | ticker | position_value_usd | 3yr_return | data_quality_flag
```

data_quality_flag values:
```
"clean"            -- full 3yr price data available
"last_price"       -- delisted/acquired, used last available price
"spinoff"          -- spinoff handled, both legs tracked
"spinoff_partial"  -- spinoff, one leg missing, partial return used
"null_excluded"    -- excluded from QPS, weights renormalized
"cusip_unresolved" -- excluded, CUSIP could not be mapped
```

---

### Stage 3 — Quarterly Performance Score (QPS)

For each fund x quarter combination:

```
weight(i) = position_value(i) / SUM(position_value) for all positions in that quarter

raw_QPS(fund, quarter) = SUM [ weight(i) x 3yr_return(i) ]

benchmark_return(quarter) = S&P 500 total return from quarter_date to quarter_date + 3 years

excess_QPS(fund, quarter) = raw_QPS(fund, quarter) - benchmark_return(quarter)
```

Excess QPS is the primary scoring metric. A positive value means the fund's
portfolio beat the S&P 500 over that 3-year window. A negative value means
it underperformed.

Only include quarters where 3-year forward data exists (i.e. filed at least 3 years ago).

Store as `fund_quarterly_scores`:
```
fund_id | quarter_date | qps_raw | qps_excess | benchmark_return | positions_included | positions_excluded_null
```

---

### Stage 4 — Time-Weighted Score (TWS)

For each fund, across all eligible quarters:

IMPORTANT: Use price at time of filing, NOT price at quarter end.

Minimum requirements before TWS is calculated:
```
  - Fund must have passed weeding with 5+ years of filing history (see Stage 1)
  - Fund must have at least 6 scoreable quarters (quarters where 3yr forward data exists)
  - If either condition is not met, fund is excluded from scoring entirely
    and marked ineligible in fund_eligibility table with
    fail_reason "insufficient_scoreable_quarters"
```

```
lambda = 0.85

w(t) = lambda ^ (quarters_from_most_recent)
  -- most recent quarter: w = 1.0
  -- 1 quarter back:      w = 0.85
  -- 2 quarters back:     w = 0.72
  -- etc.

TWS(fund) = SUM [ w(t) x excess_QPS(t) ] / SUM [ w(t) ]
```

One-hit wonder check:
```
best_quarter_contribution = MAX(w(t) x excess_QPS(t)) / SUM(w(t) x excess_QPS(t))

IF best_quarter_contribution > 0.50:
  flag fund as one_hit_wonder = true
  apply discount: TWS = TWS x 0.75
  record flag in fund_tws table
```

Store as `fund_tws`:
```
fund_id
tws
quarters_scored
oldest_quarter_included
one_hit_wonder_flag           -- bool
best_quarter_contribution     -- decimal, % of TWS from single best quarter
```

---

### Stage 5 — Turnover Rate

For each fund, for each pair of consecutive quarters:

```
turnover(t) = COUNT(tickers in t-1 NOT in t) / COUNT(tickers in t-1)

avg_turnover(fund) = MEAN( turnover(t) for all consecutive quarter pairs )

turnover_multiplier = CLAMP( 1 - (avg_turnover x 0.5), min=0.5, max=1.0 )
```

Examples:
- Fund replacing 10% per quarter → multiplier = 0.95
- Fund replacing 40% per quarter → multiplier = 0.80
- Fund replacing 100% per quarter → multiplier capped at 0.50

Store as `fund_turnover`:
```
fund_id | avg_turnover_rate | turnover_multiplier | quarter_pairs_measured
```

---

### Stage 6 — Consistency Score

```
raw_consistency(fund) = STDEV( excess_QPS(t) for all quarters )

-- Lower stdev = more consistent = better score
-- Rank all eligible funds by raw_consistency ascending
-- Convert to percentile (0 to 1), where 1 = most consistent

consistency_percentile(fund) = 1 - PERCENT_RANK(raw_consistency)
```

Must run across all funds simultaneously — percentile is relative, not absolute.

Known limitation: consistency score measures outcome consistency, not skill consistency.
Quarters with extreme market-wide events (e.g. COVID drawdown) may unfairly penalize
funds. Consider flagging macro shock quarters for optional exclusion in a future iteration.

Store as `fund_consistency`:
```
fund_id | qps_stdev | consistency_score (0-1)
```

---

### Stage 7 — Composite Fund Score

```
raw_composite = (TWS x turnover_multiplier x 0.70) + (consistency_score x 0.30)

-- Normalize to 0-100 across all eligible funds
final_score = ( (raw_composite - MIN) / (MAX - MIN) ) x 100
```

Must run after all funds complete Stages 2-6.

Final output table `fund_rankings`:
```
fund_id
fund_name
rank                          -- 1 = best
final_score                   -- 0 to 100
tws_raw                       -- raw time-weighted excess return (decimal)
avg_turnover_rate             -- 0 to 1
turnover_multiplier           -- 0.5 to 1.0
consistency_score             -- 0 to 1
one_hit_wonder_flag           -- bool
best_quarter_contribution     -- decimal, % of TWS from single best quarter
quarters_of_data              -- how many quarters were scored
avg_position_count            -- average positions per quarter
avg_aum                       -- average total portfolio value
eligible                      -- true
fail_reason                   -- null for eligible funds
```

---

## PIPELINE 2: STOCK RANKING

### Stage 1 — Restrict Universe

Only include stocks held by funds in the top 50% of fund_rankings (by final_score).

```
qualifying_funds = fund_rankings WHERE rank <= (total_eligible_funds / 2)

stock_universe = DISTINCT tickers held by qualifying_funds in most recent quarter
```

---

### Stage 2 — Per-Stock Signal Aggregation

For each stock in the universe, aggregate across all qualifying funds holding it:

**Signal 1: Weighted Fund Conviction**
```
-- How strong are the funds backing this stock, weighted by their rank score
fund_conviction(stock) = SUM [ fund_score(f) x weight(stock, f) ] / SUM [ fund_score(f) ]

where weight(stock, f) = position_value(stock in fund f) / total_portfolio_value(fund f)
```

**Signal 2: Fund Count**
```
holder_count(stock) = COUNT(qualifying funds holding this stock)
```

**Signal 3: Net Positioning Change**
```
-- Aggregate buying/selling pressure across qualifying funds QoQ
net_change(stock) = SUM across funds of:
  IF new position:      +position_value
  IF increased:         +increase_in_value
  IF decreased:         -decrease_in_value
  IF exited:            -prior_position_value

-- Normalize by total universe AUM to get a relative signal
net_change_pct = net_change / SUM(total_portfolio_value for qualifying funds)
```

**Signal 4: Average Relative Size**
```
-- How large is this position relative to each fund's total portfolio
avg_relative_size(stock) = MEAN [ position_value(stock,f) / total_portfolio_value(f) ]
  across all qualifying funds holding it
```

**Signal 5: Holding Tenure**
```
-- How many consecutive quarters has each qualifying fund held this stock
-- Funds that have held and not exited across multiple quarters signal stronger conviction

avg_tenure(stock) = MEAN [ consecutive_quarters_held(stock, f) ]
  across all qualifying funds holding it

-- consecutive_quarters_held resets to 1 if a fund exits and re-enters a position
```

---

### Stage 3 — Fundamental Variables

Pull for each stock at the time of the most recent quarter filing:

| Variable | Source | Null Handling |
|---|---|---|
| Market cap | Price x shares outstanding | Required — exclude stock if missing |
| 52-week high/low | Price history | Calculate from prices table. 52wk_range_position = (price - 52wk_low) / (52wk_high - 52wk_low). If full 52 weeks unavailable: 4+ weeks of data: use available history, set 52wk_partial = 1. Less than 4 weeks: set 52wk_range_position = NULL, set 52wk_partial = 1. Never exclude the stock entirely due to missing range data alone. Add 52wk_partial as a feature in the regression (0/1 dummy) so the model can discount partial range signals. |
| P/E ratio | Earnings data | If negative or N/A → create dummy flag pe_available (0/1), use 0 for P/E in regression |
| Sector | Sector classification table | Required — use as dummy variable |
| Gross margin % | Financial statements | If missing → flag NULL, exclude from that variable only |

---

### Stage 4 — Stock Scoring (Regression)

Run a weighted linear regression predicting 3yr_return using the signals above.

**Training set:** All historical stock x quarter observations where 3yr forward data exists,
limited to stocks held by top-50% funds at those historical quarters.

**Features (X):**
```
fund_conviction           -- weighted avg fund score of holders
holder_count              -- number of qualifying funds holding
net_change_pct            -- aggregate positioning change
avg_relative_size         -- average portfolio weight
avg_tenure                -- average consecutive quarters held across holding funds
log(market_cap)           -- log-transform to normalize
52wk_range_position       -- 0 to 1 (NULL if less than 4 weeks of data)
52wk_partial              -- 0/1 dummy, 1 = less than 52 weeks of price history
pe_ratio                  -- 0 if N/A
pe_available              -- 1/0 dummy
sector_dummies            -- one-hot encoded sectors
```

**Target (Y):**
```
3yr_return (from holding_returns table, same methodology as fund pipeline)
```

**Sector adjustment:**
After regression, compute sector-adjusted score:
```
sector_adjusted_score = raw_score - sector_mean_score
```
This prevents sector bias from dominating the rankings.

---

### Stage 5 — Confidence Score

Confidence is a composite of five signals, not a simple fund count threshold.
It measures how strongly the data supports the stock's ranking, independent
of the rank itself.

Components (each normalized 0-1 before weighting):

**1. Weighted Holder Score (30%)**
```
-- Fund count weighted by fund rank, not raw count
-- A stock held by 3 top-10 funds scores higher than one held
   by 6 funds ranked 80-100

weighted_holder_score =
  SUM [ fund_score(f) for all qualifying funds holding stock ]
  / MAX possible score across all stocks in universe
  -- normalize to 0-1 across all stocks
```

**2. Average Tenure Score (25%)**
```
-- How many consecutive quarters has each holding fund held this stock
-- Funds had multiple opportunities to exit and didn't — stronger signal

avg_tenure_score =
  MEAN [ consecutive_quarters_held(stock, f) ]
  across all qualifying funds holding stock
  -- normalize to 0-1 across all stocks
```

**3. Average Relative Size (20%)**
```
-- How large is this position relative to each fund's total portfolio
-- A stock representing 15% of a fund is a different signal than 0.5%

avg_relative_size =
  MEAN [ position_value(stock, f) / total_portfolio_value(f) ]
  across all qualifying funds holding stock
  -- normalize to 0-1 across all stocks
```

**4. Direction Agreement (15%)**
```
-- Are qualifying funds agreeing on buy vs sell direction
-- Mixed conviction (half buying, half selling) is a weak signal

buyers = COUNT(funds where net_change > 0)
sellers = COUNT(funds where net_change < 0)
total = COUNT(all qualifying funds holding stock)

direction_agreement = ABS(buyers - sellers) / total
-- 1.0 = all funds moving same direction
-- 0.0 = perfectly split between buyers and sellers
```

**5. Data Quality Score (10%)**
```
-- What fraction of the underlying holding data is flagged clean
-- Stocks where scoring relied heavily on imputed or partial data
   should carry lower confidence regardless of other signals

data_quality_score =
  COUNT(holdings where data_quality_flag = "clean")
  / COUNT(all holdings for this stock across qualifying funds)
```

Composite:
```
confidence_raw =
  (weighted_holder_score x 0.30)
  + (avg_tenure_score    x 0.25)
  + (avg_relative_size   x 0.20)
  + (direction_agreement x 0.15)
  + (data_quality_score  x 0.10)
```

Bucketing:
```
-- Normalize confidence_raw to 0-1 across all stocks in universe
-- Bucket by percentile rank, recalculated each quarter:
  Top third    (67th percentile and above) → High
  Middle third (33rd to 67th percentile)   → Medium
  Bottom third (below 33rd percentile)     → Low

-- Percentile bucketing means High/Medium/Low are always relative
   to the current quarter's universe, not fixed arbitrary thresholds
```

Store as `stock_confidence`:
```
ticker
confidence_flag           -- High / Medium / Low
confidence_raw            -- 0 to 1 composite score
weighted_holder_score     -- component 1 (0-1)
avg_tenure_score          -- component 2 (0-1)
avg_relative_size         -- component 3 (0-1)
direction_agreement       -- component 4 (0-1)
data_quality_score        -- component 5 (0-1)
confidence_percentile     -- where this stock sits in the distribution
```

---

### Stage 6 — Stock Output Tables

**Raw output** (`stock_rankings_raw`):
```
ticker
company_name
sector
rank
raw_score
sector_adjusted_score
confidence_flag           -- High / Medium / Low
confidence_raw            -- 0 to 1
holder_count
fund_conviction
net_change_pct
avg_relative_size
avg_tenure
market_cap
52wk_range_position
52wk_partial              -- 0/1 flag
pe_ratio
pe_available
gross_margin_pct
```

**Filtered output** (`stock_rankings_filtered`):
```
-- Apply ALL of:
  confidence_flag != "Low"              -- composite confidence score medium or above
  market_cap >= 300,000,000             -- min $300M market cap
  market_cap <= 4,000,000,000           -- max $4B market cap
                                        -- focuses on small and mid cap universe
                                        -- large caps excluded as less differentiated
  52wk_range_position BETWEEN 0.1       -- not at extreme ends of range
    AND 0.9                             -- (uses partial flag where applicable)
  holder_count >= 3                     -- starting point, revisit after first run
                                        -- target 30-75 stocks in filtered list

-- Then take top N by sector_adjusted_score
```

---

## WEBSITE DISPLAY REQUIREMENTS

### Fund Rankings Page

| Element | Detail |
|---|---|
| Table | Rank, Fund Name, Score (0-100), Avg AUM, Positions, Quarters of Data, Turnover Rate |
| Filter | By score range, AUM range, number of positions |
| Sort | By any column |
| Detail view | Click into a fund → see their current holdings, historical QPS chart, turnover history |
| Tooltip | On score → explain the three components (performance, turnover, consistency) |
| Staleness label | Persistent timestamp on page: quarter end date, filing deadline, approximate age of data |

### Stock Rankings Page

| Element | Detail |
|---|---|
| Two tabs | "Raw Rankings" and "Filtered Rankings" |
| Table | Rank, Ticker, Company, Sector, Score, Confidence, # Funds Holding, Net Change, Avg Tenure |
| Color coding | Net change: green = net buying, red = net selling |
| Confidence badge | High / Medium / Low — visible and explained |
| Filter | By sector, confidence level, market cap range |
| Sort | By any column |
| Detail view | Click stock → see which qualifying funds hold it, at what weight, and for how many quarters |
| Staleness disclaimer | One-line note near rankings: "Positions reflect holdings at quarter end. Holdings may have changed since filing." |

---

## Recalculation Schedule

| Pipeline | Frequency | Trigger |
|---|---|---|
| Fund weeding | Quarterly | New 13F filings ingested |
| Fund scoring (Stages 2-7) | Quarterly | After weeding completes |
| Stock universe | Quarterly | After fund scoring completes |
| Stock signals | Quarterly | After universe defined |
| Stock regression | Quarterly or ad-hoc | After signals aggregated |
| Website display | On completion | After all scoring done |

---

## Open Questions for Team

1. **lambda decay value** — 0.85 is the starting point, can tune after first run
2. **Top 50% cutoff for stock universe** — could move to top 30% or top 40% to tighten, revisit after first run
3. **Filtered stock list minimum fund count** — currently set at 3, revisit after first run based on size of filtered output, target 30-75 stocks
4. **Regression retraining** — how often do we retrain vs just re-score with existing model weights?
5. **CUSIP resolution** — confirm mapping table exists and is current before pipeline runs
6. **Macro shock quarters** — consider flagging extreme market-wide drawdown quarters for optional exclusion from consistency scoring in a future iteration

