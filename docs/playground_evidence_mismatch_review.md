# Playground Evidence Mismatch Review

## Summary

The latest daily Playground message shows many conflicts because it combines
three different evidence layers that do not currently agree:

- **Market regime:** classified as `trending_bull` with high confidence.
- **Live Playground consensus:** top consensus weights are defensive bond ETFs
  such as `IEF`, `BND`, and `TLT`.
- **Historical replay evidence:** strong, but mostly supplied by yfinance
  historical feature rows, not QC live snapshots.

This is an evidence mismatch, not automatically a data outage. The system is
correct to avoid execution because the historical evidence is strong, but the
current live allocation signal conflicts with the current regime and would
require high turnover.

## Observed Daily Message

Example fields from the message:

```text
Regime: trending_bull (high)
QC snapshots=22
yfinance history=290
Consensus top5: IEF 10.3%, BND 8.4%, TLT 6.7%, XLB 6.7%, XSD 6.2%
Historical evidence: strong (289 samples, high)
Live fit: conflicted (QC snapshots=22, forward=6)
Execution permission: human_required
Best: momentum_lite_v1 (watch_only, confidence=46.7%)
```

The key contradiction is:

```text
Regime says risk-on / trending bull.
Consensus allocation is led by defensive fixed-income assets.
```

## Evidence Sources

### QC Snapshots

QC is currently acting as the live/current snapshot source. In the example,
there are 22 recent QC snapshots but only 6 forward-return samples usable for
live replay metrics.

That means QC can help describe the current live fit, but it cannot currently
provide a long clean historical replay series. The system should not treat QC
as the source of the 289 historical forward samples.

Operationally:

- QC snapshot count is useful for live coverage.
- QC forward samples are too small for reliable live performance statistics.
- QC missing long history is expected under the current design unless a
  dedicated QC historical snapshot backfill exists.

### yfinance Historical Replay

yfinance is providing the historical feature rows used for replay. In the
example, yfinance supplies about 290 history rows and 289 forward-return
samples.

That is why historical evidence is marked strong:

- `momentum_lite_v1`: high historical reliability, 289 samples, Sharpe around
  1.56.
- `mean_reversion_lite`: high historical reliability, 289 samples, Sharpe
  around 1.42.
- `low_vol_factor`: high historical reliability, 289 samples, but much weaker
  Sharpe around 0.39.

This supports the statement: the strategy has historical evidence. It does not
prove that the current live allocation is aligned with today’s regime.

## Why Conflicts Appear

### 1. Regime And Consensus Disagree

The current regime is `trending_bull`, but the consensus top weights include
defensive assets:

- `IEF`
- `BND`
- `TLT`

For a trending bull regime, defensive bond leadership is a warning sign. It may
mean the regime classifier is too broad, the market is rotating defensively
inside an uptrend, or strategy signals are reacting to risk not captured by the
regime label.

### 2. Historical Evidence And Live Fit Are Different Claims

Historical replay says:

```text
These strategies have worked across the yfinance replay window.
```

Live fit says:

```text
The current QC/live snapshot evidence is limited and the current consensus
does not match the regime.
```

Both can be true at the same time.

### 3. Current Turnover Is High

The daily message shows current turnover around 74% to 94% depending on the
strategy. This is much higher than normal historical average turnover for some
strategies.

High current turnover means the strategy would require a large portfolio
change today. Even with good historical evidence, high turnover increases cost,
slippage, and timing risk. This is one reason strategies are marked
`watch_only`.

### 4. Bundle-Level Conflict Was Being Copied Onto Strategy Rows

Before the recent code change, a defensive consensus conflict could be stamped
onto every strategy confidence row through the `consensus_regime_conflict`
reason code.

That made the report look like every strategy individually conflicted with the
regime, even when the real issue was the aggregate consensus. The fix separates:

- bundle-level consensus conflict
- strategy-level regime conflict

Now a strategy only receives a strategy-level conflict when its own weights are
defensive-heavy in a `trending_bull` regime.

## Correct Interpretation

The correct interpretation of this message is:

```text
yfinance provides enough history to say some strategies have historical
support. QC does not provide enough live forward samples for reliable live
confirmation. Today’s consensus allocation is defensive while the regime is
trending bull, and turnover is high. Therefore the system should remain
watch-only or require human review.
```

This should not be read as:

```text
QC historical data is broken.
```

It should be read as:

```text
QC is not the historical replay source in this path; yfinance is. QC live data
is too thin to override the regime/consensus mismatch.
```

## Review Questions

1. Should `trending_bull` allow defensive bond consensus as a valid sub-state,
   such as `bull_with_defensive_rotation`?
2. Should the regime classifier incorporate bond leadership or breadth before
   assigning high-confidence `trending_bull`?
3. Should Playground display QC live replay and yfinance historical replay in
   separate sections more prominently?
4. Should high current turnover automatically cap suggested use at
   `watch_only`, even when historical replay is strong?
5. Should the consensus conflict threshold use top-three defensive names,
   defensive total weight, or both?

## Current Code Direction

The current patch keeps the bundle-level warning but avoids leaking it into
every strategy row.

Expected behavior after the patch:

- Evidence summary may still say `Live fit: conflicted`.
- Data gaps may still include a consensus/regime conflict warning.
- Individual strategies only show `strategy_regime_conflict` when their own
  weights are defensive-heavy.
- Historical yfinance evidence remains separate from QC live replay evidence.

