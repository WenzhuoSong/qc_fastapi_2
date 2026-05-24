# Historical Signal Conviction Development Plan

## Goal

Build a daily signal validation system that can answer:

```text
When a strategy emitted this EvidenceCard in the past, was it right?
Which strategy + ticker + branch combinations are reliable?
Does confidence correlate with later returns?
Which ETF roles should be capped, ignored, or watched?
```

This plan extends the current EvidenceCard layer with historical replay,
outcome labeling, and conviction calibration.

The key data-source decision:

```text
yfinance = primary historical replay source
QC = live/paper operational truth and drift validation source
```

This is not an execution rollout. All outputs are observe/shadow only until a
later, separately reviewed portfolio-construction phase.

## Why yfinance Is The Primary Historical Source

yfinance is the better source for historical signal accuracy because it:

- has long historical coverage,
- covers ETFs we did not hold,
- can reconstruct daily feature snapshots,
- supports strategy + ticker + branch level replay,
- is already used by the research feature store.

QC should not be the main long-history source in this system because local QC
snapshots are short-lived and mostly start when our system began collecting
them. QC is still essential, but for a different role:

- current holdings,
- current weights,
- execution state,
- live/paper heartbeat,
- deployment/version drift,
- realtime data freshness,
- operational truth of what the system actually saw.

The resulting conviction layers are:

```text
historical_prior_conviction
  source: yfinance_replay
  purpose: fast long-history initial estimate

live_paper_conviction
  source: fastapi_live_freeze + QC operational context
  purpose: verify signals under the actual running system

combined_conviction
  source: historical prior plus live paper correction
  purpose: future shadow portfolio construction, not execution authority
```

## Guardrails

- Do not use conviction to change production target weights in this phase.
- Do not generate execution commands.
- Do not treat yfinance historical prior as live-certified evidence.
- Do not overwrite frozen signals.
- Do not update frozen signals when outcomes arrive.
- Do not compute conviction from stale or non-synchronous signals by default.
- Do not collapse `insufficient_samples` into a bearish or bad signal.

## Core Data Model

### FrozenSignal

`FrozenSignal` is an immutable snapshot of what the strategy said at signal time.

It can be generated from historical replay or from daily live/paper operation.
Once written, it is never updated. Outcomes are written into a separate table.

Fields:

```text
signal_id
signal_source               # yfinance_replay | fastapi_live_freeze | qc_snapshot_replay
signal_date                 # feature date used to emit the signal
generated_at
tradable_from_date          # usually next trading day after signal_date
strategy_id
strategy_version
ticker
role
branch
action
signal_type
confidence
raw_score
normalized_score
max_reasonable_weight
risk_budget_cost
feature_data_date
data_lag_days
feature_source
feature_authority
regime_at_signal
vix_at_signal
evidence_contract_version
diagnostics
created_at
```

Immutability rules:

```text
Same signal_id may be inserted idempotently.
Different content with the same signal_id is a conflict.
Outcome labeler must never update FrozenSignal.
Corrections require a new signal_source or replay_version.
```

### SignalOutcome

`SignalOutcome` labels what happened after a FrozenSignal.

Fields:

```text
outcome_id
signal_id
signal_source
signal_date
label_date
strategy_id
ticker
branch
action
horizon_days                 # 1 / 5 / 20
forward_return
spy_forward_return
excess_vs_spy
drawdown_during_horizon
spy_drawdown_during_horizon
target_pool_drawdown
hit
hit_definition
excess_calculation_method    # raw in v1; beta_adjusted can be added later
outcome_source               # yfinance
data_quality
created_at
```

Append-only rules:

```text
Same signal_id + horizon_days + outcome_source is idempotent.
Do not label until label_date >= tradable_from_date + horizon_days.
watch / neutral / hold record returns but hit is null.
```

## Date Semantics

For yfinance historical replay:

```text
feature_data_date = T
signal_date = T
tradable_from_date = next trading day after T
forward return horizon starts at tradable_from_date
```

This prevents accidentally counting the same day's close-to-close return that
was already used to generate the signal.

For live/paper daily freeze:

```text
feature_data_date = latest daily feature date known at freeze time
signal_date = intended trading date of the signal
data_lag_days = signal_date - feature_data_date
```

Conviction default filter:

```text
data_lag_days <= 1
hit is not null
valid outcome data quality
```

Signals with lag > 1 may be stored for audit but excluded from default
conviction.

Historical replay must enforce this in code, not by convention:

```python
assert max_feature_date <= signal_date
```

Any replay helper that receives a full historical dataset must slice to
`trading_date <= signal_date` before generating signals, then assert that no
future feature rows remain. Tests must include a leak-detection case.

## Hit Definitions

Hit definitions are action-specific and centralized in the outcome labeler.
They must not be reimplemented by downstream consumers.

```text
increase:
  forward_return > 0.0
  and excess_vs_spy > -0.005

hedge:
  spy_forward_return < -0.02
  or spy_drawdown_during_horizon < -0.02

de_risk:
  spy_drawdown_during_horizon < -0.015
  or target_pool_drawdown < -0.015

avoid:
  forward_return < -0.01

reduce:
  forward_return < 0.0
  or excess_vs_spy < -0.005

watch:
  no hit label

neutral:
  no hit label

hold:
  no hit label
```

Important: `hedge` hit is judged by market stress, not by UVXY return. UVXY can
decay or underperform even when a hedge warning was directionally useful.

`excess_vs_spy` uses the v1 raw method:

```text
excess_vs_spy = ticker_forward_return - spy_forward_return
excess_calculation_method = raw
```

This is intentionally simple. Future work may add `beta_adjusted` or
`role_adjusted` excess without changing the outcome schema.

## PR5A: Historical Signal Replay

Goal: use yfinance history to generate initial FrozenSignals and SignalOutcomes.

Inputs:

- `market_daily_features` rows with source `yfinance`
- current strategy registry
- current EvidenceCard normalizer
- ETF/strategy knowledge mappings

Add:

```text
services/historical_signal_replay.py
tests/test_historical_signal_replay.py
```

Pipeline:

```text
1. Read yfinance feature rows.
2. Group rows by trading_date into historical snapshots.
3. For each date, run selected strategies.
4. Generate EvidenceCards.
5. Freeze each EvidenceCard as signal_source=yfinance_replay.
6. Label T+1/T+5/T+20 outcomes from future yfinance prices.
7. Return replay summary and persist if requested.
```

Acceptance criteria:

```text
□ Replay never uses future rows to generate T signal.
□ Replay contains an assertion that max_feature_date <= signal_date.
□ Tests intentionally pass future rows and verify the assertion catches leaks.
□ Forward returns start from tradable_from_date, not same-day close.
□ Generated signals include feature_data_date and data_lag_days.
□ Outcomes include action-specific hit.
□ Outcomes include excess_calculation_method="raw".
□ The replay can run for a single strategy and fixed ETF universe.
□ Replay output marks source=yfinance_replay and reliability=historical_prior.
```

## PR5B: Live/Paper Signal Ledger

Goal: from deployment day forward, freeze all daily EvidenceCards generated by
the running FastAPI/playground system.

Add:

```text
services/signal_ledger.py
db model or durable JSONL/DB table: StrategyFrozenSignal
tests/test_signal_ledger.py
cron/daily_signal_freeze.py
```

Rules:

```text
Frozen signals are immutable.
Daily reruns are idempotent.
Feature source/date/authority must be stored.
QC heartbeat/snapshot metadata can be attached as diagnostics.
```

QC role:

- record QC policy/version match status,
- record current paper/live holdings,
- record current execution mode,
- record whether QC data is stale or mismatched,
- do not use QC as the main historical return source.

Acceptance criteria:

```text
□ Daily freeze stores EvidenceCards even if no ETF was bought.
□ QC metadata is stored as operational context, not historical truth.
□ Re-running the same day does not overwrite existing signals.
□ Signal rows with data_lag_days > 1 are flagged.
```

Implementation note:

PR5B should start as early as possible, even while PR5A is being refined. Live
paper samples are time-sensitive: every day without a ledger is a day of signal
history that cannot be recovered.

## PR6: Outcome Labeler

Goal: append yfinance outcome labels to both historical replay signals and
live/paper frozen signals.

Add:

```text
services/signal_outcome_labeler.py
tests/test_signal_outcome_labeler.py
```

Inputs:

- FrozenSignal rows
- yfinance future close/return rows
- SPY benchmark rows

Rules:

```text
Label horizons: 1, 5, 20 trading days.
Do not label before horizon maturity.
Do not modify FrozenSignal.
watch/neutral/hold hit = null.
hedge hit uses SPY forward return/drawdown.
```

Acceptance criteria:

```text
□ T+5 outcome is not generated before it matures.
□ increase hit uses forward return and excess vs SPY.
□ hedge hit ignores UVXY return and uses SPY market stress.
□ outcomes are idempotent per signal_id + horizon + source.
□ excess_calculation_method is recorded for every labeled outcome.
```

## PR7: Conviction Calibrator

Goal: compute reliability profiles from FrozenSignals and SignalOutcomes.

Add:

```text
services/strategy_conviction.py
tests/test_strategy_conviction.py
storage: strategy_conviction_profiles
```

Profile keys:

```text
strategy_id
ticker
branch
action
regime_at_signal
horizon_days
source_bucket
```

Source buckets:

```text
historical_prior      # yfinance replay
live_paper            # live/paper frozen signals labeled by yfinance
combined              # weighted blend with explicit source counts
```

Conviction output:

```json
{
  "conviction": 0.42,
  "status": "early_estimate",
  "source_bucket": "historical_prior",
  "n": 14,
  "required": 30,
  "hit_rate": 0.57,
  "avg_forward_return": 0.008,
  "avg_excess_vs_spy": 0.006,
  "ic": 0.12,
  "max_adverse_drawdown": -0.041,
  "data_lag_filtered": 3,
  "requires_live_confirmation": true
}
```

Sample states:

```text
n < 10:
  status = insufficient_samples
  conviction = null

10 <= n < 30:
  status = early_estimate

n >= 30:
  status = calibrated
```

Suggested historical conviction formula:

```text
conviction =
  0.40 * hit_rate
  + 0.30 * normalized_avg_excess
  + 0.30 * positive_ic
```

Combined conviction rule:

```text
if live_n < 10:
  combined = historical_prior
  status = historical_prior_requires_live_confirmation

if live_n >= 10:
  use sample-size weighting with live credibility bonus:
    hist_weight = hist_n / (hist_n + live_n)
    live_weight = live_n / (hist_n + live_n)
    adjusted_live_weight = min(live_weight * 1.5, 0.80)
    adjusted_hist_weight = 1.0 - adjusted_live_weight
    combined = adjusted_hist_weight * historical_prior + adjusted_live_weight * live_paper

status:
  10 <= live_n < 30 -> early_live_confirmation
  live_n >= 30 -> calibrated
```

Acceptance criteria:

```text
□ insufficient_samples is explicit and not treated as bearish.
□ historical_prior and live_paper are never collapsed without source counts.
□ data_lag_days > 1 is excluded by default and counted.
□ IC is confidence vs forward return.
□ branch-level profiles can show one branch reliable and another weak.
□ conviction output is never a naked number; it includes n, source_bucket,
  status, data_lag_filtered, and source counts.
```

## PR8: Playground / Dashboard Display

Goal: make the validation state visible to the operator.

Display:

```text
signals_recorded_today
outcomes_labeled_today
pending_outcomes
historical_prior_profiles
live_paper_profiles
combined_profiles
requires_live_confirmation count
```

Per profile:

```text
strategy
ticker
branch
action
horizon
source_bucket
n
status
hit_rate
avg_excess_vs_spy
ic
conviction
last_signal_date
```

Acceptance criteria:

```text
□ Dashboard shows sample accumulation progress.
□ Telegram/Playground uses compact summary only.
□ conviction=null + insufficient_samples is rendered as "—", not 0%.
□ High-risk ETF rows show action-specific hit definition.
```

## PR9: EvidenceCard Reads Conviction

Goal: include conviction in EvidenceCard output while remaining shadow only.

Modify:

```text
services/strategy_evidence.py
```

Add fields or diagnostics:

```json
{
  "conviction": 0.42,
  "conviction_status": "early_estimate",
  "conviction_source_bucket": "combined",
  "conviction_n": 14,
  "effective_confidence": 0.34
}
```

Rules:

```text
insufficient_samples:
  effective_confidence = 0.0
  reason includes insufficient_conviction_samples

historical_prior_requires_live_confirmation:
  effective_confidence = confidence * 0.5
  may display, but does not authorize execution

early_estimate / calibrated:
  effective_confidence = confidence * conviction
```

Acceptance criteria:

```text
□ EvidenceCard can read conviction profile by strategy+ticker+branch+action.
□ conviction only affects diagnostics and shadow construction.
□ production target weights remain unchanged.
□ source bucket and sample count are visible.
```

## Implementation Order

Recommended order:

```text
PR5A: Historical Signal Replay
PR5B: Live/Paper Signal Ledger
PR6: Outcome Labeler
PR7: Conviction Calibrator
PR8: Dashboard / Playground Display
PR9: EvidenceCard Reads Conviction
```

Why this order:

- Historical replay gives immediate prior conviction without waiting weeks.
- Live/paper ledger starts collecting real operating signals as soon as possible
  and should not wait for historical replay perfection.
- Outcome labeler can serve both historical and live signals.
- Conviction can then separate historical prior from live validation.

## Expected Timeline After PR6

```text
Day 0:
  live frozen signals begin

T+1:
  first 1-day live outcomes

T+5:
  first 5-day live outcomes

About 2 weeks:
  first early_estimate live profiles

About 6 weeks:
  first calibrated live profiles
```

Historical replay can produce `historical_prior` immediately, but it must remain
marked as requiring live confirmation until enough live/paper outcomes exist.

## Final Definition Of Done

This phase is done when the system can answer, with source labels:

```text
What did a strategy say historically?
Was it right at the action-specific horizon?
Was a branch reliable or noisy?
Did confidence predict return?
Is current conviction only historical prior or live-confirmed?
Which high-risk ETF signals should remain watch-only?
```

It is not done if conviction appears as a naked number without sample count,
source bucket, status, and data-lag filtering.

## Implementation Status

Implemented components:

```text
PR5A  services/historical_signal_replay.py
PR5B  services/signal_ledger.py
      cron/daily_signal_freeze.py
PR6   services/signal_outcome_labeler.py
PR7   services/strategy_conviction.py
PR8   services/strategy_validation_dashboard.py
      services/playground.py validation_summary
PR9   services/strategy_evidence.py conviction diagnostics
Ops   services/signal_validation_refresh.py
      cron/daily_signal_validation_refresh.py
```

Persistent tables:

```text
strategy_frozen_signals
strategy_signal_outcomes
strategy_conviction_profiles
```

Default config keys:

```text
daily_signal_freeze_config
daily_signal_validation_config
```

## Runbook

Normal after-close observe-only sequence:

```bash
uv run python -m cron.yfinance_backfill
uv run python -m cron.playground_analysis
uv run python -m cron.daily_signal_freeze
uv run python -m cron.daily_signal_validation_refresh
```

`daily_signal_freeze` stores live/paper EvidenceCards as immutable signals.
`daily_signal_validation_refresh` labels mature horizons and refreshes
conviction profiles from persisted DB truth.

Both signal crons are observe-only:

```text
execution_authority = none
no target weights are changed
no execution commands are generated
```

Operator-facing outputs:

```text
PlaygroundBundle.validation_summary
EvidenceCard.conviction_status
EvidenceCard.conviction_source_bucket
EvidenceCard.conviction_n
EvidenceCard.effective_confidence
```

Validation:

```bash
uv run python -m unittest discover tests
```
