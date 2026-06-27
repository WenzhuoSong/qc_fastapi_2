# newBase FastAPI Observer Contract

FastAPI/Railway observes, records, audits, and reports. QC/newBase decides and trades.

Monitoring has eyes, not hands.

## Scope

This contract supports `newBase` as the QuantConnect trading core while keeping
FastAPI/Railway out of target generation and execution. The active QC algorithm
variant is `stronger252_target3_v1`; FastAPI stores it as
`strategy_live_snapshots.algorithm_version` and must not mix it with older
newBase live samples when calculating live profile metrics.

FastAPI may:

- ingest QC/newBase live telemetry,
- persist equity, holdings, orders/fills, fees, turnover, and benchmark context,
- produce review-only operator reports,
- flag live profile drift for human review.
- run FULL_AUTO monitoring that checks whether expected newBase telemetry has
  arrived.

FastAPI must not:

- generate or edit QC/newBase target weights,
- submit, cancel, or mutate newBase orders,
- automatically pause, reduce, resize, or reconfigure newBase from monitoring flags,
- treat registry entries as execution authorization.

## Data Model

`strategy_registry_entries`

- descriptive strategy metadata,
- `strategy_id='newbase'`,
- current expected `algorithm_version='stronger252_target3_v1'`,
- `source='QuantConnect'`,
- `benchmark_primary='QQQ'`,
- `benchmark_secondary='SPY'`,
- `execution_authority='none'`,
- `target_weight_mutation='none'`,
- `review_only=true`.

`strategy_live_snapshots`

- point-in-time QC/newBase telemetry,
- stores `algorithm_version` from QC so live profile reports can be version-aware,
- primary comparison fields are QQQ-relative,
- stores raw payload for audit,
- stores orders/fills for observation only.

## Operator Snapshot

The first line of every operator snapshot must be:

- live `newBase` cumulative/rolling excess versus QQQ.

Operator snapshots must use only the latest observed `algorithm_version` for
return/profile calculations. Prior-version rows may be counted as ignored
history, but must not be compounded into the current live result.

Secondary fields may include:

- live versus SPY,
- rolling beta versus QQQ,
- drawdown,
- turnover and fees,
- holdings/orders/fills counts,
- review-only profile drift flags.

Any red flag has exactly one allowed effect: it enters an operator pack for
human review. Automatic trade response is forbidden.

## FULL_AUTO Meaning

When `active_strategy='newbase'`, `authorization_mode='FULL_AUTO'` means
FastAPI automatically monitors the newBase telemetry stream. It does not enter
the legacy strategy construction or SetWeights execution pipeline.

The FULL_AUTO monitor may open a circuit if the expected post-close
`newbase_live_snapshot` is missing or stale. It must not send target weights,
cancel orders, or otherwise influence QC/newBase trading.

## Production Cron Policy

`cron.newbase_monitor` is the active Railway cron for newBase telemetry
freshness. It records every run in `cron_run_log`, compares the latest
`strategy_live_snapshots` record to the expected trading date, and may set a
circuit alert when telemetry is missing or stale.

When `active_strategy='newbase'`, legacy FastAPI strategy cron entrypoints are
not allowed to run their old execution logic:

- `cron.hourly_analysis` routes to the newBase monitor branch before legacy
  account-state and execution guards.
- `cron.pending_check`, dynamic scheduler timeout handling, quarterly analyst,
  weekly/monthly analysts, legacy position monitor, validation observation
  refresh, playground, signal freeze, and signal validation refresh mark
  themselves skipped or return no-op summaries.
- `cron.post_market_report` sends a newBase operator snapshot derived from
  `strategy_live_snapshots`.
- `cron.morning_health` keeps its operational checks and adds a read-only
  newBase telemetry status block.

## Command Boundary

While `active_strategy='newbase'`, FastAPI command surfaces must fail closed for
execution-grade actions:

- Telegram `/confirm`, `/cancel_orders`, `/approve_strategy`,
  `/skip_strategy`, `/pc_promotion`, and `/force_reconcile` return a
  disabled-command message.
- QC command tools block `SetWeights`, `PolicySync`, `CancelOrders`, and
  `EmergencyLiquidate` before any outbound command is sent.
- Emergency webhook handling may alert and update circuit state, but automatic
  liquidation is disabled.

## Deployment Boundary

QuantConnect owns trading decisions. The QC-side patch should only export a
`newbase_live_snapshot_v1` telemetry packet. See
`docs/qc_newbase_live_snapshot_export_spec.md`.
