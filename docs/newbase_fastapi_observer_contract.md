# newBase FastAPI Observer Contract

FastAPI/Railway observes, records, audits, and reports. QC/newBase decides and trades.

Monitoring has eyes, not hands.

## Scope

This contract supports `newBase` as the QuantConnect trading core while keeping
FastAPI/Railway out of target generation and execution.

FastAPI may:

- ingest QC/newBase live telemetry,
- persist equity, holdings, orders/fills, fees, turnover, and benchmark context,
- produce review-only operator reports,
- flag live profile drift for human review.

FastAPI must not:

- generate or edit QC/newBase target weights,
- submit, cancel, or mutate newBase orders,
- automatically pause, reduce, resize, or reconfigure newBase from monitoring flags,
- treat registry entries as execution authorization.

## Data Model

`strategy_registry_entries`

- descriptive strategy metadata,
- `strategy_id='newbase'`,
- `source='QuantConnect'`,
- `benchmark_primary='QQQ'`,
- `benchmark_secondary='SPY'`,
- `execution_authority='none'`,
- `target_weight_mutation='none'`,
- `review_only=true`.

`strategy_live_snapshots`

- point-in-time QC/newBase telemetry,
- primary comparison fields are QQQ-relative,
- stores raw payload for audit,
- stores orders/fills for observation only.

## Operator Snapshot

The first line of every operator snapshot must be:

- live `newBase` cumulative/rolling excess versus QQQ.

Secondary fields may include:

- live versus SPY,
- rolling beta versus QQQ,
- drawdown,
- turnover and fees,
- holdings/orders/fills counts,
- review-only profile drift flags.

Any red flag has exactly one allowed effect: it enters an operator pack for
human review. Automatic trade response is forbidden.

## Deployment Boundary

QuantConnect owns trading decisions. The QC-side patch should only export a
`newbase_live_snapshot_v1` telemetry packet. See
`docs/qc_newbase_live_snapshot_export_spec.md`.
