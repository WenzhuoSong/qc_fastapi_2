# FULL_AUTO Circuit / Auto-Pause Deadlock Error Report

Created: 2026-05-29

## Executive Summary

The trading system entered `DEFENSIVE` while `authorization_mode=FULL_AUTO`, causing the hourly pipeline to pause repeatedly even though the latest QC account state, data status, and policy version were healthy.

This does not appear to be an alpha/data failure. It is primarily a control-plane safety-state issue:

1. `auto_pause` interpreted old QC rejection events as current consecutive QC rejects.
2. It mixed control-plane `policy_sync` rejection with data-plane trading rejection.
3. `circuit_breaker.py` contains an unreachable `ALERT -> CLOSED` branch, so an `ALERT` state cannot naturally close even when triggers clear.
4. Once `DEFENSIVE` is reached, `FULL_AUTO` is gated before normal pipeline execution, so the system cannot naturally generate fresh successful trading events to break the old reject pattern.

Net result: a recoverable technical/ops issue became a persistent trading halt.

## Current Production State Observed

Read-only SQL was run using `.env.backup`. No secrets were printed and no database configuration was modified.

Observed at database time:

```text
2026-05-29 03:34 UTC
```

Relevant state:

```text
authorization_mode = FULL_AUTO
trading_paused.paused = false
circuit_state.value = DEFENSIVE
circuit_state.primary_trigger = persistent_alert
circuit_state.reason = ALERT persisted 3.0h > 2h -> DEFENSIVE
```

Latest QC/account state looked healthy:

```text
latest daily_feature_snapshot = 2026-05-28 20:10 UTC
latest heartbeat              = 2026-05-28 19:45 UTC
account_status                = ok
data_status                   = ok
policy_version                = sprint8a
open_order_count              = 0
has_open_orders               = false
```

Latest yfinance backfill:

```text
yfinance_backfill success
tickers = 52
failures = 0
```

Recent LLM failure trigger was clear:

```text
LLM stages last 24h: total=0, failed=0
```

Recent rejection log trigger was clear:

```text
rejection_log last 2h count = 0
```

## User-Visible Symptoms

Telegram messages received:

```text
FULL_AUTO: Circuit=ALERT is open. Pipeline paused for ALERT.

Circuit state changed to DEFENSIVE
Reason: ALERT persisted 3.0h > 2h -> DEFENSIVE
Trigger: persistent_alert

FULL_AUTO: Circuit=DEFENSIVE is open. Pipeline paused for DEFENSIVE.
```

The messages only exposed the secondary trigger (`persistent_alert`), not the original trigger that first opened `ALERT`.

## Timeline

### 2026-05-26

`analysis_205` was submitted to QC and rejected:

```text
command_id: analysis_205
command_type: weight_adjustment
qc_status: rejected
qc_rejection_reason: policy_version_mismatch_with_buy
```

Subsequent `analysis_206` to `analysis_209` were blocked before being sent:

```text
qc_status: not_sent
qc_rejection_reason: fastapi_no_qc_command
```

Those `not_sent` events are FastAPI/preflight events, not QC trading rejections.

### 2026-05-27

A policy recovery command was sent and rejected:

```text
command_id: policy_recovery_20260527_160411_1
command_type: policy_sync
qc_status: rejected
qc_rejection_reason: policy_sync_missing_roles_or_caps
```

This was a control-plane command, not a trading command.

### 2026-05-28 14:02 UTC

`hourly_analysis` skipped because `auto_pause` fired:

```text
status: skipped_auto_paused
primary_trigger: consecutive_qc_rejects
reason: 2 consecutive QC rejects >= 2
```

Evidence used by `auto_pause`:

```text
1. policy_recovery_20260527_160411_1
   command_type: policy_sync
   qc_status: rejected
   reason: policy_sync_missing_roles_or_caps

2. analysis_205
   command_type: weight_adjustment
   qc_status: rejected
   reason: policy_version_mismatch_with_buy
```

This means the auto-pause decision combined:

- one control-plane rejection, and
- one older trading rejection,
- across a long time span,
- while ignoring intervening `not_sent`/preflight-blocked attempts as streak breakers.

### 2026-05-28 17:02 UTC

Circuit breaker escalated:

```text
ALERT -> DEFENSIVE
primary_trigger: persistent_alert
reason: ALERT persisted 3.0h > 2h -> DEFENSIVE
```

After this point, FULL_AUTO pipeline was gated before normal execution.

## Code Areas Involved

### `services/auto_pause.py`

Relevant behavior:

- `load_auto_pause_verdict()` loads recent `ExecutionLog` events.
- `_consecutive_qc_rejects_trigger()` counts events where `qc_status == "rejected"`.
- It only breaks the streak on statuses:

```python
{"accepted", "filled", "partial", "submitted", "timeout_no_ack"}
```

It does not explicitly filter by:

- `command_type == "weight_adjustment"`
- data-plane vs control-plane
- event age / time window
- whether a command was actually sent to QC

Relevant file references:

- `/Users/wenzhuosong/Work/trading/qc_fastapi_2/services/auto_pause.py:47`
- `/Users/wenzhuosong/Work/trading/qc_fastapi_2/services/auto_pause.py:153`

### `services/circuit_breaker.py`

There is a likely code bug in `_compute_next_state()`.

The function has two consecutive `elif current == CircuitState.ALERT` branches:

```python
elif current == CircuitState.ALERT:
    # ALERT -> DEFENSIVE
    ...

elif current == CircuitState.ALERT:
    # ALERT -> CLOSED
    ...
```

The second branch is unreachable in Python. Therefore the intended `ALERT -> CLOSED` auto-recovery path cannot run.

Relevant file references:

- `/Users/wenzhuosong/Work/trading/qc_fastapi_2/services/circuit_breaker.py:521`
- `/Users/wenzhuosong/Work/trading/qc_fastapi_2/services/circuit_breaker.py:535`

This is probably the most important implementation defect. Even if all triggers clear, an `ALERT` state will not close through that branch.

### `services/pipeline.py`

In FULL_AUTO, an open circuit gates the pipeline before account guard / auto-pause / strategy stages can proceed:

```python
if auth_mode == "FULL_AUTO" and circuit in ("ALERT", "DEFENSIVE"):
    ...
    return None
```

Relevant file references:

- `/Users/wenzhuosong/Work/trading/qc_fastapi_2/services/pipeline.py:527`
- `/Users/wenzhuosong/Work/trading/qc_fastapi_2/services/pipeline.py:739`
- `/Users/wenzhuosong/Work/trading/qc_fastapi_2/services/pipeline.py:839`

This is desirable for true market/account risk, but harmful when the circuit was opened by stale or misclassified technical evidence.

## Root-Cause Hypotheses

### Confirmed / High Confidence

1. `auto_pause` mixed data-plane and control-plane events.

The policy recovery reject was counted as a QC reject even though it was `command_type=policy_sync`.

2. `auto_pause` used stale events.

The auto-pause trigger at 2026-05-28 14:02 used a 2026-05-26 trading reject as part of the consecutive reject evidence.

3. `not_sent` / preflight-blocked events did not break the consecutive reject streak.

Several attempts after `analysis_205` were `qc_status=not_sent`. They should probably break QC-reject streaks because QC did not reject them.

4. `ALERT -> CLOSED` is unreachable due to duplicate `elif current == CircuitState.ALERT`.

This can make `ALERT` sticky even after trigger conditions clear.

### Medium Confidence

5. `DEFENSIVE` is too sticky for technical triggers.

The current state machine only allows:

```text
DEFENSIVE -> ALERT
```

when market-style triggers clear. It does not distinguish technical auto-pause from market-risk defensive mode.

6. Operator-facing Telegram messages hide original trigger evidence.

The user saw `persistent_alert`, but not the original `auto_pause/consecutive_qc_rejects` evidence.

## Why FULL_AUTO Still Paused

`FULL_AUTO` means:

```text
If all safety gates pass, execute automatically.
```

It does not mean:

```text
Ignore circuit / account guard / auto-pause / preflight.
```

Therefore FULL_AUTO pausing is expected when safety layers trigger.

The bug is that a safety layer triggered on stale/misclassified evidence, then the circuit state machine failed to naturally recover.

## Expected Professional Behavior

The system should distinguish:

```text
Market/account risk halt:
  - stale account snapshot
  - open orders
  - large drawdown
  - VIX spike
  - repeated fresh trading rejects
  -> can latch until human reset

Technical/control-plane recoverable issue:
  - policy sync failed
  - old policy mismatch already resolved
  - LLM degradation
  - dashboard/report generation failure
  -> should alert, degrade, retry, or auto-clear after evidence clears
```

The current behavior treated a technical/control-plane pattern like a trading-risk halt.

## Recommended Fix Options

### P0 Fixes

1. Fix unreachable `ALERT -> CLOSED` branch.

Refactor `_compute_next_state()` so `ALERT` handling includes both:

- escalation to `DEFENSIVE`, and
- closure to `CLOSED` after all triggers clear and cooldown elapses.

2. Filter auto-pause consecutive rejects to data-plane trading commands only.

Suggested condition:

```text
command_type == "weight_adjustment"
qc_status == "rejected"
command was actually sent to QC
```

Exclude:

```text
policy_sync
policy_recovery
heartbeat/control commands
not_sent
preflight_blocked
```

3. Add time window to consecutive reject calculation.

For example:

```text
only events within the last 6 or 12 hours
```

A 48-hour-old reject should not trigger a fresh pause.

4. Treat `not_sent` / `preflight_blocked` as streak breakers.

They are not QC rejects and should prevent the algorithm from looking through them to older QC rejections.

### P1 Fixes

5. Add circuit trigger class.

Example:

```text
trigger_class = market_risk | account_risk | execution_risk | control_plane | diagnostics
```

Only some trigger classes should escalate to sticky `DEFENSIVE`.

6. Add technical auto-clear rules.

If circuit was opened by `auto_pause/consecutive_qc_rejects`, it should auto-clear when:

- latest QC/account state is healthy,
- policy version is aligned,
- no recent data-plane trading rejects within the configured window,
- cooldown has elapsed.

7. Improve Telegram alert evidence.

When circuit changes to `ALERT` or `DEFENSIVE`, include:

```text
original_trigger
trigger_class
evidence command_ids
whether evidence is stale
operator action: reset vs wait vs redeploy
```

### P2 Fixes

8. Persist circuit transition history in a dedicated table.

Currently only current state is stored in `system_config.circuit_state`, so original ALERT evidence can be lost or overwritten by `persistent_alert`.

Proposed table:

```text
circuit_transition_events
  id
  from_state
  to_state
  primary_trigger
  trigger_class
  reason
  evidence jsonb
  created_at
```

9. Add unit tests for auto-pause streak semantics.

Required cases:

- policy_sync reject does not count as trading reject
- old reject outside time window does not count
- not_sent breaks reject streak
- two fresh weight_adjustment QC rejects do trigger pause
- accepted/fill event breaks reject streak

10. Add circuit state-machine tests.

Required cases:

- `CLOSED -> ALERT`
- `ALERT -> CLOSED` after all triggers clear and cooldown elapsed
- `ALERT -> DEFENSIVE` after persistent alert
- technical ALERT auto-clear does not require manual reset
- market-risk DEFENSIVE remains sticky unless explicitly reset

## SQL Evidence Queries Used

The following read-only query patterns were used.

Current circuit and config:

```sql
select key,
       value->>'value' as value_text,
       value->>'primary_trigger' as primary_trigger,
       value->>'reason' as reason,
       value->>'updated_at' as value_updated_at,
       updated_at as row_updated_at,
       updated_by
from system_config
where key in ('circuit_state','authorization_mode','trading_paused','last_vix')
order by key;
```

Auto-pause event:

```sql
select key, value, updated_at, updated_by
from system_config
where key in ('auto_pause_config','auto_pause_last_event','circuit_state',
              'circuit_pause_alert','circuit_override','policy_sync_recovery_state')
order by key;
```

Recent execution events:

```sql
select executed_at, qc_ack_at, command_id, command_type, status, qc_status,
       qc_rejection_reason
from execution_log
where command_id is not null
order by coalesce(qc_ack_at, executed_at) desc
limit 12;
```

Latest account state:

```sql
select recorded_at, account_timestamp, source_packet_type, contract_version,
       account_status, data_status, policy_version, open_order_count,
       has_open_orders, is_market_open, cash_pct
from account_state_snapshots
order by recorded_at desc
limit 3;
```

Cron failures / skips:

```sql
select job_name, status, started_at, finished_at, duration_ms, rows_written,
       error_message, summary
from cron_run_log
where started_at >= now() - interval '72 hours'
  and status <> 'success'
order by started_at desc;
```

## Questions for Third-Party Review

1. Should `auto_pause` be allowed to set the global circuit to `ALERT`, or should it maintain a separate `execution_pause_state`?

2. Should `DEFENSIVE` be reserved only for market/account risk, while technical execution issues stay in `ALERT`?

3. What is the correct time window for consecutive QC trading rejects: 2h, 6h, 12h, or one trading session?

4. Should policy sync failures ever block trading if QC and FastAPI already report matching `policy_version`?

5. Should a preflight-blocked command break the QC rejection streak?

6. Should circuit closure be automatic after technical triggers clear, or always require `/reset_circuit`?

7. What evidence should Telegram include so an operator can safely decide between `/confirm`, `/reset_circuit`, redeploy, or wait?

## Immediate Operator Note

As of the observed SQL state, QC/account/policy/data looked healthy, but the system remained in `DEFENSIVE`.

However, manually resetting circuit before fixing auto-pause semantics may cause the same stale evidence to re-open `ALERT`.

Recommended order:

1. Patch auto-pause event filtering and circuit `ALERT -> CLOSED`.
2. Deploy.
3. Verify `auto_pause_last_event` no longer triggers from stale/control-plane rejects.
4. Then reset circuit.

