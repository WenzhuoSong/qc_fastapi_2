# Async QC Execution Lifecycle Development Plan

This document defines the next execution-system upgrade: moving from a short
synchronous ACK model to a fully asynchronous QuantConnect execution lifecycle.

The plan combines two work streams:

1. P0 regression safety for the circuit breaker deadlock class.
2. The main async command lifecycle that separates command acceptance from order
   fill and account reconciliation.

## 0. Priority Overview

```text
P0       Circuit breaker deadlock regression baseline
PR1      QC ACK split + heartbeat last_command_id
PR2      Active command serialization + precise command classification
PR3      Reduce-only / emergency override
PR4      FastAPI lifecycle receiver and state machine
PR5      Agent active execution gate
PR6      Heartbeat reconciliation
PR7      Dashboard and Telegram visibility
PR8      Strict mode, stale active execution, operator commands
```

P0 is a prerequisite. If the deadlock fixes are already deployed, PR1 can start,
but the P0 regression tests must remain in CI.

## 1. Purpose

The core ownership model is:

```text
Agent/FastAPI:
  - decide portfolio targets
  - enforce policy, risk, preflight, and account-state gates before sending
  - record command lifecycle
  - reconcile from QC account truth
  - avoid sending conflicting ordinary rebalance commands

QuantConnect:
  - validate execution commands independently
  - serialize execution
  - submit, cancel, and monitor orders
  - report command, order, fill, and reconciliation events asynchronously
  - remain the execution and account-state authority
```

This plan does not change alpha generation, portfolio construction objectives,
or target weights. It only makes execution state explicit, resilient, and safe
when orders take time, partially fill, fail to ACK, or need operator handling.

## 2. Why This Is Needed

Recent live-paper failures exposed the current limitation:

```text
Agent sends SetWeights
QC receives command
QC callback fails before ACK
Agent sees timeout_no_ack
Daily command / turnover caps remain conservatively consumed
Next commands are blocked even though QC did not execute anything
```

The deeper issue is that the initial ACK currently carries too much meaning.
The system needs two different acknowledgements:

```text
Command ACK:
  Did QC accept or reject responsibility for this command?

Execution lifecycle:
  What happened to the orders and the account after QC accepted?
```

`accepted` must not mean `reconciled`.

## 3. P0 Circuit Breaker Deadlock Regression Baseline

### 3.1 Failure Class

The prior deadlock class had three ingredients:

```text
1. Circuit ALERT could not naturally close due to duplicated ALERT branch logic.
2. Control-plane policy_sync rejects were counted as data-plane trading rejects.
3. Old rejects outside the current session were included in reject streaks.
```

The result was:

```text
stale/control-plane event
-> auto_pause ALERT
-> ALERT persists
-> DEFENSIVE
-> FULL_AUTO pipeline paused
-> no successful event can clear state
```

### 3.2 Required Circuit Behavior

ALERT transition logic must handle escalation and closing in the same branch:

```python
elif current == CircuitState.ALERT:
    if should_escalate_to_defensive(triggers, elapsed):
        return CircuitState.DEFENSIVE, "persistent_alert"

    if all_triggers_cleared(triggers) and elapsed >= alert_cooldown_seconds:
        return CircuitState.CLOSED, "triggers_cleared"

    return CircuitState.ALERT, "still_in_alert"
```

### 3.3 Required Auto-Pause Filtering

Only fresh data-plane trading rejects should count toward a QC reject streak.

Rules:

```text
event age > 6 hours:
  ignored

command_type != weight_adjustment:
  ignored

qc_status in not_sent / preflight_blocked / fastapi_no_qc_command:
  breaks the streak

rejection reason in active execution control reasons:
  ignored

accepted / filled / partial / submitted:
  breaks the streak
```

Active execution control reasons:

```text
active_command_in_progress
already_in_progress
deferred_by_active_execution
duplicate_command_id
```

### 3.4 P0 Regression Tests

Required tests:

```text
test_policy_sync_reject_not_trading_reject
test_old_reject_outside_time_window_ignored
test_not_sent_breaks_reject_streak
test_active_command_reject_not_counted
test_two_fresh_weight_adjustment_rejects_trigger_pause
test_accepted_breaks_reject_streak
test_alert_closes_when_triggers_clear
test_alert_escalates_to_defensive_when_persistent
test_technical_alert_does_not_require_manual_reset
```

## 4. Core Semantics

### 4.1 Control Plane vs Data Plane

Control-plane commands are system-management commands:

```text
PolicySync
CancelOrders
ForceReconcile
RecoveryPing
```

Data-plane commands are trading commands:

```text
SetWeights
EmergencyLiquidate
ReduceOnlySetWeights
```

Control-plane commands must not consume daily trading command caps and must not
be counted as trading rejects.

### 4.2 Two-Layer ACK

| ACK Type | Meaning | Terminal? |
|---|---|---|
| Command ACK | QC received, validated, and accepted/rejected command ownership | rejected is terminal; accepted is not |
| Execution event | QC submitted orders, partially filled, filled, reconciled, drifted, canceled | eventually terminal |

The key invariant:

```text
accepted != reconciled
```

### 4.3 Single Active Ordinary Command

QC must never run two ordinary rebalance commands concurrently.

At most one active ordinary execution may exist:

```text
active_command_id
active_target_weights
active_execution_status
active_started_at
active_open_order_count
```

QC must not queue ordinary rebalance commands. An old target can become stale,
so a later ordinary target should wait until account truth is known and then be
recomputed by Agent.

### 4.4 Execution Skip Reasons

These reasons are semantically different and must not be merged:

```python
class ExecutionSkipReason(str, Enum):
    THROTTLE_DEFERRED = "throttle_deferred"
    ACTIVE_EXECUTION_WAIT = "active_execution_wait"
    PREFLIGHT_BLOCKED = "preflight_blocked"
    GUARD_BLOCKED = "guard_blocked"
```

Meaning:

| Reason | Enters deferred ledger? | Consumes daily cap? | QC reject streak? |
|---|---:|---:|---:|
| `throttle_deferred` | yes | no for deferred remainder | no |
| `active_execution_wait` | no | no | no |
| `preflight_blocked` | no | no | no |
| `guard_blocked` | no | no | no |

`throttle_deferred` means "there is still a desired delta, but operational caps
defer it to a later cycle."

`active_execution_wait` means "do not create a new desired delta yet; wait for
the current execution to reconcile, then rebuild from actual holdings."

## 5. Command State Model

### 5.1 Command-Level States

| State | Meaning | Owner |
|---|---|---|
| `created` | Agent built a command locally | Agent |
| `submitted_to_qc` | Agent sent command through QC API | Agent |
| `accepted` | QC accepted command ownership | QC |
| `rejected` | QC rejected command before execution ownership | QC |
| `timeout_no_ack` | Agent did not receive ACK in the wait window | Agent |
| `timeout_no_execution_confirmed` | Later account truth proves QC did not process command | Agent reconciliation |
| `orders_submitted` | QC submitted orders | QC |
| `partial` | Some fills happened or open orders remain | QC |
| `filled` | Orders filled, not necessarily reconciled yet | QC |
| `reconciled` | Actual holdings are within tolerance of target | Agent/QC |
| `reconciliation_drift` | No open orders, but actual differs from target | Agent/QC |
| `failed_no_fill` | Accepted but no fill and no active target remains | Agent/QC |
| `superseded` | Later reduce-only/emergency command replaced this command | QC |

### 5.2 Terminal vs Non-Terminal

Non-terminal:

```text
submitted_to_qc
accepted
orders_submitted
partial
timeout_no_ack
```

Terminal:

```text
rejected
timeout_no_execution_confirmed
reconciled
reconciliation_drift
failed_no_fill
superseded
```

Timeout is not terminal until account truth confirms what happened.

## 6. QC-Side Contract

### 6.1 SetWeights Validation

When QC receives `SetWeights`, it validates:

```text
command_id present
command_id not previously processed
market open
policy version aligned, or reduce-only during mismatch
weights parse successfully
ticker universe allowed
role caps and group caps respected
no conflicting active ordinary execution
```

Rejected command ACK:

```json
{
  "cmd_id": "analysis_214",
  "status": "rejected",
  "reason": "policy_version_mismatch_with_buy",
  "account_state": { "...": "..." }
}
```

Accepted command ACK:

```json
{
  "cmd_id": "analysis_214",
  "status": "accepted",
  "execution_state": "orders_submitted",
  "active_command_id": "analysis_214",
  "actual_target_weights": { "SPY": 0.10, "QQQ": 0.16 },
  "order_summary": {
    "submitted_order_count": 11,
    "filled_order_count": 0,
    "partial_fill_count": 0,
    "open_order_count_after": 11
  },
  "account_state": { "...": "..." }
}
```

This means QC accepted ownership. It does not mean execution is finished.

### 6.2 Heartbeat Required Fields

Heartbeat must include command execution identity, otherwise Agent cannot
distinguish "ACK lost but QC processed" from "QC never processed."

Required heartbeat fields:

```json
{
  "last_command_id": "analysis_214",
  "active_command_id": "analysis_214",
  "active_execution_status": "partial",
  "processed_command_count": 12,
  "open_order_count": 3,
  "has_open_orders": true,
  "target_weights": { "...": "..." },
  "holdings_weights": { "...": "..." }
}
```

Rules:

```text
last_command_id:
  last accepted data-plane command id

active_command_id:
  command currently being executed or reconciled

active_execution_status:
  accepted / orders_submitted / partial / filled / reconciled / drift

processed_command_count:
  monotonic counter of accepted data-plane commands
```

### 6.3 Active Command Classification

QC and Agent should classify a new ordinary command against the active command:

```python
def classify_new_command_vs_active(
    new_target: dict,
    active_target: dict,
    actual_holdings: dict,
    active_open_orders: int,
    same_target_tolerance: float = 0.005,
) -> str:
    if is_within_tolerance(new_target, active_target, same_target_tolerance):
        return "already_in_progress"

    if is_reduce_only_vs_actual(new_target, actual_holdings):
        return "reduce_only_override_candidate"

    if active_open_orders > 0:
        return "active_command_in_progress"

    return "previous_command_pending_reconciliation"
```

QC behavior:

| Condition | QC Action | Reason |
|---|---|---|
| no active command, no open orders | accept | normal path |
| duplicate command id | ignore/reject | `duplicate_command_id` |
| same target within tolerance | return non-error state | `already_in_progress` |
| active command has open orders | reject ordinary command | `active_command_in_progress` |
| active command has no open orders but unreconciled | report reconciliation first | `previous_command_pending_reconciliation` |
| new target is reduce-only vs actual holdings | allow override path | `reduce_only_override_candidate` |

### 6.4 Reduce-Only / Emergency Override

Ordinary rebalance cannot override active execution.

Reduce-only and emergency commands can override active execution.

Reduce-only must be judged against actual holdings, not active target:

```python
def is_reduce_only_vs_actual(
    new_target: dict,
    actual_holdings: dict,
    tolerance: float = 0.001,
) -> bool:
    for ticker, new_weight in new_target.items():
        current = actual_holdings.get(ticker, 0.0)
        if new_weight > current + tolerance:
            return False
    return True
```

Override sequence:

```text
cancel open orders for active command
mark previous command as superseded
accept reduce-only/emergency command
execute risk-reducing command
emit lifecycle events for both commands
```

Override ACK:

```json
{
  "cmd_id": "analysis_230",
  "status": "accepted",
  "reason": "reduce_only_override",
  "superseded_command_id": "analysis_214",
  "canceled_order_count": 3
}
```

### 6.5 Partial Fill Reporting

QC should emit partial events when:

```text
any order is partially filled
open orders remain after initial submission
actual holdings are not reconciled to target
```

Example:

```json
{
  "cmd_id": "analysis_214",
  "status": "partial",
  "reason": "partial_fill_or_open_orders",
  "order_summary": {
    "submitted_order_count": 11,
    "filled_order_count": 7,
    "partial_fill_count": 2,
    "open_order_count_after": 3
  },
  "actual_holdings_weights": {
    "SPY": 0.102,
    "QQQ": 0.164
  },
  "actual_target_weights": {
    "SPY": 0.104,
    "QQQ": 0.168
  }
}
```

Partial is not a failure. It is an active execution state.

### 6.6 Reconciliation Reporting

QC should report reconciliation when open orders are gone or during scheduled
heartbeat intervals.

Outcomes:

| Outcome | Meaning |
|---|---|
| `reconciled` | actual holdings close to target |
| `reconciliation_drift` | actual holdings materially differ from target |
| `failed_no_fill` | no order filled and no open orders remain |

Initial tolerances:

```text
per_ticker_reconciliation_tolerance = 0.005
portfolio_gross_reconciliation_tolerance = 0.02
```

## 7. Agent/FastAPI Contract

### 7.1 Send Logic

Agent may send ordinary `SetWeights` only if:

```text
account state is fresh
policy version is aligned
no open orders
no active unreconciled ordinary command
final risk validation passed
command preflight passed
```

If active execution exists, ordinary rebalance is skipped:

```text
execution_status = deferred_by_active_execution
qc_status = not_sent
skip_reason = active_execution_wait
```

This must not count as:

```text
QC reject streak
daily command count
daily gross turnover
strategy failure
throttle deferred delta
```

### 7.2 Deferred Message Semantics

Telegram must distinguish operational throttle from active execution wait.

Throttle deferred:

```text
Rebalance deferred: buy delta 5.2% > daily cap 5.0%
Deferred: SOXX +1.8%, PSI +0.8%
Will re-evaluate next cycle
```

Active execution wait:

```text
Rebalance skipped: active command analysis_214 still executing
Open orders: 3 of 11
Status: partial
Will resume after reconciliation
```

### 7.3 Receive Logic

Agent must accept QC async events:

```text
accepted
rejected
orders_submitted
partial
filled
canceled
reconciled
reconciliation_drift
failed_no_fill
superseded
timeout_no_execution_confirmed
```

Each event must be written to `command_lifecycle_events`.

`execution_log.qc_status` should update only through explicit lifecycle
transition rules.

### 7.4 Behavior on Partial Fill

When command is partial:

```text
do not send ordinary rebalance
do not count as QC reject
keep command active
wait for next QC event or heartbeat
allow reduce-only/emergency override
report partial state in dashboard/Telegram
```

### 7.5 Behavior on Reconciliation Drift

When command is `reconciliation_drift`:

```text
close the command
do not blindly resend the old target
next cycle must use actual holdings from QC account snapshot
show target vs actual drift to operator
```

### 7.6 Behavior on Timeout

`timeout_no_ack` remains conservative initially.

After a grace window, Agent may classify:

```text
timeout_no_execution_confirmed
```

only if a later account snapshot proves:

```text
no matching last_command_id
no active target weights
no open orders
account status/data status acceptable
```

If a later snapshot shows matching `last_command_id`, active target, or open
orders, timeout remains pending until reconciliation.

## 8. Data Model Extensions

### 8.1 execution_log New Statuses

Statuses should support:

```text
accepted
partial
filled
reconciled
reconciliation_drift
failed_no_fill
superseded
timeout_no_execution_confirmed
deferred_by_active_execution
```

`command_payload` should include:

```json
{
  "active_execution": {
    "active_command_id": "analysis_214",
    "active_status": "partial",
    "open_order_count": 3
  },
  "timeout_reconciliation": { "...": "..." }
}
```

### 8.2 account_state_snapshots Columns

Required schema extension:

```sql
ALTER TABLE account_state_snapshots
    ADD COLUMN IF NOT EXISTS last_command_id VARCHAR(64),
    ADD COLUMN IF NOT EXISTS active_command_id VARCHAR(64),
    ADD COLUMN IF NOT EXISTS active_execution_status VARCHAR(32),
    ADD COLUMN IF NOT EXISTS processed_command_count INTEGER DEFAULT 0;
```

### 8.3 command_lifecycle_events Event Types

Required event types:

```text
created
submitted_to_qc
qc_accepted
qc_rejected
orders_submitted
partial
filled
canceled
superseded
reconciled
reconciliation_drift
failed_no_fill
qc_timeout
timeout_reconciled_no_execution
deferred_by_active_execution
force_reconciled_by_operator
cancel_orders_requested_by_operator
```

## 9. Config

Initial config:

```json
{
  "execution_lifecycle_config": {
    "enabled": true,
    "mode": "observe",
    "ack_wait_seconds": 10,
    "timeout_reconciliation_grace_minutes": 20,
    "block_ordinary_commands_when_active_execution": true,
    "allow_reduce_only_override": true,
    "allow_emergency_override": true,
    "same_target_tolerance": 0.005,
    "per_ticker_reconciliation_tolerance": 0.005,
    "portfolio_gross_reconciliation_tolerance": 0.02,
    "max_active_execution_minutes": 60,
    "auto_cancel_stale_open_orders": false
  }
}
```

Modes:

| Mode | Meaning |
|---|---|
| `observe` | record would-block / would-defer diagnostics only |
| `active` | enforce active command serialization |
| `strict` | enforce serialization and stale active command escalation |

## 10. PR Plan

### PR1: QC ACK Split and Heartbeat Command Identity

Scope:

```text
QC sends fast accepted/rejected ACK after command validation.
accepted does not mean complete execution.
ACK includes execution_state, active_command_id, order_summary, account_state.
Heartbeat includes last_command_id, active_command_id, active_execution_status,
and processed_command_count.
Agent records accepted as non-terminal.
```

Acceptance:

```text
accepted status means QC accepted command ownership.
accepted command may still have open orders.
partial is not represented as rejected.
Heartbeat last_command_id updates after accepted data-plane command.
Test: ACK lost but heartbeat last_command_id matches command.
Test: accepted != reconciled.
```

### PR2: Active Command Serialization and Classification

Scope:

```text
Add QC active command state fields.
Add ExecutionSkipReason enum.
Reject ordinary SetWeights if active command has open orders.
Return already_in_progress for same-target commands.
Classify reduce-only override candidate without blocking it as ordinary conflict.
Ensure active_command_in_progress does not count toward auto_pause streak.
```

Acceptance:

```text
Ordinary second command is blocked while active execution has open orders.
Duplicate command_id is ignored/rejected without duplicate orders.
Same target does not submit duplicate orders.
Reduce-only candidate is routed to PR3 override path.
active_command_in_progress does not consume daily command or turnover cap.
```

### PR3: Reduce-Only and Emergency Override

Scope:

```text
Detect reduce-only relative to actual holdings.
Cancel active open orders when reduce-only/emergency override is accepted.
Mark previous command superseded.
Accept new risk-reducing command.
Record superseded lifecycle event.
```

Acceptance:

```text
Ordinary rebalance cannot override active execution.
Reduce-only can override active execution.
Emergency liquidation can override active execution.
Superseded command is visible in lifecycle.
Superseded is not counted as QC reject.
```

### PR4: FastAPI Lifecycle Receiver and State Machine

Scope:

```text
Extend ACK/event receiver to parse accepted, rejected, orders_submitted,
partial, filled, canceled, reconciled, reconciliation_drift, failed_no_fill,
and superseded.
Update execution_log and command_lifecycle_events through explicit transitions.
Keep timeout reconciliation from account snapshots.
```

Non-reject statuses:

```text
partial
active_command_in_progress
already_in_progress
deferred_by_active_execution
timeout_no_ack
```

Acceptance:

```text
partial does not trigger auto_pause.
active_command_in_progress does not count as QC reject.
active_command_in_progress does not count daily command or turnover cap.
timeout_no_execution_confirmed releases daily command/turnover budgets.
Every state transition emits command_lifecycle_events.
```

### PR5: Agent Active Execution Gate

Scope:

```text
Add preflight_active_execution.
Observe mode records would_defer_by_active_execution.
Active mode blocks ordinary rebalance when active command is accepted,
orders_submitted, or partial with open orders.
Blocked ordinary command uses skip_reason=active_execution_wait.
Reduce-only/emergency bypasses the ordinary active execution gate.
```

Acceptance:

```text
Ordinary command not sent while prior command is partial/open.
active_execution_wait does not enter deferred ledger.
active_execution_wait does not consume daily cap.
active_execution_wait does not trigger QC reject streak.
Telegram distinguishes active_execution_wait from throttle_deferred.
```

### PR6: Heartbeat Reconciliation

Scope:

```text
On each account snapshot, update active execution state.
Use last_command_id to identify whether QC processed a command.
If open orders are gone, compare actual holdings to target.
Emit reconciled, reconciliation_drift, failed_no_fill, or
timeout_no_execution_confirmed.
```

Reconciliation rules:

```text
If heartbeat last_command_id != active_command_id and open_order_count == 0:
  timeout_no_execution_confirmed may be emitted.

If open_order_count > 0:
  still_active.

If open_order_count == 0:
  compare actual holdings vs target.

If drift within tolerance:
  reconciled.

If drift exceeds tolerance:
  reconciliation_drift.
```

Acceptance:

```text
accepted commands do not remain non-terminal indefinitely.
Reconciliation is based on actual holdings.
timeout_no_execution_confirmed releases daily cap.
reconciliation_drift closes command and does not auto-resend old target.
Dashboard can show accepted-without-reconciled lag.
```

### PR7: Dashboard and Telegram Visibility

Scope:

Dashboard active execution panel:

```text
active_command_id
status
submitted / filled / open order counts
started_at
elapsed
target vs actual drift
can_ordinary_rebalance
can_reduce_only
```

Telegram templates:

```text
accepted
orders_submitted
partial
reconciled
reconciliation_drift
active_blocked_ordinary
timeout_no_execution_confirmed
```

Acceptance:

```text
Operator can distinguish accepted, orders_submitted, partial, reconciled, drift.
Partial fill is not displayed as failure.
Active execution block is clearly not QC rejection.
Reconciliation drift shows concrete differences.
```

### PR8: Strict Mode, Stale Execution, and Operator Commands

Scope:

```text
Detect stale active execution after max_active_execution_minutes.
If open orders persist too long, alert operator.
If no open orders but unreconciled, trigger reconciliation.
Do not auto-cancel stale orders unless auto_cancel_stale_open_orders=true.
Add operator commands:
  /force_reconcile <command_id>
  /cancel_orders
```

Operator flows:

```text
Stale active execution with open orders:
  operator checks dashboard
  /cancel_orders if orders are stuck
  wait for QC heartbeat reconciliation

Stale active execution with no QC trace:
  /force_reconcile <command_id>
  close lifecycle using actual holdings as account truth
```

Acceptance:

```text
Stale active execution does not silently persist.
Auto-cancel defaults to disabled.
/force_reconcile exists and uses actual holdings.
/cancel_orders exists and requires QC-side CancelOrders support.
Stale alert includes command_id, elapsed time, and open_order_count.
```

## 11. Safety Rules

Final rules:

```text
1. QC does not queue ordinary rebalance commands.
2. QC does not run two ordinary rebalance commands concurrently.
3. Agent does not assume accepted means completed.
4. Partial fill is an active state, not a failure.
5. Timeout is not terminal until account truth confirms.
6. Next cycle uses QC actual holdings, not desired targets.
7. Reduce-only and emergency can override active execution.
8. Ordinary rebalance cannot override active execution.
9. Active execution block does not trigger QC reject streak.
10. Active execution block does not consume daily command/turnover cap.
11. Lifecycle state is visible to the operator.
12. Stale active execution must alert and provide operator handling paths.
13. throttle_deferred and active_execution_wait are different states.
```

## 12. Current Incident Mapping

The `analysis_214` incident maps to the fallback path:

```text
submitted_to_qc
-> QC callback crash before ACK
-> timeout_no_ack
-> later account snapshot:
     no last_command_id
     no active_command_id
     no target_weights
     no open_orders
-> timeout_no_execution_confirmed
```

Desired future behavior:

```text
submitted_to_qc
-> QC accepted/rejected quickly
-> if accepted:
     active_execution
     orders_submitted / partial / filled
     reconciled / reconciliation_drift / failed_no_fill
```

If QC callback still crashes before command ACK, timeout reconciliation remains
the fallback. That path should become rare after PR1 and PR6.

## 13. Non-Goals

This plan does not:

```text
change strategy scoring
change portfolio construction objective
relax final risk validation
bypass account state guard
increase canary command caps
force synchronous complete fills
make QC decide new portfolio targets
make QC queue ordinary targets
auto-cancel stale open orders by default
```

## 14. Definition of Done

This work is complete when:

```text
P0 circuit breaker regression tests are in place.
accepted != reconciled in code, tests, dashboard, and Telegram.
QC heartbeat contains last_command_id.
Partial fill is a first-class lifecycle state.
Agent blocks ordinary rebalance during active open orders.
Reduce-only and emergency override paths are explicit.
timeout_no_execution_confirmed releases daily cap after account truth check.
reconciliation_drift closes command and next cycle rebuilds from actual holdings.
/force_reconcile and /cancel_orders operator commands exist.
Dashboard can answer:
  current active command
  open order count
  filled order count
  target vs actual drift
  whether ordinary rebalance is allowed
  whether reduce-only override is allowed
Telegram distinguishes:
  accepted
  partial
  reconciled
  reconciliation_drift
  throttle_deferred
  active_execution_wait
No QC reject streak is triggered by partial, active_command_in_progress,
already_in_progress, superseded, timeout_no_ack, or deferred_by_active_execution.
Stale active execution alerts include command_id, elapsed time, open_order_count,
and recommended operator action.
```
