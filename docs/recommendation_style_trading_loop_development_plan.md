# Recommendation-Style Trading Loop Development Plan

> Companion plan for
> `docs/recommendation_style_trading_architecture_review.md`.
>
> Goal: turn the recommendation-style trading architecture into an executable
> implementation sequence without destabilizing the current live loop.

---

## Guiding Principle

The immediate goal is not a large data compass, ML training system, or new
ranking model. The immediate goal is a robust normal loop:

```text
data in
  -> agent market/risk analysis
  -> deterministic target generation
  -> final risk validation
  -> QC command
  -> QC execution truth
  -> latest account state back into FastAPI
```

P0 must cover both:

```text
liveness: the loop runs when inputs are healthy
safety:   the loop halts when state is untrusted
```

No PR in this plan may loosen risk validation, allow LLMs to directly own
execution weights, or promote diagnostic ranking output into execution authority.

---

## Current Baseline

Already implemented:

- QC/yfinance/news data ingestion
- agent analysis pipeline
- `OPENAI_MODEL_HEAVY=gpt-5.4-mini` compatibility helper
- target builder / position governance / position manager
- `TargetEnvelope` and `MutationLedger`
- final risk validation
- min executable weight floor
- QC command lifecycle
- execution logs
- validation observation loop

Recent fixes that must be validated in production:

- `gpt-5.4-mini` no longer causes Chat Completions parameter failures.
- `min_executable_weight_floor` is an allowed tighten-only mutation.
- Circuit has been manually reset after the LLM failure alert.

---

## PR0: Live-Loop Smoke Validation

### Goal

Confirm the latest deployment can run a market-hours `hourly_analysis` through
the LLM stages without avoidable engineering failures.

### Scope

No code changes unless validation finds a bug.

Manual validation:

```bash
uv run python -m cron.weekly_analyst
```

Expected:

- OpenAI `/v1/chat/completions` returns 200 for heavy model.
- No model parameter error.

Market-hours validation:

```bash
uv run python -m cron.hourly_analysis
```

Only run this when:

- QC account snapshot is fresh.
- `circuit_state=CLOSED`.
- Initial market-hours smoke should run in SEMI_AUTO / approval-required mode
  while PR1 and PR2 are not yet in place.
- FULL_AUTO smoke is allowed only after the operator explicitly accepts live
  command risk, or after operator halt and reconciliation blocking are deployed.

Expected:

- no `llm_failure` circuit alert
- no `unknown_post_risk_mutation_type`
- if blocked, reason is a real safety reason, not an engineering contract bug

### Definition of Done

- Weekly heavy-model smoke test passes.
- First market-hours hourly run after deploy does not trigger LLM failure.
- Before PR1 is deployed, SEMI_AUTO approval path is verified to prevent
  automatic QC command submission during smoke validation.
- Telegram message, if any, clearly identifies the blocker.

---

## PR1: Operator Halt / Resume Contract

### Goal

Add an explicit operator kill switch for the entire loop.

The architecture already has `trading_paused` and `/pause`, but the plan needs a
first-class halt/resume contract:

```text
/halt   -> stop all new analysis/execution loops immediately
/resume -> clear operator halt only after explicit operator action
```

### Rationale

Manual halt is P0 safety, not UX polish. When account truth, policy sync,
execution feedback, or market conditions are untrusted, the operator must be
able to freeze the loop with one command.

### Three-Latch Safety Model

Use independent safety latches. The target end state is:

```text
can_trade = not circuit_paused
          and not operator_halt
          and not reconciliation_halt
```

Each latch has a separate owner and reset path:

| Latch | Owner | Set By | Cleared By |
|---|---|---|---|
| `circuit_paused` | automatic safety | circuit breaker / auto-pause | `/reset_circuit` |
| `operator_halt` | human operator | `/halt` | `/resume` |
| `reconciliation_halt` | state-integrity guard | reconciliation divergence | explicit operator clear after investigation |

Do not reuse one latch for another. `/reset_circuit` must not clear
`operator_halt` or `reconciliation_halt`.

### Current-Phase Reconciliation Latch Scope

In this phase, reconciliation safety is implemented as current-run blocking,
not as a persisted halt latch:

```text
can_trade_this_run = not circuit_paused
                   and not operator_halt
                   and not reconciliation_guard.should_block_current_run
```

A reconciliation divergence already prevents new QC commands and alerts the
operator. Persistent `reconciliation_halt_state` is deferred until QC feedback
trust has demonstrated production stability. When it is added, it must be a
complete independent latch:

- persisted across restarts
- explicitly operator-cleared
- fail-safe on malformed or unknown state
- not automatically cleared by a later clean-looking snapshot

This avoids shipping a half-built safety latch that would imply stronger state
integrity guarantees than the current feedback loop has proven in production.

### Implementation

Files likely involved:

- `services/telegram_commands.py`
- `services/pipeline.py`
- `services/system_config.py` / `db.queries`
- tests for Telegram commands and pipeline guard

Suggested config:

```json
{
  "value": true,
  "reason": "operator_halt",
  "updated_at": "...",
  "updated_by": "telegram"
}
```

Use a dedicated `operator_halt_state`. Do not reuse `trading_paused`; that
state already carries broader pause semantics and can conflict with circuit
pause/resume flows.

### Behavior

- `/halt` sets the loop into halted state.
- Pipeline exits before data/LLM/execution work.
- `/resume` clears the halt state.
- `/status` shows halt state and reason.
- Halt/resume writes audit metadata.
- Halt state is persisted in `system_config`.
- On startup or config-read ambiguity, fail safe: unknown halt state should not
  silently become running.

### Tests

- `/halt` sets halted state.
- Pipeline returns skipped/halted before execution path.
- `/resume` clears halted state.
- `/status` includes halt state.
- Halt state survives simulated process restart.
- `/reset_circuit` does not clear operator halt.
- Unknown or malformed halt state fails safe according to config.

### Definition of Done

- Operator can halt and resume with Telegram commands.
- Halt state survives process restarts.
- Halt blocks both SEMI_AUTO and FULL_AUTO pipeline execution.
- Resume is explicit, audited, and scoped only to `operator_halt`.

---

## PR2a-0: Command Lifecycle Skeleton

### Goal

Create the shared command lifecycle entity before QC feedback trust,
reconciliation, target fingerprinting, and execution display start adding
their own state.

### Rationale

PR2a, PR3, and PR4b all describe the same real-world object: one command sent
from FastAPI to QC. If each PR grows its own fields, the schema will churn and
the system can end up with split truth about one command.

Build the lifecycle skeleton once, then let later PRs fill behavior into it.

### Minimal Fields

The initial entity should include:

```text
command_id
correlation_id
source_analysis_id
command_type
policy_version
target_fingerprint        # nullable until PR3
lifecycle_state
created_at
updated_at
submitted_at
latest_qc_ack_at
metadata                  # JSON, versioned if structured
```

Initial lifecycle states:

```text
created
pending_ack
accepted
rejected
orders_submitted
partial
filled
noop_reconciled
pending_reconcile
diverged
```

### Rules

- `correlation_id` and `command_id` are identity metadata.
- `target_fingerprint` may be null until PR3.
- No fingerprint hash logic is implemented in this PR.
- ACK handlers must look up the lifecycle row by command/correlation id.
- Duplicate ACKs update the existing lifecycle row, not create a new command.
- Ordinary FastAPI-originated supersede is intentionally out of scope for this
  phase. While an execution is in flight, new analysis commands are blocked,
  including risk-reducing commands. Emergency de-risking during an in-flight
  command requires operator manual intervention for now.
- `superseded` is allowed only as a QC-reported terminal lifecycle event when
  QC explicitly reports that an emergency liquidation or reduce-only override
  replaced an already-active command via `superseded_command_id`.
- `superseded` is not a normal command-row lifecycle state in this phase. It is
  an append-only event for the previous command, preserving QC execution truth
  without enabling ordinary command stacking from FastAPI.

### Tests

- Command lifecycle row can be created before send.
- ACK can resolve an existing row by command/correlation id.
- Duplicate ACK does not create a second row.
- Lifecycle state survives process restart.
- Unknown command ACK is classified and logged, not silently trusted.
- No FastAPI-originated ordinary supersede state is emitted in this phase.
- QC-reported `superseded_command_id` is recorded as an append-only lifecycle
  event for the replaced command.

### Definition of Done

- There is one shared command lifecycle entity.
- PR2a, PR3, and PR4b have an agreed place to read/write command state.
- The entity exists before feedback trust, fingerprinting, and display logic
  are layered on top.
- Supersede handling is limited to QC-reported emergency/reduce-only override
  events; ordinary in-flight command replacement remains out of scope.

---

## PR2a: QC Feedback Trust Foundation

### Goal

Extract the minimum QC execution feedback contract needed before reconciliation
divergence logic can be trusted.

### Rationale

Reconciliation divergence depends on reliable QC actual state. If feedback is
partial, stale, or missing per-leg status, a halt mechanism cannot safely
distinguish true divergence from incomplete feedback.

This PR is the trusted-input foundation for PR2b.
It consumes the command lifecycle skeleton from PR2a-0 rather than creating a
separate execution state model.

### Minimum Payload Contract

QC feedback should expose, when available:

- command/correlation id
- per-leg fill status
- post-execution quantity
- post-execution average price
- timestamp
- account equity / total value
- buying power
- holdings after execution
- open orders after execution
- policy version used

### Implementation

Likely files:

- QC webhook/ACK route
- `services/account_snapshot_store.py`
- `services/execution_log_store.py`
- `services/execution_lifecycle.py`

Behavior:

- ACK alone is not reconciled.
- ACK must resolve to a known command lifecycle row by command/correlation id;
  otherwise it is logged as `unknown_command_feedback` and not trusted for
  reconciliation.
- ACK with incomplete state is `pending_reconcile` or `partial`, not `filled`.
- If usable account state is present, write an `execution_ack` account snapshot.
- Preserve backward compatibility with older QC payloads, but mark them as
  insufficient for hard reconciliation.

### Tests

- ACK with full holdings writes account snapshot.
- ACK without enough state does not overwrite a complete heartbeat snapshot.
- Missing per-leg fill status prevents hard reconciled state.
- Partial fill stays partial/pending.
- No-op is distinguishable from filled.

### Definition of Done

- FastAPI can classify whether QC feedback is sufficient for reconciliation.
- Execution lifecycle no longer treats ACK alone as reconciled.
- PR2b can consume a clear trusted/untrusted feedback flag.

---

## PR2b: Reconciliation Divergence Block / Halt

### Goal

Stop sending new commands when FastAPI's expected holdings and QC's latest
trusted account truth diverge beyond tolerance.

### Rationale

This is the most important safety invariant:

```text
If base state is untrusted, no new command should be sent.
```

In the first implementation, divergence should block the current run and alert.
Automatic persistent `reconciliation_halt` should be enabled only after PR2a
feedback trust is stable in production.

### Implementation

Create a single reconciliation calculation service, for example:

```text
services/reconciliation_guard.py
```

Inputs:

- latest `account_state_snapshots`
- latest execution command target / TargetEnvelope
- latest execution lifecycle state
- feedback trust classification from PR2a
- reconciliation tolerance config

Output:

```json
{
  "status": "pass | diverged | insufficient_data | untrusted_feedback | in_flight",
  "should_block_current_run": true,
  "should_set_reconciliation_halt": false,
  "max_drift": 0.0,
  "drift_tickers": [
    {"ticker": "QQQ", "expected": 0.10, "actual": 0.13, "diff": 0.03}
  ],
  "reason": "holdings_reconciliation_divergence"
}
```

Config:

```json
{
  "enabled": true,
  "mode": "blocking",
  "relative_weight_tolerance": 0.0025,
  "absolute_notional_tolerance_usd": 100.0,
  "ignore_cash": true,
  "cash_tolerance_mode": "residual",
  "market_closed_behavior": "skip",
  "auto_set_reconciliation_halt": false,
  "auto_halt_min_clean_market_runs": 20,
  "auto_halt_min_clean_market_days": 5,
  "max_pending_ack_age_seconds": 300,
  "max_in_flight_age_seconds": 900
}
```

Do not reuse the dedupe tolerance here. Reconciliation tolerance must use:

```text
max(absolute notional floor, relative weight tolerance)
```

because whole-share execution and price movement make pure weight percentage
too brittle for small positions.

Cash should be treated as a residual by default. Whole-share ETF execution
means cash is the plug variable after price movement and integer share
rounding. The primary reconciliation check should compare risk-asset holdings;
cash can be shown diagnostically or checked with a much wider residual
tolerance.

### In-Flight Command Rule

Reconciliation divergence is evaluated only against a settled baseline.

If the latest command lifecycle state is any of:

```text
pending_ack
orders_submitted
partial
pending_reconcile
```

then the reconciliation guard should return:

```json
{
  "status": "in_flight",
  "should_block_current_run": true,
  "should_set_reconciliation_halt": false,
  "reason": "execution_in_flight_wait_for_settlement"
}
```

This is not a divergence. A partial fill that honestly reports holdings between
the old state and the target should not set `reconciliation_halt`.

Terminal or settled states such as `filled`, `noop_reconciled`, `rejected`, or
`diverged` can be evaluated normally.

### Stuck In-Flight Rule

In-flight is a temporary state, not an indefinite quiet skip.

If an in-flight lifecycle state exceeds its timeout:

- `pending_ack` older than `max_pending_ack_age_seconds`
- `orders_submitted`, `partial`, or `pending_reconcile` older than
  `max_in_flight_age_seconds`

then the guard should return:

```json
{
  "status": "stuck_in_flight",
  "should_block_current_run": true,
  "should_set_reconciliation_halt": false,
  "reason": "execution_in_flight_timeout",
  "stuck_command_id": "analysis_..."
}
```

This still should not be marked as divergence. It is an ops warning that the
command lifecycle may be stuck or the QC feedback path may be broken.

Pipeline behavior:

- Run after account state guard confirms snapshot is fresh.
- Skip during market-closed periods; no new execution should be initiated and
  stale snapshots are handled by PR5.
- `reconciliation_guard` is read-only with respect to command lifecycle state.
  PR4b owns lifecycle state transitions such as `filled`, `partial`, and
  `diverged`; PR2b consumes those states to block or alert.
- If diverged in blocking mode:
  - do not run target generation
  - do not send commands
  - send Telegram alert with top drift tickers
  - block current run
  - do not set a persistent `reconciliation_halt_state` in this phase
  - expose `should_set_reconciliation_halt` only as a future-ready diagnostic
    flag while `auto_set_reconciliation_halt` remains false

Auto-setting persistent `reconciliation_halt` can be enabled only after a clean
observation period, for example:

- at least `auto_halt_min_clean_market_runs` market-hours runs
- at least `auto_halt_min_clean_market_days` market days
- zero known false `untrusted_feedback` classifications
- zero duplicate ACK state regressions
- zero partial-fill cases misclassified as divergence

When those criteria are met, implement the persistent latch as a separate PR
instead of flipping a half-built state on in production.

### Tests

- Pass when expected and actual holdings are inside tolerance.
- Block current run when drift exceeds tolerance.
- Return `in_flight` and do not halt when the latest command is partial/pending.
- Return `stuck_in_flight` and alert when an in-flight command exceeds timeout.
- Do not auto-halt when feedback is untrusted or incomplete.
- Future-ready `should_set_reconciliation_halt` diagnostic is true only when
  enabled and trusted divergence is observed; this phase still does not persist
  a `reconciliation_halt_state`.
- Ignore stale snapshots if account guard has already blocked.
- Show affected tickers in diagnostics.
- Market-closed behavior skips reconciliation.
- Cash residual alone does not produce a risk-asset divergence.
- Fault injection:
  - divergent snapshot
  - partial fill
  - repeated old ACK
  - missing holdings
  - malformed fill status

### Definition of Done

- Reconciliation divergence prevents new commands.
- Telegram tells operator which tickers diverged.
- Dashboard/summary exposes reconciliation guard state.
- The reconciliation calculation is a single shared service consumed by:
  - pipeline blocking
  - Telegram summaries
  - dashboard/account truth view

---

## PR3: TargetEnvelope Idempotency Fingerprint

### Goal

Replace raw float dictionary same-target dedupe with a normalized target
fingerprint derived from the execution target contract.

### Rationale

"Same target" must be deterministic. Raw floats are not enough:

```text
0.3000 vs 0.3001 may be the same command
0.3000 vs 0.3050 may be a real rebalance
```

### Implementation

Add helper:

```text
services/target_fingerprint.py
```

Input:

- target weights
- command type
- policy version
- dedupe tolerance / rounding precision

Explicitly excluded from the hash:

- `command_id`
- `correlation_id`
- `analysis_id`
- construction epoch id
- timestamps

These are lifecycle metadata. Including any of them would make every run look
unique and would break same-target dedupe.

Output:

```json
{
  "fingerprint": "sha256...",
  "normalized_weights": {"QQQ": 0.1025, "SPY": 0.0710},
  "dedupe_tolerance": 0.0025,
  "policy_version": "sprint8a",
  "command_type": "SetWeights",
  "metadata_not_hashed": {
    "correlation_id": "analysis_...",
    "construction_epoch_id": "..."
  }
}
```

Rules:

- clean ticker names
- drop zero/near-zero weights below tolerance
- round weights to tolerance bucket
- sort tickers
- include policy version
- include command type
- hash only canonical JSON of:

```json
{
  "normalized_weights": "...",
  "policy_version": "...",
  "command_type": "..."
}
```

- never include command/correlation/epoch/timestamp metadata in the hash

Wire into:

- command lifecycle entity
- executor command payload
- execution log
- recent same-target dedupe
- Telegram dedupe message

This PR writes into the command lifecycle skeleton created in PR2a-0:

```text
command_id / correlation_id
target_fingerprint
lifecycle_state
policy_version
command_type
```

### Tests

- Same weights with different order produce same fingerprint.
- Tiny drift within dedupe tolerance produces same fingerprint.
- Material drift outside dedupe tolerance produces different fingerprint.
- Different policy version produces different fingerprint.
- Different command type produces different fingerprint.
- Different command/correlation id with the same target produces the same
  fingerprint.
- Different construction epoch id with the same target produces the same
  fingerprint.
- Fingerprint links to command lifecycle row.
- Duplicate ACK does not create a new lifecycle entity.

### Definition of Done

- Dedupe uses target fingerprint, not raw dict comparison.
- Fingerprint is visible in execution diagnostics.
- Duplicate same-target command does not consume daily cap.

---

## PR4b: QC Execution Feedback State Machine and Display

### Goal

Make QC -> FastAPI execution truth explicit enough that FastAPI can distinguish:

```text
accepted
rejected
orders_submitted
partial_fill
filled
noop_reconciled
pending_reconcile
diverged
```

### Rationale

ACK is not reconciliation. FastAPI must trust actual account state, not command
submission.

### Scope

This PR builds on PR2a-0 and PR2a. It completes lifecycle state transitions
and operator-facing display using the same command lifecycle row that PR3 uses
for fingerprints.

### FastAPI Implementation

Likely files:

- QC webhook/ACK route
- `services/account_snapshot_store.py`
- `services/execution_log_store.py`
- `services/execution_lifecycle.py`
- communicator/dashboard formatting

Behavior:

- If actual holdings are within target tolerance, mark reconciled.
- If no real orders were needed, mark `noop_reconciled`.
- If orders/fills incomplete, mark partial/pending.
- If holdings drift beyond tolerance after settlement, mark diverged and feed
  PR2b.
- Do not mark partial or pending executions as diverged solely because holdings
  have not reached target yet.
- PR4b is the owner of command lifecycle state writes. Reconciliation guard
  reads lifecycle state but should not mutate it.
- If QC reports `superseded_command_id`, append a `superseded` lifecycle event
  for the replaced command and keep the new command's normal lifecycle state.
  This records QC execution truth without introducing an automatic FastAPI
  command-stacking path.

### Tests

- Noop payload displays no-op, not filled.
- Partial fill remains pending/partial.
- Diverged holdings are not marked reconciled.
- Duplicate ACK is idempotent.
- Command lifecycle state cannot move backward incorrectly.
- QC-reported supersede records an append-only event for the replaced command,
  but does not create a FastAPI-originated supersede path.

### Definition of Done

- FastAPI does not treat ACK alone as reconciled.
- Execution state reflects actual holdings/fills.
- Telegram and dashboard show execution truth clearly.
- Command fingerprint and lifecycle state describe the same command entity.

---

## PR5: Market-Closed Account Stale Semantics

### Goal

Reduce noisy alerts when stale account state is expected because the market is
closed, while preserving strict blocking during market hours.

### Rationale

Market-closed stale account snapshots are often normal. Market-open stale
snapshots are dangerous.

Expected closed-market behavior:

- hourly/reporting jobs may run diagnostic analysis if desired
- execution path should not send QC commands
- reconciliation guard should return `skip` if invoked
- stale account snapshots are downgraded only when the stale interval is
  explained by the market-closed window

### Implementation

Add clear classification:

```text
expected_market_closed_stale
unexpected_market_open_stale
extended_closed_stale
```

Rules:

- If market is closed and stale age is explainable by closure window:
  - status info/no-action-needed
  - no repeated warning Telegram
- If market is open and stale:
  - account guard blocks
  - warning Telegram
- If stale far exceeds normal closed window:
  - warning/ops alert even if market closed

### Tests

- Closed-market stale is downgraded when within expected duration.
- Market-open stale remains blocking.
- Extended stale remains warning.
- Telegram copy distinguishes no-action-needed from warning.

### Definition of Done

- Weekend/night stale does not create false panic.
- Market-hours stale remains safety-blocking.

---

## PR6: Versioned Diagnostic Artifacts

### Goal

Start JSON-first migration toward recommendation-system events without creating
premature tables.

### Scope

Add typed diagnostic schemas for:

- `MarketRiskAssessment`
- `CandidateEvent`
- `RankingEvent`
- `PortfolioMixEvent`
- `DecisionFeatureSnapshot`

These are initially embedded in `agent_analysis` / decision diagnostics, not
new tables.

Write discipline:

```text
JSON-first is allowed.
Update-in-place is not allowed for point-in-time observations.
```

Diagnostic observations must be append-only or otherwise immutable. If they are
embedded in `agent_analysis`, append new versioned artifacts instead of
overwriting decision-time observations.

### Required Fields

Every artifact must include:

```json
{
  "schema_version": "market_risk_assessment_v1",
  "created_at": "...",
  "source_stage": "researcher",
  "execution_authority": "none",
  "analysis_id": 123
}
```

Candidate/ranking/mix artifacts should include enough ids to link back to:

- feature snapshot
- strategy id
- ticker
- agent analysis
- decision ledger

### Minimal Decision-Time Feature Snapshot

PR6 should persist a minimal feature snapshot for each decision, without
building the full raw event store yet.

This artifact exists to close the point-in-time gap before PR7 labels are
created:

```json
{
  "schema_version": "decision_feature_snapshot_v1",
  "analysis_id": 123,
  "as_of_time": "...",
  "price_source": "qc_snapshot | yfinance",
  "feature_authority": "qc_live | yfinance | mixed",
  "feature_values": {
    "SPY": {"momentum_60d": 0.0, "atr_pct": 0.0}
  },
  "raw_source_refs": ["account_snapshot:250", "yfinance_batch:..."]
}
```

This is not a full raw event store. It is the immutable decision-time input
surface needed to avoid future label leakage. If a future backfill needs fields
not present in this artifact, that backfill must declare the missing feature
scope instead of reconstructing it silently from future pulls.

If `feature_authority="mixed"`, PR7 labels should treat the record as
`feature_scope_limited` by default. Mixed QC/yfinance feature authority is
allowed for diagnostics, but it should not silently enter training-authority
datasets.

### LLM Market Risk Assessment

Start by formalizing LLM output as:

- market regime
- regime confidence
- primary risks
- risk direction
- conflicts
- operator summary

The artifact is advisory only.

### Tests

- Artifact serializers include `schema_version`.
- Missing required fields fail validation.
- `execution_authority` defaults to `none`.
- Artifacts can be embedded into `agent_analysis` JSON cleanly.
- Append-only artifact writes do not overwrite previous observations.
- DecisionFeatureSnapshot links candidate/ranking/mix artifacts to the
  decision-time input surface.
- Mixed-authority DecisionFeatureSnapshot records are flagged as
  training-limited by default.

### Definition of Done

- All new diagnostic artifacts are versioned.
- No unversioned candidate/ranking/mix JSON is introduced.
- No artifact has execution authority.
- Point-in-time artifacts are immutable or append-only.
- Future labels can reference a decision-time feature snapshot id.

---

## PR7: Point-in-Time Label Contract

### Goal

Prepare outcome labels without label leakage.

### Rationale

Outcome labels become training data later. If labels are contaminated now, ML
or ranking work will learn from false data.

### Implementation

Add label contract helpers before building a full label store.

Required label metadata:

```json
{
  "label_schema_version": "outcome_label_v1",
  "decision_time": "...",
  "as_of_time": "...",
  "horizon": "1d | 5d | 20d",
  "label_source": "qc_execution | qc_snapshot | yfinance",
  "price_source": "fill_price | qc_market_price | yfinance_adjusted_close",
  "return": 0.0,
  "max_drawdown_after_decision": 0.0
}
```

Rules:

- Outcome labels must reference PR6 `DecisionFeatureSnapshot` records for the
  decision-time input surface.
- Do not reconstruct decision-time features from future yfinance pulls.
- Prefer QC execution-side truth for executed decisions.
- If yfinance is used, mark it explicitly.
- Never mix QC and yfinance labels silently.
- If a label cannot be tied to a decision-time feature snapshot, mark it
  `feature_scope_limited` and exclude it from model training by default.

### Tests

- Label requires source metadata.
- Future `as_of_time` relative to decision is required for outcome, but feature
  snapshot ids must point to decision-time data.
- Missing source fails validation.
- Missing decision feature snapshot prevents training-authority labels.

### Definition of Done

- The system has a point-in-time-safe label contract.
- Backfill can begin without silently mixing data sources.

---

## PR8: Operator-First Telegram Summaries

### Goal

Reduce debug-noise Telegram output and make operator action obvious.

### Structure

Every major message should answer:

```text
What happened?
Can the system trade?
Why / why not?
What changed?
What should the operator do?
```

Recommended sections:

- status
- decision
- blockers
- execution truth
- next action

### Examples

Circuit alert:

```text
🟡 Circuit ALERT
Trigger: llm_failure
Reason: LLM failure rate 83% (5/6) in 1h
Pipeline: paused
Recommended: wait for deploy, then /reset_circuit
```

Market-closed stale:

```text
ℹ️ Account snapshot stale because market is closed
Latest snapshot: 20:10 ET
No action needed until next market heartbeat
```

### Tests

- Circuit messages include trigger/reason/recommended action.
- Market-closed stale uses info-level copy.
- Reconciliation divergence includes affected tickers.
- No-op execution does not say filled.

### Definition of Done

- Operator can understand whether to wait, reset, halt, resume, or investigate.

---

## PR9: Account and Holdings Truth View

### Goal

Build the first operator view around ground truth, not strategy complexity.

### Scope

Dashboard/account page should show:

- latest account snapshot age
- source packet type
- NAV / cash / buying power
- open orders
- holdings quantity / weight / average price / unrealized PnL
- target vs actual drift
- last command id
- reconciliation status
- execution state

### Non-Goal

Do not rebuild a full Grafana-like dashboard yet.

### Definition of Done

- Operator can answer: "What does QC say we actually hold right now?"
- Operator can answer: "Is FastAPI's expected target reconciled with QC?"

---

## Dependency Order

Recommended order:

```text
PR0  Live-loop smoke validation
PR1  Operator halt/resume
PR2a-0 Command lifecycle skeleton
PR2a QC feedback trust foundation
PR2b Reconciliation divergence block/halt
PR3  TargetEnvelope idempotency fingerprint
PR4b QC execution feedback state machine and display
PR5  Market-closed stale semantics
PR6  Versioned diagnostic artifacts + decision feature snapshot
PR7  Point-in-time label contract
PR8  Operator-first Telegram summaries
PR9  Account and holdings truth view
```

Why this order:

- PR1 establishes operator-controlled safety first.
- PR2a-0 creates one command lifecycle entity before later PRs add feedback,
  fingerprint, and state-machine behavior.
- PR2a fixes the data dependency needed by reconciliation checks.
- PR2b introduces reconciliation blocking only after feedback trust exists.
- PR3 and PR4b make execution repeatability and lifecycle state reliable.
- PR5 reduces false panic without weakening market-hours safety.
- PR6/PR7 prepare the recommendation-system data architecture without forcing
  premature tables.
- PR8/PR9 improve operator usability after the core contracts are clearer.

---

## Deviations & Deferred Items

These items were discovered during implementation review and are intentionally
documented here so future reviewers do not mistake them for silent gaps.

### QC-Reported Superseded Events

The original PR2a-0 wording treated `superseded` as fully out of scope. Code
review found that QC already emits `superseded_command_id` for emergency
liquidation and reduce-only override flows when an active command must be
replaced. That is an execution-truth event, not dead code.

Decision:

- keep QC-reported `superseded` event handling
- do not add FastAPI-originated ordinary command supersede in this phase
- continue blocking new analysis commands while another command is in flight
- use operator manual intervention for emergency de-risking that must override
  an active command

### Persistent Reconciliation Halt

This phase implements reconciliation safety as current-run blocking plus
operator alerting. Persistent `reconciliation_halt_state` is deferred until QC
feedback trust has demonstrated production stability:

- at least `auto_halt_min_clean_market_runs` market-hours runs
- at least `auto_halt_min_clean_market_days` market days
- zero known false untrusted-feedback classifications
- zero duplicate ACK state regressions
- zero partial-fill cases misclassified as divergence

When implemented, the latch must be independent, persisted, fail-safe, and
operator-cleared only.

### PR0 Production Smoke

PR0 cannot be fully closed by local tests. It remains a production-window
acceptance item requiring:

- market-hours run
- fresh QC snapshot
- circuit closed
- SEMI_AUTO approval path verified before automatic command submission
- no LLM parameter failure
- no `unknown_post_risk_mutation_type`
- any blocker classified as a real safety blocker, not an engineering contract
  bug

---

## Global Definition of Done

This phase is complete when:

- Market-hours hourly pipeline can complete when inputs are healthy.
- LLM failures do not recur from model-parameter incompatibility.
- Known tighten-only mutations do not block final validation.
- Operator can halt/resume the loop.
- Command lifecycle has one shared row per command/correlation id.
- Reconciliation divergence blocks new commands in the current run and alerts
  the operator.
- Same-target dedupe uses a stable target fingerprint.
- QC ACK/reconcile semantics distinguish ACK, partial, filled, noop, and
  diverged; QC-reported supersede is preserved as a terminal event for the
  replaced command.
- Market-closed stale state does not create false panic.
- New diagnostic artifacts are versioned.
- Outcome labels have a point-in-time-safe source contract.
- Persistent `reconciliation_halt_state` is explicitly deferred unless the
  clean production feedback thresholds have been met and the full latch has
  been implemented.

---

## Non-Goals

- Do not relax risk validation.
- Do not let LLM control execution weights.
- Do not promote portfolio construction or ranking models to execution authority
  in this phase.
- Do not build a full ML training pipeline yet.
- Do not build a large data compass/dashboard yet.
- Do not add guards before identifying root cause and target invariant.

---

## Review Questions

1. `/halt` should use a dedicated `operator_halt_state`. It should be one of
   three independent latches: circuit, operator, reconciliation.
2. Reconciliation divergence should initially block the current run and alert.
   Auto-setting `reconciliation_halt` should wait until PR2a feedback trust is
   stable in production.
3. Tolerance must be split:
   - dedupe fingerprint: default 0.25% weight-space bucket
   - reconciliation: `max(absolute_notional_tolerance_usd, relative_weight_tolerance)`
4. QC should send per-leg fill status before FastAPI marks a command as
   reconciled. Without it, the safest state is pending/partial.
5. Versioned artifacts can start in `agent_analysis`, but writes must be
   append-only/immutable with `schema_version` and observation timestamp.
