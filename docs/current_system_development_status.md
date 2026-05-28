# Current System Development Status

Last updated: 2026-05-28

This document summarizes the current engineering state of the trading system after the recent risk, execution, data, alpha-validation, and selective evidence-cap work. It is intended as a handoff document for review and follow-up development.

## Executive Summary

The system has moved from a "missing features" stage through a "dual-track cleanup" stage. The cleanup-phase Definition of Done is now closed.

Most major safety modules now exist:

- deterministic execution policy
- account state guard
- policy alignment guard and manual PolicySync diagnostics
- auto pause
- final risk validation
- execution throttle
- command lifecycle ledger
- selective strategy input readiness
- evidence cards and evidence caps
- alpha validation and statistical independence diagnostics

The main remaining risk is no longer missing execution/risk infrastructure. The remaining risks are now operational monitoring, alpha quality, strategy independence, and future removal of deprecated inactive paths after enough production evidence.

The current engineering direction is:

> Keep safety code-enforced, keep diagnostics visible, and use paper/live evidence to decide which deprecated or weak alpha paths should be retired.

## Most Recent Completed Fixes

### 1. Executor PolicySync Coupling Removed

Previously, the system had two PolicySync paths:

1. automatic `policy_sync_recovery` in the pipeline control plane.
2. `executor` sending PolicySync again immediately before SetWeights.

This violated the intended control-plane/data-plane separation and could reintroduce policy-sync deadlocks.

Current state:

- `agents/executor.py` no longer sends PolicySync before SetWeights.
- The executor now asserts that `account_state_guard` has already confirmed policy version alignment.
- If alignment is not confirmed, executor refuses to send a trade command.
- Runtime policy repair is no longer automatic. QC uses its compiled policy, CI/tests verify it matches FastAPI, and `services/policy_sync_recovery.py` remains available only as an explicitly enabled diagnostic/manual tool.

Relevant files:

- `agents/executor.py`
- `services/execution_preflight.py`
- `services/policy_sync_recovery.py`

### 2. Command Preflight Uses Policy Alignment

The old preflight check was named `policy_sync_success` and required same-cycle PolicySync ACK.

Current state:

- The check is now `policy_alignment_confirmed`.
- It accepts account guard policy alignment confirmation.

This makes recent QC account-state policy alignment authoritative before SetWeights.

Relevant file:

- `services/execution_preflight.py`

### 3. FULL_AUTO Safety Preconditions Are Code-Enforced

FULL_AUTO now has explicit safety preconditions before the pipeline proceeds.

Required in FULL_AUTO:

```json
{
  "account_state_guard.mode": "blocking",
  "final_risk_validation.effective_mode": "blocking",
  "auto_pause.mode": "active"
}
```

If these are not satisfied, the pipeline rejects FULL_AUTO before execution and sends a Telegram warning.

Relevant files:

- `services/full_auto_safety.py`
- `services/pipeline.py`
- `tests/test_full_auto_safety_preconditions.py`

### 4. StrategyInputBuilder Low-Coverage Gate Retired

The old strategy-wide fallback has been removed:

```text
coverage below threshold -> strategy not_scored
```

Current state:

- If no ticker is scorable, the strategy remains `not_scored`.
- If at least one ticker is scorable, excluded tickers are isolated and the strategy becomes `partially_scored`.
- Low coverage is recorded as diagnostics through `coverage_below_min_required`, `coverage_shortfall`, and `partial_scoring_reason`.
- Missing ticker features do not become score zero and do not globally disable the strategy.

Relevant file:

- `services/strategy_input_builder.py`

### 5. Telegram `/confirm` PolicySync Coupling Removed

Manual confirmation now follows the same control-plane/data-plane boundary as FULL_AUTO.

Current state:

- `/confirm` no longer sends PolicySync before SetWeights.
- `/confirm` requires recent account guard policy alignment.
- The freshness window is configurable through `manual_confirm_policy_alignment_config.max_age_seconds`.
- The default freshness window is 300 seconds.
- If the latest account guard result is stale or policy-mismatched, `/confirm` returns a clear operator message and sends no command.

Relevant files:

- `services/telegram_commands.py`
- `services/policy_alignment.py`
- `tests/test_policy_alignment.py`

### 6. Evidence Cap Calibration Freshness Added

Evidence cap gated mode now requires fresh calibration metadata.

Current state:

- `evidence_cap_config` accepts `calibration_generated_at`.
- `evidence_cap_config` accepts `max_calibration_age_days`, default 7 days.
- `require_fresh_calibration` defaults to true.
- If `mode=gated` but calibration metadata is missing, invalid, or stale, effective mode degrades to `observe`.
- Target-builder diagnostics include `calibration_freshness` so dashboards can show calibration age.
- Calibration reports include the timestamp fields needed for operator-reviewed config promotion.

Relevant files:

- `services/evidence_cap_config.py`
- `services/evidence_cap_calibration.py`
- `services/target_builder.py`
- `tests/test_evidence_cap_config.py`
- `tests/test_evidence_cap_calibration.py`

### 7. Legacy Mutation Paths Classified

The old mutation paths in `services/pipeline.py` now have an explicit ownership contract.

Current classification:

| Path | Status | Owner | Execution Authority | Decision |
|---|---|---|---|---|
| `enforce_pm_constraints` | deprecated inactive | none | none | remove after no references confirmed |
| `enforce_pm_constraints_v2` | classified | diagnostic guardrail | none | keep as non-execution adjusted-weights bound |
| `apply_regime_constraints` | classified | post-risk tighten-only | tighten-only | registered final-validation allowed mutation |

`apply_regime_constraints` now registers mutation type:

```text
regime_constraint_tighten
```

When it triggers, the mutation type is written to `risk_out.post_risk_mutation_types`, and final risk validation recognizes it as an allowed tighten-only mutation.

Observe exit criteria for any future unclassified mutation:

```json
{
  "min_cycles_before_decision": 20,
  "decision_deadline": "2026-07-01"
}
```

Relevant files:

- `services/mutation_ownership.py`
- `services/pipeline.py`
- `services/final_risk_validation.py`
- `tests/test_mutation_ownership.py`

## Current Architecture State

### Data Layer

The current design separates data responsibilities:

- yfinance: daily research features and historical replay
- QC heartbeat/account snapshots: live account state, holdings, policy version, open orders
- QC daily snapshot: QC-side feature comparison and fallback diagnostics
- news cache: contextual evidence, not authoritative trading state

The important design principle is:

> yfinance powers research features; QC powers live account truth.

Current positive state:

- Playground no longer performs hidden inline yfinance repair.
- Missing or incomplete data should be handled by cron/backfill and surfaced in diagnostics.
- ETF-level missing features should isolate the affected ETF/strategy pair, not poison the whole strategy pool.

Ongoing watch item:

- Mature ETF feature-health checks must continue to catch accidental missing daily features.
- Young ETFs such as DRAM are handled through partial scoring/abstain diagnostics; they should continue to be monitored so long-history fields do not become false global blockers.

### Strategy and Alpha Layer

The system now supports multiple strategy plugins and diagnostics:

- strategy feature contracts
- strategy input builder
- evidence cards
- vote status / abstain semantics
- empirical replay
- conviction profiles
- strategy family diagnostics
- regime gap analysis
- statistical independence diagnostics

Current positive state:

- The strategy framework is increasingly plug-in friendly.
- Missing strategy inputs are no longer supposed to mean "score = 0".
- Evidence quality can shrink maximum usable weight instead of creating binary allow/block decisions.

Remaining concern:

- Many strategies are still variants of momentum or relative strength.
- Statistical independence is now diagnosed and dashboard-visible, but execution core may still be momentum-heavy.
- Registered strategy count should not be confused with statistically independent alpha count.

### Portfolio Construction

Portfolio Construction currently focuses on:

- signal-weighted effective N
- factor concentration control
- turnover budget
- policy caps
- gated/shadow promotion flow

Current positive state:

- The objective is more explicit than before.
- PC is constrained by policy and risk layers.
- PC can be run in shadow/diagnostic mode before gated influence.

Remaining concern:

- PC is still mostly a structural optimizer, not a full expected-return/variance optimizer.
- It can dilute high-quality signals if signal quality estimates are weak.
- Evidence caps and conviction need to mature before PC should be treated as a return optimizer.

### Risk and Execution Layer

Major safety layers now include:

- `execution_policy.evaluate_policy`
- `account_state_guard`
- policy alignment guard
- `auto_pause`
- `execution_throttle`
- `final_risk_validation`
- `execution_preflight`
- QC-side independent validation

Current positive state:

- FULL_AUTO now requires blocking account guard, blocking final validation, and active auto-pause.
- Executor no longer repairs PolicySync itself.
- Policy mismatch blocks execution and requires QC compiled-policy deployment/sync evidence; automatic PolicySync recovery is disabled by default.
- Execution throttle can clip buy delta to canary limits.
- Transaction cost diagnostics are visible.

Remaining concern:

- Deprecated inactive mutation/clipping paths remain in `services/pipeline.py` until reference history confirms they can be removed safely.
- Deferred execution is persisted as a first-class diagnostic plan.
- Fill/reconciliation lifecycle depends on QC payload richness.

## Remaining Risk Register

### Resolved P0: Runtime PolicySync Removed From Automatic Trading

The FULL_AUTO executor path and `/confirm` path no longer send same-cycle PolicySync before SetWeights. Automatic PolicySync recovery is also disabled by default.

Current behavior:

- `/confirm` requires recent account guard policy alignment.
- If not aligned, it returns a message instructing the operator to deploy/sync the QC compiled policy.
- It does not call `tool_send_policy_sync`.
- `policy_sync_recovery_config.enabled` defaults to false.
- QC fallback/compiled policy reports the same version as FastAPI when the compiled roles/caps are compatible.

The word "recent" must be concrete and testable:

```json
{
  "manual_confirm_policy_alignment_config": {
    "max_age_seconds": 300
  }
}
```

Manual confirmation may proceed only if the latest account state guard result is no more than 300 seconds old and has confirmed policy alignment. If the check is older, `/confirm` asks the operator to wait for the next pipeline/account-state refresh.

Relevant files:

- `services/telegram_commands.py`
- `services/policy_alignment.py`
- `services/policy_sync_recovery.py`
- `quantconnect_files/test1.py`

### Resolved P1: Old Mutation Paths Need Ownership Review

`services/pipeline.py` still contains older weight mutation paths such as:

- `enforce_pm_constraints`
- `enforce_pm_constraints_v2`
- `apply_regime_constraints`

These paths have now been classified.

Decision rule:

- If the logic constructs target weights, move it before/into target builder.
- If the logic only tightens risk, move it into position manager or register as a final validation mutation type.
- If unclear, keep it in observe diagnostics until impact distribution is measured.

Observe mode must have an exit condition. For every unclassified mutation:

```json
{
  "min_cycles_before_decision": 20,
  "decision_deadline": "2026-07-01"
}
```

If it triggers frequently or has material drift, it must be assigned an owner before the deadline. If it does not trigger during the minimum window, it can be classified as inactive/deprecated with a removal plan. It must not remain "observe indefinitely".

Current owner assignment:

- `enforce_pm_constraints`: deprecated inactive
- `enforce_pm_constraints_v2`: diagnostic only, no execution authority
- `apply_regime_constraints`: post-risk tighten-only, mutation type `regime_constraint_tighten`

### Resolved P1: StrategyInputBuilder Moved From Global Coverage Gate to Partial Scoring

Previous behavior:

```text
coverage below threshold -> strategy not_scored
```

Current behavior:

```text
strategy scores available tickers
missing ticker/field combinations become abstain
ETF evidence cap reflects lower confidence
```

This is now implemented. A strategy becomes `not_scored` only when no ticker is scorable. If at least one ticker is scorable, the strategy becomes `partially_scored` and excluded ticker/field pairs are isolated in diagnostics.

### Completed P1: Evidence Cap Calibration Freshness

Evidence-cap enforcement should not rely on manually supplied, potentially stale observe metrics.

Implemented behavior:

- Read latest calibration result.
- Require calibration generated within a freshness window, initially 7 days.
- If stale, degrade gated evidence cap back to observe.

Current implementation:

- `calibration_generated_at` is required for gated enforcement.
- `max_calibration_age_days` defaults to 7.
- Missing, invalid, or stale calibration adds a gate blocker:
  - `missing_calibration_generated_at`
  - `invalid_calibration_generated_at`
  - `calibration_data_stale`
- Diagnostics expose `calibration_freshness.age_days` for dashboard/operator review.

### Completed P2: Deferred Execution Ledger

Execution throttle clips oversized buy deltas, and deferred deltas are now persisted as a plan.

Current implementation:

- Deferred delta is persisted by command/analysis/ticker.
- Open deferred demand is reviewed on the next cycle.
- Deferred demand is marked `still_valid`, `cancelled`, `executed`, or `carried_forward`.
- Dashboard shows deferred execution pressure and recent ledger rows.

This remains an optimization, not a safety blocker.

### Completed P2: Command Lifecycle Reconciliation Completeness

The command lifecycle model supports richer states:

- submitted
- accepted
- rejected
- timeout
- filled
- partial
- reconciled
- reconciliation drift

Full fill-level closure still depends on QC sending detailed fill/account reconciliation payloads.

Current implementation:

- The dashboard computes `accepted commands without reconciled event after X minutes`.
- The default dashboard threshold is 30 minutes.
- Commands with `reconciled` or `reconciliation_drift` lifecycle events are treated as closed for this metric.
- Commands still awaiting closure are shown as:
  - `pending`
  - `overdue`
- This is diagnostic only and does not block execution.

Relevant files:

- `services/command_lifecycle.py`
- `dashboard/app.py`
- `tests/test_command_lifecycle.py`
- `tests/test_dashboard.py`

### Completed DoD: Strategy Statistical Independence Baseline

The alpha layer now has a first-class statistical independence baseline instead of only a family label count.

Current implementation:

- Strategy return-series diagnostics compute pairwise correlation rows for active strategies.
- Low-correlation strategy pairs are surfaced using `abs(correlation) < 0.40`.
- If no low-correlation active pair exists, the baseline marks `operator_review_required=true` rather than silently treating registered strategy count as independent alpha count.
- Dashboard shows:
  - Strategy Independence Baseline
  - Low-Correlation Strategy Pairs
  - High-Correlation Strategy Pairs
  - Strategy Correlation Pair Rows
  - Strategy Family Correlation Rows
- Regime gap analysis now flags regimes where all calibrated active alpha profiles fail simultaneously.
- Dashboard shows Simultaneous Failure Regime Rows.

This is diagnostic only. It has no execution authority and does not mutate target weights.

Relevant files:

- `services/strategy_independence.py`
- `services/strategy_regime_gap_analysis.py`
- `dashboard/app.py`
- `tests/test_strategy_independence.py`
- `tests/test_strategy_regime_gap_analysis.py`
- `tests/test_dashboard.py`

## FULL_AUTO Configuration Requirements

For FULL_AUTO to run after the latest code changes, the following must be true.

### authorization_mode

```json
{
  "value": "FULL_AUTO"
}
```

### account_state_guard_config

Minimum required:

```json
{
  "enabled": true,
  "mode": "blocking",
  "require_policy_version": true,
  "require_no_open_orders": true,
  "require_buying_power": true
}
```

### final_risk_validation_config

Either direct blocking:

```json
{
  "mode": "blocking"
}
```

Or auto with FULL_AUTO effective blocking:

```json
{
  "mode": "auto",
  "full_auto_effective_mode": "blocking",
  "semi_auto_effective_mode": "observe"
}
```

### auto_pause_config

Minimum required:

```json
{
  "enabled": true,
  "mode": "active",
  "auto_pause_after_consecutive_qc_rejects": 2,
  "pause_on_account_state_guard_failure": true
}
```

If any of the above are not satisfied, FULL_AUTO will be rejected before pipeline execution.

## Verification Performed

Focused tests run:

```bash
uv run python -m unittest \
  tests.test_executor_preflight \
  tests.test_full_auto_safety_preconditions \
  tests.test_policy_alignment \
  tests.test_evidence_cap_config \
  tests.test_evidence_cap_calibration \
  tests.test_target_builder \
  tests.test_mutation_ownership \
  tests.test_final_risk_validation \
  tests.test_policy_sync_recovery \
  tests.test_policy_sync_recovery_pipeline \
  tests.test_account_state_guard \
  tests.test_auto_pause \
  tests.test_final_risk_validation_config \
  tests.test_playground_feature_merge \
  tests.test_sector_rotation \
  tests.test_evidence_vote_aggregation \
  tests.test_deferred_execution_ledger \
  tests.test_migration_safety \
  tests.test_dashboard \
  tests.test_execution_throttle \
  tests.test_command_lifecycle \
  tests.test_strategy_independence \
  tests.test_strategy_regime_gap_analysis \
  tests.test_qc_fallback_policy_contract
```

Result:

```text
Ran 190 tests
OK
```

Compile check run:

```bash
uv run python -m py_compile \
  agents/executor.py \
  services/execution_preflight.py \
  services/full_auto_safety.py \
  services/pipeline.py \
  services/policy_alignment.py \
  services/telegram_commands.py \
  tools/qc_tools.py \
  services/strategy_input_builder.py \
  services/evidence_cap_config.py \
  services/evidence_cap_calibration.py \
  services/target_builder.py \
  services/mutation_ownership.py \
  services/final_risk_validation.py \
  services/strategy_independence.py \
  services/strategy_regime_gap_analysis.py \
  dashboard/app.py \
  tests/test_executor_preflight.py \
  tests/test_full_auto_safety_preconditions.py \
  tests/test_policy_alignment.py \
  tests/test_evidence_cap_config.py \
  tests/test_evidence_cap_calibration.py \
  tests/test_target_builder.py \
  tests/test_mutation_ownership.py \
  tests/test_strategy_independence.py \
  tests/test_strategy_regime_gap_analysis.py \
  tests/test_dashboard.py \
  tests/test_qc_fallback_policy_contract.py \
  ../quantconnect_files/test1.py
```

Result:

```text
OK
```

Note:

- `pytest` is not installed in the local uv environment, so focused validation used `unittest` and `py_compile`.

## Recommended Next Development Order

### Completed Step 1: Remove Telegram `/confirm` PolicySync Coupling

Goal:

- Manual confirmation path should follow the same control-plane/data-plane boundary as FULL_AUTO.

Acceptance criteria:

- `/confirm` no longer calls `tool_send_policy_sync`. Completed.
- `/confirm` requires recent account guard policy alignment. Completed.
- If alignment is missing, it returns a clear operator message and sends no command. Completed.

### Completed Step 2: Evidence Cap Calibration Freshness

Goal:

- Avoid stale manual observe metrics enabling gated caps.

Acceptance criteria:

- Gated evidence cap requires fresh calibration. Completed.
- Stale calibration degrades to observe. Completed.
- Dashboard can show calibration age through `calibration_freshness`. Completed.

### Completed Step 3: Classify Old Pipeline Mutation Paths

Goal:

- Remove ambiguity in post-risk weight mutation ownership.

Acceptance criteria:

- Each old mutation path is assigned one owner. Completed:
  - target construction
  - position manager tighten-only
  - final validation recognized mutation
  - deprecated
- Unknown post-risk mutations are visible in final validation diagnostics. Already covered by final validation.
- Unclassified mutation observe state has a minimum-cycle window and a fixed decision deadline. Completed.

### Completed Step 4: Replace Strategy-Wide Low-Coverage Not-Scored With Partial Scoring

Goal:

- Young ETFs or partial feature gaps should not globally disable a strategy when enough tickers are scorable.

Acceptance criteria:

- Missing feature combinations produce abstain/exclusion diagnostics. Completed.
- Available tickers still receive scores. Completed.
- Evidence cap can shrink allowable weight based on evidence quality using those diagnostics. Already connected through vote aggregation and evidence-cap diagnostics.
- No missing feature is interpreted as zero score. Completed.

### Completed Step 5: Deferred Execution Ledger

Goal:

- Make execution throttle carryover explicit and auditable.

Acceptance criteria:

- Deferred buy/sell deltas are persisted. Completed.
- Next cycle explains whether each deferred item is still valid, cancelled, or executed. Completed.
- Dashboard shows deferred execution pressure. Completed.

Current state:

- `execution_throttle` still owns deterministic staging of per-command buy deltas.
- `deferred_execution_ledger` records each deferred ticker delta with current, desired, and staged weights.
- Each new pipeline cycle reviews open deferred items and marks them as:
  - `still_valid`
  - `cancelled`
  - `executed`
- The pipeline writes `risk_out.deferred_execution_ledger` as diagnostics.
- The dashboard Execution Control panel shows open deferred pressure and recent ledger rows.

Relevant files:

- `services/deferred_execution_ledger.py`
- `services/pipeline.py`
- `db/models.py`
- `db/migrations/20260528_create_deferred_execution_ledger.sql`
- `dashboard/app.py`

## Definition of Done for This Cleanup Phase

This cleanup phase is complete against the current engineering criteria:

- PolicySync has no automatic trading-chain owner; it is manual/diagnostic only. Completed.
- FULL_AUTO cannot run with observe-only safety layers. Completed.
- Every post-risk mutation has an owner and mutation type. Completed.
- Missing strategy data never becomes a zero score. Completed.
- Evidence cap gated mode cannot use stale calibration. Completed.
- Execution throttling is visible as a plan, not just a one-cycle clip. Completed.
- Strategy statistical independence baseline is established. Completed:
  - correlation matrix computed for all active strategies
  - at least two active strategies have pairwise return correlation below 0.4, or the operator explicitly accepts the current correlation structure
  - regime gap analysis shows whether any regime causes all active strategies to fail simultaneously
