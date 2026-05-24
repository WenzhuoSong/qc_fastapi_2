# Risk, Portfolio Construction, And Execution Control Development Plan

Review date: 2026-05-24
Status: draft for operator/developer review

## 1. Purpose

This document refines the development plan for the next architecture pass over:

- portfolio construction
- target building
- risk validation
- position/execution throttles
- final execution preflight
- FastAPI/QC policy alignment

The goal is not to rewrite the trading system. The goal is to make the existing
pipeline more professional by tightening ownership boundaries and ensuring that
any final execution target is validated after every possible mutation.

Current design principle remains:

```text
LLM explains and advises.
Deterministic Python constructs, validates, audits, and executes.
```

## 2. Current Problem Statement

The current main path is roughly:

```text
quant_baseline
-> portfolio_construction_shadow
-> target_builder_gated
-> risk_manager approve
-> regime/governance/position_manager may still modify target
-> final_execution_policy_cap
-> executor_preflight
-> QC command
```

The main design issue is that `risk_manager` can approve a target before later
stages modify the final target weights.

Most later edits are intended to be conservative, but from a professional
pre-trade risk perspective, any post-risk mutation must be followed by another
validation step before execution.

Reviewer note incorporated on 2026-05-24:

- final validation observe mode should start after risk manager is validate-only,
  otherwise observed drift is polluted by legacy risk-manager mutations
- post-risk `position_manager` mutations must be explicitly classified before
  final validation can block safely
- QC policy-version mismatch should allow reduce-only exits but reject any
  command with buy/increase exposure

## 3. Target Architecture

The intended end-state is:

```text
quant_baseline
-> pre_risk_position_governance
-> portfolio_construction_candidate/gated
-> target_builder
-> risk_manager_validate_only
-> position_manager_tighten_only
-> final_risk_validation
-> executor_preflight
-> QC-side policy validation
```

Layer ownership:

| Layer | Owner | Responsibility |
|---|---|---|
| Hard policy | `services/execution_policy.py` | Single source of truth for tradability, role caps, group caps, cash/equity limits, turnover limits |
| Portfolio construction | `services/portfolio_construction.py` | Portfolio-level factor exposure, basket limits, effective N, turnover budget |
| Target builder | `services/target_builder.py` | Per-ticker lifecycle target construction from deterministic inputs |
| Risk manager | `agents/risk_manager.py` | Validate target only; no target mutation |
| Position manager | `services/position_manager.py` | Quantity/frequency throttles; tighten-only post-risk adjustments |
| Final validation | new `services/final_risk_validation.py` | Revalidate the final post-mutation target before execution |
| Executor preflight | `services/execution_preflight.py` | Final command-level hard block |
| QC side | QuantConnect algorithm | Independent policy validation and command idempotency |

## 4. Non-Goals

- Do not let raw LLM weights become executable.
- Do not remove the LLM research/debate/advisory layer.
- Do not rewrite the full pipeline in one PR.
- Do not rewrite historical `qc_snapshots.raw_payload`.
- Do not promote portfolio construction to gated execution without a readiness
  observation window.

## 5. Phase 1: Unified Execution Policy Evaluator

### Objective

Make `execution_policy` the canonical hard-rule engine used by construction,
target building, final caps, executor preflight, and QC policy sync.

### Current Gap

Hard rules are spread across:

- `execution_policy`
- `target_builder`
- `risk_manager`
- `position_manager`
- `final_execution_policy_cap`
- QC-side fallback checks

This can create policy drift.

### Development Tasks

Add a canonical evaluator to `services/execution_policy.py`:

```python
def evaluate_policy(
    *,
    weights: dict[str, Any],
    current_weights: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ...
```

The evaluator should return:

```json
{
  "allowed": true,
  "policy_version": "sprint8a",
  "violations": [],
  "cap_events": [],
  "role_exposure": {},
  "checks": {
    "unknown_ticker_ok": {"pass": true},
    "single_cap_ok": {"pass": true},
    "role_group_cap_ok": {"pass": true},
    "hedge_only_ok": {"pass": true},
    "cash_floor_ok": {"pass": true},
    "turnover_ok": {"pass": true}
  }
}
```

Rules to include:

- unknown ticker positive weight is blocked
- watchlist ticker positive weight is blocked
- role-level single ticker cap
- role-level total group cap
- hedge-only instruments obey hedge policy
- minimum cash, when supplied in context
- maximum equity, when supplied in context
- max per-cycle turnover, when supplied in context
- max single delta, when supplied in context

Refactor these call sites to use the evaluator:

- `target_builder`
- `final_execution_policy_cap`
- `execution_preflight`
- portfolio construction diagnostics
- QC policy sync payload

### Acceptance Criteria

- Every final target contains `policy_version`.
- Positive weight for unknown tickers is impossible to execute.
- Executor preflight only rejects; it does not silently repair weights.
- Existing tests continue to pass.
- New tests cover role caps, unknown tickers, group caps, cash floor, and
  turnover limits.

## 6. Phase 2: Portfolio Construction Candidate Mode

### Objective

Move portfolio construction from pure shadow to a controlled candidate stage
without immediately changing execution behavior.

### New Runtime Config

Store under `system_config.portfolio_construction_promotion_config`:

```json
{
  "portfolio_construction_mode": "shadow",
  "min_shadow_cycles": 20,
  "min_pass_rate": 0.9,
  "max_material_diff": 0.015,
  "max_turnover_diff": 0.02
}
```

Modes:

- `shadow`: current behavior; no execution effect
- `candidate`: generate construction target and readiness diagnostics, but
  `target_builder` still uses existing input
- `gated`: `target_builder` uses `portfolio_construction.target_weights`

### Development Tasks

Extend `PortfolioConstructionModel.construct()` output with:

- `policy_evaluation`
- `factor_exposure_before`
- `factor_exposure_after`
- `basket_exposure_before`
- `basket_exposure_after`
- `effective_n_before`
- `effective_n_after`
- `construction_source = portfolio_construction`

Add a new pipeline step:

```text
5e_portfolio_construction_candidate
```

Record:

- input base weights
- current weights
- signal strengths
- basket reviews
- scorecard permission
- turnover budget
- output construction weights
- policy evaluation
- diff versus current live target path

Dashboard should expose:

- readiness status
- pass rate
- policy violations
- diff versus actual target
- turnover difference
- effective N change

### Acceptance Criteria

- `shadow` and `candidate` modes do not change submitted weights.
- `candidate` records enough evidence to decide promotion.
- `gated` mode is blocked unless readiness gate is explicitly eligible.
- Portfolio construction output cannot increase execution permission beyond
  scorecard/governance policy.

## 7. Phase 3: Target Builder Accepts Construction Target

### Objective

Allow target builder to use portfolio construction output as the deterministic
execution candidate while retaining base weights for audit.

### Development Tasks

Extend `build_target_weights()` with optional inputs:

```python
build_target_weights(
    *,
    base_weights: dict[str, Any],
    construction_weights: dict[str, Any] | None = None,
    construction_source: str | None = None,
    ...
)
```

Behavior:

- if construction mode is `gated`, start from `construction_weights`
- otherwise start from `base_weights`
- keep `base_weights` as audit reference
- use `construction_weight = null` when portfolio construction did not
  participate; reserve `0.0` for an explicit construction recommendation to
  clear the position
- never consume raw LLM `adjusted_weights`

Per-ticker diagnostics should include:

```json
{
  "base_weight": 0.0,
  "construction_weight": null,
  "current_weight": 0.0,
  "governance_target": null,
  "validated_llm_delta": 0.0,
  "pre_normalized_target": 0.0,
  "final_target": 0.0,
  "changed_by": []
}
```

### Acceptance Criteria

- `raw_llm_adjusted_weights_consumed` remains false.
- Target builder does not import agents.
- Target builder does not parse natural language rationale.
- Every ticker-level adjustment has a structured reason.
- Diagnostics clearly distinguish base, construction, governance, and final
  target weights.

## 8. Phase 4: Risk Manager Validate-Only

### Objective

Convert risk manager into a pure validation stage. It should no longer repair or
construct execution targets.

### Current Gap

Risk manager already prefers `target_builder_gated`, but it still has legacy
overlay code and deterministic base fallback behavior.

### Development Tasks

Risk manager should:

- require deterministic target-builder input
- validate the target
- emit approval token only if all checks pass
- reject rather than repair if checks fail
- report `no_mutation = true`

Move or retire mutation-style overlays:

- scorecard/style clipping should move before risk
- hard-risk additions should be blocked by target builder/governance before risk
- transmission tilt should be portfolio construction input, not risk mutation

Fallback behavior:

- SEMI_AUTO: target-builder failure creates rejected diagnostic, no execution
- FULL_AUTO: target-builder failure cannot produce new buy exposure
- FULL_AUTO reduce-only governance trim can remain as a special emergency path,
  but must be labeled and final-validated

### Acceptance Criteria

- Risk manager input target equals risk manager output target when approved.
- Risk manager approval implies the exact target has passed checks.
- Target-builder failure cannot lead to base-weight buy execution.
- Approval token is not issued for fallback targets that bypass target builder.

## 9. Phase 5: Final Risk Validation

### Objective

Guarantee that the actual target sent to executor has been validated after all
post-risk stages.

Final validation should be introduced only after Phase 4 makes Risk Manager
validate-only. Otherwise observe-mode data will mix two different baselines:

```text
legacy risk mutation drift
post-risk governance/position-manager drift
```

The useful observation target is only the second category.

### Post-Risk Mutation Contract

Before final validation can block execution, post-risk mutation types must be
enumerated and emitted by `position_manager`, `final_execution_policy_cap`, and
any emergency reduce-only path.

Allowed mutation types:

```python
ALLOWED_POST_RISK_MUTATIONS = {
    "cap_new_buy_to_current",       # buy/new exposure reduced back toward current
    "cap_single_buy_delta",         # excessive buy delta moved to CASH
    "cap_trade_count_buys",         # lower-priority buy trades deferred
    "cash_raise_from_policy_cap",   # policy cap releases weight to CASH
    "emergency_reduce_only",        # explicit emergency/risk-reducing path
}
```

Conditionally allowed mutation types:

```python
CONDITIONAL_POST_RISK_MUTATIONS = {
    "turnover_scale_toward_current",
    "defer_sell_due_to_min_hold_days",
}
```

Conditional mutations are not automatically tighten-only because they may defer
a risk-reducing sell. They are allowed only when:

- they do not create new exposure
- they do not increase any ticker above current weight
- the affected ticker is not under hard risk, critical alert, forced trim, or
  scorecard no-hold/no-add restriction
- material drift from the risk-approved target is either below threshold or
  requires human confirmation

Any post-risk weight change without a mutation type is treated as unsafe drift.

### New Module

Create:

```text
services/final_risk_validation.py
```

Suggested API:

```python
def validate_final_execution_target(
    *,
    risk_approved_target: dict[str, Any],
    final_target: dict[str, Any],
    current_weights: dict[str, Any],
    risk_context: dict[str, Any],
    policy_context: dict[str, Any],
) -> dict[str, Any]:
    ...
```

Checks:

- execution policy allowed
- cash floor
- max equity
- max turnover
- max single delta
- max daily trade count
- no unknown tickers
- no stale/blocking data quality issue
- post-risk target drift versus risk-approved target
- policy version present

If final target differs materially from risk-approved target:

- allow only if all changes are classified as tighten-only
- otherwise reject or require human confirmation

Initial observe mode should still hard-block obviously unsafe cases:

- unknown ticker positive weight
- watchlist ticker positive weight
- role single cap exceeded by `cap * 1.20`; for example, a 7.5% thematic
  single cap hard-blocks above 9.0%
- role group cap exceeded by `cap * 1.20`
- positive exposure to a hard-risk ticker with no existing position

### Acceptance Criteria

- `risk_out["final_validation"]["approved"] == true` is required before executor.
- All post-risk cap events are visible in `risk_out`.
- Final validation failure blocks execution even if risk manager previously
  approved.
- Executor preflight becomes the last hard block, not the first place final
  violations are discovered.

## 10. Phase 6: Command-Level Execution Control

### Objective

Upgrade from weight-level safety to command-level safety.

### FastAPI Tasks

Extend `services/execution_preflight.py` to check:

- command idempotency
- same `analysis_id` cannot submit more than once
- ACK timeout does not trigger automatic duplicate command
- max daily command count
- max gross turnover per day
- max buy delta
- max sell delta
- successful QC policy sync required
- FastAPI `policy_version` present in command payload

Execution log should record:

- command id
- analysis id
- policy version
- final target weights
- preflight result
- QC submission result
- QC ack status
- QC rejection reason

### QC-Side Tasks

QC algorithm should validate every command independently:

- reject duplicate `command_id`
- reject missing `policy_version`
- on mismatched `policy_version`, allow reduce-only commands and reject any
  command that increases or creates exposure
- reject unknown ticker
- reject cap violations
- reject command if local fallback policy is stricter than FastAPI policy
- record processed command ids
- send ACK with:
  - `accepted` or `rejected`
  - reason
  - policy version
  - actual target weights if accepted

Policy mismatch rule:

```text
if policy_version mismatches:
  allow only if every non-cash target weight <= current QC weight
  reject otherwise with reason = policy_version_mismatch_with_buy
```

ACK payload should include `policy_mismatch=true` when a reduce-only command is
accepted under a version mismatch.

### Acceptance Criteria

- FastAPI cannot send duplicate command for the same analysis.
- QC cannot execute duplicate command id.
- Policy mismatch blocks all buy/increase exposure.
- Policy mismatch permits reduce-only exits with explicit ACK metadata.
- QC rejection reason is visible in dashboard and Telegram.

## 11. Recommended PR Split

### PR 1: Unified Execution Policy Evaluator

- Add `evaluate_policy`.
- Refactor preflight/final cap diagnostics to use it.
- Add unit tests for policy invariants.
- No behavior change intended except better diagnostics.

### PR 2: Risk Manager Validate-Only

- Remove mutation behavior from approved path.
- Reject target-builder failure instead of base fallback buy path.
- Preserve special reduce-only emergency path with explicit labeling.
- Add tests proving risk manager approved target is not mutated.

### PR 3: Final Risk Validation In Observe Mode

- Add `final_risk_validation.py`.
- Define and emit post-risk mutation types.
- Run it after position manager/final cap.
- Record output and hard-block only obviously unsafe cases.
- Add dashboard surfacing if simple.

### PR 4: Portfolio Construction Candidate Mode

- Add candidate mode config.
- Add construction diagnostics.
- Keep execution unchanged.
- Add readiness/promotion metrics.

### PR 5: Target Builder Construction Input

- Allow construction weights as deterministic start point.
- Expand diagnostics.
- Use `construction_weight = null` when construction did not participate.
- Keep LLM boundary tests.

### PR 6: Final Validation Blocking Mode

- Require final validation pass before executor.
- Final target drift beyond threshold requires reject/human confirmation.
- Document `material_drift_threshold` and justify it with observe-mode final
  validation data before blocking mode is enabled.
- Update communicator/dashboard to show final validation result.

### PR 7: Executor And QC Command Hardening

- Command idempotency.
- Policy version handshake.
- QC duplicate command rejection.
- ACK enrichment.

### PR 8: Portfolio Construction Gated Rollout

- Only after candidate/readiness criteria pass.
- Start in SEMI_AUTO.
- Require at least 5 SEMI_AUTO confirmed cycles before FULL_AUTO.
- Observe before enabling FULL_AUTO.

## 12. Test Plan

Add or update tests covering:

- final target always passes execution policy
- unknown ticker positive weight is rejected
- watchlist ticker positive weight is rejected
- hedge ETF over cap is clipped before risk or rejected before execution
- role group cap is enforced
- target builder never consumes raw LLM weights
- risk manager does not mutate approved target
- post-risk mutation triggers final validation
- post-risk mutations must carry an allowed or conditional mutation type
- final validation blocks unsafe post-risk mutation
- final validation observe mode hard-blocks obviously unsafe cases
- portfolio construction candidate mode has no execution effect
- portfolio construction gated mode changes target-builder input only when enabled
- target-builder diagnostics use `construction_weight = null` when construction
  did not participate
- command id duplicate is rejected
- policy version mismatch blocks buy/increase exposure
- policy version mismatch permits reduce-only exits with ACK metadata
- QC ack rejection is stored and surfaced

## 13. Rollout Strategy

Recommended order:

```text
single policy evaluator
-> risk manager validate-only
-> observe-only final validation
-> candidate portfolio construction
-> final validation blocking
-> SEMI_AUTO gated portfolio construction
-> FULL_AUTO gated portfolio construction
```

Do not enable portfolio construction gated mode until:

- at least 20 comparable cycles are observed
- readiness pass rate is at least configured threshold
- material diff rate is acceptable
- no recurring policy violations
- operator has reviewed dashboard evidence

Do not enable portfolio construction gated mode in FULL_AUTO until:

- gated mode has run in SEMI_AUTO
- at least 5 SEMI_AUTO proposals were manually reviewed/confirmed or explicitly
  skipped for understood reasons
- no final-validation or QC policy-version mismatch violations occurred in that
  SEMI_AUTO window

## 14. Open Questions

1. Should portfolio construction be allowed to reduce existing hedge exposure
   even when scorecard is otherwise no-add?
   - Close before Phase 2 readiness metrics are finalized.
2. What material drift threshold should apply to conditional post-risk
   mutations such as turnover scaling or min-hold sell deferral?
   - Close before PR 6 blocking mode; prefer observe-mode drift distribution
     over a fixed upfront guess.
3. Should command idempotency be stored only in `execution_log`, or also synced
   to QC-local memory/state?

## 15. Summary

The highest-value first step is not portfolio optimization. It is safety
boundary cleanup:

```text
single policy evaluator
-> risk manager validate-only
-> final risk validation
-> command-level execution controls
```

After those are in place, portfolio construction can be promoted from shadow to
candidate and eventually gated with much lower operational risk.
