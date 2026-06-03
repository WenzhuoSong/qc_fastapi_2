# Risk Contract Convergence Development Plan

> Goal: make `TargetEnvelope` the post-risk execution authority, while keeping
> the existing risk logic and moving the old dict-based chain into shadow
> diagnostics until it can be safely removed.

This is not a risk relaxation plan. It is a contract convergence plan:

- Keep existing hard-risk, policy cap, min-hold, turnover, forced-trim, and
  execution-throttle rules.
- Replace naked weight dict handoffs with a single post-risk target container.
- Make every executable post-risk weight change auditable through
  `MutationLedger`.
- Let final validation validate accounting and safety, instead of guessing
  drift ownership from scattered strings.

---

## Current Failure Mode

Recent FULL_AUTO runs showed two related blockers:

1. `conditional_mutation_contract_violation`
   - `turnover_scale_toward_current` touched restricted tickers.
   - The direction was still risk-reducing, but final validation treated any
     touch as a violation.

2. `incomplete_mutation_ledger`
   - `position_governance` changed QQQ/XLE/XLK/XLU targets.
   - Those trims were valid risk-reducing actions, but they were not fully
     represented in the post-risk mutation ledger.
   - Final validation saw drift without complete attribution and blocked.

The root issue is not that risk logic is too strict. The root issue is that
post-risk authority is split across modules and final validation has to infer
what happened.

---

## Design Decision

We will replace the current post-risk execution authority with `TargetEnvelope`.

The old chain will not disappear immediately. It will run as shadow diagnostics
until all consumers are migrated:

```text
Risk Manager approved target
        |
        v
TargetEnvelope created
        |
        v
position_governance / position_manager / final_policy_cap
write executable mutations into TargetEnvelope
        |
        v
final_validation validates TargetEnvelope
        |
        v
executor receives envelope.final_target

Legacy dict outputs remain as diagnostic mirrors during migration.
```

This means the change can take effect early without discarding existing risk
logic.

---

## Non-Goals

- Do not loosen execution policy caps.
- Do not bypass account state guard, auto pause, circuit, or QC validation.
- Do not make scorecard/data-quality warnings directly trade-authoritative.
- Do not remove existing position governance or position manager logic in the
  first PR.
- Do not let old `post_risk_mutation_types` string lists remain authoritative.

---

## TargetEnvelope Contract

Create `services/target_envelope.py`.

```python
@dataclass
class TargetEnvelope:
    current_weights: dict[str, float]
    risk_approved_target: dict[str, float]
    stage_base_target: dict[str, float]
    final_target: dict[str, float]
    ledger: MutationLedger
    stage_snapshots: list[dict]
    authority: str = "post_risk_envelope"
```

Required methods:

```python
def mutate(ticker, new_weight, mutation_type, reason, metadata=None) -> None
def apply_stage_target(new_weights, mutation_type, reason, stage) -> None
def advance_stage(stage_name) -> None
def replay_ledger() -> dict[str, float]
def accounting_check() -> list[dict]
def safety_diagnostics(policy_context) -> dict
def to_dict() -> dict
```

### Accounting Contract

Accounting is not "ticker appears in the ledger". Accounting is:

```text
replay(stage_base_target, MutationLedger) == final_target
```

Any final weight drift that cannot be reproduced by ledger replay is a hard
block:

```text
accounting_contract_violation
```

### Safety Contract

Safety answers whether the final executable target increases risk.

Allowed:

- restricted ticker is reduced vs current
- hard-risk ticker is reduced vs current
- forced-trim target executes as risk reduction

Blocked:

- unknown/watchlist ticker gets positive weight
- final target exceeds execution policy cap
- restricted/hard-risk ticker is increased vs current
- conditional mutation reverses a risk trim
- hard-risk trim is suppressed below minimum required trim

---

## Rollout Mode

Add a config key:

```json
target_envelope_config = {
  "enabled": true,
  "mode": "active",
  "shadow_compare_enabled": true,
  "block_on_accounting_failure": true,
  "block_on_safety_failure": true
}
```

Mode semantics:

- `shadow`: envelope computes diagnostics only; legacy target still executes.
- `active`: envelope final target is execution authority.
- `strict`: active plus legacy dict authority checks must be clean; used after
  migration.

For FULL_AUTO, the intended target after PR2 is `active`.

If envelope accounting or safety fails in active mode, execution blocks. It
does not fall back to legacy execution.

---

## PR1: TargetEnvelope Foundation

Scope:

- Add `services/target_envelope.py`.
- Add ledger replay accounting.
- Add weight invariant checks through `weight_ops.assert_invariants`.
- Add unit tests for:
  - final target returns a copy
  - `mutate()` records before/after correctly
  - ledger replay exactly reproduces final target
  - unaccounted direct drift fails accounting
  - CASH changes are represented through replay diagnostics

Non-scope:

- No pipeline execution change yet.
- No final validation behavior change yet.

Acceptance:

```text
TargetEnvelope can represent current -> risk-approved -> final target path.
Accounting check fails if any non-CASH drift bypasses mutate().
```

Implementation status: completed.

- `services/target_envelope.py` added.
- `tests/test_target_envelope.py` added.
- PR2 extended the foundation with `apply_stage_ledger()` so bridge adapters
  can preserve existing per-ticker mutation types during migration.

---

## PR2: Make Envelope Execution-Authoritative

This is the first PR where the change takes effect.

Scope:

- Create `TargetEnvelope` after risk manager approval.
- Feed current post-risk module outputs into the envelope through bridge
  adapters:
  - `position_governance` output -> envelope stage mutation
  - `position_manager` output -> envelope stage mutation
  - `final_policy_cap` output -> envelope stage mutation
  - `execution_throttle` output -> envelope stage mutation
- Set executable `risk_out["target_weights"]` from `envelope.final_target`.
- Attach `risk_out["target_envelope"] = envelope.to_dict()`.
- Final validation reads envelope when present.
- Legacy dict outputs remain in step logs as diagnostics.

Bridge rule:

```python
envelope.apply_stage_target(
    new_weights=module_output_weights,
    mutation_type=stage_default_mutation_type,
    reason="stage output imported into TargetEnvelope",
    stage="position_governance",
)
```

Important:

- Bridge adapters are temporary.
- They allow the new contract to become authoritative before every module is
  rewritten to call `envelope.mutate()` directly.

Acceptance:

```text
FULL_AUTO execution authority is envelope.final_target.
Old dict chain is shadow diagnostic only after final validation.
Current QQQ/XLE/XLK/XLU trim + SPY/IWM/XLI min-hold case passes accounting.
Any envelope accounting failure blocks.
Any envelope safety failure blocks.
```

Implementation status: completed.

- Pipeline creates `TargetEnvelope` after risk manager approval.
- Regime constraint, position governance, position manager, final policy cap,
  and execution throttle outputs are imported through bridge adapters.
- In active/strict mode, final validation receives `envelope.final_target`,
  `envelope.risk_approved_target`, and `envelope.ledger` as authoritative
  inputs.
- `target_envelope_config` is seeded with `mode=active`.
- Envelope accounting or bridge failures block execution in active/strict mode.

---

## PR3: Final Validation Split

Scope:

- Refactor final validation into two explicit checks:
  - `validate_accounting_contract(envelope)`
  - `validate_safety_contract(envelope, policy_context)`
- Stop using scattered legacy string lists as authoritative input.
- `post_risk_mutation_types` becomes diagnostic only.
- `MutationLedger.mutations[].mutation_type` remains authoritative.

Safety tests:

- restricted reduce is allowed
- hard-risk reduce is allowed
- restricted increase is blocked
- trim reversal is blocked
- hard-risk trim suppression is blocked
- policy cap exceeded is blocked
- unknown/watchlist positive weight is blocked

Acceptance:

```text
Final validation no longer guesses drift ownership from loose dicts.
It validates envelope accounting plus envelope safety.
```

Implementation status: completed.

- `validate_accounting_contract()` added.
- `validate_safety_contract()` added.
- In envelope active/strict mode, conditional material drift no longer requires
  human confirmation by itself; it is allowed or blocked by safety direction.
- `post_risk_mutation_types` is retained as diagnostics, while
  `MutationLedger.mutations[].mutation_type` is the authoritative mutation
  source for conditional and unknown-mutation checks.

---

## PR4: Position Governance Direct Envelope Mutation

Scope:

- `position_governance` still returns its existing output for dashboard/logging.
- Internally or through a wrapper, executable weight changes are written through
  `envelope.mutate()`.
- Governance trim mutation types:
  - hard-risk/reduce-only path: `emergency_reduce_only`
  - loss/winner/review trim path: `loss_trim`
  - policy cap path remains handled by final policy cap

Acceptance:

```text
Governance trims are represented as per-ticker ledger mutations.
No governance executable drift exists outside the envelope ledger.
```

Implementation status: completed.

- Position governance executable changes now write directly through
  `TargetEnvelope.apply_stage_target()`.
- The governance step log still includes a mutation ledger, but that ledger is
  sliced from the envelope mutations created by this stage rather than rebuilt
  as an independent authority.
- If governance tries to introduce an out-of-contract increase through a
  tighten-only mutation type, the envelope records a bridge error and final
  validation blocks in active/strict mode.

---

## PR5: Position Manager Direct Envelope Mutation

Scope:

- `min_hold_defer_sell` writes through envelope.
- `turnover_scale_toward_current` writes through envelope.
- `cap_single_buy_delta`, trade-count caps, decay auto-reduce write through
  envelope.
- Protected tickers:
  - hard-risk
  - forced-trim
  - scorecard restricted trim

Rules:

- hard-risk and forced-trim sells are not deferred by min-hold.
- hard-risk and forced-trim sells are not scaled by ordinary turnover throttle.
- ordinary tickers still obey min-hold and turnover controls.

Acceptance:

```text
Position manager cannot modify executable target outside envelope.mutate().
Protected trims are not diluted by ordinary turnover/min-hold controls.
```

Implementation status: completed.

- Position manager executable mutations now apply through
  `TargetEnvelope.apply_stage_mutation_ledger()`.
- PM's legacy output ledger remains available as
  `diagnostic_legacy_mutation_ledger`, while the stage authority ledger is
  sliced from the envelope mutations created by this stage.
- Unlike the PR2 bridge, PR5 does not fallback-guess a mutation type for
  unledgered PM drift. Any unaccounted non-CASH difference between PM output
  and envelope target is recorded as `target_envelope_errors` and blocks in
  active/strict mode.

---

## PR6: Final Policy Cap and Execution Throttle Direct Envelope Mutation

Scope:

- `final_policy_cap` writes cap reductions through envelope.
- `execution_throttle` writes buy-delta staging through envelope.
- Deferred execution ledger remains separate, but its executable target impact
  must be represented in envelope.

Acceptance:

```text
All final executable cap/throttle edits are replayable from the envelope ledger.
```

Implementation status: completed.

- Final policy cap mutations now apply through the structured envelope ledger
  stage helper.
- Execution throttle mutations now apply through the structured envelope ledger
  stage helper.
- Both stages keep their legacy service-produced ledgers as diagnostics, while
  their execution-authoritative mutation ledgers are sliced from the envelope.
- No fallback mutation-type guessing is used for these stages; unaccounted
  non-CASH drift becomes a `target_envelope_errors` block in active/strict mode.

---

## PR7: Legacy Cleanup

Scope:

- Remove final validation reliance on:
  - `post_risk_mutation_types`
  - `post_risk_mutation_details`
  - scattered post-risk drift reconstruction
- Keep legacy fields only in step logs for a short compatibility window.
- Delete old drift-guessing helpers once dashboard/API no longer consume them.

Acceptance:

```text
Final validation depends on TargetEnvelope + MutationLedger, not loose strings.
```

Implementation status: completed.

- `post_risk_mutation_types` remains visible as `legacy_mutation_types` and
  `diagnostic_mutation_types`, but it no longer drives validation decisions.
- `post_risk_mutation_details` remains visible as diagnostics, but it no
  longer builds or completes the authoritative `MutationLedger`.
- `_conditional_detail_tickers` and the legacy-details-to-ledger path were
  removed.
- `mutation_types` now reports authoritative ledger mutation types only.

---

## PR8: Dashboard and Operator Visibility

Dashboard must show three executable truths:

1. `actual_holdings` from QC
2. `risk_approved_target`
3. `envelope_final_target`

And three diagnostic/shadow surfaces:

- legacy dict final target
- advisory/LLM weights
- PC shadow/reference weights

Required panel:

```text
Target Path:
  risk_approved -> governance -> position_manager -> policy_cap -> throttle -> final

For each stage:
  changed tickers
  mutation type
  before / after
  safety effect: reduce / increase / neutral
```

Acceptance:

```text
Operator can see exactly why a target changed and whether it reduced or added risk.
Advisory weights cannot be mistaken for executable weights.
```

Implementation status: completed.

- `services.target_path_visibility` converts `TargetEnvelope` into a
  dashboard/API payload with:
  - executable truths: QC actual holdings, risk-approved target, envelope final
    target
  - diagnostic surfaces: legacy dict final target, advisory/LLM weights, PC
    shadow/reference weights
  - stage-by-stage mutation attribution with before/after and
    `safety_effect`
- `dashboard/app.py` exposes this payload under
  `latest_analysis.target_path_visibility`.
- The dashboard renders a dedicated Target Path panel and also shows it inside
  Latest Decision near Final Risk Validation.

---

## Required Tests

### Accounting

```python
def test_replay_ledger_reconstructs_final_target()
def test_unaccounted_drift_fails_accounting()
def test_ticker_with_wrong_ledger_amount_fails_accounting()
def test_cash_accounting_is_consistent()
def test_final_target_returns_copy()
```

### Safety

```python
def test_restricted_reduce_allowed()
def test_restricted_increase_blocked()
def test_restricted_trim_reversal_blocked()
def test_hard_risk_reduce_allowed()
def test_hard_risk_trim_suppressed_blocked()
def test_policy_cap_exceeded_blocked()
def test_unknown_positive_weight_blocked()
```

### Current Regression Case

```python
def test_current_governance_trim_plus_min_hold_case_passes_envelope_validation()
```

Scenario:

- governance trims QQQ/XLE/XLK/XLU
- min-hold defers SPY/IWM/XLI sells
- final target reduces restricted names
- accounting replay covers every ticker drift
- final validation passes

### Protection

```python
def test_forced_trim_not_scaled_by_turnover()
def test_hard_risk_not_deferred_by_min_hold()
def test_ordinary_ticker_still_scaled_by_turnover()
def test_ordinary_ticker_still_deferred_by_min_hold()
```

---

## Deployment Plan

1. Deploy PR1 in shadow only.
2. Deploy PR2 with `target_envelope_config.mode = active`.
3. Watch next two pipeline cycles:
   - envelope accounting OK
   - safety OK
   - no `incomplete_mutation_ledger`
   - no `conditional_touches_restricted_ticker`
4. If active envelope blocks, it must report either:
   - `accounting_contract_violation`
   - `safety_contract_violation`
   with per-ticker details.
5. Do not fall back to legacy execution after envelope failure.

---

## Definition of Done

```text
□ TargetEnvelope exists and is used in post-risk execution path.
□ In FULL_AUTO active mode, executor receives envelope.final_target.
□ Legacy dict chain is shadow diagnostic, not execution authority.
□ Every non-CASH executable post-risk drift is replayable from MutationLedger.
□ Final validation has separate accounting and safety checks.
□ Risk-reducing trims are allowed.
□ Risk-increasing restricted/hard-risk changes are blocked.
□ hard-risk/forced-trim is not delayed by min-hold or ordinary turnover scaling.
□ Current repeated blocker case no longer blocks.
□ Dashboard shows target path and stage-by-stage mutation attribution.
```
