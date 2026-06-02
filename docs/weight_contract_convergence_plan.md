# Weight Contract Convergence Plan

> Goal: move from feature-module stacking to a unified weight contract.
> This work does not add another guard. It converges duplicate weight arithmetic,
> standardizes mutation records, and isolates legacy advisory/reporting paths.

## Root Cause

Several modules implemented their own version of weight arithmetic:

- normalization
- cap handling
- CASH residual assignment
- buy/sell throttling
- post-risk mutation recording

Those implementations were similar but not identical. In `SEMI_AUTO`, a human
could interpret warnings and ignore noisy diagnostics. In `FULL_AUTO`, the code
must carry the exact semantics. Any ambiguity becomes a blocker, false positive,
or silent weight drift.

## PR1a: `services/weight_ops.py`

Create the single shared weight arithmetic module. Do not migrate existing
callers in PR1a.

Required functions:

- `normalize_cash_first()`
- `normalize_proportional()`
- `apply_single_caps_cash_first()`
- `apply_group_caps_cash_first()`
- `tighten_buy_delta()`
- `tighten_sell_delta()`
- `assert_invariants()`

Key semantics:

- `normalize_cash_first()` protects non-cash weights and assigns `CASH` as the
  residual. It never preserves an old CASH value if that would make total weight
  exceed 1.
- `normalize_proportional()` scales all weights, including CASH, and is only for
  initial construction.
- cap functions release excess weight into CASH and do not round internally.
- `tighten_buy_delta()` is tighten-only.
- `tighten_sell_delta()` is conditional because it can keep a position above
  the intended target while still reducing relative to current holdings.

PR1a tests must include property tests for:

- normalized total never exceeds 1
- cash-first normalization never inflates non-cash weights
- extra CASH input cannot make total exceed 1
- cap released amount equals CASH increment
- buy-delta tightening never increases a ticker above target

## PR1b: Final Execution Policy Cap Migration

Migrate `final_execution_policy_cap.py` first because it is closest to executor.
Use `apply_single_caps_cash_first()` and `normalize_cash_first()`.

Acceptance:

- capped weights cannot be renormalized above policy cap
- released cap amount is assigned to CASH
- existing final cap tests continue to pass

## PR1c: Legacy Diagnostic Normalize Migration

Migrate diagnostic-only hard-clip and strategy-use normalization paths to
`weight_ops`. They should not influence execution, but their diagnostics must
not show inflated advisory weights.

## PR2: Mutation Ledger

Introduce `services/mutation_ledger.py`.

Every post-risk weight change must be recorded as a ticker-level mutation:

```python
{
    "type": "cap_single_buy_delta",
    "ticker": "XLK",
    "before": 0.1672,
    "after": 0.15,
    "reason": "policy cap",
}
```

Mutation classes:

- `TIGHTEN_ONLY`: after must be less than or equal to before.
- `CONDITIONAL`: allowed to keep weight above the desired target, but must be
  reviewed by final validation.

`sell_delta_throttle`, `min_hold_defer_sell`, and
`turnover_scale_toward_current` are conditional, not tighten-only.

## PR3: Position Manager Ledger Integration

`position_manager` writes ticker-level ledger records for every mutation it
creates.

## PR4: Final Risk Validation Consumes Ledger

`final_risk_validation` should read the structured ledger instead of guessing
from global mutation-type strings.

Rules:

- every non-CASH drift between risk-approved target and final target must have a
  ledger entry
- conditional mutations on hard-risk tickers are blocked unless explicitly
  reviewed
- incomplete ledger detail falls back to conservative review

## PR5: Target Builder / Portfolio Construction Migration

Migrate high-blast-radius construction layers after PR1a and PR2 are stable.

- `target_builder`: use cash-first cap/normalization helpers
- `portfolio_construction`: use proportional normalization only for initial
  construction

## PR6: Legacy Diagnostic Isolation

Add code-level assertions and dashboard labels so advisory/reference weights
cannot be confused with executable weights.

Executable target key conventions:

| Key | Source | May Enter Target Builder |
|---|---|---|
| `pc_candidate_weights` | PC gated mode | yes |
| `pc_shadow_weights` | PC shadow mode | no |
| `llm_adjusted_weights` | LLM advisory | no |
| `baseline_reference_weights` | quant baseline | no |
| `target_weights` | target builder | executable output |

## Definition of Done

- `weight_ops.py` is the only module implementing shared normalize/cap/tighten
  arithmetic.
- CASH accounting property tests pass.
- no cap-preserving path can re-inflate capped non-cash weights.
- every post-risk weight change has ticker-level mutation detail.
- final validation consumes structured mutation details and falls back
  conservatively when details are incomplete.
- legacy advisory weights are visibly and programmatically isolated from
  executable target weights.
