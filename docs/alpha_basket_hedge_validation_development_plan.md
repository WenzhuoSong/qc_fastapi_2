# Alpha, Basket, and Hedge Validation Development Plan

> Goal: convert the theory review into executable engineering work.
>
> Core posture: execution and risk infrastructure is mature enough to run.
> Alpha quality, active basket sizing, and inverse-ETF hedge thresholds are
> still empirical questions. This plan adds the data and diagnostics needed to
> calibrate them without weakening risk controls.

---

## Executive Summary

The system should not keep adding execution guards as the default response to
uncertainty. The next phase is evidence-building:

1. Prove whether strategy returns contain residual alpha after beta/factor
   attribution.
2. Estimate true strategy breadth through correlation and independence, not
   ticker count or effective_N alone.
3. Upgrade portfolio construction from `maximize_effective_N` to
   `maximize_effective_N within active_basket_policy`, first in shadow mode.
4. Log hedge-intent outcomes so `-1x` ETF usage can be calibrated from actual
   missed/profitable hedge cases.

All work in this plan is diagnostics-first. No PR in this plan may directly
increase trade authority, bypass risk validation, or loosen execution policy.

---

## Non-Goals

- Do not relax `risk_manager`, `final_risk_validation`, `execution_policy`, or
  QC-side validation.
- Do not make `portfolio_construction` directly executable in the first PRs.
- Do not treat `effective_N` as Fundamental Law breadth (`BR`).
- Do not treat `monitoring_ready` or 30 samples as proof of alpha.
- Do not promote `-1x` ETF buying by lowering hedge thresholds without
  outcome evidence.
- Do not implement CVaR before scenario stress and beta-shock diagnostics are
  stable.

---

## Current Baseline

Execution/risk state:

- `TargetEnvelope` and `MutationLedger` now make post-risk mutations auditable.
- `weight_ops` centralizes cap, normalize, floor, and cash accounting.
- `final_execution_policy_cap` can repair final executable weights as a
  backstop.
- `active_basket_policy` exists as `diagnostic_only`.
- `minimum_executable_weight` clears economically meaningless sub-0.5%
  positions into cash.

Remaining validation gap:

- Alpha attribution is not yet the promotion source of truth.
- Strategy independence exists as diagnostics, but it is not yet connected to
  basket calibration.
- Active basket targets such as 4-10 positions are engineering starting points,
  not calibrated optimal values.
- Hedge severity thresholds are empirical and need outcome logging.

---

## Terminology Contract

### Conviction Status Names

These names must be used in reports and dashboards:

```text
<30 samples      insufficient
30-99 samples    monitoring_ready
100-299 samples  early_signal
300-782 samples  indicative
783+ samples     statistically_meaningful
```

Rules:

- `calibrated` must not be used as a statistical status.
- `monitoring_ready` means only that the profile is worth watching.
- `statistically_meaningful` is a naming threshold, not an execution override.
- Harvey-style `t > 3` is an alpha-claim threshold, not a trade blocker.

### Breadth vs Effective N

```text
effective_N = portfolio weight diversification
BR          = statistically independent signal breadth
```

`effective_N` is useful for portfolio shape. It must not be reported as
independent alpha breadth. `strategy_correlation_matrix` and cluster counts are
the correct diagnostics for estimated breadth.

---

## PR1: Terminology and Reporting Cleanup

### Goal

Remove the misleading statistical word `calibrated` from conviction reports and
replace it with sample-count-aware status names.

### Scope

Update:

- conviction profile status formatter
- Telegram daily/strategy reports
- dashboard status labels
- playground/evidence bundle summaries
- tests that assert legacy `calibrated` wording

Backward compatibility:

- Existing persisted fields can remain if required for old consumers.
- New user-facing fields must use the revised status names.
- If legacy field `calibrated` remains internally, label it
  `legacy_operational_status`.

### Acceptance Criteria

```text
□ Reports no longer call 30-sample profiles calibrated.
□ Statistical status uses insufficient / monitoring_ready / early_signal /
  indicative / statistically_meaningful.
□ Any remaining legacy field is explicitly marked as legacy.
□ Tests cover 29, 30, 100, 300, and 783 sample boundaries.
```

---

## PR2: Monthly Alpha Attribution Report

### Goal

Answer whether portfolio/strategy returns contain residual alpha after market
beta and simple factor exposure.

### Data Inputs

Primary:

- portfolio daily return series
- strategy signal or strategy return replay series
- SPY daily return

Optional later:

- QQQ return
- UMD-style momentum proxy
- sector factor proxies
- risk-free proxy from SGOV/T-bill data

### Service

Create:

```text
services/alpha_attribution_report.py
```

Core output:

```python
{
    "report_version": "alpha_attribution_report_v1",
    "execution_authority": "none",
    "target_weight_mutation": "none",
    "sample_count": 42,
    "sample_status": "monitoring_ready",
    "factor_model": "spy_single_factor_v1",
    "beta_vs_spy": 0.72,
    "alpha_daily": 0.00012,
    "alpha_annualized": 0.0302,
    "alpha_t_stat": 1.14,
    "alpha_p_value": 0.26,
    "r_squared": 0.48,
    "meets_t2_suggestive": False,
    "meets_harvey_t3_threshold": False,
    "honest_interpretation": "early_monitoring",
}
```

Interpretation rules:

```text
t_stat < 2.0       early_monitoring
2.0 <= t_stat < 3  suggestive_not_proven
t_stat >= 3.0      statistically_meaningful_with_multiple_testing_caution
```

### Storage

Persist monthly report JSON if a suitable diagnostics table already exists.
Otherwise expose through daily report/playground first and add persistence in a
later PR. Do not block this PR on schema migration.

### Acceptance Criteria

```text
□ Report computes beta, residual alpha, t-stat, p-value, R^2.
□ Report refuses to overstate insufficient samples.
□ Output is explicitly diagnostics-only.
□ Telegram/dashboard can show the honest interpretation.
□ Unit tests cover insufficient, suggestive, and t>=3 cases.
```

---

## PR3: Strategy Correlation Matrix and Breadth Calibration

### Goal

Use signal/return correlation to estimate true independent strategy breadth.
This feeds active basket calibration and strategy promotion decisions.

### Existing Work

`alpha_validation_v3_statistical_independence_plan.md` already implemented
strategy independence diagnostics. This PR should not duplicate that work.
Instead it should:

- standardize the output contract for dashboard/Telegram
- expose cluster counts as estimated breadth
- connect the output to active basket diagnostics

### Output Contract

```python
{
    "report_version": "strategy_breadth_calibration_v1",
    "execution_authority": "none",
    "target_weight_mutation": "none",
    "total_strategies": 15,
    "estimated_independent_clusters": 5,
    "duplication_ratio": 0.67,
    "high_correlation_pairs": [
        {"a": "momentum_lite_v1", "b": "absolute_trend_following_lite", "corr": 0.82}
    ],
    "diversifying_pairs": [
        {"a": "momentum_lite_v1", "b": "mean_reversion_lite", "corr": -0.28}
    ],
    "minimum_overlap": 60,
    "insufficient_overlap_pairs": 12,
}
```

### Acceptance Criteria

```text
□ Correlation output separates duplicate-alpha pairs from diversifying pairs.
□ Cluster count is clearly labeled as an approximation of BR.
□ Pairs with insufficient sample overlap do not produce fake correlations.
□ Active basket diagnostics can read estimated cluster count.
□ No execution path consumes this output as a trade authority signal.
```

---

## PR4: Active Basket Calibration Report

### Goal

Turn `active_basket_policy` from a static diagnostic into a calibrated report
that explains whether the current basket range is reasonable.

### Service

Create or extend:

```text
services/active_basket_policy.py
```

Add:

```python
def build_active_basket_calibration_report(
    *,
    active_basket_diagnostics: dict,
    strategy_breadth_report: dict,
    transaction_cost_summary: dict,
    realized_contribution_summary: dict,
) -> dict:
    ...
```

Output:

```python
{
    "report_version": "active_basket_calibration_v1",
    "execution_effect": "diagnostic_only",
    "current_policy": {
        "target_active_count_min": 4,
        "target_active_count_max": 10,
    },
    "observed_active_count": 8,
    "estimated_independent_clusters": 5,
    "subscale_position_count": 1,
    "floor_cleared_count": 2,
    "transaction_cost_drag_pct": 0.0008,
    "suggested_range": [4, 8],
    "suggestion_reason": [
        "estimated_breadth_5",
        "subscale_positions_present",
        "low_contribution_tail"
    ],
    "operator_action": "review_only",
}
```

### Calibration Logic

Initial heuristic:

```text
suggested_min = max(3, min(4, estimated_independent_clusters))
suggested_max = min(12, max(6, estimated_independent_clusters + 3))
```

Adjust down if:

- many positions are repeatedly below role min weight
- tail positions have near-zero contribution
- transaction-cost drag is material

Adjust up only if:

- strategy breadth is high
- tail positions have persistent positive contribution
- turnover/cost stays acceptable

### Acceptance Criteria

```text
□ Calibration report is generated but does not change execution.
□ It explains why 4-10 should stay, shrink, or expand.
□ It uses estimated independent clusters, not effective_N alone.
□ It includes subscale and floor-cleared diagnostics.
□ Tests cover low-breadth and high-breadth scenarios.
```

---

## PR5: Portfolio Construction Shadow Objective Upgrade

### Goal

Upgrade shadow portfolio construction from:

```text
maximize_effective_N
```

to:

```text
maximize_effective_N within active_basket_policy
```

### Constraints

This is shadow-only.

```python
{
    "execution_authority": "none",
    "target_weight_mutation": "none",
    "pc_mode": "shadow",
}
```

### Objective

Candidate score:

```text
score =
  alpha_support_score
  + diversification_score
  - turnover_penalty
  - concentration_penalty
  - active_basket_violation_penalty
  - subscale_position_penalty
```

Required constraints:

- global active count stays inside active basket range
- role max positions are respected
- hedge role requires hedge_intent
- sub-min-executable positions are excluded
- role min weight violations are penalized
- policy caps are respected

### Output Contract

```python
{
    "pc_objective_version": "maximize_effective_n_with_active_basket_v1",
    "execution_authority": "none",
    "target_weight_mutation": "none",
    "candidate_weights": {...},
    "basket_evaluation": {...},
    "objective_terms": {
        "alpha_support_score": 0.62,
        "diversification_score": 0.48,
        "turnover_penalty": 0.11,
        "active_basket_violation_penalty": 0.0,
        "subscale_position_penalty": 0.0,
    },
    "ready_for_gated_review": False,
}
```

### Acceptance Criteria

```text
□ PC shadow emits candidate weights and basket evaluation.
□ Candidate output cannot enter target_builder.
□ Candidate respects execution policy caps in diagnostics.
□ Shadow report shows objective terms separately.
□ Tests prove shadow candidate is not consumed by execution path.
```

---

## PR6: Basket Readiness Gate

### Goal

Promote basket-aware portfolio construction only after enough shadow evidence.

### Gate Criteria

Suggested starting gate:

```text
min_cycles: 20
basket_policy_ok_rate >= 90%
policy_ok_rate >= 95%
turnover_ok_rate >= 80%
mean_abs_weight_deviation <= 1.5%
subscale_position_rate <= 10%
no_unclassified_mutations = true
```

### Output

```python
{
    "status": "shadow_only",
    "ready": False,
    "cycles": 12,
    "pass_rate": 0.67,
    "blockers": ["insufficient_cycles", "basket_policy_ok_rate_below_threshold"],
}
```

### Acceptance Criteria

```text
□ Readiness gate remains diagnostic until explicit config promotes it.
□ Gate reasons are visible in dashboard/Telegram.
□ Existing portfolio_construction promotion checks are not weakened.
□ Tests cover insufficient cycles, low pass rate, and ready states.
```

---

## PR7: Hedge Intent Outcome Log

### Goal

Record every hedge decision, including non-events, so `-1x` ETF thresholds can
be calibrated from outcomes.

### Data Contract

Create service:

```text
services/hedge_intent_outcome_log.py
```

Record:

```python
{
    "report_version": "hedge_intent_outcome_v1",
    "date": "2026-06-05",
    "triggered": True,
    "severity": 0.52,
    "add_hedge_etf": False,
    "selected_instrument": None,
    "why_not_add_hedge": "severity_0.52_below_threshold_0.70",
    "trim_targets": ["QQQ", "XLK"],
    "cash_raise_pct": 0.05,
    "regime": "defensive",
    "vix": 27.2,
    "breadth": 0.38,
    "portfolio_beta_estimate": 0.64,
    "outcome_status": "pending_t5",
    "spy_return_5d": None,
    "hedge_instrument_return_5d": None,
    "hedge_would_have_helped": None,
    "threshold_assessment": None,
}
```

### T+5 Backfill

After five trading days, fill:

- `spy_return_5d`
- selected hedge instrument return, or candidate hedge return if no hedge was
  selected
- whether hedge would have helped
- threshold assessment

Assessment rules:

```text
not_triggered and SPY_5d <= -3%:
  threshold_assessment = too_conservative

add_hedge_etf and SPY_5d >= +2%:
  threshold_assessment = too_aggressive

triggered_no_hedge and SPY_5d <= -5%:
  threshold_assessment = severity_threshold_too_high

otherwise:
  threshold_assessment = appropriate_or_inconclusive
```

### Acceptance Criteria

```text
□ Every hedge_intent run creates a log record or diagnostics row.
□ Non-triggered decisions are recorded.
□ T+5 outcome fill can run idempotently.
□ Telegram/dashboard can summarize recent threshold assessments.
□ No hedge threshold changes are made in this PR.
```

---

## PR8: Scenario Stress and Beta Shock

### Goal

Add robust risk diagnostics before attempting CVaR.

### Scenario Stress

Use historical windows:

- 2020-03 COVID crash
- 2022 rate shock
- 2018 Q4 selloff
- 2023 tech rebound

Output:

```python
{
    "report_version": "scenario_stress_v1",
    "execution_authority": "none",
    "current_weights": {...},
    "scenarios": [
        {
            "name": "covid_crash_2020_03",
            "portfolio_return": -0.084,
            "spy_return": -0.124,
            "relative_return": 0.040,
            "top_loss_contributors": [...]
        }
    ],
}
```

### Beta Shock

Compute simple shocks:

```text
SPY -10%, -20%, -30%
QQQ -10%, -20%, -30%
sector shock by role
```

### Acceptance Criteria

```text
□ Stress report does not require covariance estimation.
□ Report shows top loss contributors.
□ Output is visible to dashboard/operator.
□ No execution blocker is introduced.
```

---

## Rollout Order

```text
PR1  Terminology cleanup
PR2  Monthly alpha attribution report
PR3  Strategy breadth calibration contract
PR4  Active basket calibration report
PR5  Portfolio construction shadow objective upgrade
PR6  Basket readiness gate
PR7  Hedge intent outcome log
PR8  Scenario stress and beta shock
```

Why this order:

- PR1 prevents misleading statistical language immediately.
- PR2 and PR3 create the evidence needed to calibrate basket size.
- PR4 turns basket policy from static rules into reviewable diagnostics.
- PR5 applies the basket policy in shadow mode only.
- PR6 prevents premature promotion.
- PR7 calibrates inverse ETF use without changing thresholds first.
- PR8 adds practical risk diagnostics without fake covariance precision.

---

## Dashboard Requirements

Add three panels over time:

### Alpha Evidence Panel

Shows:

- sample status
- alpha t-stat
- beta vs SPY
- R-squared
- Harvey threshold flag
- top correlated strategy pairs
- estimated independent clusters

### Active Basket Panel

Shows:

- current active count vs policy range
- role counts
- subscale positions
- floor-cleared positions
- suggested range from calibration report
- readiness gate status

### Hedge Calibration Panel

Shows:

- last 30 hedge decisions
- severity distribution
- no-hedge decisions followed by drawdown
- hedge buys followed by rebound losses
- threshold assessment counts

Default view should show only actionable summaries. Full matrices and event rows
should be expandable.

---

## Definition of Done

```text
□ Statistical labels no longer overstate 30-sample evidence.
□ Monthly alpha report exists and labels alpha claims honestly.
□ Strategy breadth uses correlation/cluster diagnostics, not effective_N.
□ Active basket policy has a calibration report.
□ Portfolio construction emits basket-aware candidates in shadow mode.
□ Basket candidate cannot enter execution without readiness gate + explicit config.
□ Hedge intent decisions are logged even when no hedge is bought.
□ T+5 hedge outcome backfill can classify threshold behavior.
□ Scenario stress and beta shock are available before CVaR work starts.
□ All outputs are diagnostics-first and preserve existing risk controls.
```

