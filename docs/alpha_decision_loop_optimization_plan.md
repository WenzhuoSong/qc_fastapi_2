# Alpha Decision Loop Optimization Plan

Last updated: 2026-05-28

## Purpose

The system now has strong execution safety, account-state validation, command
lifecycle tracking, selective ETF evidence handling, alpha diagnostics,
statistical independence diagnostics, and performance attribution foundations.

The next optimization phase is not another execution-safety phase. It is the
phase that turns alpha diagnostics into a disciplined decision loop.

The core question changes from:

```text
Can the system safely execute a target?
```

to:

```text
Which strategy evidence is strong enough, independent enough, and net-profitable
enough to influence allocation?
```

This plan defines how to connect:

```text
strategy signal
  -> live/historical outcome
  -> residual alpha attribution
  -> statistical confidence
  -> independence adjustment
  -> cost adjustment
  -> promotion/degradation
  -> portfolio construction
  -> target_builder
```

No PR in this plan may bypass existing risk, preflight, account-state, or QC
execution validation.

## Current Diagnosis

The current system is advanced in execution control:

- QC independent validation
- policy version alignment guard
- account state guard
- FULL_AUTO safety preconditions
- final risk validation
- execution throttle
- command lifecycle ledger
- auto pause
- selective strategy evidence caps

The remaining structural weakness is alpha decision quality:

- many strategies may still express similar momentum or relative-strength risk
- statistical independence is diagnosed but not yet consumed by allocation
- conviction status is visible but not conservative enough in downstream
  weighting
- performance attribution exists but does not yet drive promotion/degradation
- cost is visible but mostly diagnostic
- PC can diversify structure without proving the underlying signals are
  statistically independent

This plan is meant to close that gap.

## Non-Goals

This phase does not:

- add new execution bypasses
- allow LLMs to construct final targets
- promote any strategy automatically without operator-visible evidence
- remove QC-side independent validation
- replace target_builder ownership
- require 300 live samples before the system can run
- force all weak evidence to zero exposure immediately
- treat statistical diagnostics as guarantees of future alpha

The first implementation stages are observe/recommendation only. Gated
allocation impact requires explicit promotion criteria.

## Core Design Principles

### 1. Execution Safety Is Necessary but No Longer the Main Bottleneck

Execution safety remains mandatory. However, the next marginal improvement
comes from better alpha selection, not from adding more command gates.

### 2. Diagnostic Visibility Is Not Decision Integration

A dashboard metric is not the same thing as a decision input.

The following diagnostics must move from "visible" to "consumed under strict
rules":

- strategy correlation
- independent alpha count
- residual alpha attribution
- statistical conviction tier
- net-of-cost edge
- regime-specific weakness

### 3. Abstain Is Still Not Score Zero

Partial ETF/strategy evidence remains valid.

A missing long-horizon field should not make a young ETF bearish. It should
reduce the evidence quality available to strategies that require that field.

### 4. Statistical Maturity Must Be Conservative

Thirty samples are useful for early monitoring, not proof of edge.

The system must avoid giving near-full trust to a strategy merely because it has
crossed an operational minimum sample count.

### 5. Independence Must Reduce Duplicate Credit

If five strategies are all momentum variants, they should not receive five full
votes in allocation.

Correlation does not make a strategy useless, but it should reduce duplicate
portfolio influence.

### 6. Attribution Must Affect Promotion and Degradation

If a strategy looks good before attribution but has negative residual alpha
after beta/factor/cost adjustment, it should not be promoted as alpha.

### 7. Cost Must Be Applied to Net Edge

Trading cost does not have to immediately block execution, but strategy
promotion and allocation quality should be evaluated on net edge, not gross
signal return.

## New Decision Objects

### AlphaDecisionProfile

Create a unified alpha decision profile for each strategy/family/regime/epoch
combination.

This is not a new execution authority. It is a structured decision input for
promotion/degradation and, later, Portfolio Construction.

Expected fields:

| Field | Meaning |
|---|---|
| strategy_id | strategy identifier |
| strategy_family | canonical family such as momentum, reversion, macro, hedge |
| regime | market regime bucket |
| construction_epoch_id | PC/policy epoch for path-dependence separation |
| sample_count | outcome sample count used for decision |
| statistical_status | conservative statistical tier |
| hit_rate | hit rate for action-specific definition |
| hit_rate_ci_width | uncertainty width |
| residual_alpha | beta/factor-adjusted alpha estimate |
| residual_alpha_status | positive, neutral, negative, insufficient |
| cost_adjusted_edge | estimated edge after IBKR-style cost proxy |
| cost_status | ok, watch_costs, low_edge_after_cost, insufficient |
| independence_cluster_id | correlation cluster id |
| redundancy_penalty | discount for high correlation with active strategies |
| decision_multiplier | final multiplier used by promotion/PC decision layers |
| decision_status | eligible, watch_only, needs_more_samples, degraded, blocked |
| execution_authority | always none |
| target_weight_mutation | always none |

### AlphaDecisionPolicy

Add a config-driven policy for how diagnostics are consumed.

Initial mode should be observe or recommendation, not gated.

Important fields:

| Field | Initial Value | Meaning |
|---|---:|---|
| mode | observe | observe, recommendation, gated |
| min_status_for_promotion | indicative | minimum statistical tier for promotion recommendation |
| min_status_for_allocation_full_credit | statistically_meaningful | tier required for full multiplier |
| require_positive_residual_alpha | true | strategy must show positive residual alpha for promotion |
| max_full_credit_correlation | 0.40 | below this, no redundancy penalty |
| max_allowed_duplicate_correlation | 0.80 | above this, duplicate-alpha warning |
| cost_model | ibkr_proxy | default cost model until fill calibration is mature |
| require_cost_adjusted_edge_positive | true | promotion requires net edge |
| min_observe_cycles_before_gated | 20 | minimum observe cycles before any allocation effect |
| operator_approval_required_for_gated | true | gated decision consumption requires approval |

## Construction Epoch Semantics

`construction_epoch_id` separates conviction and alpha evidence generated under
materially different portfolio-construction or policy contexts.

The goal is to avoid treating samples from different execution regimes as one
homogeneous population.

### Epoch Triggers

A new epoch is created when any of the following happens:

| Trigger | Meaning |
|---|---|
| `pc_mode_change` | Portfolio Construction mode changes, such as shadow -> candidate -> gated |
| `construction_objective_change` | PC objective version changes materially |
| `policy_version_change` | FastAPI execution policy version changes |
| `manual_operator_reset` | Operator explicitly starts a new evidence clock |

Minor display-only config changes should not create a new epoch.

### Cross-Epoch Evidence Rule

Old epoch evidence is not discarded. It remains useful as historical context.

However:

```text
Old epoch data may contribute to historical_prior diagnostics.
Old epoch data must not be merged into new-epoch live_paper conviction.
New-epoch live_paper conviction starts from the first signal in the new epoch.
```

If a decision layer consumes cross-epoch evidence, it must label the evidence as
historical prior requiring live confirmation.

This prevents silent path-dependence leakage when PC mode or policy changes.

## Statistical Status Semantics

The current system exposes both operational and statistical maturity. Decision
logic should use the stricter statistical interpretation.

Decision tiers:

| Sample Count | Statistical Status | Decision Meaning |
|---:|---|---|
| 0-29 | insufficient | no positive promotion; can only diagnose |
| 30-99 | early_signal | useful for monitoring, not proof |
| 100-299 | indicative | can support advisory promotion if other checks pass |
| 300+ | statistically_meaningful | can receive full statistical credit if other checks pass |

Important rule:

```text
The legacy/operational word "calibrated" must not be used as a promotion or
allocation gate unless mapped through statistical_status.
```

Recommended decision multiplier by status:

| Statistical Status | Promotion Credit | Allocation Credit |
|---|---:|---:|
| insufficient | 0.00 | 0.00 |
| early_signal | 0.15 | 0.10 |
| indicative | 0.45 | 0.35 |
| statistically_meaningful | 1.00 | 1.00 |

Evidence cap may still keep a small floor for observation-sized exposure, but
promotion and PC allocation credit should be stricter.

## Residual Alpha Semantics

Performance attribution should classify strategy/family/regime evidence as:

| Status | Meaning |
|---|---|
| insufficient | not enough attribution samples |
| positive | residual alpha is positive after beta/factor adjustment |
| neutral | residual alpha is near zero or statistically unclear |
| negative | residual alpha is negative over the review window |

Promotion should require positive or at least non-negative residual alpha,
depending on mode.

Degradation should trigger review when:

- residual alpha is negative for consecutive review windows
- residual alpha is negative in the strategy's main intended regime
- gross return is positive but residual alpha is negative
- strategy appears profitable only through beta or momentum factor exposure

## Independence Semantics

Statistical independence should reduce duplicate strategy credit.

Correlation bands:

| Correlation | Meaning | Suggested Treatment |
|---:|---|---|
| < 0.20 | highly diversifying | full independent credit |
| 0.20-0.40 | acceptable independence | near-full credit |
| 0.40-0.65 | related strategy | partial credit |
| 0.65-0.80 | duplicate family risk | heavy penalty |
| > 0.80 | likely duplicate alpha | no duplicate promotion credit |

Negative correlation should not be treated as duplicate-alpha risk. It may be
useful hedge or diversifier evidence, but it still needs separate validation.

### Redundancy Multiplier

Use a fixed, testable, piecewise multiplier. The input correlation is the
strategy's maximum positive correlation with already-actionable strategies in
the same decision universe.

| Max Positive Correlation | Redundancy Multiplier |
|---:|---:|
| < 0.20 | 1.00 |
| 0.20-0.40 | 0.85 |
| 0.40-0.65 | 0.50 |
| 0.65-0.80 | 0.20 |
| >= 0.80 | 0.05 |

The multiplier is never zero. A highly related strategy may still have small
incremental value through universe, implementation, or parameter differences,
but it should not receive full duplicate-alpha credit.

## Cost Semantics

The default cost model remains the IBKR-style proxy until fill-level
calibration is complete.

Cost should affect:

- promotion/degradation
- weak signal deferral recommendations
- PC allocation credit
- dashboard net alpha reporting

Cost should not immediately become a hard command blocker unless explicitly
configured. The first step is to ensure every strategy decision can show:

```text
gross edge
estimated cost
net edge
cost status
```

## Development Phases

## PR1: Conviction Decision Semantics Hardening

### Goal

Make statistical maturity the only valid basis for alpha decision credit.

### Scope

- Keep existing conviction profiles for compatibility.
- Add a decision-facing interpretation that ignores the optimistic meaning of
  legacy `calibrated`.
- Ensure promotion, degradation, evidence cap, and PC diagnostics consume
  `statistical_status`, not operational status.
- Add conservative decision multipliers by statistical tier.
- Dashboard must show legacy status only as operational metadata, not as the
  decision gate.

### Acceptance Criteria

- No promotion logic can treat 30 samples as full or near-full confidence.
- Any profile with fewer than 100 samples cannot become more than early
  advisory/watch-level evidence.
- All decision outputs show sample count and statistical tier.
- Tests prove `calibrated` without `indicative/statistically_meaningful` does
  not pass promotion gates.
- Before gated use of stricter evidence caps, operator receives a current versus
  post-PR1 impact report showing expected cap changes, affected ETFs, affected
  strategies, and estimated cash/allocation change.

### Non-Goals

- Do not change target weights.
- Do not change QC execution behavior.
- Do not delete legacy fields yet.

## PR2: AlphaDecisionProfile Construction

### Goal

Create a unified read-only profile that combines conviction, attribution,
cost, independence, regime, and construction epoch.

### Scope

- Build one profile per strategy/family/regime/epoch where enough data exists.
- Include missing-data and insufficient-sample reasons.
- Include execution authority metadata set to none.
- Store or expose profiles in a dashboard-consumable structure.
- Make profiles available to promotion/degradation and PC diagnostics.

### Inputs

- conviction profiles
- strategy independence diagnostics
- performance attribution rows
- transaction cost diagnostics
- regime gap analysis
- construction epoch metadata
- strategy family metadata

### Acceptance Criteria

- Every profile has sample count, statistical status, residual alpha status,
  cost status, independence cluster, and epoch.
- Profiles with missing components are not discarded; they receive explicit
  `insufficient_*` diagnostics.
- No profile can mutate target weights.

## PR3: Attribution-to-Promotion / Degradation Loop

### Goal

Make residual alpha attribution influence strategy promotion and degradation
recommendations.

### Prerequisite: Attribution Model Quality Check

Before PR3 promotion/degradation logic consumes residual alpha, the attribution
model must be checked for basic sufficiency.

Minimum checks:

- residual distribution sanity by strategy/family
- residual autocorrelation check
- regime-specific beta stability check
- factor omission warnings when residuals remain strongly structured
- explicit attribution model version in every consumed row

If the attribution model is insufficient, PR3 may still expose diagnostics, but
it must not use residual alpha as a promotion/degradation gate until the model
is improved or the weakness is explicitly accepted by the operator.

### Scope

- Promotion requires non-negative or positive residual alpha depending on mode.
- Degradation review triggers on repeated negative residual alpha.
- Gross return and Sharpe are no longer enough for promotion if residual alpha
  is negative.
- Regime-specific attribution should affect regime-specific recommendations.

### Initial Rules

Promotion blockers:

- statistical status below `indicative`
- residual alpha negative
- cost-adjusted edge negative
- high correlation with already-actionable strategy without incremental edge
- mixed or weak regime evidence

Degradation review triggers:

- residual alpha negative for 3 consecutive review windows
- residual alpha negative in primary target regime
- net edge after cost negative for 3 consecutive windows
- strategy family shows broad regime gap with no compensating regime strength

Minimum sample prerequisites:

| Condition | Initial Threshold |
|---|---:|
| samples per review window | 20 |
| total samples before degradation recommendation | 60 |
| consecutive negative windows | 3 |

If total samples are below 60, the system may emit watch/needs-more-samples
diagnostics, but not a degradation recommendation.

### Acceptance Criteria

- Promotion recommendation includes residual alpha status.
- Degradation recommendation includes residual alpha trend.
- A strategy with positive gross return but negative residual alpha cannot be
  promoted without operator override.
- Degradation recommendations require the minimum sample prerequisites.
- Regime-specific residual alpha is shown when available and marked insufficient
  when not available.
- Recommendations remain recommendation-only unless later gated.

## PR4: Independence Consumption in Strategy Weighting

### Goal

Move strategy independence from dashboard-only diagnostics into strategy
decision credit.

### Scope

- Assign strategies to correlation clusters.
- Penalize duplicate highly correlated strategies when computing family and
  strategy decision credit.
- Prevent multiple momentum variants from receiving full independent-alpha
  count.
- Preserve hedge/diversifying strategies with negative correlation as separate
  cases.

### Decision Logic

For each active strategy candidate:

```text
base_credit = statistical_credit * residual_alpha_credit * cost_credit
independence_credit = base_credit * redundancy_multiplier
```

`redundancy_multiplier` decreases as correlation with already-actionable
strategies rises.

### Acceptance Criteria

- Effective independent alpha count is consumed by promotion/degradation.
- Highly correlated strategies can still be watched but cannot all receive full
  allocation credit.
- Dashboard shows raw strategy count versus independence-adjusted count.
- Tests cover high positive correlation, low correlation, and negative
  correlation cases.
- Tests verify the piecewise redundancy multiplier exactly.

## PR5: Portfolio Construction Objective Upgrade

### Goal

Upgrade PC from structural diversification toward alpha-quality-aware
diversification.

### New Objective

Move from:

```text
maximize signal_weighted_effective_N
```

to:

```text
maximize independence_adjusted_net_signal_effective_N
```

Subject to:

- execution policy caps
- ETF evidence caps
- factor concentration limits
- turnover budget
- cost-aware weak-signal constraints
- max cluster exposure by correlated strategy group

### Scope

- Add independence-adjusted signal weights as PC diagnostics.
- Keep shadow mode first.
- In candidate/gated mode, target_builder still owns final target construction.
- PC output must show how much weight comes from each strategy cluster.

### Acceptance Criteria

- PC reports both old and new objective values.
- PC identifies if diversification is mostly within one correlated cluster.
- PC does not reward ten highly correlated momentum signals as ten independent
  alpha sources.
- No live target effect until observe behavior is reviewed.

## PR6: Cost-Aware Net Alpha Decision

### Goal

Use cost diagnostics to decide whether a strategy's edge is economically
meaningful.

### Scope

- Combine IBKR-style cost proxy with strategy expected edge.
- Add net edge status to AlphaDecisionProfile.
- Feed net edge status into promotion/degradation and PC diagnostics.
- Mark weak signals as defer/watch when cost consumes expected edge.

### Initial Cost Statuses

| Status | Meaning |
|---|---|
| insufficient | not enough data for cost proxy |
| ok | estimated edge comfortably exceeds cost |
| watch_costs | cost is material but not dominant |
| low_edge_after_cost | net edge is too small |
| negative_after_cost | cost-adjusted edge is negative |

### Acceptance Criteria

- Promotion cannot ignore negative after-cost edge.
- Dashboard shows gross edge, cost estimate, and net edge.
- Cost remains diagnostic/recommendation first, not a sudden hard execution
  blocker.

## PR7: Gated Decision Policy and Operator Review

### Goal

Allow alpha decision outputs to influence allocation only after observe data is
reviewed.

### Scope

- Add `alpha_decision_policy_config.mode`.
- Modes: observe, recommendation, gated.
- Observe mode records would-affect decisions only.
- Recommendation mode affects promotion/degradation suggestions only.
- Gated mode allows approved multipliers to affect PC/strategy allocation
  credit, still flowing through target_builder and risk gates.

### Gated Promotion Criteria

Before gated mode:

- at least 20 observe cycles
- no unexpected mature-strategy degradation false positives
- no evidence cap stale calibration
- no naked conviction numbers in dashboard
- operator reviewed raw versus adjusted allocation diagnostics
- at least one full dry-run report showing target deltas under gated mode

### Acceptance Criteria

- Gated mode cannot be enabled without explicit config.
- Gated mode never bypasses target_builder.
- FULL_AUTO safety preconditions remain unchanged.
- Operator can compare pre-gated and post-gated target diagnostics.

### Implementation Notes

- `services.alpha_decision_policy` is the single deterministic policy evaluator.
- `alpha_decision_policy_config` defaults to `observe` in `system_config` seed.
- Blocked `gated` requests degrade to recommendation effect only; allocation
  effect remains disabled.
- Portfolio Construction receives the policy context as diagnostics and still
  reports `execution_authority=none` and `target_weight_mutation=none`.
- Dashboard surfaces current mode, effective mode, gated blockers, decision
  rules, and before/after alpha-decision objective diagnostics.

## PR8: Dashboard and Review Surface

### Goal

Make the alpha decision loop understandable without reading raw JSON.

### Required Views

1. AlphaDecisionProfile table
   - strategy
   - family
   - regime
   - statistical status
   - sample count
   - residual alpha
   - cost-adjusted edge
   - independence cluster
   - decision status

2. Strategy cluster view
   - raw strategies
   - correlation clusters
   - effective independent count
   - duplicate-alpha warnings

3. Promotion/degradation review
   - recommendation
   - blockers
   - supporting evidence
   - residual alpha trend
   - operator action

4. PC before/after objective view
   - current objective
   - independence-adjusted objective
   - cluster concentration
   - would-change target deltas

5. Net alpha view
   - gross edge
   - estimated IBKR cost
   - net edge
   - trend by strategy/family/regime

### Acceptance Criteria

- No conviction is displayed without sample count and status.
- No promotion recommendation is displayed without residual alpha and cost
  status.
- No strategy count is displayed without effective independent count nearby.
- Dashboard clearly marks observe/recommendation/gated mode.

### Implementation Notes

- Dashboard now includes an `Alpha Decision Review Surface` that joins policy,
  AlphaDecisionProfile, promotion/degradation, PC objective, attribution, and
  strategy-independence diagnostics.
- The review surface includes explicit checklist flags for naked conviction,
  residual-alpha/cost fields, effective independent strategy counts, PC
  raw-versus-adjusted diagnostics, and target-builder bypass prevention.
- Promotion rows now expose residual alpha and net-cost edge fields directly,
  including explicit `not_applicable` markers for family-level research gaps.
- Net alpha rows show gross edge, IBKR estimated cost, cost-adjusted edge,
  edge-to-cost ratio, redundancy multiplier, and decision multiplier by
  strategy/family/regime/epoch.

## PR9: Retirement and Cleanup

### Goal

Remove or deprecate old decision paths that conflict with the new alpha loop.

### Scope

- Remove any remaining use of operational `calibrated` as decision maturity.
- Deprecate dashboard widgets that show strategy count without independence.
- Deprecate promotion logic that ignores residual alpha.
- Ensure docs and tests describe alpha decision loop as the canonical flow.

### Acceptance Criteria

- Grep/checks confirm decision code uses statistical status.
- Promotion/degradation consumes AlphaDecisionProfile or equivalent.
- PC diagnostics consume independence-adjusted signal strength.
- Documentation reflects the new canonical alpha decision flow.

### Implementation Notes

- `strategy_promotion_recommendations` now uses `statistical_status` as the
  promotion/degradation maturity gate. Operational `calibrated` remains only a
  diagnostic label and cannot by itself trigger promotion credit.
- Dashboard strategy-count cards now show effective independent alpha counts
  alongside raw counts.
- Recommendation rows include residual alpha, net edge, IBKR cost proxy,
  redundancy multiplier, and correlation context.
- The Alpha Decision Review Surface is the canonical operator review flow for
  alpha maturity, independence, residual attribution, net cost, and PC
  raw-versus-adjusted diagnostics.

## Rollout Strategy

### Stage 1: Observe

PR1-PR6 produce diagnostics and recommendations only.

No target changes.

### Stage 2: Recommendation

Promotion/degradation recommendations become stricter and operator-visible.

Still no automatic target changes.

### Stage 3: Shadow Allocation

PC computes independence-adjusted allocation in parallel with current PC.

Target_builder reports would-change diagnostics.

No live target effect.

### Stage 4: Gated Allocation

After observe criteria are met, approved alpha decision multipliers may affect
PC/strategy allocation credit.

Still:

- target_builder owns final target
- risk_manager validates
- position_manager tighten-only
- final risk validation blocks in FULL_AUTO
- preflight checks command safety
- QC independently validates execution

## Configuration Plan

Add or extend a single config namespace:

```text
alpha_decision_policy_config
```

Initial recommended values:

```json
{
  "mode": "observe",
  "min_status_for_promotion": "indicative",
  "min_status_for_allocation_full_credit": "statistically_meaningful",
  "require_positive_residual_alpha": true,
  "require_cost_adjusted_edge_positive": true,
  "max_full_credit_correlation": 0.4,
  "max_allowed_duplicate_correlation": 0.8,
  "cost_model": "ibkr_proxy",
  "min_observe_cycles_before_gated": 20,
  "operator_approval_required_for_gated": true
}
```

This config must not authorize direct execution. It only controls how alpha
diagnostics are interpreted by recommendation and PC layers.

## Testing Strategy

Required test categories:

1. Statistical maturity
   - 30 samples cannot receive full credit.
   - 100 samples can become indicative but not statistically meaningful.
   - 300 samples can receive full status if other checks pass.

2. Residual alpha
   - positive gross return but negative residual alpha blocks promotion.
   - negative residual alpha over repeated windows triggers degradation review.

3. Independence
   - high positive correlation reduces duplicate credit.
   - low correlation preserves credit.
   - negative correlation is not treated as duplicate-alpha risk.

4. Cost
   - high estimated cost can block promotion recommendation.
   - cost status appears in every recommendation.

5. PC objective
   - independence-adjusted objective penalizes correlated strategy clusters.
   - PC does not mutate live targets in observe mode.

6. Authority
   - AlphaDecisionProfile has `execution_authority=none`.
   - No PR bypasses target_builder, risk validation, preflight, or QC.

## Global Definition of Done

This phase is not complete until:

1. Strategy promotion/degradation consumes statistical maturity, residual alpha,
   independence, and net cost evidence.

2. The word `calibrated` is not used as a decision gate without stricter
   `statistical_status` mapping.

3. Portfolio Construction can show an independence-adjusted objective next to
   its structural objective.

4. Dashboard shows raw strategy count and effective independent alpha count.

5. Every strategy recommendation shows sample count, residual alpha, cost
   status, and correlation cluster.

6. Cost-adjusted edge is visible before any strategy is promoted.

7. No alpha diagnostic directly sends trades or mutates final targets.

8. Gated mode requires explicit operator approval and observe-period evidence.

9. The system can answer which strategies have positive regime-specific
   residual alpha in the current regime, or clearly state that evidence is
   insufficient.

10. The system can answer:

```text
After beta, factor exposure, duplicate strategy correlation, and estimated
costs, which strategies still appear to have positive edge?
```

## Expected End State

After this phase, the system should no longer be best described as:

```text
An institution-grade execution framework around a momentum rotation core.
```

The target state is:

```text
A deterministic execution and risk platform whose strategy allocation is
conditioned on statistically mature, attribution-positive, cost-aware, and
independence-adjusted alpha evidence.
```

## Completion Status

Completed: 2026-05-28

All planned PRs in this phase have been implemented and verified:

| PR | Status | Canonical Evidence |
|---|---|---|
| PR1 Conviction Decision Semantics | Complete | `services.conviction_decision`, statistical discounts, evidence/cost gates consume statistical status |
| PR2 AlphaDecisionProfile | Complete | `services.alpha_decision_profile`, dashboard AlphaDecisionProfile tables |
| PR3 Attribution Promotion Loop | Complete | promotion/degradation consumes residual alpha and attribution model quality |
| PR4 Independence Consumption | Complete | redundancy multipliers and effective independent alpha counts are consumed |
| PR5 PC Objective Upgrade | Complete | PC reports old structural and new independence-adjusted objectives |
| PR6 Cost-Aware Net Alpha | Complete | gross edge, IBKR cost proxy, cost-adjusted edge, and net edge status are surfaced |
| PR7 AlphaDecisionPolicy | Complete | `alpha_decision_policy_config` controls observe/recommendation/gated interpretation |
| PR8 Dashboard Review Surface | Complete | Alpha Decision Review Surface joins policy, profiles, PC, attribution, recommendations, and independence |
| PR9 Retirement Cleanup | Complete | promotion maturity uses `statistical_status`; operational `calibrated` is diagnostic only |

Final verification:

- Full test suite passes.
- Promotion/degradation uses statistical maturity, residual alpha, independence,
  and net cost evidence.
- Portfolio Construction diagnostics consume independence-adjusted signal
  strength but do not bypass `target_builder`.
- Alpha diagnostics keep `execution_authority=none` and
  `target_weight_mutation=none`.
- Dashboard shows raw strategy counts beside effective independent alpha counts.
- Gated mode requires explicit operator approval and observe evidence before
  allocation effect.
