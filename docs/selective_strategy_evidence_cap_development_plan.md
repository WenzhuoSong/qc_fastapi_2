# Selective Strategy Evidence Cap Development Plan

## Purpose

This plan defines how the system will move from binary ETF eligibility to
selective strategy connectivity:

```text
Do not ask: "Can this ETF be used?"
Ask instead: "For this ETF, which strategies can vote today, and what is the
maximum safe weight under current evidence quality?"
```

The goal is to let young or partially covered ETFs, such as `DRAM`, participate
in strategies whose required features are available, while preventing missing
long-horizon features from contaminating long-horizon strategy scores,
Portfolio Construction, target weights, or execution.

This plan is intentionally staged. PR1-PR4 must not change live target weights.
Enforcement is allowed only after observe-mode evidence cap behavior is visible
and accepted by the operator.

## Core Semantics

### Vote Status

Every EvidenceCard-like strategy output must distinguish strategy intent from
vote eligibility.

`action` remains the strategy semantic suggestion:

```text
increase, reduce, hold, watch, avoid, hedge, de_risk, neutral
```

`vote_status` describes whether this card can participate in downstream
aggregation:

```text
voted
abstain
watch
mapping_error
```

Meanings:

```text
voted
  The ETF has complete required inputs for this strategy and the score was
  successfully mapped into a meaningful action. It can participate in ETF-level
  actionable aggregation.

abstain
  The strategy has no vote for this ETF in this run. This is not a negative
  score and must never be converted into score=0.

watch
  The strategy produced a score and the system understood it, but the semantic
  result is non-executable or safety-limited observation. This is normal and
  should not alert.

mapping_error
  The strategy produced a score, but the system cannot safely interpret it due
  to missing or broken knowledge mapping. This should be visible to operations
  as a knowledge/configuration issue, but PR1 must not send Telegram alerts.
```

### Abstain Reasons

`abstain_reason` values:

```text
insufficient_history
  The ETF is young or lacks enough lookback for a required field. This should
  naturally resolve as history accumulates.

field_not_applicable
  The field does not make sense for this ETF type or role. This is normal and
  should not trigger data repair.

data_stale
  The required field exists but is stale. This should be visible as a data
  health issue.

strategy_universe_mismatch
  The strategy does not cover this ETF role or universe. This does not count
  against evidence coverage.

missing_required_field
  The field should exist for this ETF/strategy pair but does not. This is a data
  contract issue and should be visible.
```

### Coverage Ratio

Coverage ratio must not use all registered strategies as the denominator.
Only eligible strategies count:

```python
eligible_strategies = [
    strategy
    for strategy in all_strategies
    if strategy_can_apply_to_etf_role(strategy, etf.role)
    and abstain_reason != "strategy_universe_mismatch"
]

coverage_ratio = voted_count / max(len(eligible_strategies), 1)
```

This prevents strategies that never intended to score a given ETF role from
penalizing that ETF.

## Evidence Quality Cap

Evidence quality affects weight caps, not raw strategy scores.

Use weighted average plus a floor, not multiplicative compression:

```python
raw = (
    0.4 * coverage_ratio
    + 0.4 * conviction_discount
    + 0.2 * history_discount
)

multiplier = max(raw, min_multiplier)
```

Initial defaults:

```text
min_multiplier = 0.10

conviction_discount:
  statistically_meaningful: 1.00
  calibrated: 0.80
  indicative: 0.60
  early_signal: 0.45
  early_estimate: 0.45
  insufficient_samples: 0.30
  missing_profile: 0.30
```

`conviction_discount` must come from live/historical conviction profile status,
not from static ETF metadata. Young ETFs should naturally graduate as samples
accumulate.

Fallback rule:

```python
DEFAULT_CONVICTION_DISCOUNT = 0.30

def get_conviction_discount(status: str | None) -> float:
    return CONVICTION_DISCOUNT_MAP.get(status, DEFAULT_CONVICTION_DISCOUNT)
```

Any unknown or legacy conviction status must fall back to the conservative
default instead of raising an exception.

`history_discount`:

```python
history_discount = min(history_days / 252.0, 1.0)
```

## Cap Ownership

Caps must be computed in layers. Do not recalculate all caps in every layer.

```text
Layer 1: static cap
  owner: knowledge / execution policy / target construction input
  formula: min(etf_profile_cap, role_cap)

Layer 2: evidence adjusted cap
  owner: evidence aggregation
  formula: static_cap * evidence_quality_multiplier

Layer 3: runtime cap
  owner: post-risk runtime constraints
  formula: min(evidence_adjusted_cap, liquidity_cap, decay_cap, command_cap)

Layer 4: validation
  owner: final_risk_validation
  responsibility: validate final target against boundaries, not compute them.
```

## Action Thresholds

Increase and reduce should be asymmetric:

```python
VOTING_THRESHOLDS = {
    "increase": {
        "min_voted_count": 2,
        "or_single_conviction_status": [
            "calibrated",
            "statistically_meaningful",
        ],
    },
    "reduce": {
        "min_voted_count": 1,
        "min_confidence": 0.65,
    },
    "hedge": {
        "min_voted_count": 1,
        "requires_regime": [
            "defensive",
            "alert",
            "high_vol",
            "risk_off",
        ],
    },
}
```

These are initial observe-mode thresholds. PR6 may recalibrate them from live
paper evidence.

## DRAM Example

Assume `DRAM` has recent short-term features but lacks long-horizon features.

```text
mean_reversion_lite: voted
sector_theme_relative_value_reversion_lite: voted
momentum_lite_v1: abstain, insufficient_history:mom_252d
low_vol_factor: abstain, insufficient_history:mom_252d
```

If only the first two strategies are eligible for the ETF role in this context:

```text
coverage_ratio = 2 / 4 if all four are role-eligible
coverage_ratio = 2 / 2 if the other two are strategy_universe_mismatch
```

Example using 2/4, early signal status, and 55 history days:

```text
coverage_ratio = 0.50
conviction_discount = 0.45
history_discount = 55 / 252 = 0.218

multiplier = max(0.4*0.50 + 0.4*0.45 + 0.2*0.218, 0.10)
           = 0.424

static_cap = 5.0%
evidence_adjusted_cap = 2.12%
```

This allows small participation without giving a young ETF mature-ETF sizing.

## Enforcement Criteria

Evidence cap enforcement must not be enabled until observe-mode behavior is
reviewed.

Initial criteria:

```python
EVIDENCE_CAP_ENFORCEMENT_CRITERIA = {
    "min_observe_cycles": 10,
    "max_would_clip_rate": 0.30,
    "no_false_positive_degradation": True,
    "young_etf_cap_within_expected_range": True,
}
```

Operational interpretation:

```text
min_observe_cycles
  At least 10 pipeline cycles with evidence cap diagnostics.

max_would_clip_rate
  Fewer than 30% of observed ETF targets would have been clipped.

no_false_positive_degradation
  Mature ETFs with complete data should not be unintentionally downgraded.

young_etf_cap_within_expected_range
  Young ETFs such as DRAM should land in a reasonable small-observation range,
  for example 1%-3% when their short-term evidence is valid.
```

## PR1: EvidenceCard Vote Contract

### Goal

Add vote semantics to EvidenceCard without changing scores, weights, Portfolio
Construction, risk, execution, database writes, or alerts.

### Files

Expected files:

```text
services/strategy_evidence.py
tests/test_strategy_evidence.py
docs/selective_strategy_evidence_cap_development_plan.md
```

### Contract Changes

Add fields to `EvidenceCard`:

```python
vote_status: str = "voted"
abstain_reason: str | None = None
vote_diagnostics: dict[str, Any] = field(default_factory=dict)
```

Do not rename or remove:

```text
action
confidence
max_reasonable_weight
reason
diagnostics
```

Docstring requirement:

```text
action describes what the strategy says.
vote_status describes whether that statement has aggregation voting rights.
```

### Status Mapping

Successful compatibility mapping:

```text
vote_status = voted
abstain_reason = None
```

Normal non-executable observation:

```text
vote_status = watch
```

Cases:

```text
action in watch, avoid, neutral
no_score_threshold_match
action_not_allowed_by_asset_profile
```

Knowledge/configuration problem:

```text
vote_status = mapping_error
```

Cases:

```text
missing_asset_profile
missing_strategy_profile
missing_compatibility_mapping
missing_required_safety_field
unknown_weight_formula
```

PR1 must not generate true `abstain` cards yet. Missing required strategy
features are currently handled by `StrategyInputBuilder`; PR2 will connect that
layer to aggregation.

### Vote Diagnostics

Per-card minimal schema:

```python
{
    "reason_code": str | None,
    "missing_fields": list[str],
    "mapping_role": str | None,
    "requested_action": str | None,
    "allowed_actions": list[str],
    "data_age_days": int | None,
    "history_days": int | None,
    "dedupe_key": str | None,
    "alert_class": str | None,
}
```

Rules:

```text
mapping_error:
  alert_class = knowledge_mapping_error
  dedupe_key = "{strategy}:{ticker}:{reason_code}"

watch:
  alert_class = None
  dedupe_key = None
```

### Summary Changes

Extend `summarize_evidence_cards()` with:

```python
vote_statuses: dict[str, int]
mapping_error_count: int
watch_vote_count: int
abstain_count: int
```

Keep existing keys:

```text
cards_generated
missing_mapping_count
fallback_count
actions
max_weight_by_action
conviction_statuses
```

### Tests

Required tests:

```text
test_voted_card_has_vote_status_voted
test_action_watch_card_has_vote_status_watch
test_missing_compatibility_mapping_is_mapping_error
test_action_not_allowed_is_watch_not_mapping_error
test_summary_counts_vote_statuses
test_existing_action_field_is_not_replaced
```

### Non-Goals

PR1 must not:

```text
change strategy scores
change target weights
change Portfolio Construction
change target_builder
change risk validation
send Telegram alerts
write database state
create abstain cards from StrategyInputBuilder
```

## PR2: ETF-Level Vote Aggregation

### Goal

Create ETF-level vote summaries from strategy EvidenceCards and
StrategyInputBuilder exclusions. Still observe-only.

### Interface

PR2 consumes two inputs. EvidenceCards alone are not enough, because true
`abstain` reasons come from StrategyInputBuilder exclusions:

```python
def aggregate_etf_evidence(
    *,
    evidence_cards: list[EvidenceCard | dict],
    input_builder_exclusions: dict[str, dict[str, list[dict]]],
) -> dict[str, EtfVoteSummary]:
    ...
```

Expected `input_builder_exclusions` shape:

```python
{
    "momentum_lite_v1": {
        "DRAM": [
            {
                "type": "insufficient_history",
                "field": "mom_252d",
            }
        ]
    }
}
```

PR2 may build this structure from existing per-strategy
`StrategyInputBuilder.excluded_tickers` outputs.

### Expected Output

Per ticker:

```python
{
    "ticker": "DRAM",
    "voted_count": 2,
    "watch_count": 1,
    "abstain_count": 2,
    "mapping_error_count": 0,
    "eligible_strategy_count": 4,
    "coverage_ratio": 0.5,
    "supporting_actions": {
        "increase": 1,
        "watch": 1,
    },
    "abstain_reasons": [
        {
            "strategy": "momentum_lite_v1",
            "reason": "insufficient_history",
            "fields": ["mom_252d"],
        }
    ],
}
```

### Rules

```text
abstain never becomes score=0
strategy_universe_mismatch does not count in coverage denominator
mapping_error is counted separately from watch
only voted cards can contribute to actionable score
```

### Non-Goals

```text
no target weight changes
no cap changes
no alerts
no dashboard dependency required yet
```

### Required Tests

```text
test_abstain_is_never_score_zero
test_strategy_universe_mismatch_excluded_from_denominator
test_missing_history_abstain_counts_in_denominator
test_mapping_error_count_separate_from_watch_count
```

## PR3a: Evidence Quality Cap Observe Backend

### Goal

Compute evidence quality multiplier and evidence-adjusted caps, but do not
enforce them.

### Expected Output

Per ticker:

```python
{
    "ticker": "DRAM",
    "static_cap": 0.05,
    "coverage_ratio": 0.5,
    "conviction_status": "early_signal",
    "conviction_discount": 0.45,
    "history_days": 55,
    "history_discount": 0.218,
    "evidence_quality_multiplier": 0.424,
    "evidence_adjusted_cap": 0.0212,
    "would_clip": true,
    "current_or_target_weight": 0.03,
}
```

Canonical pipeline context key:

```python
pipeline_context["evidence_cap_diagnostics"] = {
    "DRAM": {
        "static_cap": 0.05,
        "evidence_adjusted_cap": 0.0212,
        "would_clip": True,
        "would_clip_to": 0.0212,
        "evidence_quality_multiplier": 0.424,
        "coverage_ratio": 0.5,
        "conviction_status": "early_signal",
        "history_days": 55,
    }
}
```

PR4 must consume this schema rather than recomputing evidence caps.

### Rules

```text
use weighted-average formula with min_multiplier
read conviction status from profiles when available
do not enforce cap
write diagnostics to pipeline/risk output only
```

## PR3b: Degradation Visibility

### Goal

Make evidence degradation visible to the operator before enforcement exists.

### Dashboard

Show:

```text
top degraded tickers
voted/watch/abstain/mapping_error counts
coverage_ratio
static_cap
evidence_adjusted_cap
would_clip
main abstain reasons
mapping_error count
```

### Telegram

Keep concise. Only surface meaningful degradation:

```text
Evidence cap observe:
- DRAM cap 5.0% -> 2.1%, voted=2, abstain=2, reason=insufficient_history:mom_252d
```

No Telegram spam for repeated mapping errors in this PR. If needed, only show
top summary lines already included in regular pipeline messages.

### Mapping Error Visibility

PR3b owns operator visibility for `mapping_error`.

Rules:

```text
dedupe_key = "{strategy}:{ticker}:{reason_code}"
dedupe horizon = one trading day
maximum Telegram display = 3 mapping_error rows per pipeline summary
do not send one Telegram message per mapping_error
```

The source data comes from PR1 `vote_diagnostics.alert_class` and
`vote_diagnostics.dedupe_key`.

## PR4: Target Builder Shadow Consumption

### Goal

Let target_builder read evidence cap diagnostics and record `would_apply_cap`.
Do not clip actual targets.

### Input Schema

PR4 reads:

```python
pipeline_context["evidence_cap_diagnostics"]
```

This schema is produced by PR3a. PR4 must not recalculate evidence quality
multiplier or evidence-adjusted caps.

### Expected Diagnostics

```python
{
    "evidence_cap_shadow": {
        "enabled": true,
        "would_apply_count": 3,
        "rows": [...]
    }
}
```

### Non-Goals

```text
no production clipping
no Portfolio Construction changes
no final_risk_validation enforcement
```

## PR5: Gated Evidence Cap Enforcement

### Goal

Allow evidence-adjusted caps to affect target construction only after observe
criteria are met and config explicitly enables enforcement.

### Required Config

```python
evidence_cap_config = {
    "mode": "observe",  # observe | gated | off
    "min_observe_cycles": 10,
    "max_would_clip_rate": 0.30,
    "min_multiplier": 0.10,
}
```

### Enforcement Gate

All enforcement criteria must pass before `mode=gated` is allowed to clip.

If config says gated but criteria fail:

```text
effective_mode = observe
blocked_reason = enforcement_criteria_not_met
```

### Non-Goals

```text
do not bypass execution_policy role caps
do not change final_risk_validation ownership
do not allow evidence cap to increase any cap above static caps
```

## PR6: Calibration

### Goal

Use live paper and historical evidence to tune:

```text
coverage weight
conviction weight
history weight
min_multiplier
increase/reduce/hedge thresholds
young ETF expected cap ranges
```

### Inputs

```text
FrozenSignal / SignalOutcome
historical replay profiles
live conviction profiles
would_clip history
actual/rejected command history
operator review notes
```

### Outputs

```text
calibration report
recommended config
dashboard trend
no automatic config mutation unless explicitly approved
```

## Global Definition Of Done

This work is not complete if any downstream layer treats missing strategy data
as a zero score.

Required properties:

```text
abstain is visible and non-voting
watch is normal and non-alerting
mapping_error is visible as knowledge/config issue
coverage denominator excludes strategy_universe_mismatch
evidence cap is observe-only before enforcement criteria pass
dashboard shows cap degradation before gated enforcement
final_risk_validation validates but does not compute caps
all old EvidenceCard consumers remain backward compatible
```
