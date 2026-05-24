# Strategy EvidenceCard Development Plan

## Goal

This plan extends the existing trading knowledge base so strategy-specific ETF
logic can be consumed by downstream layers through one stable evidence contract.

The core design is:

```text
Strategy.score()
  -> ScoredTicker raw output
  -> ETF profile + strategy profile + compatibility mapping
  -> EvidenceCard
  -> Playground observability
  -> later portfolio construction shadow mode
```

The first delivery only solves semantic normalization. It must not change
production target weights, execution commands, or main trading authority.

## Existing Foundation

The system already has the right foundation:

- `knowledge/assets/*.yaml` for ETF and asset profiles.
- `knowledge/strategies/*.yaml` for strategy profiles.
- `services/knowledge_base.py` for loading compact static knowledge.
- `services/knowledge_resolver.py` for merging knowledge with computed facts.
- `services/evidence_bundle.py` for attaching resolved knowledge to the factual
  evidence bundle.
- `strategies/base.py` for strategy metadata and `ScoredTicker` output.
- `services/universe_policy.py` for separating ordinary research tickers from
  leveraged, inverse, and hedge-like products.

The missing layer is:

```text
Strategy output -> ETF-specific meaning -> EvidenceCard
```

For example, `TQQQ score=1.0` and `UVXY score=1.0` must not mean the same
thing. `TQQQ` can be a risk-on amplifier signal, while `UVXY` is a hedge or
de-risk signal with a much lower permissible weight.

## Non-Goals For PR1-4

- Do not change target weights.
- Do not make playground evidence executable.
- Do not let LLM-generated text override deterministic validators.
- Do not make missing safety fields silently tradable.
- Do not replace `ScoredTicker`; add `EvidenceCard` beside it.
- Do not promote leveraged, inverse, or volatility products to default ordinary
  strategy scoring.

## EvidenceCard Contract

`EvidenceCard` is the normalized semantic output consumed by downstream layers.

```python
@dataclass
class EvidenceCard:
    ticker: str
    strategy: str
    strategy_version: str
    role: str
    action: str
    signal_type: str
    horizon: str
    confidence: float
    conviction: float
    raw_score: float | None
    normalized_score: float
    max_reasonable_weight: float
    risk_budget_cost: float
    branch: str | None
    reason: str
    diagnostics: dict
```

Field semantics:

- `confidence`: current signal clarity for this run, usually derived from raw
  score, threshold band, and data readiness. It changes every run.
- `conviction`: historical reliability for this strategy + ticker + branch.
  PR1-6 must default it to `0.0`. PR7 may fill it from replay and live
  observation data.
- `raw_score`: original strategy score, without semantic interpretation.
- `normalized_score`: score after threshold mapping, bounded to `0.0-1.0`.
- `max_reasonable_weight`: semantic cap for this ETF, mode, and mapping. This
  is not a target weight.
- `risk_budget_cost`: approximate risk budget cost from ETF profile.
- `branch`: strategy branch if available. Source priority:
  1. `ScoredTicker.raw_factors["branch"]`
  2. compatibility mapping `branch_label_template`
  3. `None`

Allowed `action` values:

```text
increase
reduce
hold
watch
avoid
hedge
de_risk
neutral
```

Downstream layers must not rank ETFs only by `raw_score`. They should consume
`action`, `role`, `signal_type`, `confidence`, `conviction`,
`max_reasonable_weight`, and `risk_budget_cost`.

## Safety Field Rules

Static knowledge can remain loadable when older YAML files are incomplete, but
EvidenceCard generation must treat safety fields as strict requirements for any
ETF that appears in a strategy evidence card.

Required safety fields:

```text
allowed_actions
max_reasonable_weight
risk_budget_cost
decay_risk
```

Optional descriptive fields:

```text
underlying
leverage
inverse
issuer
expense_ratio
```

Implementation rule:

```text
load_knowledge_base()
  missing optional field -> warning
  missing safety field -> warning only, to preserve old knowledge loading

build_evidence_cards()
  missing safety field on involved ETF -> strict fallback:
    action = watch
    max_reasonable_weight = 0.0
    confidence <= 0.25
    reason includes missing_required_safety_field
```

If a caller explicitly requests strict mode, missing safety fields may raise a
`ValueError`. Production-facing consumers should use strict fallback unless a
test is specifically validating the exception path.

## Formula Rules

Compatibility mappings may reference weight formulas, but YAML must not contain
arbitrary executable expressions.

Allowed pattern:

```yaml
weight_formula: confidence_cap_multiplier
```

Python owns the whitelist:

```python
FORMULAS = {
    "zero": _zero_weight,
    "cap_only": _cap_only,
    "confidence_cap_multiplier": _confidence_cap_multiplier,
}
```

Unknown formula IDs must fall back to `zero` and record
`unknown_weight_formula` in diagnostics.

## PR1: Knowledge Cards And Safety Schema

Add ETF profiles for the leveraged allocator universe:

- `TQQQ`
- `SPXL`
- `TECL`
- `SQQQ`
- `TECS`
- `UVXY`
- `BSV`

Add a strategy profile:

- `knowledge/strategies/leveraged_etf_momentum_allocator.yaml`

ETF profile example:

```yaml
id: TQQQ
type: asset
asset_class: leveraged_etf
role: leveraged_long
sector_group: tech_growth
summary: 3x leveraged Nasdaq-100 ETF with daily reset compounding risk.
underlying: QQQ
leverage: 3
inverse: false
decay_risk: high
allowed_actions: [increase, reduce, hold, watch]
max_reasonable_weight:
  playground: 1.0
  semi_auto: 0.08
  full_auto: 0.05
risk_budget_cost: 0.90
intended_uses: [risk_on_amplifier, tactical_exposure]
positive_regimes: [trending_bull, risk_on]
weak_regimes: [high_vol, mean_reverting, risk_off]
holding_policy:
  allow_long_hold: false
  max_holding_style: short_term_only
governance_notes:
  - require explicit strategy support
  - cap exposure before any production use
  - monitor daily reset compounding and gap risk
sources: [issuer_fact_sheet, leveraged_etf_policy]
last_reviewed: 2026-05-24
```

Acceptance criteria:

- New ETFs no longer appear as missing knowledge when selected by the strategy.
- Safety fields are available for every ETF in the leveraged allocator universe.
- Existing knowledge loading remains backward compatible.
- Tests cover missing safety field fallback and strict-mode exception behavior.

## PR2: Strategy-ETF Compatibility Mapping

Add compatibility mappings to the strategy knowledge card. The mapping defines
how raw score thresholds translate into actions for each ETF role.

Example:

```yaml
compatibility_mappings:
  - role: leveraged_long
    horizon: short_tactical
    score_thresholds:
      - gte: 0.70
        action: increase
        signal_type: risk_on_amplifier
      - gte: 0.40
        action: watch
        signal_type: risk_on_monitor
      - lt: 0.40
        action: neutral
        signal_type: no_signal
    max_weight_multiplier: 1.0
    weight_formula: confidence_cap_multiplier

  - role: vol_hedge
    horizon: very_short_tactical
    score_thresholds:
      - gte: 0.70
        action: hedge
        signal_type: tail_risk_hedge
      - gte: 0.40
        action: watch
        signal_type: tail_risk_monitor
      - lt: 0.40
        action: avoid
        signal_type: no_hedge_signal
    max_weight_multiplier: 0.25
    weight_formula: confidence_cap_multiplier

  - role: defensive_cash_proxy
    horizon: short_tactical
    score_thresholds:
      - gte: 0.70
        action: de_risk
        signal_type: defensive_fallback
      - gte: 0.40
        action: watch
        signal_type: defensive_monitor
      - lt: 0.40
        action: neutral
        signal_type: no_signal
    max_weight_multiplier: 1.0
    weight_formula: confidence_cap_multiplier
```

Threshold rules:

- Threshold evaluation is owned by `build_evidence_cards()`, not downstream
  callers.
- `gte` boundaries are inclusive.
- `lt` boundaries are exclusive.
- If multiple thresholds match, choose the first match in YAML order and record
  the threshold in diagnostics.
- If no threshold matches, fallback to `watch` and `max_reasonable_weight=0.0`.

Acceptance criteria:

- `TQQQ score=1.0` becomes `increase / risk_on_amplifier`.
- `UVXY score=1.0` becomes `hedge / tail_risk_hedge`.
- `BSV score=1.0` becomes `de_risk / defensive_fallback`.
- Same raw score can produce different actions and caps for different ETF roles.

## PR3: EvidenceCard Normalizer

Add:

```text
services/strategy_evidence.py
tests/test_strategy_evidence.py
```

Public entry point:

```python
def build_evidence_cards(
    *,
    strategy: Strategy,
    scored: list[ScoredTicker],
    knowledge_context: dict,
    mode: str = "playground",
    strict: bool = False,
) -> list[EvidenceCard]:
    ...
```

Fallback behavior:

- Missing asset profile:
  - action `watch`
  - max weight `0.0`
  - reason includes `missing_asset_profile`
- Missing strategy profile:
  - action `watch`
  - max weight `0.0`
  - reason includes `missing_strategy_profile`
- Missing compatibility mapping:
  - action `watch`
  - max weight `0.0`
  - reason includes `missing_compatibility_mapping`
- Mapping action not allowed by asset `allowed_actions`:
  - action `watch`
  - max weight `0.0`
  - reason includes `action_not_allowed_by_asset_profile`
- Missing safety field:
  - action `watch`
  - max weight `0.0`
  - reason includes `missing_required_safety_field`
- Unknown weight formula:
  - use zero formula
  - diagnostics include `unknown_weight_formula`

Weight cap calculation:

```text
asset max_reasonable_weight[mode]
  * compatibility max_weight_multiplier
  * formula output
```

This value is evidence metadata only. It does not authorize execution.

Acceptance criteria:

- EvidenceCard generation is deterministic.
- Confidence is bounded `0.0-1.0`.
- Conviction is `0.0` until historical calibration exists.
- Tests cover threshold edges, missing mappings, missing safety fields, and mode
  specific caps.

## PR4: Playground Output Integration

Modify playground strategy results to include EvidenceCard output beside
existing results.

Required shape:

```json
{
  "strategy_name": "leveraged_etf_momentum_allocator",
  "scored_tickers": [],
  "selected_tickers": ["UVXY"],
  "evidence_contract_version": "v1",
  "evidence_cards": [],
  "evidence_summary": {
    "cards_generated": 0,
    "missing_mapping_count": 0,
    "fallback_count": 0,
    "actions": {}
  }
}
```

Compatibility requirements:

- Existing `strategy_results` fields remain unchanged.
- `ScoredTicker` based consensus remains unchanged.
- Telegram and dashboard may display compact evidence summaries, but must not use
  evidence cards to change allocation.
- Old playground results without `evidence_contract_version` must remain readable.

Acceptance criteria:

- Playground can run all existing strategies unchanged.
- The leveraged allocator emits branch-aware evidence cards.
- Evidence summaries show fallback counts and action distribution.
- No production weight or execution behavior changes.

## Required Tests

Core tests for PR1-4:

```python
def test_tqqq_score_1_translates_to_increase():
    cards = build_evidence_cards(strategy, scored(TQQQ=1.0), kb, mode="semi_auto")
    assert cards[0].action == "increase"
    assert cards[0].signal_type == "risk_on_amplifier"


def test_uvxy_score_1_translates_to_hedge():
    cards = build_evidence_cards(strategy, scored(UVXY=1.0), kb, mode="semi_auto")
    assert cards[0].action == "hedge"
    assert cards[0].max_reasonable_weight == 0.03


def test_missing_compatibility_mapping_fallback():
    cards = build_evidence_cards(unknown_strategy, scored(SPY=0.8), kb)
    assert cards[0].action == "watch"
    assert cards[0].max_reasonable_weight == 0.0
    assert "missing_compatibility_mapping" in cards[0].reason


def test_tqqq_and_uvxy_same_score_different_weight():
    cards = build_evidence_cards(strategy, scored(TQQQ=0.8, UVXY=0.8), kb)
    tqqq = next(c for c in cards if c.ticker == "TQQQ")
    uvxy = next(c for c in cards if c.ticker == "UVXY")
    assert tqqq.max_reasonable_weight > uvxy.max_reasonable_weight


def test_score_equals_threshold_uses_gte_match():
    cards = build_evidence_cards(strategy, scored(TQQQ=0.70), kb)
    assert cards[0].action == "increase"


def test_action_not_allowed_by_asset_profile_falls_back_to_watch():
    cards = build_evidence_cards(strategy, scored(UVXY=1.0), kb_without_hedge_allowed)
    assert cards[0].action == "watch"
    assert cards[0].max_reasonable_weight == 0.0
    assert "action_not_allowed_by_asset_profile" in cards[0].reason


def test_full_auto_uvxy_cap_is_zero_or_lower_than_semi_auto():
    semi = build_evidence_cards(strategy, scored(UVXY=1.0), kb, mode="semi_auto")[0]
    full = build_evidence_cards(strategy, scored(UVXY=1.0), kb, mode="full_auto")[0]
    assert full.max_reasonable_weight <= semi.max_reasonable_weight
```

Strict-mode safety test:

```python
def test_safety_fields_missing_raises_in_strict_mode():
    with pytest.raises(ValueError, match="missing required safety field"):
        build_evidence_cards(
            strategy=strategy,
            scored=scored("NEWETF", 1.0),
            knowledge_context=bad_kb,
            strict=True,
        )
```

## Later PRs

PR5: Evidence Aggregator

- Aggregate EvidenceCards into ticker-level and role-level summaries.
- Show support, conflict, action distribution, and suggested tactical bias.
- Do not create target weights.

PR6: Portfolio Construction Shadow Integration

- Consume aggregator output in observe mode only.
- Produce candidate diagnostics beside current target builder.
- Do not change production target weights.

PR7: Historical Conviction Calibration

- Compute conviction from playground replay and live forward samples.
- Calibrate by strategy + ticker + branch + regime.
- Keep conviction data-generated, not LLM-written.

## Final Guardrail

PR1-4 are complete only when the system can explain:

```text
UVXY score=1.0 means hedge or de-risk evidence with strict cap.
TQQQ score=1.0 means risk-on amplifier evidence with a different cap.
BSV score=1.0 means defensive fallback evidence.
```

They are not complete if downstream code still has to guess what raw strategy
scores mean.
