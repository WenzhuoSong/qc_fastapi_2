# Agent Evidence and Market Scorecard Plan

## Purpose

The current system has many useful parts: market brief, quant baseline,
playground, news, memory, researcher, bull/bear debate, synthesizer, risk
manager, and position manager. The next improvement should not add more agents.
It should make the existing agents use the same organized evidence and follow
clear action permissions.

Goal:

1. Collect all important data into one structured evidence bundle.
2. Convert evidence into a market condition scorecard.
3. Require every decision agent to reference the scorecard.
4. Let Python risk controls enforce the scorecard before execution.
5. Make Telegram messages explain why an action is reasonable or blocked.

## Current Problem

The system already sees many data sources, but the evidence is spread across
several objects:

- `brief`
- `quant_baseline`
- `playground`
- `research_report`
- `memory_context`
- `sector_rotation`
- `risk_out`

This creates three risks:

1. Agents may over-focus on one data source, such as strategy playground output.
2. Weak data quality may be mentioned in text but not enforced in allocation.
3. Market condition and action permission are not represented as one auditable
   object.

Example:

The Telegram sandbox message says the regime is `trending_bull`, but the
consensus top ETFs include defensive bond ETFs (`IEF`, `TLT`, `BND`) and the
sample size is weak. A good system should translate that into something like:

```json
{
  "market_condition": "bullish_but_mixed",
  "confidence": "medium",
  "data_quality": "limited",
  "investment_permission": "small_overweight_only",
  "max_adjustment_from_base": 0.03
}
```

## Target Architecture

```text
Raw Data
  -> Evidence Bundle
  -> Market Condition Scorecard
  -> Strategy Comparison
  -> Research / Debate / PM Decision
  -> Risk + Position Controls
  -> Telegram / Execution
```

The important change is that `Evidence Bundle` and `Market Condition Scorecard`
become explicit contracts used by downstream stages.

## Evidence Bundle

Create:

```text
services/evidence_bundle.py
```

The evidence bundle should combine the data currently scattered across the
pipeline.

Suggested schema:

```python
{
    "generated_at": "2026-05-15T10:00:00Z",
    "max_age_seconds": 1800,
    "source_timestamps": {
        "qc_snapshot": "2026-05-15T09:59:30Z",
        "daily_feature_snapshot": "2026-05-15T09:45:00Z",
        "macro_news_cache": "2026-05-15T09:30:00Z",
        "ticker_news": "2026-05-15T09:50:00Z"
    },
    "market": {
        "regime": "trending_bull",
        "regime_confidence": "medium",
        "spy_mom_20d": 0.03,
        "spy_mom_60d": 0.07,
        "spy_rsi": 61.2,
        "vix": 18.4,
        "drawdown_pct": 0.02,
        "breadth_pct": 0.65,
        "avg_atr_pct": 0.014,
        "risk_on_score": 0.08
    },
    "rotation": {
        "rotation_label": "mixed_rotation",
        "risk_appetite_score": 0.01,
        "leaders": [],
        "laggards": [],
        "notes": []
    },
    "news": {
        "macro_signals": [],
        "ticker_signals": {},
        "calendar_events": [],
        "data_quality": "fresh",
        "warnings": []
    },
    "strategies": {
        "playground_available": true,
        "snapshot_count": 7,
        "forward_return_samples": 2,
        "consensus_top5": [],
        "strategy_results": [],
        "turnover_warnings": []
    },
    "memory": {
        "recent_regime_trend": "Past days consistently trending_bull",
        "similar_cases": [],
        "warnings": []
    },
    "data_quality": {
        "overall": "limited",
        "warnings": [
            "Only 7 daily snapshots available",
            "Only 2 forward return samples available"
        ]
    }
}
```

Fallback when the latest Playground result is missing:

```python
{
    "strategies": {
        "playground_available": False,
        "snapshot_count": 0,
        "forward_return_samples": 0,
        "consensus_top5": [],
        "strategy_results": [],
        "turnover_warnings": [],
        "data_quality": "missing",
        "warnings": [
            "No recent Playground result available; strategy comparison cannot influence allocation"
        ]
    }
}
```

This matters because Playground is produced by a separate cron job
(`cron/playground_analysis.py`). The main pipeline must not assume Playground
output exists when `market_brief` or the evidence bundle is built.

Rules:

- The bundle should be built in Python.
- It should not make final trading decisions.
- It should preserve raw facts and data-quality warnings.
- It should be saved in `AgentStepLog` for audit.
- It should carry freshness metadata:
  - `generated_at`
  - `max_age_seconds`
  - per-source timestamps where available
- Risk Manager must reject or require human confirmation if the bundle is stale
  at execution time.

## Market Condition Scorecard

Create:

```text
services/market_scorecard.py
```

The scorecard should convert the evidence bundle into a compact, enforceable
market state.

Suggested schema:

```python
{
    "market_condition": "bullish_but_mixed",
    "regime": "trending_bull",
    "confidence": "medium",
    "trend": "positive",
    "volatility": "normal",
    "breadth": "moderate",
    "risk_appetite": "mixed",
    "rotation": "mixed_rotation",
    "macro_risk": "medium",
    "data_quality": "limited",
    "investment_permission": "small_overweight_only",
    "max_adjustment_from_base": 0.03,
    "max_equity_weight": 0.85,
    "min_cash_weight": 0.10,
    "max_turnover_per_cycle": 0.20,
    "allow_new_positions": true,
    "require_human_confirmation": true,
    "reasons": [
        "SPY trend is positive",
        "Regime confidence is only medium",
        "Strategy replay sample is limited",
        "Strategy turnover is high"
    ],
    "warnings": []
}
```

The scorecard should answer:

1. What kind of market are we in?
2. How confident are we?
3. What action level is allowed?
4. What limits should risk manager enforce?
5. What must be shown to the user?

## Scorecard Conflict Resolution

Multiple scorecard rules can fire at the same time. The implementation should
use a deterministic conflict resolver rather than relying on rule order hidden
inside scattered `if` blocks.

Create a helper in `services/market_scorecard.py`:

```python
def resolve_conflicts(triggered_rules: list[dict]) -> dict:
    ...
```

Conflict policy:

1. Use the most conservative intersection of all triggered constraints.
2. For numeric limits:
   - `max_adjustment_from_base`: take the minimum.
   - `max_equity_weight`: take the minimum.
   - `max_turnover_per_cycle`: take the minimum.
   - `max_single_position`: take the minimum.
   - `min_cash_weight`: take the maximum.
3. For boolean permissions:
   - `allow_new_positions`: false wins.
   - `require_human_confirmation`: true wins.
   - `prefer_hedges`: true wins.
4. For `investment_permission`, use this severity order:

```text
cash_only
  > reduce_risk_only
  > defensive_only
  > hold_or_trim
  > small_overweight_only
  > normal_rebalance
  > aggressive_allowed
```

`cash_only` is reserved for extreme cases, such as severe drawdown, unavailable
market data, or volatility shock. It means no non-cash ETF exposure should be
opened or maintained except where manual override explicitly allows it.

Earlier version:

```text
reduce_risk_only
  > defensive_only
  > hold_or_trim
  > small_overweight_only
  > normal_rebalance
  > aggressive_allowed
```

5. Record all triggered rules in `reasons`.
6. Record the most restrictive rule in `dominant_constraint`.

Example output:

```python
{
    "investment_permission": "small_overweight_only",
    "max_adjustment_from_base": 0.03,
    "min_cash_weight": 0.15,
    "max_turnover_per_cycle": 0.20,
    "require_human_confirmation": True,
    "dominant_constraint": "limited_data_quality",
    "triggered_rules": [
        "limited_data_quality",
        "high_volatility",
        "bullish_but_mixed_rotation"
    ]
}
```

## Scorecard Decision Rules

Initial deterministic rules:

### Data Quality

If strategy sample size is weak:

```text
snapshot_count < 20 or forward_return_samples < 10
```

Then:

```text
data_quality = limited
max_adjustment_from_base <= 0.03
require_human_confirmation = true
```

This rule should be evaluated when the evidence bundle is created, not only
when Telegram copy is generated.

### Conflicting Market Signals

If regime is bullish but rotation is defensive or bond-heavy:

```text
regime = trending_bull
and rotation in defensive_rotation / risk_off_rotation
```

Then:

```text
market_condition = bullish_but_mixed
investment_permission = small_overweight_only
max_equity_increase = limited
uncertainty_flag = true
```

### High Volatility

If:

```text
VIX > 30 or SPY ATR > 2.5%
```

Then:

```text
volatility = high
min_cash_weight >= 0.15
max_single_position <= 0.15
prefer_hedges = true
```

### Defensive Portfolio State

If:

```text
current_drawdown_pct > 10%
```

Then:

```text
investment_permission = reduce_risk_only
allow_new_positions = false
min_cash_weight >= 0.20
max_equity_weight <= 0.50
```

Extreme variant:

If:

```text
current_drawdown_pct > 20%
or VIX > 50
or critical market data is unavailable
```

Then:

```text
investment_permission = cash_only
allow_new_positions = false
min_cash_weight = 1.00
max_equity_weight = 0.00
require_human_confirmation = true
```

### High Turnover Strategy Output

If preferred strategy turnover is above 50%:

```text
turnover > 0.50
```

Then:

```text
require_human_confirmation = true
max_turnover_per_cycle <= 0.20
add warning: high turnover may erode returns
```

## Agent Responsibility Updates

### Market Brief

Current role:

- Reads QC snapshot.
- Reads news cache.
- Computes key facts.
- Builds sector rotation.

Update:

- Also builds `evidence_bundle`.
- Also builds `market_scorecard`.
- Adds both to `brief`.

### Quant Baseline

Current role:

- Computes base weights from technical factors.
- Classifies regime.

Update:

- Keep as deterministic math.
- Feed regime result into evidence bundle.
- Do not decide action permission alone.

### Playground

Current role:

- Compares strategies.
- Produces advisory sandbox output.

Update:

- Remain advisory only.
- Add structured fields for:
  - `snapshot_count`
  - `forward_return_samples`
  - `data_quality`
  - `can_influence_allocation`
  - `turnover_risk`
  - `consensus_confidence`
  - `strategy_fit_by_regime`

### Researcher

Current role:

- Synthesizes quant, news, macro, and ticker evidence.
- No weights.

Update:

- Must cite `evidence_bundle` and `market_scorecard`.
- Must explicitly explain:
  - agreement with scorecard
  - disagreement with scorecard, if any
  - data gaps
  - whether strategy evidence is strong enough to influence allocation

### Bull and Bear Researchers

Current role:

- Argue pro-risk and anti-risk cases.

Update:

- Both must use the same scorecard.
- Bull cannot ignore scorecard restrictions.
- Bear must identify which scorecard warnings could invalidate the bullish case.

### Synthesizer / PM

Current role:

- Arbitrates Bull/Bear output.
- Produces adjusted weights.

Update:

- Must obey:
  - `investment_permission`
  - `max_adjustment_from_base`
  - `max_equity_weight`
  - `min_cash_weight`
  - `allow_new_positions`
- Output should include:
  - `scorecard_alignment`
  - `action_permission_used`
  - `data_quality_adjustment`
  - `why_this_trade_is_reasonable`
- Python-side parser validation must require these fields. Prompt-only
  compliance is not enough.
- If Synthesizer proposes weights outside scorecard limits, the proposal should
  be marked `scorecard_non_compliant` before Risk Manager clips it.

### Risk Manager

Current role:

- Applies overlays.
- Runs risk checks.

Update:

- Enforce scorecard limits deterministically.
- Add a final scorecard compliance check:
  - target weights do not exceed permission
  - cash floor satisfies scorecard
  - turnover satisfies scorecard
  - new positions allowed only if scorecard allows them
- Validate evidence freshness before approving execution.
- If clipping is needed, record both:
  - `target_weights_pre_scorecard_clip`
  - `target_weights_post_scorecard_clip`
- Re-normalize after clipping and record any cash/equity redistribution.
- Clipped or excess weight should default to `CASH`, not proportional
  redistribution across equity holdings.
- After cash-first redistribution, run scorecard compliance again. This avoids a
  reduce-risk clip accidentally re-expanding equity exposure during
  normalization.
- If clipping materially changes the proposal, add a Telegram-visible warning.

### Position Manager

Current role:

- Caps trade quantity, frequency, turnover, and min hold.

Update:

- Accept scorecard turnover and trade-size limits as dynamic overrides.

### Communicator

Current role:

- Produces Telegram summary.

Update:

- Always show:
  - market condition
  - scorecard confidence
  - data quality
  - investment permission
  - top reasons
  - final action
  - what was clipped or blocked

Example:

```text
Market: bullish_but_mixed (medium)
Data quality: limited
Permission: small_overweight_only
Reason: SPY trend positive, but bond ETFs lead consensus and replay sample is weak.
Action: small tilt only; no aggressive rebalance.
```

## Implementation Phases

### Phase 1: Contracts Only

Add files:

```text
services/evidence_bundle.py
services/market_scorecard.py
tests/test_market_scorecard.py
```

Acceptance criteria:

- Evidence bundle builds from existing `brief`, `quant_baseline`, and
  playground output.
- Market scorecard returns deterministic permissions.
- Scorecard has a tested `resolve_conflicts()` path.
- Evidence bundle includes `generated_at`, `max_age_seconds`, and source
  freshness metadata.
- Unit tests cover:
  - bullish clean market
  - bullish but mixed rotation
  - high volatility
  - defensive drawdown
  - limited data quality
  - high turnover
  - multiple simultaneous constraints
  - stale evidence bundle

### Phase 2: Pipeline Wiring

Update:

```text
services/pipeline.py
services/market_brief.py
```

Acceptance criteria:

- `brief["evidence_bundle"]` exists.
- `brief["market_scorecard"]` exists.
- Playground-derived data quality is represented in the evidence bundle during
  this phase.
- If no recent Playground result exists, evidence bundle uses
  `playground_available = false`, empty strategy results, and a data-quality
  warning.
- Both are written to `AgentStepLog`.
- Existing behavior remains unchanged if scorecard is unavailable.

### Phase 3: Agent Prompt Integration

Update:

```text
agents/researcher.py
agents/bull_researcher.py
agents/bear_researcher.py
agents/synthesizer.py
```

Acceptance criteria:

- Researcher explains scorecard alignment.
- Synthesizer output includes scorecard compliance fields.
- Synthesizer cannot propose adjustments above scorecard limits.
- Synthesizer parser rejects missing scorecard compliance fields.
- Parser marks overweight proposals as non-compliant when they exceed the
  scorecard, even if the LLM explanation claims compliance.
- Tests cover prompt/parser contract where possible.

### Phase 4: Risk Enforcement

Update:

```text
agents/risk_manager.py
services/position_manager.py
```

Acceptance criteria:

- Risk Manager clips or rejects target weights violating scorecard.
- Position Manager uses scorecard turnover and trade-size overrides.
- Final risk output includes `scorecard_compliance`.
- Final risk output includes pre-clip and post-clip weights when clipping
  occurs.
- Post-clip excess weight is moved to `CASH` first, then the target is checked
  again for scorecard compliance.
- Post-clip weights pass the normal six risk checks.
- Stale evidence is rejected or forced into human confirmation.

### Phase 5: Telegram and Audit

Update:

```text
agents/communicator.py
services/playground.py
cron/playground_analysis.py
```

Acceptance criteria:

- Telegram messages clearly distinguish:
  - research-only sandbox output
  - executable proposal
  - scorecard permission
  - data-quality warnings
- Playground output includes a warning when data is too weak to influence
  allocation.
- Telegram output includes any scorecard clipping and whether the final action
  differs from the LLM proposal.

## Suggested First Pull Request

Scope:

1. Add `services/market_scorecard.py`.
2. Add unit tests for scorecard decision rules.
3. Implement and test `resolve_conflicts()`.
4. Add documentation for the schema.
5. Do not change trading behavior yet.

Reason:

This creates the core contract without risking execution changes.

## Suggested Second Pull Request

Scope:

1. Add `services/evidence_bundle.py`.
2. Build evidence bundle from existing pipeline outputs.
3. Add freshness metadata and stale-evidence classification.
4. Attach evidence bundle and scorecard to `brief`.
5. Save both to step logs.

Reason:

This makes the data visible and auditable before agents start relying on it.

## Suggested Third Pull Request

Scope:

1. Inject scorecard into Researcher and Synthesizer.
2. Require Synthesizer to explain scorecard compliance.
3. Add Python-side Synthesizer schema validation for scorecard fields.
4. Keep Risk Manager as the final enforcement layer.

Reason:

Agents can start making better use of data while Python still protects
execution.

## Success Criteria

The update is successful when every executable recommendation can answer:

1. What market condition did the system detect?
2. What data supports that condition?
3. How reliable is the data?
4. What action level was allowed?
5. Did the final target weights obey that permission?
6. What was blocked or clipped by risk controls?

## Non-Goals

Do not add a new LLM agent unless a specific gap remains after the scorecard is
in place.

Do not let Playground directly control allocation. It should influence only
through evidence quality and strategy-fit fields.

Do not loosen execution controls. The scorecard should make execution more
auditable and more conservative when data quality is weak.
