# News and Decision Style Upgrade Plan

## Purpose

The evidence bundle and market scorecard now give the system a stronger
decision contract. The next upgrade should improve two things:

1. Use news more effectively without letting news directly force trades.
2. Add explicit analysis and trade styles so agent output is more consistent,
   auditable, and enforceable.

The goal is not to make the LLM "smarter by itself." The goal is to structure
the inputs and limits so the LLM can reason better while Python remains the
source of enforcement.

## Core Principle

```text
News tells us what may be changing.
Quant tells us what is already happening.
Scorecard decides how aggressive we are allowed to be.
Decision style decides how to express the trade.
Risk Manager enforces the final limits.
```

LLM responsibilities:

- interpret structured evidence
- compare explanations
- identify conflicts
- explain why a style fits the market
- produce a readable rationale

Python responsibilities:

- score news quality
- assign action permissions
- enforce style limits
- enforce risk limits
- block stale or low-quality evidence
- clip final weights

## Current Gaps

### 1. News Is Useful But Not Fully Ranked

The system already reads macro news, per-ticker news, RSS, Alpha Vantage, and
Finnhub. However, downstream agents need a more explicit ranking of:

- source credibility
- relevance to ETF universe
- freshness
- market impact
- time horizon
- confidence
- action bias

Without this, an LLM may overreact to a headline that is stale, indirect, or
low-impact.

### 2. Analysis Style Is Implicit

Sometimes the system behaves conservatively because the prompt says to be
cautious. Sometimes it leans into momentum because the regime is bullish. These
styles should become explicit and configurable.

### 3. Trade Style Is Not Separated From Analysis

The system can conclude that an ETF is attractive, but it also needs to decide
how to express that view:

- enter immediately
- step in slowly
- hold unless evidence is strong
- reduce risk quickly
- avoid turnover

This should be a deterministic trade-style layer, not just LLM wording.

## Target Architecture

```text
Raw News + Market Data
  -> News Evidence Scoring
  -> Evidence Bundle
  -> Market Scorecard
  -> Decision Style Resolver
  -> Researcher / Bull / Bear / Synthesizer
  -> Risk Manager
  -> Position Manager
  -> Telegram / Execution
```

## News Evidence Scoring

Create:

```text
services/news_evidence.py
```

This service should convert raw/structured news into a ranked, agent-ready
contract.

Suggested schema:

```python
{
    "macro_news_score": {
        "overall_bias": "positive" | "neutral" | "negative" | "mixed",
        "confidence": "high" | "medium" | "low",
        "dominant_themes": ["fed_hawkish", "ai_capex", "credit_stress"],
        "market_impact": "high" | "medium" | "low",
        "time_horizon": "intraday" | "short_term" | "medium_term",
        "data_quality": "fresh" | "stale" | "limited" | "missing",
        "warnings": []
    },
    "ticker_news_scores": {
        "XLK": {
            "bias": "positive",
            "confidence": "medium",
            "relevance": "direct",
            "source_credibility": 0.90,
            "freshness": "fresh",
            "market_impact": "medium",
            "time_horizon": "short_term",
            "action_bias": "allow_overweight" | "reduce_or_wait" | "ignore",
            "supporting_items": [],
            "conflicting_items": []
        }
    },
    "hard_risk_events": {
        "XLF": ["bank_crisis", "credit_stress"]
    },
    "data_gaps": []
}
```

## News Scoring Rules

### Source Credibility

Initial source scores:

```text
Reuters / Bloomberg / official releases: 0.95
CNBC / WSJ / MarketWatch / Financial Times: 0.85
Yahoo / Seeking Alpha / analyst blogs: 0.65
Unknown source: 0.40
```

Source credibility is only the source-quality prior. It should not be used
alone. Each item should also produce:

```python
effective_credibility = source_credibility * impact_multiplier * relevance_multiplier * freshness_multiplier
```

Suggested multipliers:

```text
impact_multiplier:
  high   = 1.20
  medium = 1.00
  low    = 0.70

relevance_multiplier:
  direct   = 1.20
  sector   = 1.00
  macro    = 1.00
  indirect = 0.70
  noise    = 0.00

freshness_multiplier:
  fresh               = 1.10
  usable              = 1.00
  stale_for_trading   = 0.60
  stale               = 0.20
```

This handles the difference between, for example, a Reuters market recap and a
Reuters report about a major Fed surprise. The same source can have different
effective credibility depending on impact, relevance, and freshness.

### Freshness

```text
< 6 hours      -> fresh
6-24 hours    -> usable
1-3 days      -> stale_for_trading, usable for context
> 3 days      -> stale
```

### Relevance

```text
direct     -> article directly concerns ticker, ETF, sector, macro event
sector     -> related sector/theme
macro      -> broad market driver
indirect   -> weak relationship
noise      -> should not influence allocation
```

### Market Impact

```text
high:
  Fed surprise, CPI shock, credit stress, war escalation, major earnings shock

medium:
  sector leadership, policy comments, important company/sector developments

low:
  generic commentary, repeated analysis, price-target article, recap
```

### Action Bias

News should not create target weights directly. It should create one of:

```text
allow_overweight
confirm_existing_signal
reduce_or_wait
block_new_buy
ignore
```

Example:

```text
positive quant + positive high-quality news -> confirm_existing_signal
positive quant + negative macro shock       -> reduce_or_wait
weak quant + noisy positive news            -> ignore
hard risk event                             -> block_new_buy
```

Noise filtering:

```python
MIN_EFFECTIVE_CREDIBILITY_FOR_AGENT = 0.10
```

If an item has:

```text
relevance = noise
or effective_credibility < 0.10
```

Then:

```text
action_bias = ignore
```

and the item should be excluded from agent-visible `supporting_items`.

It may still be retained in an audit/debug field:

```python
"ignored_items": [...]
```

This prevents low-quality or noisy news from influencing the LLM simply because
it appears in the prompt.

Hard-risk override:

If a news item is classified as a hard risk event, such as:

```text
bank_crisis
credit_stress
trading_halt
fraud
lawsuit_material
liquidity_crisis
sanctions
war_escalation
```

Then:

```text
action_bias = block_new_buy
```

This override is deterministic and cannot be relaxed by LLM or decision style.
Risk Manager remains the final enforcement layer through hard-risk and
scorecard constraints.

## Decision Style Layer

Create:

```text
services/decision_style.py
```

This service should resolve analysis style and trade style from:

- market scorecard
- news evidence score
- strategy evidence
- volatility
- data quality
- user/system config

Suggested schema:

```python
{
    "analysis_style": "balanced",
    "trade_style": "step_in",
    "style_reason": "Bullish trend but limited strategy sample and mixed rotation",
    "style_limits": {
        "max_adjustment_multiplier": 0.8,
        "max_turnover_per_cycle": 0.15,
        "max_single_trade_pct": 0.04,
        "max_new_buys_per_cycle": 2,
        "min_cash_floor_addition": 0.03,
        "rebalance_threshold_boost": 0.01
    },
    "triggered_style_rules": [
        "limited_data_quality",
        "mixed_rotation",
        "high_turnover_strategy"
    ]
}
```

## Analysis Styles

Analysis style weights must have a concrete operational meaning. They are not
free-form prompt decoration.

Use these fields in two ways:

1. Deterministic evidence weighting in `decision_style.py`.
2. Prompt instruction text generated from the deterministic style result.

Suggested formula:

```python
weighted_conviction =
    quant_score * quant_weight
    + news_score * news_weight
    + macro_score * macro_weight
    - risk_penalty * risk_weight
```

Where:

```text
quant_score   = normalized quant/strategy agreement in [-1, +1]
news_score    = news action bias score in [-1, +1]
macro_score   = macro news score in [-1, +1]
risk_penalty  = volatility/drawdown/data-quality penalty in [0, +1]
```

The weighted conviction should not write target weights directly. It should
influence:

- selected analysis style
- confidence
- allowed max adjustment multiplier
- whether evidence is strong enough for `normal_rebalance`

Initial mapping:

```python
CONVICTION_THRESHOLDS = {
    "normal_rebalance":    0.60,
    "step_in":             0.30,
    "hold_unless_strong":  0.10,
    "risk_reduce_fast":   -0.20,
    "cash_only":          -0.50,
}
```

Interpretation:

```text
weighted_conviction >= 0.60:
  evidence is strong enough for normal_rebalance, if scorecard allows it

0.30 <= weighted_conviction < 0.60:
  constructive but not decisive; prefer step_in

0.10 <= weighted_conviction < 0.30:
  weak edge; hold_unless_strong

-0.20 <= weighted_conviction < 0.10:
  no positive edge; hold or trim

-0.50 <= weighted_conviction < -0.20:
  negative edge; risk_reduce_fast

weighted_conviction < -0.50:
  severe negative evidence; cash_only candidate, subject to scorecard/risk confirmation
```

Create a helper:

```python
def conviction_to_style(weighted_conviction: float, scorecard: dict) -> dict:
    ...
```

Scorecard still has priority. For example, if conviction is `0.75` but
scorecard says `small_overweight_only`, the final trade style cannot exceed
`step_in` or the scorecard's stricter permission.

If a weight is used only for LLM wording, name it `prompt_emphasis`, not
`news_weight` or `quant_weight`.

### balanced

Default mode.

```python
{
    "news_weight": 1.0,
    "quant_weight": 1.0,
    "macro_weight": 1.0,
    "max_adjustment_multiplier": 1.0,
    "turnover_tolerance": "normal"
}
```

Use when:

- data quality is fresh
- market scorecard allows normal rebalance
- news and quant are broadly aligned

### conservative

Capital protection first.

```python
{
    "news_weight": 1.2,
    "risk_weight": 1.4,
    "max_adjustment_multiplier": 0.6,
    "min_cash_floor_addition": 0.05,
    "turnover_tolerance": "low"
}
```

Use when:

- data quality is limited
- regime confidence is low or medium
- rotation conflicts with regime
- recent performance/memory is unstable

### momentum_confirmed

Trend-following style, but only when confirmation is strong.

```python
{
    "quant_weight": 1.3,
    "news_weight": 0.8,
    "requires_breadth_confirmation": true,
    "max_adjustment_multiplier": 1.1
}
```

Use when:

- regime is trending bull
- breadth is broad
- risk appetite is risk-on
- news does not contradict the trend
- strategy evidence has enough samples

### macro_defensive

Macro risk dominates ETF selection.

```python
{
    "macro_weight": 1.5,
    "news_weight": 1.4,
    "max_adjustment_multiplier": 0.7,
    "prefer_hedges": true,
    "min_cash_floor_addition": 0.08
}
```

Use when:

- high-volatility scorecard
- Fed/rates shock
- credit stress
- recession risk
- geopolitical shock

### low_turnover

Avoid frequent changes.

```python
{
    "max_turnover_per_cycle": 0.10,
    "rebalance_threshold_boost": 0.02,
    "max_adjustment_multiplier": 0.7
}
```

Use when:

- strategy turnover is high
- confidence is medium or low
- current positions are not clearly wrong

## Trade Styles

### normal_rebalance

Use target weights normally within scorecard and risk constraints.

### step_in

Enter risk gradually.

```python
{
    "max_single_trade_pct": 0.04,
    "max_new_buys_per_cycle": 2,
    "max_turnover_per_cycle": 0.15
}
```

Use when:

- setup is attractive but confidence is not high
- data quality is limited
- news is supportive but not decisive

### risk_reduce_fast

Sell risk faster than buying risk.

```python
{
    "sell_priority": true,
    "max_sell_trade_pct": 0.15,
    "max_buy_trade_pct": 0.03,
    "allow_new_positions": false
}
```

Use when:

- scorecard says reduce_risk_only
- volatility or drawdown triggers defensive mode
- macro shock appears

### hold_unless_strong

Trade only if evidence is strong.

```python
{
    "rebalance_threshold": 0.04,
    "max_adjustment_multiplier": 0.5,
    "max_turnover_per_cycle": 0.10
}
```

Use when:

- evidence is mixed
- strategy disagreement is high
- news is noisy

### cash_only

No non-cash exposure.

Use when:

- market scorecard says `cash_only`
- critical data is missing
- extreme drawdown or volatility shock

## Style Conflict Resolution

Multiple style rules can trigger together. Example:

```text
trending_bull             -> momentum_confirmed
limited data quality      -> conservative
high strategy turnover    -> low_turnover
mixed rotation            -> step_in
```

Create a helper in `services/decision_style.py`:

```python
def resolve_style_conflicts(triggered_styles: list[dict]) -> dict:
    ...
```

Conflict policy:

1. Numeric limits take the most conservative intersection:
   - `max_adjustment_multiplier`: minimum
   - `max_turnover_per_cycle`: minimum
   - `max_single_trade_pct`: minimum
   - `max_new_buys_per_cycle`: minimum
   - `rebalance_threshold_boost`: maximum
   - `min_cash_floor_addition`: maximum
2. Boolean restrictions:
   - `allow_new_positions`: false wins
   - `prefer_hedges`: true wins
   - `sell_priority`: true wins
3. `analysis_style` should record the dominant interpretive style by severity:

```text
macro_defensive
  > conservative
  > low_turnover
  > momentum_confirmed
  > balanced
```

4. `trade_style` should record the dominant execution style by severity:

```text
cash_only
  > risk_reduce_fast
  > hold_unless_strong
  > step_in
  > normal_rebalance
```

5. Preserve all triggered styles in `triggered_style_rules`.
6. Record `dominant_style_constraint`.

Example output:

```python
{
    "analysis_style": "conservative",
    "trade_style": "step_in",
    "style_limits": {
        "max_adjustment_multiplier": 0.6,
        "max_turnover_per_cycle": 0.10,
        "max_single_trade_pct": 0.04,
        "max_new_buys_per_cycle": 2,
        "min_cash_floor_addition": 0.05,
        "rebalance_threshold_boost": 0.02
    },
    "dominant_style_constraint": "limited_data_quality",
    "triggered_style_rules": [
        "trending_bull",
        "limited_data_quality",
        "high_turnover_strategy"
    ]
}
```

## Limit Merge Order

The system will have three tightening layers:

```text
Scorecard limits
  -> Style limits
  -> Risk Manager limits
```

The invariant:

```text
Each layer may only tighten limits. No layer may loosen a previous layer.
```

Implementation shape:

```python
base_limits = scorecard_limits
style_adjusted_limits = apply_style_limits(base_limits, decision_style)
final_limits = apply_risk_limits(style_adjusted_limits, risk_params)
```

Examples:

```text
scorecard max_turnover = 0.20
style max_turnover     = 0.10
final max_turnover     = 0.10

scorecard min_cash     = 0.15
style min_cash_floor_addition   = 0.05
final min_cash         = 0.20

scorecard allow_new_positions = false
style allow_new_positions     = true
final allow_new_positions     = false
```

Risk Manager should assert this invariant when style enforcement is added.

Cash-floor semantics:

`min_cash_weight` is an absolute floor from the scorecard.

`min_cash_floor_addition` is an additive style adjustment.

Implementation:

```python
final_min_cash = base_min_cash + min_cash_floor_addition
```

Example:

```text
scorecard min_cash_weight = 0.22
style min_cash_floor_addition = 0.05
final min_cash_weight = 0.27
```

Cap final cash floor at `1.00`.

## How Styles Affect Agents

### Researcher

Researcher should see:

- news evidence score
- selected analysis style
- why the style was selected

Researcher should explain whether evidence supports or contradicts the style.

### Bull / Bear

Bull must argue within style limits. For example, under `conservative`, Bull can
argue for selective overweight but should not advocate aggressive risk-on.

Bear should identify which style warnings invalidate bullish interpretation.

### Synthesizer

Synthesizer must output:

```python
{
    "style_compliance": {
        "analysis_style_used": "conservative",
        "trade_style_used": "step_in",
        "style_alignment": "aligned",
        "style_adjustments": [
            "reduced max adjustment due to limited data",
            "used step-in trade style due to mixed rotation"
        ]
    }
}
```

Python parser should require this field after the style layer is introduced.

### Risk Manager

Risk Manager should enforce style limits:

- max adjustment multiplier
- max equity
- min cash boost
- max turnover
- max single trade
- allow/block new positions

### Position Manager

Position Manager should receive style overrides:

```python
{
    "max_single_trade_pct": 0.04,
    "max_new_buys_per_cycle": 2,
    "max_turnover_per_cycle": 0.15
}
```

This makes `step_in`, `low_turnover`, and `risk_reduce_fast` enforceable.

## Config

Add system config:

```python
"decision_style_config": {
    "default_analysis_style": "balanced",
    "default_trade_style": "normal_rebalance",
    "allow_auto_style_switch": true,
    "style_overrides": {},
    "news_scoring_enabled": true
}
```

Optional user override examples:

```python
{
    "force_analysis_style": "conservative",
    "force_trade_style": "step_in"
}
```

## Implementation Phases

### Phase 1: News Evidence Scoring

Add:

```text
services/news_evidence.py
tests/test_news_evidence.py
```

Acceptance criteria:

- Scores source credibility, freshness, relevance, impact, and action bias.
- Produces `effective_credibility`.
- Filters `effective_credibility < 0.10` and `noise` relevance out of
  agent-visible supporting items.
- Hard-risk events deterministically override `action_bias` to `block_new_buy`.
- Produces macro and ticker news scores.
- Handles missing or stale news.
- Does not create target weights.
- Does not modify pipeline behavior yet.

### Phase 2: Decision Style Resolver

Add:

```text
services/decision_style.py
tests/test_decision_style.py
```

Acceptance criteria:

- Selects analysis style and trade style from scorecard/news/strategy evidence.
- Handles forced config overrides.
- Produces `style_limits`.
- Implements `resolve_style_conflicts()`.
- Defines operational evidence weighting formula for
  `news_weight`, `quant_weight`, `macro_weight`, and `risk_weight`.
- Implements `conviction_to_style()` using explicit conviction thresholds.
- Uses `min_cash_floor_addition` as an additive style limit, not an absolute
  replacement for scorecard `min_cash_weight`.
- Uses conservative conflict resolution when multiple style rules trigger.
- Does not modify pipeline behavior yet.

### Phase 3: Pipeline Wiring

Update:

```text
services/pipeline.py
services/evidence_bundle.py
services/market_scorecard.py
```

Acceptance criteria:

- Evidence bundle includes news evidence.
- Pipeline context includes `decision_style`.
- Stage logs save news evidence and decision style.
- Existing behavior remains safe if style resolver fails.

### Phase 4: Agent Prompt Integration

Update:

```text
agents/researcher.py
agents/bull_researcher.py
agents/bear_researcher.py
agents/synthesizer.py
```

Acceptance criteria:

- Agents receive news evidence and decision style.
- Synthesizer must output `style_compliance`.
- Parser validates required style compliance fields.
- LLM cannot bypass style limits.

### Phase 5: Risk and Position Enforcement

Update:

```text
agents/risk_manager.py
services/position_manager.py
```

Acceptance criteria:

- Risk Manager enforces analysis style limits.
- Position Manager enforces trade style limits.
- Final risk output includes `style_enforcement`.
- Risk Manager asserts the one-way tightening invariant:
  `scorecard -> style -> risk`.
- Telegram shows style clipping/blocking.

### Phase 6: Telegram and Reports

Update:

```text
agents/communicator.py
cron/daily_analyst.py
cron/weekly_analyst.py
```

Acceptance criteria:

- Telegram shows:
  - news bias
  - news confidence
  - analysis style
  - trade style
  - style reason
  - style enforcement
- Daily/weekly reports can review whether styles worked.

## Recommended First PR

Start with News Evidence Scoring only.

Scope:

1. Add `services/news_evidence.py`.
2. Add tests for source/freshness/relevance/action-bias rules.
3. Attach `news_evidence` to the evidence bundle.
4. Do not change trading behavior yet.

Reason:

News is the largest uncontrolled interpretation layer. Structuring it first
will improve Researcher quality without touching execution.

## Recommended Second PR

Add Decision Style Resolver.

Scope:

1. Add `services/decision_style.py`.
2. Add config defaults.
3. Add style resolver tests.
4. Attach style output to pipeline context and step logs.
5. Do not enforce style limits yet.

## Success Criteria

The upgrade is successful when every proposal can answer:

1. What did the news say?
2. How credible and fresh was the news?
3. Did news confirm or contradict quant signals?
4. Which analysis style was selected?
5. Which trade style was selected?
6. What limits did the style impose?
7. Did final weights obey those limits?
8. What was clipped or blocked?

## Non-Goals

- Do not let news directly write target weights.
- Do not add a new LLM agent just for style selection.
- Do not let style override scorecard or Risk Manager.
- Do not loosen current execution controls.

## Model Capability Note

The current model setup is sufficient for this architecture:

```text
gpt-4o-mini: news summarization, Telegram, lightweight structuring
gpt-4o: Researcher, Bull/Bear, Synthesizer reasoning
```

The models are strong enough for interpretation and explanation, but Python must
own the final limits and execution gates.
