# Adding a Strategy / Alpha Plugin

## Goal

Add new alpha sources one by one without weakening the execution system.

In this codebase, a new strategy becomes technically pluggable when it can:

1. score ETF candidates through `Strategy.score()`
2. produce target weights through `Strategy.optimize()`
3. register in `STRATEGY_REGISTRY`
4. run in Playground
5. produce ETF-aware EvidenceCards
6. accumulate conviction profiles before promotion

This does not mean it is trusted for live allocation. Execution authority still
comes from strategy confidence, certification, conviction, promotion review,
risk validation, execution policy, and QC-side validation.

## Strategy Lifecycle

```text
alpha idea
-> StrategySpec
-> Strategy implementation
-> knowledge profile + asset safety fields
-> Playground / watch-only
-> historical replay + live paper frozen signals
-> conviction by strategy/ticker/action/branch/regime
-> promotion/degradation recommendation
-> operator approval
-> advisory or primary use
```

## Current Priority Families

Add strategies according to actual portfolio gaps, not according to backtest
excitement.

1. `mean_reversion`
2. `low_vol_defensive`
3. `volatility_hedge`
4. `carry_or_cash_proxy`
5. `seasonality_flow`
6. `trend_following`
7. additional `momentum` variants only after non-momentum coverage improves

## Hedge-Only Inverse ETF Rule

Inverse equity ETFs are not ordinary alpha candidates in this system. They may
be researched only as explicit hedge tools.

Allowed first implementation:

```text
id: inverse_equity_hedge_lite
family: event_risk_avoidance
universe: SQQQ, SPXS, SOXS, TECS
allowed_actions: hedge, watch, avoid
max_single_weight: 0.02
max_total_weight: 0.05
min_cash_pct: 0.95
max_hold_days: asset-profile max_hold_days
default_playground: yes
promotion: conviction and operator approval required
```

Rules:

- do not output ordinary `increase`
- do not treat inverse ETFs as persistent shorts or long-term portfolio assets
- require elevated market stress plus underlying breakdown evidence
- risk-on or trending-bull regimes should return all cash
- full-auto production adds remain blocked if asset-level caps are zero

## Leveraged Long ETF Amplifier Rule

Leveraged long ETFs are not core holdings in this system. They may be researched
only as tiny risk-on amplifiers after the underlying ETF trend confirms.

Allowed first implementation:

```text
id: leveraged_long_amplifier_lite
family: leveraged_rotation -> canonical_family: momentum
universe: TQQQ, SOXL, TECL, SPXL
allowed_actions: increase, watch, neutral
max_single_weight: 0.02
max_total_weight: 0.06
min_cash_pct: 0.94
max_hold_days: asset-profile max_hold_days
default_playground: yes
promotion: conviction and operator approval required
```

Rules:

- do not treat leveraged ETFs as substitutes for core equity exposure
- require risk-on regime plus underlying ETF confirmation
- high-vol, defensive, risk-off, or mean-reverting regimes should return all cash
- keep the strategy canonicalized to `momentum`; it does not add a new independent alpha family
- asset-level decay, max-hold, auto-reduce, and execution-policy caps still apply

## Next Alpha Feasibility: Seasonality vs Macro

The next plugin should close an independence gap, not add another momentum
variant. Two candidates are useful:

### `seasonality_month_end_lite`

Purpose:

- capture turn-of-month / month-end structural flow evidence
- diversify away from pure ETF momentum and defensive carry
- run with existing daily feature data plus a deterministic calendar signal

Why it is feasible now:

- no external macro feed is required
- no LLM is required
- the signal can be generated deterministically from `signal_date`
- the universe can stay small and liquid: `SPY`, `QQQ`, `IWM`
- position size can be capped tightly because this is a statistical tendency,
  not a high-conviction directional forecast

Primary traps:

- same-day close leakage: the signal must be frozen after T close and tradable
  from T+1
- calendar overfitting: do not tune exact day windows from recent backtest
  results
- regime dependency: turn-of-month flow can fail in acute volatility or
  defensive regimes

Recommended first implementation:

```text
family: seasonality_flow
universe: SPY, QQQ, IWM
required_features: mom_20d, mom_60d, hist_vol_20d, atr_pct
calendar_window: last 2 calendar days of month or first 3 calendar days
max_single_weight: 0.04
max_total_weight: 0.12
min_cash_pct: 0.88
default_playground: yes
promotion: conviction required before advisory/primary use
```

### `macro_rate_duration_lite`

Purpose:

- use rate/inflation macro context to choose between duration-sensitive
  defensive ETFs and cash-like exposure
- improve behavior in rising-rate or falling-rate regimes

Why it should come after seasonality:

- it depends on macro event/news quality, not just deterministic daily features
- current macro context is useful for diagnostics, but noisier as direct alpha
- implementation should wait until macro data freshness and event interpretation
  have stronger validation

Recommended status:

```text
family: macro_rate -> canonical_family: carry_or_cash_proxy
universe: SGOV, BSV, BND, IEF, TLT
required_features: hist_vol_20d, atr_pct, mom_20d, mom_60d
optional_context: rate_regime_label, macro_context
max_single_weight: 0.05
max_total_weight: 0.20
min_cash_pct: 0.80
default_playground: yes
promotion: stricter than seasonality_flow
```

Implementation rule:

- do not make macro/news data a hard dependency in the first version
- use ETF-implied duration trend and volatility as deterministic primary inputs
- use rate-regime context only as an optional adjustment
- canonicalize to `carry_or_cash_proxy` so it does not inflate independent alpha
  family count

### `sector_theme_relative_strength_lite`

Purpose:

- use the existing sector/theme ETF pool instead of forcing all risk-on
  decisions through broad-market ETFs
- identify relative leadership across technology, semiconductors, AI,
  cybersecurity, energy, industrials, and real estate
- provide ETF-aware EvidenceCards for sector/theme assets

Why it is important with the current ETF list:

- many available ETFs are sector or thematic funds
- without a dedicated plugin, these tickers can appear in generic momentum
  results without enough role-specific semantics
- this closes an asset coverage gap, but it should still canonicalize to
  `momentum` because it is not an independent alpha family

Primary traps:

- hidden concentration: `SOXX`, `XSD`, `PSI`, `FTXL`, and `SOXL` are highly
  related semiconductor exposures
- theme crowding: `XLK`, `QQQ`, `AIQ`, `CIBR`, and `BOTZ` can express the same
  growth beta
- overclaiming alpha diversity: sector relative strength is useful, but it is
  still momentum-family evidence

Recommended first implementation:

```text
family: sector_theme_rotation -> canonical_family: momentum
universe: XLK, SOXX, XSD, PSI, FTXL, AIQ, CIBR, BOTZ, XLE, XLI, XLRE
required_features: mom_20d, mom_60d, mom_252d, hist_vol_20d, atr_pct, rsi_14
max_single_weight: 0.05
max_total_weight: 0.18
max_group_weight: 0.10
min_cash_pct: 0.82
default_playground: yes
promotion: conviction required before advisory/primary use
```

## StrategySpec Template

Create this before code.

```yaml
id: new_strategy_id
family: mean_reversion | low_vol_defensive | volatility_hedge | event_risk_avoidance | carry_or_cash_proxy | macro_rate | seasonality_flow | trend_following | momentum
alpha_source: true
hypothesis: >
  Why this should have positive expectancy.
alpha_origin:
  type: risk_premia | behavioral | structural_flow | defensive_risk_control | hedge
  explanation: >
    The economic or behavioral mechanism. Do not use "backtest looks good" as the reason.
expected_regimes: [mean_reverting]
weak_regimes: [trending_bull]
universe: [SPY, QQQ, IWM]
required_features: [mom_20d, rsi_14, hist_vol_20d]
signal_formula: >
  Precise formula or decision tree. Must be implementable without future data.
rebalance_frequency: daily | weekly | monthly
weighting_rule: equal | score_weighted | inverse_vol | capped_sleeve
risk_bounds:
  max_single_weight: 0.05
  max_total_weight: 0.20
  min_cash_pct: 0.80
evidence_mapping:
  actions: [increase, de_risk, hedge, watch, neutral, avoid]
  horizon: short_tactical
known_failure_modes:
  - where it loses money
  - market regimes that invalidate it
validation_plan:
  historical_replay: true
  walk_forward: true
  live_paper_signal_ledger: true
promotion_rule: >
  Requires calibrated conviction and operator approval.
```

## Code Integration Steps

### 1. Implement Strategy Class

Add a file under `strategies/`, for example:

```text
strategies/new_strategy.py
```

The class must inherit `Strategy` and define:

- `name`
- `version`
- `description`
- `required_fields`
- `optional_fields`
- `family`
- `core_idea`
- `best_regimes`
- `bad_regimes`
- `signals_used`
- `failure_modes`
- `agent_guidance`
- optional `universe_tickers`
- optional `allow_hedge_research_tickers`
- `score()`
- `optimize()`

Rules:

- `score()` returns sorted `ScoredTicker` rows.
- `optimize()` must always return a dict with `CASH`.
- `optimize()` must apply explicit caps.
- If data is missing, return `[]` from `score()` and `{"CASH": 1.0}` from `optimize()`.
- Do not call LLMs from strategy code.
- Do not read database state from strategy code.
- Do not mutate execution config.

### 2. Register Strategy

Update:

```text
strategies/__init__.py
```

Add the import and registry entry:

```python
from strategies.new_strategy import NewStrategy

STRATEGY_REGISTRY = {
    ...
    "new_strategy_id": NewStrategy,
}
```

### 3. Decide Default Playground Inclusion

Update:

```text
services/playground.py
```

Add to `DEFAULT_PLAYGROUND_STRATEGIES` only if the strategy should run every
cycle. If it is expensive, experimental, or niche, leave it registered but do
not add it to the default list.

### 4. Add Knowledge Strategy Profile

Add:

```text
knowledge/strategies/new_strategy_id.yaml
```

Minimum fields:

- `id`
- `type: strategy`
- `category`
- `horizon`
- `summary`
- `best_regimes`
- `weak_regimes`
- `required_features`
- `positive_signals`
- `failure_modes`
- `governance_implications`
- `compatibility_mappings`
- `sources`
- `last_reviewed`

`compatibility_mappings` are required for EvidenceCards. Without them the
strategy can still score, but EvidenceCards will fall back to `watch`.

Example:

```yaml
compatibility_mappings:
  - role: core_market
    horizon: short_tactical
    score_thresholds:
      - gte: 0.70
        action: increase
        signal_type: risk_on_signal
      - gte: 0.40
        action: watch
        signal_type: monitor
      - lt: 0.40
        action: neutral
        signal_type: no_signal
    max_weight_multiplier: 1.0
    weight_formula: confidence_cap_multiplier
    branch_label_template: "{regime}_{strategy}_{ticker}"
```

### 5. Add / Verify Asset Safety Fields

Every ticker that may appear in EvidenceCards must have an asset profile under:

```text
knowledge/assets/{TICKER}.yaml
```

Safety-critical fields:

- `role`
- `allowed_actions`
- `max_reasonable_weight`
- `risk_budget_cost`
- `decay_risk`

If these are missing, EvidenceCards should not become actionable.

### 6. Update Regime Profiles

If the strategy is meant to cover a regime gap, update the relevant files:

```text
knowledge/regimes/*.yaml
```

Use:

- `supports_strategies`
- `weak_strategies`
- `sources`
- `last_reviewed`

### 7. Add Source Registry Entries

If the strategy cites a new paper, factor, or internal rule, update:

```text
knowledge/sources/registry.yaml
```

Do not cite vague internet claims as high reliability.

## Required Tests

Add or update tests, usually in:

```text
tests/test_alpha_strategy_plugins.py
```

Minimum coverage:

1. registered in `STRATEGY_REGISTRY`
2. optional default Playground inclusion
3. correct canonical family
4. `score()` ranks expected tickers on synthetic data
5. `optimize()` returns `CASH` and respects caps
6. knowledge profile builds non-fallback EvidenceCards
7. missing data produces empty score / all cash
8. hedge or leveraged outputs stay within tight caps

## Validation Commands

Focused:

```bash
uv run python -m unittest tests.test_alpha_strategy_plugins tests.test_evidence_bundle tests.test_strategy_diversity
```

Syntax:

```bash
uv run python -m py_compile strategies/new_strategy.py strategies/__init__.py services/playground.py
```

Full:

```bash
uv run python -m unittest discover tests
```

## Acceptance Criteria

A strategy PR is not complete unless:

- it is registered
- its family maps to a canonical alpha family
- it can run with synthetic data
- it fails safe to cash
- it has explicit caps
- it has a knowledge strategy profile
- all output tickers have safety fields
- EvidenceCards do not fall back because of missing mapping or missing safety fields
- tests pass
- no execution bypass is introduced

## Do Not Do

- Do not add a strategy directly to production weights.
- Do not bypass `target_builder`.
- Do not let conviction directly mutate target weights.
- Do not treat historical Sharpe as certification.
- Do not add unregistered tickers without execution policy review.
- Do not use future data or same-day close as if it were tradable before close.
- Do not add another momentum variant before checking whether it closes a real regime/family gap.

## Current Implemented Examples

Use these as templates:

- `strategies/absolute_trend_following_lite.py`
- `strategies/seasonality_month_end_lite.py`
- `strategies/sector_theme_relative_strength_lite.py`
- `strategies/relative_value_reversion_lite.py`
- `strategies/defensive_quality_rotation_lite.py`
- `strategies/macro_rate_duration_lite.py`
- `strategies/carry_cash_proxy_lite.py`
- `strategies/volatility_hedge_lite.py`
- `strategies/inverse_equity_hedge_lite.py`
- `strategies/leveraged_long_amplifier_lite.py`
- `knowledge/strategies/absolute_trend_following_lite.yaml`
- `knowledge/strategies/seasonality_month_end_lite.yaml`
- `knowledge/strategies/sector_theme_relative_strength_lite.yaml`
- `knowledge/strategies/relative_value_reversion_lite.yaml`
- `knowledge/strategies/defensive_quality_rotation_lite.yaml`
- `knowledge/strategies/macro_rate_duration_lite.yaml`
- `knowledge/strategies/carry_cash_proxy_lite.yaml`
- `knowledge/strategies/volatility_hedge_lite.yaml`
- `knowledge/strategies/inverse_equity_hedge_lite.yaml`
- `knowledge/strategies/leveraged_long_amplifier_lite.yaml`
- `tests/test_alpha_strategy_plugins.py`
