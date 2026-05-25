# Alpha Strategy Plugin Inventory

## Status

The system supports hot-pluggable strategies through the `Strategy` interface:

1. implement `score()` and `optimize()`
2. register the class in `strategies.STRATEGY_REGISTRY`
3. add the strategy name to `DEFAULT_PLAYGROUND_STRATEGIES` when it should run by default
4. add a knowledge strategy profile for EvidenceCard semantics
5. let existing certification, conviction, promotion/degradation, and risk gates decide whether it may influence allocation

## Newly Added Strategy Families

### `absolute_trend_following_lite`

- family: `trend_following` canonicalized to `momentum`
- universe: `SPY`, `QQQ`, `IWM`, `SGOV`, `IEF`
- role: non-leveraged absolute trend baseline with defensive fallback
- best regimes: `trending_bull`, `risk_on`, `persistent_trend`
- output: capped ordinary ETF trend sleeve plus `CASH`
- authority: strategy output only; certification and conviction decide whether it may influence allocation

### `seasonality_month_end_lite`

- family: `seasonality_flow`
- universe: `SPY`, `QQQ`, `IWM`
- role: turn-of-month structural-flow candidate, independent from pure momentum
- best regimes: `risk_on`, `trending_bull`, `mean_reverting`
- output: very small capped broad-market sleeve plus `CASH`; outside the calendar window it stays cash
- authority: strategy output only; certification and conviction decide whether it may influence allocation

### `sector_theme_relative_strength_lite`

- family: `sector_theme_rotation` canonicalized to `momentum`
- universe: `XLK`, `SOXX`, `XSD`, `PSI`, `FTXL`, `AIQ`, `CIBR`, `BOTZ`, `XLE`, `XLI`, `XLRE`
- role: sector/theme relative-strength lens for ETF leadership
- best regimes: `trending_bull`, `risk_on`, `sector_rotation`
- output: capped sector/theme sleeve plus `CASH`, with a sector-group cap to reduce hidden concentration
- authority: strategy output only; certification and conviction decide whether it may influence allocation

### `leveraged_long_amplifier_lite`

- family: `leveraged_rotation` canonicalized to `momentum`
- universe: `TQQQ`, `SOXL`, `TECL`, `SPXL`
- role: tiny risk-on amplifier for leveraged long ETFs
- best regimes: `trending_bull`, `risk_on`, `sector_rotation`
- output: tiny capped leveraged-long sleeve plus `CASH`; non-risk-on regimes stay cash
- authority: strategy output only; leveraged ETF caps, max-hold rules, certification, and conviction still apply

### `carry_cash_proxy_lite`

- family: `carry_or_cash_proxy`
- universe: `SGOV`, `BND`, `IEF`, `TLT`
- role: defensive carry / cash-like sleeve
- best regimes: `defensive`, `high_vol`, `risk_off`, `cash_only`
- output: capped satellite-sized defensive weights plus `CASH`
- authority: strategy output only; execution remains controlled by certification and risk gates

### `volatility_hedge_lite`

- family: `volatility_hedge`
- universe: `UVXY`, `VIXY`, `SGOV`, `TLT`
- role: very short-term tail-risk hedge evidence
- best regimes: `high_vol`, `defensive`, `acute_risk_off`, `volatility_spike`
- output: tiny hedge sleeve plus defensive fallback and `CASH`
- authority: strategy output only; volatility ETP caps and hold-day rules still apply

### `inverse_equity_hedge_lite`

- family: `event_risk_avoidance`
- universe: `SQQQ`, `SPXS`, `SOXS`, `TECS`
- role: hedge-only inverse equity ETF signal for short-term drawdown protection
- best regimes: `high_vol`, `defensive`, `risk_off`, `crash_breakdown`
- output: tiny inverse ETF hedge sleeve plus `CASH`; risk-on regimes stay cash
- authority: strategy output only; inverse ETF caps, max-hold rules, and full-auto asset caps still apply

### `relative_value_reversion_lite`

- family: `mean_reversion`
- universe: `SPY`, `QQQ`, `IWM`
- role: small tactical relative-value reversion sleeve
- best regimes: `mean_reverting`, `range_bound`, `risk_on_chop`
- output: capped long-only broad-market ETF weights plus `CASH`
- authority: strategy output only; certification and conviction decide whether it may influence allocation

### `sector_theme_relative_value_reversion_lite`

- family: `mean_reversion`
- universe: `XLK`, `QQQ`, `SOXX`, `XSD`, `PSI`, `FTXL`, `AIQ`, `CIBR`, `BOTZ`, `XLE`, `XLI`, `XLRE`
- role: cluster-relative short-term reversion lens for sector/theme ETFs
- best regimes: `mean_reverting`, `risk_on_chop`, `sector_rotation`, `range_bound`
- output: capped sector/theme reversion sleeve plus `CASH`, with group caps to reduce hidden concentration
- authority: strategy output only; certification and conviction decide whether it may influence allocation

### `defensive_quality_rotation_lite`

- family: `low_vol_defensive`
- universe: `SGOV`, `BND`, `IEF`, `TLT`
- role: defensive quality / low-vol sleeve
- best regimes: `defensive`, `high_vol`, `risk_off`, `late_cycle`
- output: capped defensive ETF weights plus `CASH`
- authority: strategy output only; execution remains controlled by certification and risk gates

### `macro_rate_duration_lite`

- family: `macro_rate` canonicalized to `carry_or_cash_proxy`
- universe: `SGOV`, `BSV`, `BND`, `IEF`, `TLT`
- role: rate/duration selector for defensive bond and cash-like ETFs
- best regimes: `defensive`, `high_vol`, `risk_off`, `falling_rate_expectation`, `stable_rates`
- output: capped defensive duration sleeve plus `CASH`, preferring SGOV/BSV when rate risk is unclear or rising
- authority: strategy output only; execution remains controlled by certification and risk gates

### `macro_cyclical_inflation_rotation_lite`

- family: `macro_cycle_rotation` canonicalized to `macro_regime`
- universe: `XLE`, `XLI`, `IWM`, `XLRE`, `SGOV`, `TLT`
- role: macro-cycle lens for inflation-sensitive, cyclical, rate-sensitive, and defensive ETF sleeves
- best regimes: `risk_on`, `trending_bull`, `inflationary_growth`, `broadening_bull`, `falling_rate_expectation`, `stable_growth`
- output: capped macro-cycle sleeve plus `CASH`, falling back to `SGOV`/`TLT` when macro evidence is defensive
- authority: strategy output only; conviction, certification, cost, and risk gates decide whether it may influence allocation

## Safety Contract

These strategies are now tradable in the technical sense: they can produce
target weights in Playground and EvidenceCards. They are not automatically
trusted. The downstream chain still applies:

- strategy confidence
- strategy certification
- conviction profile maturity
- promotion/degradation recommendations
- target builder ownership
- risk manager validate-only checks
- final risk validation
- execution policy caps
- QC-side policy validation

No new strategy bypasses those layers.
