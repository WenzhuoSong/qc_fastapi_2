# Alpha Validation Development Plan

## Goal

Move the system from safe execution toward verifiable positive-expectancy
decision making.

The current system is strong at:

- deterministic risk validation
- QC execution control
- account-state guardrails
- command lifecycle audit

The next phase must answer:

- Did portfolio return come from beta, factor exposure, or residual edge?
- Did the signal justify the trading cost?
- Which strategy works in which regime?
- Are multiple strategies genuinely diverse, or just momentum variants?

## Phase Order

1. Performance Attribution MVP
2. Transaction Cost Gate observe mode
3. Regime-Level Conviction Dashboard
4. Strategy Diversity Framework
5. Signal-Weighted Portfolio Construction shadow objective
6. Portfolio VaR / CVaR diagnostic

## PR1: Performance Attribution MVP

Status: implemented

Files:

- `services/performance_attribution.py`
- `cron/weekly_performance_attribution.py`
- `db/migrations/20260525_create_performance_attribution.sql`
- `tests/test_performance_attribution.py`

Method:

```text
portfolio_return
= SPY beta contribution
+ QQQ / growth beta contribution
+ momentum proxy contribution
+ residual_alpha_candidate
```

The MVP uses daily returns and a small factor regression. The residual is
explicitly named `residual_alpha_candidate`, not proven alpha.

Data sources:

- `portfolio_timeseries.total_value` and/or `daily_pnl_pct`
- `market_daily_features.return_1d`
- factor tickers: `SPY`, `QQQ`, and `MTUM` when available
- fallback momentum proxy: `QQQ - SPY`

Acceptance criteria:

- data insufficient -> `status=insufficient_data`
- enough samples -> attribution row with beta/factor/residual breakdown
- every row records method, sample count, source tickers, and data quality
- weekly cron persists a row and records cron audit telemetry

Implemented notes:

- residual is reported as `residual_alpha_candidate`, not proven alpha
- `MTUM` is used when available; otherwise momentum proxy is `QQQ - SPY`
- `content_hash` excludes generation time so identical inputs are deterministic
- `cron.weekly_performance_attribution` is analytics-only and cannot mutate execution

## PR2: Attribution Dashboard

Status: implemented

Dashboard should show:

- portfolio return
- SPY beta contribution
- QQQ beta contribution
- momentum factor contribution
- residual alpha candidate
- R-squared
- sample count and data quality

Implemented notes:

- dashboard reads `performance_attribution` rows in read-only mode
- residual is displayed under a `Residual Contract` with `execution_authority=none`
- latest run, factor model, return breakdown, recent runs, and status counts are visible

## PR3: Transaction Cost Gate Observe Mode

Status: implemented

Default broker cost model:

```text
broker = IBKR
```

The cost model must use return-drag units, not mix dollars and returns.

MVP formula:

```text
cost_drag = abs(delta_weight) * estimated_cost_rate
expected_edge = abs(delta_weight) * confidence * conviction_discount * expected_horizon_return_proxy
edge_to_cost_ratio = expected_edge / cost_drag
```

Initial cost assumptions:

- default broker model: IBKR
- ordinary ETF: IBKR-style low cost baseline
- leveraged ETF: higher spread/slippage proxy
- volatility ETP: highest spread/slippage/decay penalty
- rates are internal return-drag assumptions, not a live broker fee schedule;
  calibrate with actual fills once Command Lifecycle reconciliation has enough data

Mode:

- observe only
- no target weight mutation in PR3
- output warnings for low edge-to-cost trades

Implemented notes:

- `services.transaction_cost_gate` defaults to `broker=IBKR`, `mode=observe`
- cost rates are return-drag proxies by bucket: ordinary ETF, leveraged ETF, volatility ETP
- buy/increase actions are checked for `edge_to_cost_ratio`; sells are diagnostic-only by default
- pipeline writes `risk_out.transaction_cost_gate` and `6cc_transaction_cost_gate` step telemetry
- dashboard surfaces the latest Transaction Cost Gate summary and per-action rows

## PR4: Regime-Level Conviction Dashboard

Status: implemented

Group conviction profiles by:

- strategy
- ticker
- action
- branch
- regime_at_signal
- source_bucket

Display:

- hit rate
- avg excess vs SPY
- IC
- n
- status
- data-lag filtered count

Implemented notes:

- `services.strategy_validation_dashboard` emits `regime_level_profiles`
  at strategy/ticker/action/branch/regime/source_bucket granularity
- `regime_summary_rows` aggregates profile count, sample count, calibrated/early/insufficient profile counts, hit rate, excess vs SPY, IC, and data-lag filtered samples
- dashboard displays both regime-level summary rows and underlying profile rows in the Live Signal Conviction section

## PR5: Strategy Diversity Framework

Status: implemented

Every strategy must declare a family:

- momentum
- low_vol_defensive
- mean_reversion
- carry_or_cash_proxy
- event_risk_avoidance
- volatility_hedge

Same-family strategies must not be counted as independent alpha sources.

Implemented notes:

- legacy strategy families are preserved in `strategy_card.family`
- `strategy_card.canonical_family` maps old names such as `trend_following`,
  `dual_momentum`, and `leveraged_rotation` into `momentum`
- `strategy_card.alpha_source` distinguishes true alpha candidates from
  benchmarks such as equal weight and risk parity
- evidence bundle emits `strategy_diversity` with family rows, strategy rows,
  independent alpha family count, and same-family warnings
- dashboard displays diversity diagnostics under ETF / Strategy Evidence
- this layer is diagnostics-only and has `execution_authority=none`

## PR6: Signal-Weighted Portfolio Construction

Status: implemented

Current objective:

```text
maximize_effective_n
```

Shadow objective:

```text
maximize_signal_weighted_effective_n
```

This prevents diversification from blindly diluting higher-quality signals.

Implemented notes:

- `ConstructionObjective.primary` is now `maximize_signal_weighted_effective_n`
- objective constraints explicitly include `signal_quality_not_diluted`
- PC output includes signal-weighted effective N, signal alignment, coverage,
  unscored weight, negative-signal weight, and per-ticker signal objective rows
- current implementation is diagnostics-first: it does not mutate target weights
  based on signal objective metrics
- non-alpha benchmark strategy rows are ignored when building construction
  signal strengths
- dashboard displays signal-weighted objective metrics and per-ticker rows

## PR7: Portfolio VaR / CVaR Diagnostic

Status: implemented

Add historical simulation:

- VaR 95%
- CVaR 95%
- scenarios: SPY -3%, QQQ -5%, TQQQ -15%, UVXY spike/decay

This is diagnostic first, not a blocking rule.

Implemented notes:

- `services.portfolio_risk_diagnostic` computes historical VaR/CVaR and
  deterministic stress scenarios for current and final target weights
- historical simulation uses `market_daily_features.return_1d` via yfinance
  rows when the pipeline runs
- output includes positive loss metrics (`var_95_loss`, `cvar_95_loss`) and
  return metrics (`var_95_return`, `cvar_95_return`)
- fixed scenarios include SPY -3% / QQQ -5%, leveraged ETF flush, and UVXY
  decay day
- pipeline writes `risk_out.portfolio_risk_diagnostic` and a
  `6cd_portfolio_var_cvar` step log
- dashboard displays historical VaR/CVaR and scenario losses
- this layer is diagnostics-only and has `execution_authority=none`
