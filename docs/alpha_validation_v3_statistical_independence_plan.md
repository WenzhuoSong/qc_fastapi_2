# Alpha Validation v3 Statistical Independence Plan

## Goal

Close the biggest remaining gap in the alpha-validation layer: proving whether
the strategy pool is statistically diverse, not just semantically diverse.

The system already has strong execution control, risk governance, strategy
metadata, conviction profiles, cost diagnostics, and performance attribution.
The next phase answers harder questions:

- Are the strategies genuinely independent, or mostly momentum variants?
- Is a conviction profile statistically meaningful, or just early noise?
- How much decay drag do leveraged and volatility ETFs impose?
- Did a portfolio-construction mode change invalidate old conviction samples?
- Are spread/liquidity costs material for weaker signals?

All work in this phase is diagnostics-first. No PR in this phase may directly
increase target weights or bypass risk validation.

## Phase Order

1. Strategy Independence / Correlation Diagnostics
2. Statistical Conviction Statuses and Confidence Intervals
3. Leveraged and Volatility ETF Decay Diagnostics
4. Portfolio-Construction Epoch Tracking
5. Spread and Liquidity Proxy Diagnostics
6. Promotion Policy Tightening from Statistical Evidence

## PR1: Strategy Independence / Correlation Diagnostics

Status: implemented

Add a diagnostics-only layer that estimates strategy return series from
historical yfinance replay data and computes:

- pairwise strategy return correlations
- high-correlation strategy pairs
- inverse/diversifying pairs
- family-level correlation rows
- effective independent alpha strategy count

The key distinction:

```text
strategy family diversity != statistical independence
```

Momentum variants may have different code paths but still fail together. This
diagnostic must make that visible before any strategy is counted as an
independent alpha source.

Rules:

- uses only historical feature rows available on the signal date
- next-day return is used only as forward outcome
- diagnostics are read-only
- `execution_authority = none`
- `target_weight_mutation = none`
- benchmark/non-alpha strategies are reported but excluded from effective alpha
  counts

Acceptance criteria:

- high positive correlations are surfaced as warnings
- negative/inverse correlations are separated from duplicate-alpha warnings
- effective independent count uses positive correlation penalty only
- every metric shows sample overlap
- insufficient overlap suppresses the correlation instead of inventing a value

Implemented notes:

- `services.strategy_independence` builds strategy T+1 replay return series
  from historical daily feature snapshots
- feature rows are asserted to be no later than `signal_date`
- pair rows include overlap, family labels, correlation, and status
- Playground and Evidence Bundle now expose `strategy_independence`
- output is diagnostics-only with `execution_authority=none` and
  `target_weight_mutation=none`

## PR2: Statistical Conviction Statuses and Confidence Intervals

Status: implemented

The current `calibrated` threshold is operationally useful but statistically too
optimistic. Replace naked status interpretation with explicit sample tiers:

```text
< 30      insufficient
30-99     early_signal
100-299   indicative
>= 300    statistically_meaningful
```

Add Wilson confidence intervals for hit rate and require dashboards to show:

- `n`
- status tier
- confidence interval width
- source bucket
- data-lag filtered count

Backward compatibility:

- keep old `calibrated` field available for existing consumers
- add `statistical_status` and `hit_rate_ci` as the stricter interpretation
- promotion logic must use the stricter status before approving use changes

Implemented notes:

- operational `status` remains unchanged for backward compatibility
- `ConvictionProfile.to_dict()` now exposes `statistical_status`,
  `hit_rate_ci`, `hit_rate_ci_width`, and sample thresholds
- persisted profile rows store the statistical interpretation inside
  `diagnostics`, so no schema migration is required
- dashboard rows and regime summaries now show statistical status counts and
  Wilson hit-rate CI width
- promotion recommendations now require `statistical_status` to be
  `indicative` or `statistically_meaningful` before promotion; operationally
  calibrated but statistically early profiles produce
  `require_statistical_maturity`

## PR3: Leveraged and Volatility ETF Decay Diagnostics

Status: implemented

ETF universe selection is itself an alpha decision. Add decay diagnostics for:

- leveraged long ETFs: TQQQ, SOXL, SPXL, TECL
- inverse ETFs: SQQQ, SOXS, SPXS, TECS
- volatility ETPs: UVXY, VIXY

Estimate realized decay/drag using:

```text
realized_etf_return - leverage_factor * underlying_return
```

For volatility ETPs, track rolling return decay and hold-period drag rather than
pretending they have a stable leverage multiplier.

Output:

- decay cost by ticker and horizon
- hold-period risk warnings
- regime-specific decay severity
- whether current `max_hold_days` is conservative enough

Implemented notes:

- `services.etf_decay_diagnostics` computes leveraged/inverse daily-reset drag
  as `etf_return - leverage * underlying_proxy_return`
- proxy mapping uses `QQQ` for `TQQQ/SQQQ`, `SPY` for `SPXL/SPXS`, `XLK` for
  `TECL/TECS`, and `SOXX` for `SOXL/SOXS`
- volatility ETPs such as `UVXY` and `VIXY` are evaluated through rolling
  hold-period return and SPY-up-market decay, not a fake stable leverage ratio
- asset knowledge fields such as `decay_risk`, `max_hold_days`, and
  `auto_reduce_after_days` are included in each diagnostic row
- Playground and Evidence Bundle now expose `etf_decay_diagnostics`
- output is diagnostics-only with `execution_authority=none` and
  `target_weight_mutation=none`; no schema migration is required

## PR4: Portfolio-Construction Epoch Tracking

Status: implemented

Conviction samples are path-dependent. A signal generated under shadow
construction is not equivalent to one generated under gated construction.

Add a `construction_epoch` concept to signal and conviction diagnostics:

- `pc_mode`
- `construction_objective_version`
- `policy_version`
- `promotion_config_hash`

When PC mode or objective changes, dashboards must separate old and new
conviction samples instead of merging them as one homogeneous population.

Implemented details:

- `services.construction_epoch` builds deterministic epoch fingerprints with
  `execution_authority=none` and `target_weight_mutation=none`
- live frozen signals record the current portfolio-construction config and
  FastAPI policy version in `diagnostics.construction_epoch`
- historical replay signals use a fixed `historical_replay_no_pc_v1` epoch
- legacy/missing rows fall back to a stable `unknown` epoch instead of being
  silently mixed with new samples
- conviction profile ids and grouping keys now include `construction_epoch_id`
- validation dashboards expose `construction_epoch_id`, `pc_mode`,
  `construction_objective_version`, `policy_version`, and grouped regime rows
  by epoch
- promotion/degradation recommendations carry `construction_epoch_ids` and do
  not dedupe profiles across different epochs
- no schema migration is required; epoch metadata is carried through existing
  diagnostics JSON

## PR5: Spread and Liquidity Proxy Diagnostics

Status: implemented

Execution quality differs by ETF. Add read-only liquidity diagnostics:

- dollar volume bucket
- ATR/spread proxy
- opening/closing window risk
- low-liquidity ETF warning

This is not live quote spread monitoring yet. It is a historical proxy that
helps decide whether weak signals should be deferred.

Implemented details:

- `services.liquidity_proxy_diagnostics` evaluates historical yfinance-style
  OHLCV feature rows and produces `liquidity_proxy_diagnostics_v1`
- output includes median dollar volume, p10 dollar volume, liquidity bucket,
  ATR/range spread proxy, Amihud-style return-per-dollar-volume proxy,
  opening-gap risk, and closing-window risk
- execution quality is classified as `robust`, `watch_costs`,
  `defer_weak_signals`, `no_trade_review`, or `insufficient_data`
- low-liquidity ETFs and wide spread proxies are surfaced as structured
  warnings and `execution_review_rows`
- Playground and Evidence Bundle now expose `liquidity_proxy_diagnostics`
- this is not live bid/ask monitoring and cannot authorize or mutate targets:
  `execution_authority=none`, `target_weight_mutation=none`
- no schema migration is required; diagnostics are computed from existing
  market daily feature rows

## PR6: Promotion Policy Tightening

Status: implemented

Use outputs from PR1-PR5 to tighten strategy promotion:

- do not promote a strategy as independent alpha if it is highly correlated with
  an already-actionable strategy
- do not promote if statistical status is only `early_signal`
- require regime coverage, decay diagnostics, and cost diagnostics to agree
- every promotion remains recommendation-only and requires operator approval

Implemented details:

- `strategy_promotion_recommendations` now consumes the evidence-bundle
  `strategy_independence`, `etf_decay_diagnostics`, and
  `liquidity_proxy_diagnostics` sections
- positive conviction can still produce `promote_to_advisory_review`, but only
  when evidence gates pass
- if a strong strategy is highly correlated with an already actionable strategy,
  the system emits `require_promotion_evidence_alignment` instead of promotion
- mixed regime evidence blocks global promotion and requires regime-scoped
  review
- high/extreme ETF decay, max-hold policy warnings, weak liquidity proxies,
  high estimated strategy cost, high turnover, or recent transaction-cost-gate
  low-edge warnings all become structured promotion blockers
- recommendation rows now include `evidence_checks`, `blockers`, and
  `construction_epoch_ids`
- promotion remains recommendation-only:
  `execution_authority=none`, `target_weight_mutation=none`, and
  `operator_approval_required`

## Definition of Done

This phase is not done if the system can say "we have many strategies" without
also showing:

- their pairwise return correlation
- effective independent alpha count
- sample size tier and confidence interval
- ETF decay/cost diagnostics
- PC epoch used to produce the evidence
