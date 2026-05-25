# Alpha Validation v2 Diagnostics Persistence Plan

## Goal

Turn one-off alpha, cost, construction, and risk diagnostics into a persistent
validation layer that can be trended, reviewed, and used to decide whether a
strategy family is improving or degrading.

The first alpha validation phase made the system able to compute:

- performance attribution
- transaction cost gate diagnostics
- regime-level conviction
- strategy diversity
- signal-weighted portfolio construction objective
- portfolio VaR / CVaR

This phase makes those diagnostics cumulative.

## Phase Order

1. Persist per-pipeline alpha validation snapshots
2. Add dashboard trend view
3. Add strategy family / regime gap analysis
4. Add promotion and degradation recommendations
5. Calibrate transaction cost model from fills

## PR1: Alpha Validation Runs Persistence

Status: implemented

Add `alpha_validation_runs`, one row per pipeline analysis.

Captured fields:

- transaction cost status and edge-to-cost metrics
- VaR / CVaR and max deterministic scenario loss
- signal-weighted portfolio construction metrics
- independent alpha family count
- conviction status counts from EvidenceCards
- structured warnings
- raw diagnostic payload

Rules:

- analytics-only
- no target mutation
- no execution blocking
- one row per `analysis_id`

## PR2: Alpha Validation Trend Dashboard

Status: implemented

Display recent alpha validation runs with:

- latest run summary
- recent rows
- status counts
- data quality counts
- rolling averages for key metrics

The dashboard must avoid naked numbers. Every trend metric must show source,
sample count, and diagnostics-only authority.

## PR3: Strategy Family / Regime Gap Analysis

Status: implemented

Use persisted conviction profiles and alpha validation runs to answer:

- which regimes have no calibrated strategy coverage?
- is momentum the only actionable alpha family?
- does a family degrade in defensive or mean-reverting regimes?
- which strategy family should be researched next?

Implemented notes:

- `services.strategy_regime_gap_analysis` loads latest conviction profiles and
  recent alpha validation runs in read-only mode
- calibrated alpha profiles are grouped by canonical strategy family and market
  regime
- duplicate historical/live/combined profiles are resolved with combined first,
  then live paper, then historical prior
- dashboard surfaces coverage rows, family rows, weak family/regime rows,
  warnings, and a research queue
- this layer is diagnostics-only and has `execution_authority=none` and
  `target_weight_mutation=none`

## PR4: Promotion / Degradation Recommendations

Status: implemented

Produce recommendations only, not automatic execution changes:

- promote watch-only strategy to advisory
- demote advisory strategy to watch-only
- archive strategy family in weak regimes
- require more samples before interpreting conviction

Implemented notes:

- `services.strategy_promotion_recommendations` combines latest conviction
  profiles, latest strategy evidence, recent alpha validation runs, and PR3 gap
  diagnostics
- recommendations include `promote_to_advisory_review`,
  `demote_to_watch_only_review`, `archive_family_regime_review`,
  `demote_family_regime_to_watch_review`, `require_more_samples`, and
  `research_family_for_regime`
- all recommendations carry sample count, profile count, regime/family context,
  reasons, blockers, and an operator action
- every use-changing recommendation includes `operator_approval_required`
- dashboard surfaces recommendation overview, counts, rows, policy, and warnings
- this layer is recommendation-only and has `execution_authority=none` and
  `target_weight_mutation=none`

## PR5: Fill-Based Cost Calibration

Status: planned

Use command lifecycle fill/reconciliation data when available to calibrate:

- estimated cost rate by ticker
- estimated cost rate by role
- slippage proxy
- edge-to-cost thresholds

Until fill data is complete, the IBKR return-drag proxy remains the default.
