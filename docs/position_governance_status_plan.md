# Position Governance Status And Plan

Last updated: 2026-05-16

## Purpose

Position governance manages the lifecycle of each holding after alpha/research
signals are produced. It answers:

- Why hold, add, trim, exit, or review a position?
- Which actions are allowed under current risk permissions?
- How should target weights be adjusted before execution?
- When should LLM advice be allowed to influence a position decision?

The design principle is:

```text
LLM = researcher / advisor / explainer
Deterministic governance = risk and execution referee
Executor = sends weights only after approval gates pass
```

## Current State

### Completed: Deterministic Governance v1

Implemented in:

- `services/position_governance.py`
- `services/pipeline.py`
- `agents/communicator.py`
- `tests/test_position_governance.py`

Current pipeline position:

```text
Risk Manager
  -> Regime hard constraints
  -> Position Governance
  -> Position Manager quantity/frequency controls
  -> Communicator / Executor
```

Position Governance runs in two modes:

- `execution`: risk approved; governance may adjust target weights before
  Position Manager.
- `diagnostic_only`: risk rejected; governance uses current weights to produce
  explanations only and must not change execution targets.

Current governance output:

```json
{
  "adjusted_weights": {},
  "position_decisions": [],
  "blocked_actions": [],
  "forced_trims": [],
  "replacements": [],
  "trade_summary": {},
  "portfolio_summary": {},
  "config": {}
}
```

Each `position_decision` includes:

- `ticker`
- `decision`: `hold`, `hold_review`, `trim_review`, `trim`, `add`
- `action_permission`
- `allowed_actions`
- `strategy_support`
- `supporting_strategies`
- `group`
- `group_exposure`
- `group_limit`
- `group_headroom`
- `raw_risk_contribution`
- `sector_crowding_multiplier`
- `risk_contribution`
- `risk_budget_status`
- `risk_rank`
- `unrealized_pnl_pct`
- `atr_pct`
- `current_weight`
- `target_before`
- `target_after`
- `reason_codes`
- `exit_triggers`

### Completed: Hard Risk Governance Rules

Current deterministic rules include:

- Weak strategy support blocks adds.
- Unrealized loss review starts at `loss_review_pct` default `-4%`.
- Deep loss trim starts at `loss_trim_pct` default `-8%`, only when strategy support is weak.
- Hard-risk tickers are limited to trim/exit permission.
- High ATR blocks adds and can trigger trim review.
- Crowded sector/theme exposure blocks additional adds.
- Large winner with high weight can be trimmed for risk budget.
- `human_required` removes add/replacement permission.
- Scorecard permissions such as `hold_or_trim`, `defensive_only`, `cash_only`, and `reduce_risk_only` remove add permission.

### Completed: Replacement Logic v1

Replacement logic is conservative:

- It only uses cash freed by governance trims.
- It is disabled when human confirmation is required.
- It is disabled under restrictive permissions such as `hold_or_trim`, `defensive_only`, `cash_only`, or `reduce_risk_only`.
- It only considers `primary` or `advisory` strategy-supported candidates.
- It excludes candidates with hard risk, high ATR, weak support, loss review, winner trim, or concentration issues.
- Defaults:
  - `replacement_max_single_pct = 2%`
  - `replacement_max_total_pct = 5%`

Telegram can show:

```text
Position governance
  FTXL: hold_review | support=none | target 6.0%->3.0% | unrealized_loss_review,strategy_support_weak
  XLK: trim | support=advisory | target 16.0%->14.0% | winner_risk_budget_review
  replacements: SPY +2.0% (advisory)
```

### Completed: Position Governance v1.1 Risk Contribution

Risk contribution is now computed per ticker:

```text
risk_contribution = current_weight * atr_pct * sector_crowding_multiplier
```

The sector crowding multiplier increases risk contribution when a group is
above its configured exposure limit. This keeps small but very volatile
positions visible and avoids treating every large low-volatility position as
equally risky.

Per-ticker output now includes:

```json
{
  "raw_risk_contribution": 0.0028,
  "sector_crowding_multiplier": 1.2,
  "risk_contribution": 0.00336,
  "risk_rank": 1,
  "risk_budget_status": "high"
}
```

Default group limits:

- `semiconductors_limit_pct = 25%`
- `tech_growth_limit_pct = 35%`
- `defensive_bonds_limit_pct = 35%`
- `cyclicals_limit_pct = 30%`
- `real_estate_limit_pct = 15%`

### Completed: Position Governance v1.3 Portfolio Summary

The governance output now includes portfolio-level summary fields:

```json
{
  "portfolio_summary": {
    "group_exposures": {},
    "top_risk_contributors": [],
    "governance_counts": {}
  }
}
```

Telegram can now show concentration with configured limits and remaining
headroom:

```text
Position governance
  risk concentration: semiconductors 22.0% [limit 25.0%, headroom +3.0%]
  top risk: XLK 0.32% (high); QQQ 0.28% (medium)
```

### Completed: Non-LLM Execution Safety

The system no longer lets LLM copy imply execution when risk rejected the plan:

- `approved=false` forces deterministic communicator fallback.
- Rejected messages say `Rebalance rejected by risk` and `No execution this round`.
- Duplicate rejected notifications are suppressed for 6 hours when the rejection fingerprint is unchanged.

## Design Rationale

### 1. Position Governance Must Be Repeatable

The same inputs should produce the same governance result. This is required for
auditability and debugging. LLM text, prompt ordering, or sampling should not
change hard position-management behavior.

Status: completed for v1 via deterministic Python rules.

### 2. Risk Controls Must Not Depend On LLM Self-Discipline

Hard constraints are enforced by deterministic layers, not prompt compliance.
Examples:

- Maximum add/replacement size
- Scorecard permission
- Human confirmation requirement
- No adds without actionable strategy support
- Hard-risk trim/exit permission
- Turnover and trade count limits

Status: completed for v1 through scorecard, strategy-use constraints, position
governance, and position manager.

### 3. LLM Can Be Narratively Useful But Should Not Execute Alone

LLM may discover thesis decay, interpret macro/news, or explain regime conflict.
But LLM must not directly bypass governance limits.

Status: LLM is advisory through researcher/synthesizer context, communicator
explanation, and validated position advisory proposals. Deterministic
governance remains the final referee.

### 4. Alpha Evidence And Governance Are Separate

Alpha evidence sources:

- Strategy Playground
- yfinance historical replay
- QC live snapshots
- Researcher/synthesizer reasoning
- News/macro evidence

Governance layers:

- Market scorecard
- Decision style
- Strategy-use constraints
- Position governance
- Position manager
- Risk manager/executor gates

Status: completed for v1.

### Completed: Position Governance v1.2 Better Replacement Ranking

Replacement ranking now scores candidates deterministically instead of using
only strategy `suggested_use` and selected ticker order.

Current ranking inputs:

- Strategy `suggested_use`
- Strategy confidence
- selected ticker order
- ATR penalty
- Current holding status
- Theme/group concentration
- Scorecard permission

Replacement records and `portfolio_summary.replacement_candidates` now include:

```json
{
  "replacement_candidates": [
    {
      "ticker": "SPY",
      "score": 0.72,
      "support": "advisory",
      "strategy_name": "momentum_lite_v1",
      "why": ["advisory_strategy", "high_strategy_confidence", "low_atr", "group_headroom"]
    }
  ]
}
```

### Completed: Position Governance v2 LLM Advisory Override Validator

Implemented goal:

```text
Deterministic baseline decision
  + LLM advisory override proposal
  + Python validator
  = final bounded decision
```

Synthesizer may now emit optional advisory-only proposals:

```json
{
  "ticker": "FTXL",
  "llm_advisory": "trim",
  "target_weight": 0.03,
  "reason": "semiconductor thesis weakening and live consensus defensive",
  "confidence": 0.62
}
```

Validator output:

```json
{
  "ticker": "FTXL",
  "deterministic_decision": "hold_review",
  "llm_advisory": "trim",
  "llm_reason": "semiconductor thesis weakening and live consensus defensive",
  "validator_result": "accepted_as_trim_1.00%",
  "final_decision": "trim"
}
```

LLM advisory cannot bypass:

- max delta
- scorecard permission
- strategy confidence
- hard risk
- turnover cap
- human confirmation

Current validator behavior:

- Accept advisory only if it is within deterministic action permission.
- Clip advisory size to governance max delta.
- Reject advisory that increases risk under `human_required`.
- Reject add advisory when strategy support is not `primary` or `advisory`.
- Convert unsupported exit advisory into `hold_review` unless hard risk or exit trigger is active.
- Log every accepted/rejected/converted advisory decision in `advisory_overrides`.

### Completed: Position Governance v2.1 Advisory Quality Feedback Diagnostics

The system now records diagnostic feedback for LLM advisory proposals without
changing execution behavior.

Implemented:

- Track accepted/rejected/converted/noop proposals by ticker and action.
- Store advisory diagnostics in `portfolio_summary.advisory_quality`.
- Surface current-run advisory quality in Telegram.
- Store `position_advisory_overrides` and `position_advisory_quality` in
  daily decision memory.
- Provide pure forward-return scoring helper:
  - `add` is good when ticker forward return beats benchmark.
  - `trim` / `exit` is good when ticker forward return trails benchmark.

Current status:

```json
{
  "diagnostic_only": true,
  "current_run": {
    "total": 3,
    "accepted": 1,
    "rejected": 1,
    "converted": 1
  },
  "historical_feedback": {
    "sample_size": 0,
    "verdict": "insufficient"
  },
  "execution_impact": "none"
}
```

This feedback is intentionally not used to relax or tighten execution yet.

### Completed: Position Governance v2.2 Advisory Outcome Backfill

Daily analyst now backfills accepted advisory proposal outcomes when next-day
market feature data is available.

Implemented:

- Read accepted `position_advisory_overrides` from daily decision memory.
- Read ticker-level `return_1d` from `market_daily_features` using yfinance
  source.
- Use SPY `return_1d` as benchmark when available.
- Score advisory outcomes:
  - `add` is correct when ticker return beats benchmark.
  - `trim` / `exit` is correct when ticker return trails benchmark.
- Write back:
  - `position_advisory_outcomes`
  - updated `position_advisory_quality`
  - `position_advisory_benchmark_return`
  - `position_advisory_outcome_backfilled_at`

Still diagnostic-only:

```json
{
  "execution_impact": "none"
}
```

### Completed: Position Explanation Report v1

The governance output now includes deterministic position explanations inside
`portfolio_summary.position_explanations`.

Each explanation answers:

```json
{
  "ticker": "FTXL",
  "position_state": "loss_review",
  "why_hold": ["loss is above hard trim threshold", "no hard-risk event requires immediate exit"],
  "why_not_add": ["position is in unrealized loss review", "strategy support is weak or absent"],
  "why_not_exit": ["no hard-risk event is active", "governance uses staged trim before full exit"],
  "next_trigger": "trim if loss <= -8% and strategy support remains weak"
}
```

Implementation notes:

- Built from deterministic `position_decisions`, not free-form LLM text.
- Includes current weight, target weight, PnL, risk contribution, strategy
  support, action permission, blocked actions, and next trigger.
- Telegram shows the top problem holdings as compact `explain TICKER` lines.
- Full explanation is stored in the position governance step log through
  `portfolio_summary`.
- Rejected pipeline runs also generate diagnostic-only explanations so blocked
  reports can still answer why holdings are held / not added / not exited.

## Next Required Work

### 1. Deployment Verification And Data Integrity

Before adding more decision complexity, deploy the current governance stack and
observe real runs.

Required checks:

- Telegram wording must not imply execution when Risk Manager blocked the plan.
- `position_advisory_overrides` appears only when the LLM actually made a
  proposal.
- `position_advisory_outcomes` is backfilled by daily analyst after yfinance
  `return_1d` is available.
- Missing ticker returns are reported and do not break DQS/advisory backfill.
- `position_governance` step logs contain adjusted weights, decision reasons,
  replacement candidates, advisory overrides, and advisory quality diagnostics.

Acceptance criteria:

```text
Run for several trading days without schema/runtime failures.
Backfill produces diagnostic rows or explicit no-data reasons.
No Telegram message says or implies that blocked actions were executed.
```

### 2. Position Explanation Report Verification

The first implementation is complete. Next step is to verify live Telegram
output and step logs:

- Top problem holdings should be shown without flooding the message.
- Explanations must not imply execution.
- `why_hold`, `why_not_add`, and `why_not_exit` should be understandable for
  losing holdings.
- Full explanations should be available in step logs for all holdings.

## Backlog / Not Current Mainline

### Advisory Quality Aggregation

This was proposed as a possible v2.3, but it is not part of the immediate
required plan. It should wait until real advisory outcome samples exist.

Possible future enhancement:

- Aggregate advisory outcome score by action, ticker group, and market regime.
- Feed diagnostic warning into Synthesizer prompt only after enough samples.
- Keep deterministic validator as final authority.

## Next Development Plan

Recommended sequence:

1. **Deploy and verify current stack**
   - Confirm Telegram semantics.
   - Confirm advisory outcome backfill.
   - Confirm no schema/runtime failures.

2. **Verify Position Explanation Report**
   - Confirm Telegram readability.
   - Confirm all holdings have full step-log explanations.
   - Tune priority ordering only after seeing real messages.

3. **Observe samples before feedback-driven changes**
   - Collect enough advisory outcomes.
   - Review if LLM proposals are useful or noisy.
   - Keep advisory quality diagnostic-only until sample size is meaningful.

4. **Optional backlog**
   - Advisory quality aggregation by action/group/regime.
   - Parameter calibration after enough samples.

## Current Acceptance Criteria

The current v1 should satisfy:

- Losing positions are not silently held.
- A losing position with weak support cannot be increased.
- A deep losing position with weak support can be trimmed.
- Hard-risk tickers are moved to trim/exit permission.
- Crowded exposure blocks additional adds.
- Large winners can be trimmed for risk budget.
- Freed cash can be conservatively replaced only when risk permission allows.
- Rejected proposals do not imply execution.
- Position governance is visible in Telegram and step logs.
