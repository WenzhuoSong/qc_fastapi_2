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

## Not Yet Done

### Position Governance v2.1 Advisory Quality Feedback

The next enhancement is to measure whether accepted LLM advisory proposals
improve outcomes over deterministic baseline:

- Track accepted vs rejected proposals by ticker and reason.
- Compare forward returns after accepted trims/adds.
- Penalize proposal types that consistently reduce performance.
- Surface advisory quality in Telegram only as diagnostics, not execution logic.

## Next Development Plan

Recommended sequence:

1. **v1.1 Risk Contribution**
   - Add risk contribution fields.
   - Use risk contribution in trim priority.
   - Add tests for high-ATR small positions and low-ATR large positions.

2. **v1.2 Replacement Ranking**
   - Add quant baseline score input to governance.
   - Rank replacement candidates by support, score, volatility, and concentration.
   - Add `replacement_candidates` output.

3. **v1.3 Portfolio Summary**
   - Add `portfolio_governance_summary`.
   - Show concentration and top risk contributors in Telegram.

4. **v2 LLM Advisory Override**
   - Add LLM advisory proposal generation. Completed.
   - Add Python validator. Completed.
   - Log accepted/rejected override decisions. Completed.

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
