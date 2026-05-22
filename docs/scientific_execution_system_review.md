# Scientific Execution System Review

> Review date: 2026-05-21  
> Scope: recent Sprint 1-7 optimization series plus execution/universe-policy fixes.

This document summarizes the current code-level design of the trading system after the recent optimization work. It is intended for operator and developer review, not as a marketing roadmap.

## 1. Executive Summary

The system has moved from a simple "LLM suggests weights, risk checks weights" model toward a layered execution architecture:

```text
Market data / news / memory
  -> quant baseline
  -> LLM research and advisory semantics
  -> deterministic target builder
  -> risk manager
  -> position governance
  -> decision ledger
  -> communicator / executor
```

The core principle is:

```text
LLM output can influence research and advisory context, but raw LLM weights are not direct execution authority.
```

The deterministic layers now handle:

- group and factor exposure contracts
- basket review and correlated loss review
- portfolio construction shadow audit
- target weight construction
- strategy certification audit
- walk-forward validation
- thesis review scheduling
- per-ticker execution-chain explanation
- circuit breaker override behavior
- hedge/universe role policy

The biggest remaining architectural issue is not that the system has too many constraints. The issue is that some constraints still live in multiple places and must be unified into a shared execution policy, especially QC-side final guardrails versus FastAPI-side target construction.

## 2. Current Pipeline Flow

Main orchestration lives in:

- `services/pipeline.py`

High-level stages:

1. `_guard_and_config()`
   - reads runtime config, circuit state, risk params, active strategy, position governance config
   - blocks `MANUAL` or paused mode
   - handles `FULL_AUTO` + open circuit behavior

2. `build_market_brief()`
   - reads holdings, market features, news context, sector rotation
   - filters non-tradable research rows

3. `run_quant_baseline_async()`
   - produces deterministic base weights

4. LLM research stages
   - researcher
   - bull researcher
   - bear researcher
   - cross examination
   - synthesizer

5. proposal shaping / position governance / target builder
   - proposal shaper protects against obvious bad raw LLM outputs
   - position governance validates per-position state and permissions
   - target builder creates deterministic target weights

6. risk manager
   - validates final target weights
   - produces approval token when approved

7. decision ledger
   - builds per-ticker audit trail
   - explains how each ticker moved through strategy, LLM, construction, risk, and governance

8. communicator / executor
   - Telegram reporting
   - SEMI_AUTO pending proposal or FULL_AUTO execution

## 3. Circuit Breaker Behavior

Relevant files:

- `services/pipeline.py`
- `services/telegram_commands.py`
- `services/circuit_breaker.py`

### Current Behavior

When `authorization_mode == FULL_AUTO` and circuit is `ALERT` or `DEFENSIVE`, `_guard_and_config()` checks for a one-shot circuit override.

If no override exists:

- pipeline pauses
- Telegram alert is sent
- alert is throttled through `circuit_pause_alert`

If `/confirm` is received and no pending proposal exists:

- `services/telegram_commands.py` creates `circuit_override`
- value is `ONE_SHOT`
- `uses_remaining = 1`
- expiry is 30 minutes

The next FULL_AUTO pipeline run consumes this override and continues.

Important: this does not close the circuit. It only allows the next run to proceed under DEFENSIVE risk context.

`/reset_circuit` remains the command for closing the circuit after the underlying condition is resolved.

### Current Risk

The override is time-based and one-shot, which is good. However, operator messaging should remain precise:

- `/confirm`: "run once under defensive constraints"
- `/reset_circuit`: "risk condition resolved"

## 4. Group And Factor Contract

Relevant file:

- `services/group_contract.py`

The system now separates two concepts:

### Primary Group

Each ticker has one `primary_group`.

Used for:

- basket review
- governance
- correlated loss review

Example:

```python
PRIMARY_GROUP["SOXX"] = "semiconductors"
PRIMARY_GROUP["PSI"] = "semiconductors"
PRIMARY_GROUP["FTXL"] = "semiconductors"
```

### Factor Tags

Each ticker can have multiple factor tags.

Used for:

- factor exposure calculation
- portfolio construction

Example:

```python
FACTOR_TAGS["SOXX"] = ("tech_growth", "semiconductors")
FACTOR_TAGS["PSI"] = ("tech_growth", "semiconductors")
FACTOR_TAGS["FTXL"] = ("tech_growth", "semiconductors")
```

This solves the old conflict where a ticker had to belong to exactly one group even though it might contribute to multiple exposure dimensions.

### Review Point

This contract should probably become the source of truth for both FastAPI and QuantConnect-side role/limit policy. Right now QC still has its own universe lists and caps.

## 5. Portfolio Construction Model

Relevant file:

- `services/portfolio_construction.py`

Current state: shadow mode only.

The model:

- starts from base weights
- applies factor limits
- tightens baskets under active review
- applies no-add permissions
- allocates turnover budget by signal strength
- computes factor exposure and effective N
- emits deterministic diagnostics

It explicitly does not consume raw LLM weights.

Pipeline integration:

- stage: `5e_portfolio_construction_shadow`
- stored in `pipeline_context["portfolio_construction_shadow"]`
- later attached to `risk_out`
- surfaced in decision ledger

### Review Point

This is currently not execution-gated. It is an audit layer. The next architectural decision is whether to promote it from shadow to gated:

```text
base_weights -> portfolio_construction -> target_builder -> risk_manager
```

Instead of:

```text
base_weights -> target_builder -> risk_manager
portfolio_construction shadow only
```

## 6. Target Builder

Relevant file:

- `services/target_builder.py`

Current state: gated execution input.

The target builder creates deterministic target weights from:

- base weights
- current weights
- market scorecard
- decision style
- position governance
- validated advisory deltas
- turnover and single-delta constraints

It does not directly consume raw LLM `adjusted_weights`.

Important diagnostic fields:

- `mode`
  - `target_builder_gated`
  - `target_builder_shadow`
- `raw_llm_adjusted_weights_consumed = False`
- `target_construction_source = deterministic_target_builder`

Pipeline integration:

- gated stage: `5e_target_builder_gated_input`
- shadow comparison stage can still run later

### Review Point

The target builder enforces FastAPI-side risk constraints, but it does not currently know the QC-side satellite/hedge caps unless those caps are represented upstream. That caused the PSI rejection issue.

## 7. Strategy Evidence, Certification, And Walk-Forward Validation

Relevant files:

- `services/evidence_bundle.py`
- `services/strategy_certification.py`
- `services/walk_forward_validation.py`
- `services/playground.py`

### Evidence Bundle

`build_evidence_bundle()` combines:

- market facts
- sector rotation
- news evidence
- strategy evidence
- knowledge base resolution
- calibration
- certification

Strategy confidence is calibrated before certification.

### Certification

`certify_strategies()` returns:

- `items`
- `summary`
- `audit`
- `policy`

The audit layer reports:

- promotion candidates
- suggested advisory but not certified for execution
- disabled or experimental strategies
- operator review requirements
- walk-forward status
- blockers and demotion reasons

Certification does not directly grant execution authority.

### Walk-Forward Validation

`validate_walk_forward()` audits strategy returns across chronological folds.

It returns:

- fold count
- valid fold count
- pass rate
- positive Sharpe rate
- median and worst Sharpe
- stability score
- level: `high`, `medium`, `weak`, `insufficient`
- `execution_authority = none`

Weak or insufficient walk-forward evidence can block or demote strategy certification.

### Review Point

Walk-forward is currently an evidence quality layer. It should stay separated from execution authority. Execution should continue to flow through risk manager and governance.

## 8. Sector Rotation Signal Upgrade

Relevant files:

- `services/sector_rotation.py`
- `services/market_brief.py`
- `services/portfolio_construction.py`

Sector rotation has been upgraded from prompt-only context to deterministic signal input.

`rotation_signal_strengths()` converts sector rotation data into signed deterministic signals. These signals are included in market brief and can influence portfolio construction signal strength.

### Review Point

This gives sector rotation real portfolio influence, but in a controlled way. It should be reviewed alongside turnover allocation to avoid overreacting to short-term sector moves.

## 9. Thesis Review Scheduler

Relevant file:

- `services/thesis_scheduler.py`

The scheduler decides when a position requires thesis review.

Triggers include:

- loss review
- loss trim candidate
- basket review
- hard risk review
- never reviewed
- scheduled review after 5 days
- PnL drift greater than 3%

It has no execution authority.

Position governance includes the queue under:

```python
portfolio_summary["thesis_review_queue"]
```

### Review Point

This creates a refresh loop for position thesis quality. The next useful improvement would be persisting review completion metadata consistently so the scheduler can distinguish "review required" from "review already handled."

## 10. Position Governance

Relevant file:

- `services/position_governance.py`

Position governance produces:

- per-ticker decision
- allowed actions
- action permission
- reason codes
- basket reviews
- manual action hints
- thesis status summary
- thesis review queue
- position explanations

The original `position_explanations` are generated by:

- `_position_explanations()`
- `_explain_position()`
- `_why_hold()`
- `_why_not_add()`
- `_why_not_exit()`
- `_next_trigger()`

These explanations are rule-based and deterministic. They are good for governance compliance, but by themselves they can be formulaic when multiple tickers share the same state and reason codes.

## 11. Decision Ledger And Enhanced Position Explanation

Relevant files:

- `services/decision_ledger.py`
- `dashboard/app.py`
- `agents/communicator.py`

The decision ledger is now the authority for "why did this ticker end up here?"

For each ticker it builds:

- current holding state
- historical evidence
- intraday evidence
- position governance evidence
- source effects
- trade lifecycle
- LLM advisory compact summary
- proposed action
- final action
- execution status
- enhanced explanation

### Trade Lifecycle

Per ticker:

```text
current_weight
base_weight
strategy_target
synthesizer_target
diagnostic_llm_target
portfolio_construction_target
target_builder_target
validated_advisory_delta
risk_target
governance_target
final_target
changed_by
```

This is the most important audit structure for reviewing LLM-to-execution behavior.

### Enhanced Explanation Fields

The ledger enriches governance explanation with:

- `strategy_intent`
- `llm_effect`
- `construction_effect`
- `risk_governance_effect`
- `final_explanation`
- `execution_chain`

Example meaning:

```text
strategy_intent:
  quant baseline=12.0%; strategy consensus=10.0%; support=advisory

llm_effect:
  raw LLM target recorded as diagnostic only

construction_effect:
  target builder deterministic target=11.0%

risk_governance_effect:
  governance decision=hold; scorecard requires human confirmation

final_explanation:
  QQQ hold at 11.0%; PnL=4.9%; risk_budget=medium; strategy_support=advisory; changed_by=target_builder_target
```

Dashboard merges governance explanations with ledger explanations in:

- `dashboard/app.py`
- `_enrich_position_explanations_from_ledger()`

Telegram compact decision ledger also includes final explanation.

### Update Timing

These fields update when the pipeline reaches:

```text
6d_decision_ledger
```

The dashboard reads the latest `AgentAnalysis.risk_output.decision_ledger`. Old analysis records are not retroactively updated.

## 12. Execution And QuantConnect Rejection Issue

Relevant files:

- `tools/qc_tools.py`
- `agents/executor.py`
- `quantconnect_files/test1.py`

Current FastAPI behavior:

- `tool_send_weight_command()` posts a command to QuantConnect Live Command API.
- If API returns `success=True`, FastAPI treats the command as submitted.
- Executor currently sends "Order executed" after successful command submission.

Important distinction:

```text
QC API accepted the command != QC algorithm accepted and executed the weights.
```

The QC algorithm still performs its own validation in `on_command()` and `_validate_weights()`.

Recent rejection example:

```text
single weight rejected: PSI=5.35% > 5.00%
```

Cause:

- FastAPI target output sent PSI above 5%.
- QC algorithm had satellite single cap of 5%.
- QC rejected the whole SetWeights command.
- FastAPI only saw command-submission success, so Telegram did not report final rejection.

### Required Fix

Two changes are still needed:

1. Mirror QC execution limits in FastAPI before command submission.
2. Change Telegram wording from "Order executed" to "Command submitted" unless we have algorithm-level confirmation.

Best longer-term fix:

- QC algorithm sends command result callback to FastAPI.
- FastAPI updates `ExecutionLog` and Telegram with final accepted/rejected status.

## 13. Universe Policy And Hedge Instruments

Relevant files:

- `quantconnect_files/test1.py`
- `services/universe_policy.py`

Original setup:

- `CORE_UNIVERSE`
- `SATELLITE_UNIVERSE`
- `WATCHLIST_ONLY`

The issue:

- `WATCHLIST_ONLY` meant "observe but never trade."
- This was too restrictive for inverse/leveraged/volatility ETFs if they are intended as hedge tools.

Recent adjustment:

- `DRAM` moves into satellite.
- leveraged/inverse/volatility products move into hedge universe.
- hedge products become tradable but tightly capped.

Current intended policy:

```text
Core:
  normal long ETF exposure, highest cap

Satellite:
  thematic / sector / tactical long exposure, moderate cap

Hedge:
  inverse / leveraged / volatility tools, tradable only under hedge intent and tighter caps
```

Proposed QC hedge caps:

```text
single hedge max: 3%
total hedge max: 8%
```

FastAPI research policy:

- hedge tickers should not enter ordinary strategy scoring
- they should be introduced through explicit hedge/risk/defensive logic

### Review Point

The user preference is that these instruments can be traded as hedges. That is compatible with the current direction, but the hedge intent path is not fully built yet. Right now they are tradable at QC level, but the FastAPI strategy stack still mostly excludes them from ordinary strategy selection.

## 14. Current Safety Boundaries

Current system safety boundaries:

- raw LLM adjusted weights are not direct execution authority
- portfolio construction shadow does not approve execution
- target builder is deterministic and gated
- risk manager remains final approval gate
- position governance controls per-position actions
- certification and walk-forward are evidence quality, not execution authority
- thesis scheduler creates review queue, not trades
- QC algorithm remains final last-resort guardrail

## 15. Known Gaps

### 15.1 FastAPI and QC Limits Are Not Fully Unified

QC has universe and target caps. FastAPI has scorecard/risk/target-builder limits. They are related but not yet sourced from one shared policy.

This caused PSI rejection.

Recommended next step:

Create a shared execution policy contract:

```text
ticker -> role
role -> max_single_weight
role -> max_total_group_weight
role -> execution permissions
```

Then use it in:

- FastAPI target builder
- risk manager
- position governance
- QuantConnect algorithm
- dashboard explanations

### 15.2 Execution Acknowledgement Is Incomplete

FastAPI currently knows command submission result, not algorithm-level execution result.

Recommended next step:

- rename Telegram message to "Command submitted"
- add QC callback endpoint for command accepted/rejected
- update `ExecutionLog` after QC algorithm result

### 15.3 Portfolio Construction Is Still Shadow

It is useful but does not yet drive final targets.

Recommended next step:

Promote it to gated only after comparing shadow outputs against actual final targets for several cycles.

### 15.4 Hedge Intent Path Is Not Fully Designed

Hedge instruments are now allowed at QC level, but ordinary strategy scoring still excludes them.

Recommended next step:

Create explicit hedge proposal logic:

- allowed only in defensive/high-vol/drawdown regimes
- tied to net exposure reduction
- capped by hedge policy
- shown separately in decision ledger

### 15.5 Test Discovery Import Pollution

`uv run python -m unittest discover tests` still has a known import-order/mock-pollution failure around `test_sector_rotation` and `MarketDailyFeature`.

Targeted and wide test sets pass. This is a test isolation issue, not currently a business logic failure.

## 16. Review Checklist

Use this checklist during review:

- Does `decision_ledger.explanation.final_explanation` answer why each ticker ended where it did?
- Are `llm_effect` messages clear enough to show raw LLM weights were not executed?
- Should portfolio construction move from shadow to gated?
- Should satellite single cap remain 5%, or should thematic ETFs have a 7.5% middle tier?
- Should hedge instruments use 3% single / 8% total, or another policy?
- Should QC execution result be callback-driven before Telegram says "executed"?
- Should all role/cap policy move into one shared contract?
- Should dashboard expose hedge/satellite/core roles in the Position Explanations table?

## 17. Suggested Next Sprint

Recommended Sprint 8:

```text
Execution Policy Contract + QC Ack Fix
```

Scope:

1. Add shared execution policy module.
2. Encode role caps: core, sector, thematic, satellite, hedge.
3. Apply caps before command submission.
4. Align QC algorithm constants with policy.
5. Change Telegram execution wording.
6. Add optional QC command result callback.
7. Extend decision ledger explanations with role/cap effects.

This would close the biggest current gap: FastAPI approving weights that QC later rejects.

