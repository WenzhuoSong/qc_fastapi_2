# Position Lifecycle And Proposal Shaping Plan

Last updated: 2026-05-18

## Purpose

The system now has strong risk rejection and audit visibility, but recent live
runs show a new problem:

```text
large proposal
  -> risk/style rejection
  -> no execution
  -> repeated Telegram blocked reports
```

The next goal is to reduce obviously invalid proposals before Risk Manager while
keeping Risk Manager, Position Governance, and Executor as the final referees.

Core rule:

```text
Proposal shaping narrows execution space.
It does not approve trades.
It does not replace Position Governance.
```

## Current Problems

- `data=limited` is too vague. It mixes fresh QC heartbeat, sparse daily
  snapshots, live-fit insufficiency, and strong yfinance history into one label.
- PM/synthesizer proposals can still be too large under `human_required`,
  `small_overweight_only`, or `data_limited`.
- Loss-review tickers can still appear as add proposals upstream, even though
  later governance/risk layers block them.
- Theme/correlation risk is under-modeled. `XLK + QQQ + SOXX + PSI + FTXL` is
  multiple tickers but one large tech/growth/semiconductor risk cluster.
- `why_hold` explains current rule state, but not whether the original thesis is
  still intact.

## Phase A: Low-Risk Preflight Controls

Status: implemented.

Implemented in:

- `services/proposal_shaper.py`
- `services/pipeline.py`
- `agents/communicator.py`
- `tests/test_proposal_shaper.py`

Current behavior:

- Blocks adds into current holdings whose unrealized PnL is at or below `-4%`.
- Under constrained execution states, caps single-ticker delta before Risk
  Manager sees the proposal.
- Under constrained execution states, caps pre-risk turnover before Risk Manager.
- Writes `proposal_shaping` into synthesizer output.
- Logs full shaper output in `AgentStepLog` stage `5d_proposal_shaper` when it
  clips.
- Telegram displays:
  - `Data quality detail`
  - `Proposal shaping`

Constrained states:

- `require_human_confirmation = true`
- `investment_permission` in `small_overweight_only`, `hold_or_trim`,
  `reduce_risk_only`, `defensive_only`, or `cash_only`
- scorecard `data_quality` in `limited`, `missing`, `stale`, or `unknown`
- trade style in `hold_unless_strong`, `risk_reduce_fast`, or `cash_only`

Default Phase A limits:

```text
loss_review_no_add_threshold = -4%
constrained_max_single_delta = 1.5%
constrained_max_turnover = 5%
```

Acceptance checks:

- A ticker in loss review cannot be increased by the pre-risk proposal.
- `human_required + data_limited` proposals are clipped before Risk Manager.
- Shaper output is observability-only for approval; Risk Manager still decides.

## Phase B: Position Lifecycle V1

Status: implemented.

Add deterministic lifecycle states:

```text
supported_winner
unsupported_winner
normal_hold
loss_review
loss_trim_candidate
hard_risk_review
replacement_candidate
```

Implemented improvements:

- Different loss thresholds for core versus satellite/thematic ETFs.
- Cluster-level `basket_review` when multiple correlated tickers are in
  loss-review state.
- Manual-confirmation path for risk-reducing trims when `human_required` blocks
  FULL_AUTO execution.
- Telegram summary of lifecycle state for top problem holdings.

Implemented in:

- `services/position_governance.py`
- `services/pipeline.py`
- `agents/communicator.py`
- `tests/test_position_governance.py`

Current lifecycle states:

```text
supported_winner
unsupported_winner
normal_hold
loss_review
loss_trim_candidate
hard_risk_review
replacement_candidate
```

Human confirmation behavior:

- Risk-reducing trims remain non-executing when execution is constrained.
- The governance output records `manual_action_hints`.
- Telegram can display `manual trim review` rows for operator action.

## Phase B.1: Explanation Correctness

Status: implemented.

Nature:

```text
Explanation-only fix. Does not change target weights, decisions, or execution.
```

Problem:

- `hard_risk_review` must never explain a holding as "no deterministic rule
  requires reduction".
- Repeated loss-review explanations should include basket context when a
  correlated basket is under review.
- `advisory` support should not be worded like strong protection for a losing
  satellite/thematic position.

Required `explanation_facts` contract:

```json
{
  "severity": "hard_risk|basket_review|loss_review|normal",
  "primary_reason": "hard_risk_event_active",
  "execution_blocker": "human_required|risk_rejected|null",
  "risk_action": "manual_trim_review|hold_review|trim|hold",
  "basket_context": {
    "group": "semiconductors",
    "tickers": ["FTXL", "PSI", "SOXX"],
    "trigger": "multiple_loss_review_positions"
  },
  "thesis_status": {
    "status": "intact|weakening|broken|unknown",
    "evidence": []
  }
}
```

Basket-review trigger:

```text
same group
AND at least 2 held tickers with current_weight > 1%
AND at least 2 tickers have unrealized_loss_review OR risk_budget_status=high
```

Acceptance criteria:

- Hard-risk explanations always mention hard-risk/manual review.
- Basket-review explanations mention correlated basket risk.
- Advisory-only support is described as limited support, not strong support.
- Explanation facts include `thesis_status`.
- No target weight or decision changes are introduced in this phase.

Implemented in:

- `services/position_governance.py`
- `tests/test_position_governance.py`

## Phase C: Thesis Status

Status: implemented.

Add a structured research judgment for each problem holding:

```text
thesis_status = intact | weakening | broken | unknown
```

Required evidence:

- strategy support and suggested use
- live fit and current regime alignment
- relative strength / momentum
- news bias and hard-risk events
- theme/basket behavior
- current PnL and drawdown
- macro conflict

Guardrails:

- LLM may propose thesis status.
- Python validator records whether supporting evidence exists.
- `thesis_status` alone cannot execute trades.
- `broken` can escalate to trim/exit review only through Position Governance.

Contract:

```text
Owner: position_governance
LLM role: advisory proposal only
Validator: position_governance Python validator
Execution authority: none
```

Raw LLM `thesis_status` must never change target weights, force trims, or
create an exit/trim decision by itself. It can only become part of the validated
thesis record and downstream explanation.

Implemented in:

- `services/position_governance.py`
- `agents/synthesizer.py`
- `agents/communicator.py`
- `tests/test_position_governance.py`
- `tests/test_synthesizer_contract.py`

Current behavior:

- Position Governance computes `thesis_status` for every decision row.
- LLM-proposed `thesis_status` from `position_advisory_proposals` is accepted
  only when supported by deterministic evidence; otherwise the validator
  records an override/rejection.
- LLM thesis-only proposals cannot change target weights, force trims, or alter
  position decisions.
- `portfolio_summary.thesis_status_summary` reports weakening/broken tickers.
- Telegram can show thesis status for problem holdings.
- `thesis_status.execution_authority = none`; target changes still come only
  from governance/risk rules.

## Phase D: Advisory Support Governance Change

Status: implemented with FULL_AUTO gated risk-reduction execution.

Nature:

```text
Governance behavior change. This is not an explanation-only fix.
```

Potential rule:

```text
satellite/thematic ETF
AND loss <= -6%
AND support <= advisory
AND basket_review active
=> trim_review or manual_trim_review
```

Implemented default behavior:

- Satellite/thematic basket losers at or below `-6%` with advisory-or-weaker
  support are escalated to `trim_review`.
- The rule adds `advisory_basket_loss_review` to reason codes.
- It records `manual_trim_review` hints with a default 1% review trim target.
- In `SEMI_AUTO`/`MANUAL`, it does not change target weights by default.
- In `FULL_AUTO`, deep basket losers at or below the configured trim threshold
  can auto-trim by a small fixed step.
- Default auto-trim step is `advisory_basket_loss_auto_trim_pct = 1%`.
- The auto-trim path adds `advisory_basket_loss_auto_trim` to reason codes.
- Telegram manual trim hints explicitly label this case as
  `advisory=weak-positive, basket loss review`.

Required guardrails before implementation:

- Separate tests from explanation-only changes.
- Default to diagnostic/manual hints first.
- FULL_AUTO trims must stay risk-reducing only: no adds, no replacements, no
  LLM-only execution, and no full exits from this rule.
- Telegram must explicitly show that advisory support was treated as
  weak-positive support, not primary support.

Implemented in:

- `services/position_governance.py`
- `services/pipeline.py`
- `agents/communicator.py`
- `tests/test_position_governance.py`
- `tests/test_communicator_scorecard.py`

## Live Validation Checklist

For the next Railway pipeline runs:

- Telegram includes `Data quality detail`.
- Telegram includes `Proposal shaping` when PM proposal is clipped.
- Loss-review tickers no longer show large add proposals before Risk Manager.
- Failed checks show fewer huge single-ticker deltas and lower turnover.
- Decision ledger still distinguishes proposed, final, and actual execution.
- Risk Manager remains the final approval gate.
