# Decision Evidence Utilization Plan

Last updated: 2026-05-18

## Purpose

The system already collects useful trading information: news analysis, ETF
features, historical replay, live QC data, strategy confidence, scorecard
permissions, position governance, risk output, and execution audit.

The next problem is not adding more data. The problem is proving that every
action recommendation actually used the relevant evidence, and proving why a
position was held, trimmed, blocked, or not executed.

Goal:

```text
raw information
  -> structured evidence
  -> risk/governance validation
  -> initial per-ticker decision ledger
  -> communicator / executor
  -> execution audit attachment
  -> Telegram/final report
  -> daily memory/backfill
```

The core deliverable is a per-ticker decision ledger. It should make each
tradable symbol answer:

- What evidence was available?
- Which evidence was actually used?
- What action was proposed?
- What did risk/governance clip or block?
- What final action was executable?
- Was anything actually sent or filled?

## Current Coverage

Existing modules already cover most building blocks:

- `services/evidence_bundle.py`
  - Collects market, news, strategy, knowledge, memory, and data-quality facts.
- `services/market_scorecard.py`
  - Converts evidence into deterministic market permission.
- `services/knowledge_resolver.py`
  - Merges static knowledge with computed facts and emits constraints/conflicts.
- `services/strategy_confidence_calibrator.py`
  - Applies accepted confidence adjustments exactly once.
- `services/strategy_certification.py`
  - Labels strategy evidence as experimental, research-supported, advisory, or disabled.
- `services/empirical_profiles.py`
  - Generates data-driven ETF behavior profiles.
- `services/position_governance.py`
  - Produces deterministic per-position decisions and explanations.
- `services/execution_audit.py`
  - Records proposed/sent/accepted/rejected/skipped execution outcomes.
- `agents/communicator.py`
  - Displays scorecard, strategy certification, governance, and execution status.

Related docs:

- `docs/agent_evidence_scorecard_plan.md`
- `docs/position_governance_status_plan.md`
- `docs/trading_knowledge_base.md`
- `docs/system_optimization_backlog.md`

## Main Gap

Evidence is still distributed across several objects:

- `brief`
- `evidence_bundle`
- `market_scorecard`
- `strategy_confidence`
- `strategy_certification`
- `position_governance`
- `risk_out`
- `execution_log`
- `decision_memory`

This makes the system powerful but harder to audit. A report can say QQQ should
be trimmed, but the user should be able to inspect one object and see:

```json
{
  "ticker": "QQQ",
  "evidence_used": {
    "news": [],
    "historical": {},
    "intraday": {},
    "strategy": {},
    "position": {},
    "scorecard": {}
  },
  "proposed_action": "trim",
  "risk_result": "blocked",
  "final_action": "none",
  "execution_status": "not_sent",
  "reason_codes": ["style_delta_limit", "human_required"]
}
```

Without this ledger, Telegram can be understandable but still not fully
auditable.

## Design Principles

### 1. Evidence Must Be Structured Before It Reaches LLMs

LLMs can explain and propose advisory changes, but the system should not rely on
free-form text to decide whether evidence exists.

Required:

- every evidence source has source, timestamp, freshness, and confidence
- missing data is explicit
- stale data lowers permission or requires human confirmation
- execution text cannot imply a trade happened unless audit logs confirm it

### 2. Per-Ticker Decisions Must Be Reproducible

The same input bundle should produce the same decision ledger. LLM advisory may
be attached as an opinion, but deterministic validators decide final action.

### Boundary With Position Governance

`decision_ledger` is an aggregation and audit layer. It is not a new position
governance engine.

Source of truth:

- `position_governance.position_decisions` remains the source of truth for
  position action state such as hold, hold_review, trim_review, trim, add, and
  exit review.
- `position_governance.portfolio_summary.position_explanations` remains the
  source of truth for `why_hold`, `why_not_add`, `why_not_exit`, and
  `next_trigger`.
- `position_governance.blocked_actions`, `forced_trims`, and `replacements`
  remain the source of truth for governance-level intervention records.

Allowed ledger behavior:

- normalize governance fields into a per-ticker audit shape
- join governance output with current holdings, risk approval, proposed weights,
  and later execution audit
- display compact explanations and reason codes
- record missing governance output as a warning

Not allowed:

- recompute hold/trim/add/exit decisions
- infer governance decisions from PnL, ATR, strategy support, or scorecard when
  `position_governance` output is absent
- create a fallback governance path
- overwrite governance reason codes with independently derived reason codes

If position governance output is missing, the ledger should emit:

```json
{
  "warnings": ["position_governance_missing"],
  "tickers": {
    "QQQ": {
      "governance_available": false,
      "final_action": "unknown",
      "reason_codes": ["position_governance_missing"]
    }
  }
}
```

This is intentionally conservative. A missing governance layer should reduce
audit confidence, not create a guessed decision.

### Freshness Semantics

Each upstream source owns its own freshness policy. The ledger displays
freshness status but does not reinterpret timestamps across sources.

Common source freshness schema:

```json
{
  "source": "qc_intraday",
  "as_of": "2026-05-18T13:30:00Z",
  "evaluated_at": "2026-05-18T13:35:00Z",
  "is_stale": false,
  "state": "fresh",
  "policy": "qc_intraday_30m",
  "reason": "fresh"
}
```

Policy ownership:

- news freshness is owned by the news evidence/cache layer
- yfinance freshness is owned by historical feature and empirical-profile layers
- QC live/intraday freshness is owned by QC snapshot/provenance layers
- operational health can summarize freshness, but the ledger should not replace
  per-source policy decisions

Required ledger behavior:

- preserve `is_stale`, `state`, `as_of`, `evaluated_at`, and `policy` when
  available
- expose missing freshness metadata as `missing_evidence`
- avoid comparing source age windows directly, because `stale` has different
  meanings for news, yfinance, and QC

### 3. Historical And Live Data Have Different Jobs

yfinance historical data:

- replay
- Sharpe/hit-rate/drawdown
- strategy certification
- ETF empirical behavior
- historical relative strength

QC live and intraday data:

- current holdings
- latest prices
- current weight
- ATR/live volatility
- intraday momentum
- execution readiness
- fresh risk state

The ledger should show both instead of merging them into one vague confidence
number.

### 4. Proposed Trades And Executed Trades Are Different Objects

Reports must distinguish:

- strategy suggestion
- synthesizer proposal
- risk-clipped proposal
- governance-adjusted target
- position-manager order plan
- executor outcome

This prevents messages like "sell QQQ" from being mistaken for actual execution
when risk rejected the plan.

### 5. Decision Ledger Has A Two-Step Lifecycle

The ledger is first built before execution, then updated after execution audit.

Pre-execution ledger:

- built after risk manager, position governance, and position manager
- records evidence, proposed action, final validated action, and target weights
- may show `execution_status = not_sent`, `pending`, or `unknown`
- must not claim that a broker/QC order was sent or filled

Post-execution ledger update:

- attaches executor audit through `apply_execution_audit_to_decision_ledger(...)`
- records `execution_status` from the executor/audit payload
- records `actual_execution_action`
- preserves `final_action` as the planned/validated action

Field semantics:

- `proposed_action`: what upstream alpha/synthesizer wanted
- `final_action`: what deterministic validation allowed before execution
- `actual_execution_action`: what actually happened after audit; rejected,
  failed, or skipped execution maps to `none`
- `execution_status`: audit status such as proposed, sent, accepted, rejected,
  skipped, failed, or filled

Accurate pipeline shape:

```text
position manager
  -> initial decision ledger
  -> communicator / executor
  -> execution audit
  -> updated decision ledger
  -> daily memory / review
```

## Target Contract

Create:

```text
services/decision_ledger.py
```

Primary function:

```python
def build_decision_ledger(
    *,
    evidence_bundle: dict,
    market_scorecard: dict,
    strategy_output: dict | None,
    synthesizer_output: dict | None,
    risk_output: dict | None,
    position_governance: dict | None,
    execution_audit: dict | None,
    current_holdings: dict | None,
) -> dict:
    ...
```

Suggested output:

Note: `trade_lifecycle` is completed in Phase 3. Phase 1 should reserve this
field as `null` or a sparse placeholder instead of trying to reconstruct the
full lifecycle early.

```json
{
  "generated_at": "2026-05-18T13:30:00Z",
  "portfolio_summary": {
    "market_permission": "small_overweight_only",
    "require_human_confirmation": true,
    "risk_approved": false,
    "execution_status": "not_sent",
    "turnover": 0.2275
  },
  "tickers": {
    "QQQ": {
      "current": {
        "weight": 0.1175,
        "quantity": 18,
        "unrealized_pnl_pct": 0.0681,
        "holding_days": 24
      },
      "evidence_used": {
        "news": {
          "bias": "neutral",
          "hard_risk": false,
          "confidence": "medium",
          "source_state": "fresh"
        },
        "historical": {
          "source": "yfinance",
          "relative_strength": "positive",
          "empirical_profile_status": "fresh"
        },
        "intraday": {
          "source": "QC",
          "atr_pct": 0.018,
          "momentum_15m": "mixed",
          "source_state": "fresh"
        },
        "strategy": {
          "support": "advisory",
          "supporting_strategies": ["momentum_lite_v1"],
          "confidence": 0.568,
          "certification": "research_supported"
        },
        "scorecard": {
          "permission": "small_overweight_only",
          "human_required": true
        },
        "position_governance": {
          "decision": "trim_review",
          "risk_budget_status": "medium"
        }
      },
      "trade_lifecycle": {
        "base_weight": 0.118,
        "strategy_target": 0.100,
        "synthesizer_target": 0.097,
        "risk_clipped_target": 0.118,
        "governance_target": 0.118,
        "final_target": 0.118
      },
      "proposed_action": "trim",
      "final_action": "none",
      "execution_status": "not_sent",
      "reason_codes": [
        "human_required",
        "style_delta_limit",
        "risk_rejected"
      ],
      "explanation": {
        "why_hold": ["risk manager rejected executable rebalance"],
        "why_not_add": ["scorecard requires human confirmation"],
        "why_not_exit": ["no hard-risk exit trigger is active"],
        "next_trigger": "manual confirmation or lower-risk proposal required"
      }
    }
  },
  "missing_evidence": [],
  "warnings": []
}
```

## Evidence Source Requirements

### News Analysis

Required fields:

- `bias`: positive / neutral / negative
- `confidence`: low / medium / high
- `impact`: low / medium / high
- `affected_tickers`
- `hard_risk_tickers`
- `source_state`: fresh / stale / missing
- `reason_codes`

Usage:

- hard-risk tickers enter position governance as trim/exit review
- negative high-confidence news can block adds
- stale news does not block execution alone, but lowers confidence or requires
  human confirmation when combined with weak data quality

### ETF Data

Historical daily layer:

- source: yfinance
- lookback samples
- returns and volatility
- drawdown
- relative strength versus SPY
- empirical profile status
- correlation/risk group if available

Live/intraday layer:

- source: QC
- current price
- current weight
- latest ATR
- intraday momentum, preferably 15-minute features
- live data timestamp
- stale/missing warnings

Usage:

- historical layer supports strategy replay and empirical behavior
- live layer governs execution readiness and current risk
- missing live data should prevent aggressive new risk

### Strategy Information

Required fields:

- strategy confidence score
- suggested use: primary / advisory / watch_only / ignore
- certification status
- historical samples and reliability
- live samples and fit
- turnover and cost risk
- selected tickers and target weights
- reason codes

Usage:

- primary/advisory support can justify holds/adds within scorecard limits
- watch_only cannot create adds
- disabled/ignore cannot influence allocation
- high turnover triggers stricter clipping

### Trade Plan Information

Required fields:

- current weights
- base weights
- strategy weights
- synthesizer proposed weights
- risk-clipped weights
- governance-adjusted weights
- final executable weights
- proposed action by ticker
- final action by ticker
- execution audit outcome

Usage:

- Telegram and daily memory must show whether proposal changed after risk
- executor can only act on final executable weights
- decision analysis compares proposed versus actual execution, not text summary

### Position And PnL

Required fields:

- current quantity
- current market value
- current weight
- average cost
- unrealized PnL and PnL %
- holding days
- group exposure and group headroom
- risk contribution

Usage:

- losing holdings enter review state
- deep loss plus weak support can trigger trim
- winners can be trimmed for risk budget
- held positions get deterministic explanations

### Execution Result

Required fields:

- proposed / sent / accepted / rejected / filled / skipped
- reason for skipped/rejected
- broker/QC response where available
- actual executed quantity/weight when available
- same-day execution count

Usage:

- Telegram cannot say action was taken unless execution status confirms it
- daily analyst can compare intended versus executed activity
- position manager can enforce actual daily trade count

## Development Plan

### Phase 1: Per-Ticker Decision Ledger

Status: completed.

Implemented in:

- `services/decision_ledger.py`
- `tests/test_decision_ledger.py`

Current behavior:

- Builds one row for every current holding and every proposed trade ticker.
- Uses `risk_output`, `current_holdings`, and `position_governance`.
- Keeps `position_governance` as the source of truth for action state, reason
  codes, and explanations.
- Risk-rejected plans produce `final_action = none`.
- Missing position governance emits `position_governance_missing` and does not
  trigger fallback hold/trim/add inference.

Original implementation scope:

- `services/decision_ledger.py`
- `tests/test_decision_ledger.py`

Scope:

- Build ticker rows from current holdings, risk output, and position governance.
- Record proposed action, final action, and execution status.
- Include reason codes from risk checks and position governance.
- Directly reference position governance explanations when available.
- Store missing evidence explicitly.
- Pre-reserve fields for scorecard, execution audit, ETF historical evidence,
  and ETF intraday evidence as `null` or empty objects, but do not validate
  their contents in Phase 1.

Explicitly out of Phase 1:

- no scorecard reason-code merge beyond simple risk-output fields already
  present
- no execution-audit interpretation beyond default `not_sent` or `unknown`
- no ETF historical/intraday evidence hydration
- no freshness-policy computation
- no fallback governance inference

Acceptance criteria:

- every current holding has a ledger row
- every proposed trade ticker has a ledger row
- risk-rejected plans produce `final_action = none`
- execution status is `not_sent` when Risk Manager rejects
- no LLM text is needed to compute final action
- explanation fields are copied from
  `position_governance.portfolio_summary.position_explanations`; the ledger
  does not generate new explanation prose
- `trade_lifecycle` is `null` or sparse in Phase 1 and is not required to
  contain full base/strategy/synthesizer/risk/governance/final weights until
  Phase 3
- missing position governance emits `position_governance_missing` warning
- missing position governance does not trigger fallback hold/trim/add inference
- scorecard, execution audit, and ETF evidence placeholders may be present but
  are not required to be populated in Phase 1

### Phase 2: ETF Evidence Layer Contract

Status: completed for the current available data sources.

Implemented behavior:

- `historical` evidence is hydrated from yfinance empirical profiles when
  present in the evidence bundle / knowledge resolver output.
- `intraday` evidence is hydrated from current holdings / QC live fields such as
  price, returns, momentum, RSI, ATR, and weight drift when present.
- `feature_sources` are preserved as upstream provenance.
- Ledger does not recompute freshness. It only exposes upstream source/freshness
  metadata when available.
- Missing historical or intraday evidence remains explicit as `None` or
  placeholder `missing`.

Original implementation scope:

- historical daily evidence section from yfinance/empirical profiles
- live intraday evidence section from QC facts already present in brief/snapshot
- feature provenance and freshness fields per ticker

Acceptance criteria:

- ledger separates `historical` and `intraday`
- missing yfinance does not erase QC live facts
- missing QC live facts blocks or downgrades aggressive actions
- stale features appear in `missing_evidence` or `warnings`

### Phase 3: Trade Lifecycle Trace

Status: completed for currently stable pipeline stages.

Implemented behavior:

- Per-ticker lifecycle now records:
  - current weight
  - Stage 2 base weight
  - Playground consensus / strategy target
  - Stage 5 synthesizer target
  - Risk Manager target
  - Position Governance target
  - final target
- `changed_by` records which stages changed the target.
- Risk-rejected plans keep `final_target = current_weight`.
- Lifecycle remains an audit trace only and does not decide `final_action`.

Original implementation scope:

- normalized weight trace per ticker:
  - current
  - base
  - strategy
  - synthesizer
  - risk clipped
  - governance adjusted
  - final executable
- lifecycle summary at portfolio level

Acceptance criteria:

- Telegram/debug logs can show exactly where a trade changed
- clipped/blocked reason is attached to the stage that caused it
- risk rejected plans cannot produce executable target deltas

### Phase 4: Pipeline And Memory Wiring

Status: completed.

Implemented behavior:

- Pipeline builds `risk_out["decision_ledger"]` after Position Governance /
  Position Manager and before finalization/communicator.
- Pipeline writes full ledger to `AgentStepLog` stage `6d_decision_ledger`.
- Ledger build failure is non-fatal and is recorded as a failed step log.
- Daily decision memory stores compact ledger fields via
  `services/decision_ledger_memory.py`.

Original integration scope:

- `services/pipeline.py`
- `services/decision_memory.py`
- `AgentStepLog`

Acceptance criteria:

- `decision_ledger` is written to step logs every pipeline run
- daily memory stores compact portfolio summary and ticker decisions
- missing ledger does not break execution, but emits an explicit warning

### Phase 5: Telegram And Final Report

Status: completed for deterministic/fallback communicator path.

Implemented behavior:

- Communicator compacts full ledger into at most five top decisions.
- Fallback Telegram output shows `proposed_action -> final_action`.
- Sorting prioritizes blocked proposed trades, hard-risk/review/trim decisions,
  and higher-risk rows.
- Complete ledger remains in step logs; Telegram only shows compact projection.
- LLM prose cannot alter ledger fields. The deterministic fallback uses ledger
  final action directly.

Remaining enhancement:

- Approved LLM-formatted communicator path can later receive stricter prompt
  rules requiring it to cite the compact ledger, but execution correctness
  already remains outside the LLM path.

Original communicator target:

```text
Decision evidence
  News: negative/high, hard risk XLRE,XLV
  Historical: strong, yfinance samples 289
  Live QC: insufficient, snapshots 7
  Strategy: advisory only, no primary
  Scorecard: human_required
  Final: no execution

Top decisions
  QQQ: proposed trim -> blocked | human_required, style_delta_limit
  XLK: hold | winner_trim_review, no add under human_required
  XLRE: manual review | hard_risk
```

Acceptance criteria:

- report distinguishes proposed action from final action
- report distinguishes risk rejection from execution failure
- top ticker explanations come from ledger fields
- LLM prose is secondary and cannot contradict ledger final action
- Telegram shows at most five ticker decision lines by default
- ticker lines are sorted by governance severity first, then risk rank when
  available
- the complete ledger is stored in step logs rather than fully printed to
  Telegram

### Phase 6: Backfill And Decision Quality Review

Status: completed for diagnostic proposed-vs-final review.

Implemented behavior:

- `services/decision_ledger_memory.py` builds compact ledger memory.
- Daily decision memory stores:
  - `decision_ledger`
  - `decision_ledger_available`
  - `decision_ledger_review`
- `decision_ledger_review` summarizes:
  - proposed action counts
  - final action counts
  - blocked proposed actions
  - changed stages
  - top examples
- Daily analyst logs a diagnostic-only `Decision ledger review` summary.
- Existing advisory outcome backfill remains the base path; no duplicate
  backfill job was introduced.
- Review output has `execution_impact = none` and does not affect execution,
  DQS, strategy selection, or validators.

Execution audit closure:

- `apply_execution_audit_to_decision_ledger(...)` attaches executor audit
  status to the ledger after `_save_execution`.
- `final_action` remains the planned/validated action.
- `actual_execution_action` records whether execution actually happened:
  accepted/sent/filled/proposed map to the planned action; rejected/failed/
  skipped map to `none`.
- Pipeline writes the updated ledger back into `AgentAnalysis.risk_output`.

Original review target:

Extend the existing advisory outcome backfill path rather than creating a
parallel review system immediately.

Existing base:

- `position_advisory_outcomes`
- `position_advisory_quality`
- next-day return versus SPY diagnostic labels

Future name if it becomes broader:

```text
decision_quality_review
```

Use the ledger for later daily review:

- proposed action versus final action
- final action versus next-day/next-week outcome
- blocked actions and whether block was helpful
- missing evidence frequency
- strategy support versus realized outcome

Acceptance criteria:

- daily analyst can evaluate decisions using ledger rows
- advisory quality diagnostics can reference final action and execution status
- no feedback metric affects execution until sample size is meaningful
- no duplicate backfill job is introduced until the existing advisory outcome
  backfill is explicitly extended or replaced

## Testing Requirements

Status: completed for the implemented scope.

Current focused verification:

```bash
uv run python -m unittest tests.test_decision_memory_advisory tests.test_decision_ledger tests.test_evidence_bundle tests.test_knowledge_resolver tests.test_position_governance tests.test_execution_audit tests.test_communicator_scorecard tests.test_risk_scorecard_enforcement tests.test_agent_prompt_context tests.test_strategy_use_constraints
```

Latest result: 62 related tests passed.

Unit tests:

- risk rejected means final action none
- accepted execution means Telegram may say action was sent/accepted
- proposed trade without ledger row fails test
- holding with no proposed trade still gets hold explanation
- missing yfinance creates warning but preserves QC live evidence
- missing QC live blocks or downgrades aggressive add
- LLM advisory rejected by validator appears as rejected, not final action

Integration tests:

- pipeline attaches ledger to context
- communicator uses ledger final action
- decision memory stores compact ledger summary
- risk output and execution audit remain source of truth for execution status

## Non-Goals

- Do not add another LLM agent.
- Do not let the ledger decide trades by itself.
- Do not let missing historical data automatically liquidate positions.
- Do not let LLM explanation override `final_action`.
- Do not use advisory quality feedback to loosen validators without a later
  counterfactual model and sufficient samples.

## Current Status

The decision evidence utilization mainline is complete:

```text
evidence bundle
  -> initial decision ledger
  -> pipeline step log
  -> compact Telegram display
  -> executor / execution audit when applicable
  -> updated decision ledger
  -> compact daily memory
  -> proposed-vs-final diagnostic review
```

Completed files:

- `services/decision_ledger.py`
- `services/decision_ledger_memory.py`
- `services/pipeline.py`
- `services/decision_memory.py`
- `cron/daily_analyst.py`
- `agents/communicator.py`
- `tests/test_decision_ledger.py`
- `tests/test_decision_memory_advisory.py`
- `tests/test_communicator_scorecard.py`

## Deployment Validation Checklist

Use the next live Railway runs to verify behavior end to end:

- Pipeline creates an `AgentStepLog` row with stage `6d_decision_ledger`.
- `risk_out.decision_ledger` is present on the latest `AgentAnalysis`.
- Telegram fallback output shows a compact `Decision ledger` block with at most
  five ticker rows.
- Risk-rejected plans show `proposed_action -> none`, not executed wording.
- After executor/audit runs, ledger rows include `execution_status` and
  `actual_execution_action`.
- Daily analyst memory contains `decision_ledger`, `decision_ledger_available`,
  and `decision_ledger_review`.
- Daily analyst logs a diagnostic-only `Decision ledger review` summary.
- No ledger diagnostic field changes risk approval, scorecard permission,
  position governance, or execution.

## Remaining Enhancements

These are future improvements, not blockers for the current plan:

- Add deeper outcome analysis after enough real ledger samples exist.
- Add counterfactual scoring for blocked actions before using any feedback to
  influence prompts, validators, or strategy confidence.
- Add stricter approved-path LLM communicator prompt language around compact
  ledger fields.
- Extend source-specific freshness schemas when upstream news/yfinance/QC
  providers expose richer `is_stale/state/policy` metadata.
- Add fill-level execution details if QC/broker audit exposes final filled
  quantity, fill price, and partial-fill status.
- Add DB indexes or views only if querying full ledger rows by analysis id and
  ticker becomes common.
- Add operator-facing tooling to query full ledger rows by analysis id and
  ticker.
