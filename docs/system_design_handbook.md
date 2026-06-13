# AI Trading System Design Handbook

Version: 2026-05-20

This handbook describes the current production architecture after the
LLM-to-deterministic migration in `docs/big_change.md`.

The core rule is simple:

```
LLM explains and advises.
Deterministic Python validates, constructs targets, audits, and executes.
```

Two execution-control invariants sit above the layer model:

```
Execution safety is symmetric.
Risk-intent admission is intentionally asymmetric.
```

Symmetric execution safety means both buy and sell targets must pass the same
account truth, lifecycle, fingerprint/dedupe, preflight, QC ACK, and
reconciliation controls before they count as real execution.

Asymmetric risk admission means increasing risk is harder than reducing risk.
Adds require execution-grade evidence and pass through scorecard, governance,
target-builder, sizing, cost, and risk controls. Risk-reducing trims remain
available in degraded/defensive states, but they are still capped, logged,
audited, and reconciled.

Invariant guard tests:

- `tests/test_strategy_certification.py::test_degraded_strategy_data_quality_fails_closed`
- `tests/test_strategy_certification.py::test_execution_evidence_kill_switch_round_trip_has_no_residual_state`
- `tests/test_position_governance.py::test_uncertified_strategy_downgrade_blocks_add_without_forced_exit`
- `tests/test_position_governance.py::test_uncertified_strategy_downgrade_still_allows_risk_reducing_trim`
- `tests/test_target_builder.py::test_single_delta_and_turnover_caps_are_deterministic`
- `tests/test_target_builder.py::test_single_delta_and_turnover_caps_constrain_risk_reducing_sells`
- `tests/test_scorecard_execution_semantics.py::test_insufficient_execution_evidence_blocks_automatic_adds`
- `tests/test_full_auto_llm_advisory_boundary.py::test_full_auto_llm_advisory_is_trim_only_source_contract`

Any future change that lets collected labels, alpha validation, or other data
automatically affect live-money behavior must satisfy all four conditions:

1. Failure direction is conservative.
2. Impact is bounded by existing sizing/risk limits, or those limits remain
   unchanged.
3. A kill switch or equivalent one-step rollback exists.
4. Every activation leaves a frozen audit event.

The strategy execution evidence gate satisfies this template: it changes
admission into existing small-add constraints, does not change sizing limits,
fails closed on degraded evidence, has `force_advisory_only`, and freezes
strategy evidence into the decision-funnel artifacts.

---

## 1. System Boundary

The system is not an LLM portfolio allocator. It is a deterministic trading
pipeline with LLM research support.

LLM agents may:

- summarize evidence
- compare bull/bear theses
- identify thesis weakening or uncertainty
- propose advisory lifecycle changes
- write human-readable rationale
- emit diagnostic legacy weights for observability

LLM agents must not:

- decide final action
- create reason codes
- bypass market scorecard, governance, or risk constraints
- construct executable target weights
- calculate order quantity, price, or execution commands

Executable portfolio targets are owned by deterministic services.

---

## 2. Layer Model

Current layer ownership:

```
Layer 0  Data ingestion and storage
Layer 1  Evidence bundle
Layer 2  Market scorecard / knowledge / strategy confidence
Layer 3  LLM research and advisory
Layer 4  Proposal shaping compatibility guardrails
Layer 5  Position governance and target builder
Layer 6  Risk validation
Layer 6.5 Position manager execution constraints
Layer 7  Decision ledger and execution audit
Layer 8  Communicator and dashboard monitor
Layer 9  Execution / pending proposal
```

The handoff between layers is structured data. Natural language can explain a
decision, but it cannot become an execution input.

---

## 3. Current Data Flow

The current execution flow is:

```
QC / yfinance / news / base knowledge
  -> evidence_bundle
  -> market_scorecard + decision_style
  -> quant_baseline.base_weights
  -> researcher / bull / bear / cross_exam
  -> synthesizer advisory contract
  -> position_governance validates advisory proposals
  -> target_builder constructs deterministic target_weights
  -> risk_manager validates target_builder target
  -> position_governance execution controls
  -> position_manager frequency / quantity controls
  -> decision_ledger
  -> communicator + dashboard
  -> SEMI_AUTO pending proposal or FULL_AUTO executor
```

Important split:

```
synthesizer.adjusted_weights
  = diagnostic legacy field only

target_builder.target_weights
  = deterministic execution target input
```

Risk Manager records:

```
target_construction_mode:
  target_builder_gated | deterministic_base_fallback

raw_llm_adjusted_weights_consumed:
  false
```

If target-builder input is unavailable, Risk Manager can only fall back to
`quant_baseline.base_weights`. It must not fall back to LLM weights.

---

## 4. Key Components

### 4.1 Evidence Bundle

Owner: `services/evidence_bundle.py`

Purpose:

- combines QC snapshots, yfinance history, news evidence, base knowledge, and
  strategy evidence
- preserves source quality and freshness metadata
- feeds both LLM context and deterministic constraints

Design rule:

Freshness must stay source-specific. A fresh QC heartbeat does not imply fresh
news, historical evidence, or live fit.

### 4.2 Market Scorecard

Owner: `services/market_scorecard.py`

Purpose:

- determines market permission
- sets broad constraints such as no-add, cash-only, defensive-only, human
  required, max equity, max delta, and data-quality restrictions

Downstream services may tighten scorecard constraints but must not loosen them.

### 4.3 Synthesizer

Owner: `agents/synthesizer.py`

Purpose:

- arbitrates research debate
- emits advisory proposals
- emits diagnostic `adjusted_weights` for audit compatibility

Required advisory contract:

```
position_advisory_proposals: [
  {
    ticker,
    llm_advisory,
    target_weight,
    delta_hint,
    thesis_status,
    thesis_reason,
    confidence,
    execution_authority: "none"
  }
]
```

Forbidden in LLM advisory output:

- `reason_codes`
- `final_action`
- execution authority

### 4.4 Proposal Shaper

Owner: `services/proposal_shaper.py`

Purpose:

- compatibility guardrail for diagnostic weight proposals
- blocks obvious no-add violations before risk
- logs why a diagnostic proposal would have been clipped

It is not the final target constructor.

### 4.5 Position Governance

Owner: `services/position_governance.py`

Purpose:

- computes lifecycle state
- validates LLM advisory proposals
- creates deterministic action permissions
- emits advisory overrides and manual action hints
- produces reasoned structured governance output

It is the validator between semantic advisory and deterministic target building.

### 4.6 Target Builder

Owner: `services/target_builder.py`

Purpose:

- constructs deterministic target weights from:
  - base weights
  - current weights
  - market scorecard
  - decision style
  - position governance
  - validated advisory overrides
  - explicit turnover / delta constraints

Contract:

- does not import agents
- does not read natural language rationale
- does not consume raw LLM `adjusted_weights`
- produces repeatable output for identical input
- emits per-ticker lifecycle diagnostics and build steps

### 4.7 Risk Manager

Owner: `agents/risk_manager.py`

Purpose:

- validates deterministic target-builder output
- checks hard-risk exposure
- checks critical alert exposure
- validates scorecard and style compliance without mutating target-builder target
- computes rebalance actions and cost
- approves or rejects

Risk Manager no longer constructs execution targets from LLM weights.

### 4.8 Position Manager

Owner: `services/position_manager.py`

Purpose:

- applies quantity/frequency controls
- caps turnover and single trade size
- enforces daily trade limits
- supports position monitor diagnostics

This is the last deterministic adjustment before execution actions.

### 4.9 Decision Ledger

Owner: `services/decision_ledger.py`

Purpose:

- aggregates evidence, advisory, governance, target-builder, risk, and execution
  state into one audit object
- does not recompute decisions
- records the lifecycle from proposal to final target

Important fields:

```
diagnostic_llm_target
target_builder_target
validated_advisory_delta
final_target
changed_by
source_effects
advisory_validator_result
target_construction_mode
raw_llm_adjusted_weights_consumed
```

### 4.10 Communicator

Owner: `agents/communicator.py`

Purpose:

- summarizes the decision for Telegram or fallback text
- must clearly distinguish:
  - diagnostic LLM target
  - target-builder target
  - final execution target
  - advisory validator result
  - risk rejection vs execution failure

It is an observability layer, not a decision layer.

---

## 5. Execution Modes

### SEMI_AUTO

Flow:

```
deterministic target -> risk approval -> pending proposal -> human confirm
```

The pending proposal stores deterministic `risk_out.target_weights`.

### FULL_AUTO

Flow:

```
deterministic target -> risk approval -> proposal relevance validation -> executor
```

FULL_AUTO must not execute if:

- scorecard requires human confirmation
- risk checks fail
- live validation blocks
- proposal is stale or invalidated
- circuit state requires human intervention

---

## 6. Observability And Monitor

The system monitor replaces external W&B tracking.

Primary durable observability stores:

- `AgentStepLog`
- `AgentAnalysis.risk_output`
- `CronRunLog`
- `ExecutionLog`
- dashboard `/api/summary`
- dashboard HTML view

### 6.1 AgentStepLog

Each pipeline stage writes:

```
analysis_id
stage
agent_name
input_data
output_data
duration_ms
model
token_usage
failed
created_at
```

This is the canonical stage telemetry store.

### 6.2 Dashboard

Owner: `dashboard/app.py`

Dashboard currently shows:

- operational health
- latest decision
- scorecard
- governance explanations
- decision ledger lifecycle
- pipeline stage telemetry
- replay diagnostics
- cron runs
- latest execution

Stage telemetry is read from `AgentStepLog`, not W&B.

### 6.3 Local Tracker Facade

Owner: `tracking/monitor_client.py`

`PipelineRunTracker` remains as a compatibility facade so pipeline call sites do
not need to change shape. It does not import W&B, does not require network
credentials, and does not send external telemetry.

Durable monitor data should be written through existing database artifacts, not
through an external experiment tracker.

---

## 7. Safety Invariants

These invariants should be treated as tests, not preferences.

### LLM Boundary

```
raw_llm_adjusted_weights_consumed == false
```

Risk Manager must not consume `synthesizer.adjusted_weights` as execution input.

### Target Builder Boundary

`target_builder` must not:

- accept `adjusted_weights`
- accept `raw_llm_adjusted_weights`
- import `agents/*`
- parse natural language rationale

### Ledger Boundary

Decision Ledger must not:

- recompute governance
- create fallback explanations that imply execution authority
- hide disagreement between proposed, target-builder, and final targets

### Monitor Boundary

Monitor must not:

- require W&B
- require external network telemetry
- block pipeline execution if telemetry fails

---

## 8. Testing Expectations

Core migration tests should cover:

- target-builder deterministic repeatability
- target-builder rejects raw LLM weight inputs at API level
- Risk Manager works without LLM `adjusted_weights`
- Risk Manager reports `raw_llm_adjusted_weights_consumed = false`
- scorecard no-add and hard-risk constraints block adds
- decision ledger records diagnostic, target-builder, advisory, and final target
- dashboard displays lifecycle and stage telemetry
- communicator exposes target mode and raw LLM consumption state

Known current unrelated blockers:

- root `unittest discover` imports `test_openai_integration.py`, which requires
  the `openai` package in the local environment
- `unittest discover tests` still has an unrelated `MarketDailyFeature` import
  issue in `tests/test_sector_rotation.py`

---

## 9. Future Cleanup

Recommended next cleanup items:

1. Rename legacy `adjusted_weights` terminology in comments and tests where it
   now means diagnostic-only.
2. Add a dedicated monitor page section for target-builder diffs, violations,
   and fallback mode.
3. Add an explicit live validation rule that fails if
   `raw_llm_adjusted_weights_consumed` is ever true.
4. Decide whether to remove diagnostic `adjusted_weights` entirely after a
   stable observation window.
5. Fix test discovery blockers so full local discovery is meaningful.

---

## 10. Design Summary

The intended final shape is now mostly implemented:

```
LLM advisory
  -> deterministic governance
  -> deterministic target_builder
  -> risk validation
  -> position manager
  -> ledger / monitor
  -> execution
```

This architecture supports LLM reasoning without letting LLM output become an
execution primitive. The monitor is the canonical observability surface, and W&B
is no longer part of runtime telemetry.
