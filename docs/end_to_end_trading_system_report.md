# End-to-End Trading System Report

Last updated: 2026-05-28

This document describes the current end-to-end trading system from QC-side
data generation, through the FastAPI/agent analysis and review pipeline, to
the final command sent back to QuantConnect. It intentionally avoids code-level
implementation details and focuses on ownership, data contracts, validation
logic, execution authority, and failure handling.

## 1. Executive Summary

The system is a deterministic trading and risk-control pipeline with LLM-assisted
research and reporting.

The most important design rule is:

```text
LLM explains and advises.
Deterministic services construct, validate, throttle, audit, and execute.
QC independently validates every executable command before placing orders.
```

The system has three major planes:

| Plane | Responsibility | Can Change Weights? | Can Send Orders? |
|---|---|---:|---:|
| Data plane | QC snapshots, yfinance features, news, knowledge base | No | No |
| Analysis plane | deterministic analytics, strategy scoring, LLM review, diagnostics | No direct final authority | No |
| Execution plane | target builder, risk validation, preflight, executor, QC callback | Yes, only in designated layers | Yes, only after all gates pass |

The current architecture is designed around hard ownership boundaries:

- QC is the live account truth and final independent execution validator.
- yfinance is the primary daily research-feature source.
- FastAPI is the analysis, target-construction, risk, and orchestration layer.
- LLM agents are advisory and explanatory only.
- `target_builder` is the only FastAPI layer that constructs executable target
  weights.
- `risk_manager` validates; it does not construct or mutate final targets.
- `position_manager` can only tighten risk after target construction.
- `final_risk_validation`, `execution_preflight`, and QC callback validation
  are hard gates.

## 2. High-Level End-to-End Flow

```text
QC live algorithm
  -> heartbeat / account snapshot / daily feature snapshot
  -> FastAPI ingestion
  -> database persistence
  -> yfinance feature refresh and QC/yfinance audit
  -> account_state_guard
  -> auto_pause / circuit state checks
  -> market brief and quant baseline
  -> strategy input builder and playground diagnostics
  -> evidence bundle and evidence cards
  -> alpha validation / conviction / attribution diagnostics
  -> AlphaDecisionProfile / AlphaDecisionPolicy interpretation
  -> LLM research review
  -> deterministic target_builder
  -> risk_manager validation
  -> position_manager tighten-only controls
  -> final_risk_validation
  -> execution throttle and deferred execution ledger
  -> execution_preflight
  -> executor sends SetWeights to QC
  -> QC independent policy validation
  -> QC order placement or rejection
  -> ACK / lifecycle events / account reconciliation
  -> next cycle uses QC as source of truth
```

No single analysis module is allowed to directly bypass this chain.

## 3. QC-Side Responsibilities

### 3.1 QC Algorithm Role

The QC live algorithm has four responsibilities:

1. Maintain the live trading account state.
2. Emit heartbeat and account snapshots to FastAPI.
3. Accept or reject commands from FastAPI.
4. Independently validate policy constraints before executing trades.

QC is not merely a broker adapter. It is an execution-side safety boundary.
Even if FastAPI sends a bad command, QC must reject it if it violates the
compiled policy contract.

### 3.2 QC Policy Contract

QC carries a compiled policy snapshot that must match FastAPI's execution
policy.

Current design:

- Runtime automatic `PolicySync` is disabled by default.
- QC should already be deployed with the correct compiled policy.
- CI/tests and deployment discipline are responsible for keeping FastAPI and QC
  policy contracts aligned.
- `PolicySync` remains available as a manual/diagnostic control-plane tool, not
  as part of normal automatic trading.

The expected runtime behavior is:

| Situation | QC Behavior |
|---|---|
| Policy version aligned | Normal command validation proceeds |
| Policy mismatch with buy/increase exposure | Reject command |
| Policy mismatch with reduce-only targets | May allow reduce-only risk exits |
| Missing or malformed policy data | Reject executable command |

This keeps emergency de-risking possible while preventing new exposure under a
policy mismatch.

### 3.3 QC Heartbeat

QC heartbeat is the primary live operational signal from the QC side.

It should communicate:

- algorithm is alive
- current policy version
- policy source
- current holdings
- target weights known to QC
- cash and buying power
- account total value
- open order count
- market open state
- recent feature values available inside QC
- packet type and contract version

FastAPI does not assume the account is tradable merely because the pipeline is
running. It requires recent QC account evidence.

### 3.4 QC Account Snapshot

The account snapshot is the FastAPI-side persisted version of QC account truth.

It includes:

- recorded timestamp
- account timestamp
- packet type
- contract version
- account status
- data status
- policy version
- total account value
- cash
- cash percentage
- buying power
- open order count
- `has_open_orders`
- market open flag
- holdings weights
- target weights
- raw QC snapshot

The snapshot is used by `account_state_guard`, preflight, dashboards, and
reconciliation logic.

### 3.5 QC Daily Feature Snapshot

QC can also emit daily feature snapshots for comparison against yfinance.

These are not the main historical research source. They are used to:

- compare QC live feature values against yfinance daily features
- detect unit mismatch
- detect heartbeat lag
- detect stale QC-side feature values
- audit contract drift between QC and FastAPI

The current design treats yfinance as the primary daily research-feature source,
and QC as the live account authority plus QC-side comparison source.

### 3.6 QC Command Callback

When QC receives a command, it should validate:

- command target and command type
- command id idempotency
- policy version alignment
- whether mismatch is reduce-only or exposure-increasing
- ticker existence
- role caps
- group caps
- total target exposure
- forbidden tickers or roles
- order feasibility
- duplicate command handling

QC sends back ACK/rejection status, including rejection reason when applicable.

## 4. FastAPI Data Ingestion

### 4.1 Data Sources

The system currently uses four main data sources:

| Source | Primary Role | Authority Level |
|---|---|---|
| QC heartbeat/account snapshot | Live account state, policy version, holdings, open orders | Highest for live execution |
| QC daily feature snapshot | QC-side feature comparison and fallback diagnostics | Diagnostic |
| yfinance | Daily historical features, replay, strategy research, alpha validation | Primary research feature source |
| News cache | Contextual evidence, macro/company/theme background | Advisory context only |

The key design principle is:

```text
yfinance powers research features.
QC powers live account truth.
News supports explanation and context, not execution authority.
```

### 4.2 Persistence Layer

Important persisted objects include:

| Data Object | Purpose |
|---|---|
| QC raw snapshots | Preserve inbound QC packets for audit |
| Account state snapshots | Normalized account state for guards and dashboard |
| Market daily features | Daily yfinance/QC feature rows by ticker/date |
| Feature audit results | QC/yfinance drift and unit checks |
| Frozen signals | Immutable daily strategy signals |
| Signal outcomes | Forward-return labels attached after horizons mature |
| Conviction profiles | Historical/live/combined strategy reliability summaries |
| Alpha decision profiles | Strategy/family/regime/epoch decision evidence after statistical, attribution, cost, and independence adjustment |
| Execution log | High-level command state and preflight result |
| Command lifecycle events | Detailed command event chain |
| Deferred execution ledger | Deferred delta tracking after throttle clipping |
| Performance attribution | Weekly/monthly beta/factor/residual attribution |
| System config | Runtime modes, gates, caps, pause state, circuit state |

### 4.3 Feature Authority and Unit Handling

The system distinguishes between raw feature mismatch and expected unit mismatch.

Examples:

- QC may report some momentum values in percent-like units.
- yfinance daily features may store normalized decimal returns.
- Heartbeat values may lag the latest daily snapshot.

The audit layer classifies:

- expected unit mismatch
- severe unit risk
- heartbeat lag class
- high-drift class
- daily snapshot contract error

The desired state is not "QC and yfinance always match raw numbers." The desired
state is:

```text
After contract normalization, daily feature values should be within expected
tolerance, and any heartbeat lag should be visible rather than silently treated
as a model signal.
```

### 4.4 yfinance Health

yfinance data health is evaluated per ETF and per required feature.

The current design avoids global poisoning:

- Missing data for one ETF should not disable all strategies.
- A young ETF with insufficient long lookback should still be usable by short
  horizon strategies.
- Missing required fields become per-strategy/per-ticker abstentions, not score
  zero.
- Mature ETF feature gaps should still be flagged as data-health issues.

This is especially important for young ETFs such as DRAM. DRAM may lack long
history fields while still having valid short-horizon fields.

## 5. Pipeline Trigger and Runtime Configuration

### 5.1 Pipeline Entry

The main analysis pipeline can be triggered by scheduled cron, manual command,
or internal automation.

At the beginning of a cycle, the pipeline reads runtime configuration:

- authorization mode
- circuit state
- trading pause state
- account state guard config
- auto pause config
- final risk validation config
- portfolio construction config
- execution command limits
- evidence cap config
- active strategy and strategy blend settings
- dashboard and notification settings

### 5.2 Authorization Modes

Typical modes:

| Mode | Meaning |
|---|---|
| SEMI_AUTO | System can analyze and prepare proposals; human confirmation required |
| FULL_AUTO | System can send executable commands if all safety gates pass |

FULL_AUTO is not just a config value. It requires code-enforced safety
preconditions.

### 5.3 FULL_AUTO Safety Preconditions

In FULL_AUTO, the pipeline requires:

- account state guard effectively blocking
- final risk validation effectively blocking
- auto pause active
- circuit state allowing execution
- trading not paused
- policy alignment recently confirmed by account state guard

If these are not satisfied, the pipeline must not proceed as if FULL_AUTO were
safe. This is a code-level safety contract, not an operator suggestion.

### 5.4 Circuit and Pause State

The system supports circuit and pause states:

- closed / normal
- alert
- defensive or degraded modes
- manually paused
- auto-paused

Circuit and pause state affect whether the pipeline can continue to command
construction and execution. A paused or alert state should be visible in
Telegram and dashboard outputs.

## 6. Account State Guard

### 6.1 Purpose

`account_state_guard` protects the system from trading on stale, inconsistent,
or unsafe account information.

It runs before target construction and execution.

### 6.2 Inputs

Inputs include:

- latest QC account snapshot
- latest QC heartbeat
- expected policy version from FastAPI execution policy
- current system config
- tolerance for holdings mismatch
- freshness thresholds
- open-order policy

### 6.3 Checks

The guard checks:

- heartbeat freshness
- account snapshot freshness
- account status
- data status
- policy version alignment
- open orders
- holdings consistency
- cash and buying power sanity
- market state if required by the run mode

### 6.4 Outcomes

Possible outcomes:

| Outcome | Meaning |
|---|---|
| pass | Account state is fresh and aligned enough for the pipeline to continue |
| observe warning | Issue visible but not blocking in current mode |
| block trading | Pipeline must not send SetWeights |
| policy mismatch | Trading blocked unless reduce-only path is explicitly allowed by QC |

The current design does not rely on automatic PolicySync recovery during normal
trading. If policy mismatch persists, the operational fix is to deploy/sync the
correct QC compiled policy and verify heartbeat alignment.

## 7. Auto Pause and Circuit Rules

### 7.1 Purpose

Auto pause converts repeated operational anomalies into a protective system
state.

It is designed to catch patterns such as:

- repeated QC rejects
- repeated command timeouts
- stale heartbeat/account state
- unresolved policy mismatch
- consecutive guard failures
- unexpected command lifecycle gaps

### 7.2 Relationship to Account Guard

`account_state_guard` evaluates whether this cycle can safely trade.

`auto_pause` evaluates whether repeated failures indicate the system should
pause or alert until reviewed.

These are different responsibilities:

- account guard is a per-cycle gate
- auto pause is a cross-cycle safety escalation mechanism

### 7.3 Expected Behavior

When auto pause is active:

- hard repeated failures can set a pause or alert state
- operator should receive a clear reason
- pipeline should stop sending executable commands
- dashboards should show the triggering condition and recent evidence

## 8. Market Brief and Quant Baseline

### 8.1 Market Brief

The market brief builds the current market and portfolio context.

It can use:

- QC account state
- current holdings
- yfinance features
- recent market returns
- volatility indicators
- news cache
- macro/regime diagnostics

Its output is context. It does not directly create executable target weights.

### 8.2 Quant Baseline

The quant baseline is a deterministic reference allocation or diagnostic
baseline.

It helps answer:

- what would a simple deterministic model do?
- how far is the current target from baseline?
- is the system drifting toward cash, risk-on, defensive, or concentrated
  exposure?

The baseline can feed downstream construction, but it is still subject to
target-builder ownership and risk validation.

## 9. Strategy Input Builder

### 9.1 Purpose

`StrategyInputBuilder` is the strategy data-preparation boundary.

It prevents every strategy consumer from implementing its own feature merge,
fallback, stale-data handling, and missing-field behavior.

### 9.2 Key Principle

The system no longer treats missing data as a global strategy failure when
partial scoring is possible.

Instead:

```text
If a ticker has enough features for a strategy, that ticker can be scored.
If a ticker lacks a required feature, that ticker/strategy pair abstains.
Abstain is not a zero score.
```

### 9.3 Strategy Score Status

Strategy outputs can be:

| Status | Meaning |
|---|---|
| scored | Strategy scored the eligible universe normally |
| partially_scored | Some tickers scored; some excluded/abstained |
| not_scored | No ticker could be scored, or the strategy was not applicable |

This prevents one problematic ETF from forcing a global cash fallback or
incorrectly lowering all strategy confidence.

### 9.4 Exclusion Reasons

Per-ticker exclusions distinguish:

- insufficient history
- field not applicable
- stale data
- strategy universe mismatch
- missing required field

These reasons drive different operational responses. For example:

- insufficient history usually resolves over time
- field not applicable is normal
- stale data may require backfill repair
- missing required field may indicate a data contract bug

## 10. Strategy and ETF Evidence

### 10.1 Evidence Cards

Evidence cards translate raw strategy outputs into structured, auditable
strategy evidence.

Important concepts:

- `action`: what the strategy semantically suggests
- `confidence`: current signal strength
- `vote_status`: whether the card can participate in aggregation
- `abstain_reason`: why the strategy has no vote for this ticker
- diagnostics: feature, mapping, history, and readiness metadata

### 10.2 Vote Status

Vote statuses:

| Vote Status | Meaning |
|---|---|
| voted | Strategy has complete inputs and semantic mapping; it can vote |
| abstain | Strategy has no vote; not bearish, not score zero |
| watch | Strategy output is understood but non-executable or observation-only |
| mapping_error | Strategy output exists but knowledge mapping is missing/broken |

This distinction is essential. It prevents incomplete data from becoming a
false negative signal.

### 10.3 ETF Knowledge Base

ETF profiles describe:

- ticker role
- underlying exposure
- leverage/inverse behavior
- decay risk
- allowed actions
- max reasonable weight
- role-specific risk budget cost
- special holding constraints

High-decay or leveraged products require extra care. A volatility ETF can have
valid short-term hedge use while still being dangerous as a long-hold position.

### 10.4 Strategy-to-ETF Compatibility

Compatibility mapping answers:

```text
If this strategy scores this ETF highly, what does that mean?
```

The same score can mean different things:

- TQQQ high score may mean risk-on amplifier.
- UVXY high score may mean hedge signal.
- SGOV high score may mean cash/defensive preference.

Therefore, raw score alone is not enough. The system must map score meaning
through ETF role and strategy family.

### 10.5 Evidence Quality Cap

Evidence quality can reduce the maximum usable weight of an ETF without
blocking the ETF entirely.

Inputs include:

- strategy coverage ratio
- conviction status
- ETF history length
- static ETF/profile cap
- role cap

The design goal is:

```text
Do not ask whether an ETF can be used.
Ask how much weight is safe under current evidence quality.
```

In observe mode, evidence caps produce diagnostics and would-clip records.
In gated mode, fresh calibration is required before caps can affect targets.

## 11. Playground and Strategy Review

### 11.1 Purpose

The playground is a diagnostic and research comparison surface.

It should:

- evaluate strategies under consistent inputs
- isolate ETF-level data problems
- compare historical and live evidence
- show strategy confidence
- show whether a strategy is actionable, watch-only, or ignored
- show evidence gaps and coverage degradation

It should not:

- repair yfinance data inline
- silently replace missing data with cash
- convert missing fields into zero scores
- directly create executable target weights

### 11.2 Current Interpretation

When the playground reports a strategy as ignored or watch-only, the reason
should identify whether the issue is:

- data not ready
- live sample too small
- weak walk-forward validation
- poor regime fit
- high turnover
- missing compatibility mapping
- insufficient conviction

The operator should be able to distinguish a weak strategy from a data issue.

## 12. Alpha Validation and Conviction

### 12.1 Frozen Signals

Daily signals are frozen after generation.

The core rule:

```text
A signal generated for day T must never be modified later.
Outcomes are appended separately after forward horizons mature.
```

Frozen signal records preserve:

- signal date
- strategy id
- ticker
- action
- confidence
- raw score
- feature data date
- data lag
- regime at signal
- source bucket

### 12.2 Signal Outcomes

Outcomes are attached after horizons mature.

They can include:

- forward return
- excess return versus SPY
- drawdown during horizon
- hit/miss label
- horizon length
- outcome source

Hit definitions depend on action type. A hedge signal is not judged the same
way as an increase signal.

### 12.3 Conviction Profiles

Conviction profiles summarize historical and live evidence.

They should always be shown with:

- sample count
- status
- source bucket
- hit rate
- IC or equivalent signal/return relationship
- data-lag filtering status
- confidence interval or statistical caveat where available

A conviction number without sample count and status is not sufficient for
professional review.

The system now distinguishes operational readiness from statistical maturity.
The legacy/operational label `calibrated` is diagnostic only. It is not by
itself a promotion or allocation gate.

Decision-facing maturity is based on conservative statistical tiers:

| Sample Count | Statistical Status | Decision Meaning |
|---:|---|---|
| 0-29 | insufficient | no positive promotion credit |
| 30-99 | early_signal | monitoring only, not proof |
| 100-299 | indicative | can support advisory promotion if other checks pass |
| 300+ | statistically_meaningful | can receive full statistical credit if other checks pass |

This prevents 30 live samples from being treated as statistically proven alpha.

### 12.4 Statistical Independence

The system now diagnoses and consumes strategy independence in the alpha
decision loop.

This matters because multiple strategy names may still express the same
momentum factor. Professional review should distinguish:

- registered strategy count
- statistically independent alpha count

Correlation matrix, regime gap analysis, and promotion/degradation diagnostics
help prevent strategy diversity from being only cosmetic.

High positive correlation reduces duplicate decision credit through a
redundancy multiplier. Low or negative correlation is treated differently:
negative correlation may be useful diversifier evidence, but still requires its
own outcome validation.

### 12.5 Performance Attribution

Attribution attempts to decompose results into:

- market beta contribution
- factor contribution
- residual alpha candidate
- trading cost effect

This is essential for deciding whether returns come from actual edge or simply
from market exposure.

Attribution now feeds promotion and degradation recommendations. A strategy
with positive gross return but negative residual alpha should not be promoted
without explicit operator review or override.

The system still needs enough live and historical samples for attribution to be
meaningful. Attribution model quality remains an important review item because
an incomplete factor model can mislabel factor exposure as residual alpha.

### 12.6 Alpha Decision Profiles

`AlphaDecisionProfile` is the canonical read-only object that combines:

- statistical status and sample count
- residual alpha status
- cost-adjusted edge using the IBKR proxy
- independence cluster and redundancy multiplier
- regime and strategy family
- construction epoch
- decision status and decision multiplier

It has no execution authority:

```text
execution_authority = none
target_weight_mutation = none
```

The profile answers:

```text
After beta/factor attribution, duplicate strategy correlation, and estimated
costs, does this strategy/family/regime still appear to have positive edge?
```

`construction_epoch_id` separates evidence generated under materially different
construction or policy contexts.

Epoch triggers:

- Portfolio Construction mode change, such as shadow -> candidate -> gated
- material Portfolio Construction objective change
- execution policy version change
- operator manual reset

Cross-epoch policy:

- old epoch data is retained as historical prior
- old epoch data must not be merged into new-epoch live-paper conviction
- new-epoch live-paper conviction starts from the epoch boundary
- cross-epoch evidence must be labeled as historical prior requiring live
  confirmation

### 12.7 Alpha Decision Policy

`AlphaDecisionPolicy` is the deterministic interpreter for alpha decision
evidence.

Modes:

| Mode | Meaning |
|---|---|
| observe | records would-affect diagnostics only |
| recommendation | affects promotion/degradation recommendations only |
| gated | allows approved decision multipliers to affect PC/strategy allocation credit |

Gated mode requires explicit config, observe evidence, and operator review. It
does not authorize direct execution and does not bypass target_builder, risk
validation, preflight, or QC validation.

The current canonical alpha decision loop is:

```text
strategy signal
  -> frozen signal / outcome
  -> conviction profile
  -> attribution and residual alpha
  -> cost-adjusted edge
  -> independence adjustment
  -> AlphaDecisionProfile
  -> AlphaDecisionPolicy
  -> recommendation / PC diagnostics
  -> target_builder only if gated and approved
```

## 13. LLM Research and Review

### 13.1 LLM Authority Boundary

LLMs are used for:

- summarizing market evidence
- generating bull and bear arguments
- identifying uncertainty
- explaining strategy diagnostics
- writing review summaries
- helping the operator understand trade rationale

LLMs are not allowed to:

- create executable final weights
- bypass deterministic scorecard or policy
- bypass risk validation
- send commands to QC
- decide final execution authority

### 13.2 Research Debate Flow

The research layer can include:

- researcher
- bull case
- bear case
- cross-examination
- synthesizer

Their outputs are advisory. They can influence structured diagnostics and
human-readable rationale, but deterministic services must own executable
portfolio construction.

### 13.3 Diagnostic Weights

Some legacy or diagnostic LLM outputs may still include adjusted weights.

Current design:

```text
LLM adjusted weights are diagnostic only.
target_builder target weights are the executable input.
```

Any system component consuming raw LLM weight proposals as execution targets
would violate the architecture.

## 14. Portfolio Construction

### 14.1 Purpose

Portfolio Construction attempts to improve target structure under constraints.

Current focus:

- signal-weighted effective N
- independence-adjusted net signal effective N
- factor concentration control
- turnover budget
- role/policy caps
- candidate versus gated promotion
- diagnostics on concentration and diversification

### 14.2 Modes

| Mode | Meaning |
|---|---|
| shadow | PC runs and reports differences but does not affect executable targets |
| candidate | PC output is available for target-builder consideration but still controlled |
| gated | target_builder may use PC output under promotion and safety constraints |

### 14.3 Objective Caveat

PC now reports both a structural objective and an alpha-decision-adjusted
objective.

That means:

- it can improve diversification and concentration
- it can control turnover
- it can show whether apparent diversification comes from independent strategy
  evidence or correlated duplicate signals
- it should still not be mistaken for proof of alpha
- it depends heavily on the quality of signal, attribution, cost, and
  independence inputs

The alpha-aware objective is currently:

```text
maximize independence_adjusted_net_signal_effective_N
```

subject to policy caps, evidence caps, turnover budget, factor concentration,
cost-aware weak-signal constraints, and cluster exposure diagnostics.

### 14.4 Relationship to Target Builder

PC does not directly execute trades.

If PC is allowed to influence execution, it must do so through target_builder,
which remains the owner of final target construction.

### 14.5 Alpha Decision Relationship

PC consumes alpha decision context as diagnostics and, only when explicitly
allowed by `AlphaDecisionPolicy`, as allocation credit.

PC must report:

- raw objective value
- independence-adjusted objective value
- raw strategy count
- effective independent strategy count
- cluster concentration
- gross edge
- IBKR estimated cost
- cost-adjusted edge
- decision multiplier
- before/after diagnostics

Even in gated mode, PC does not send orders and does not mutate final targets
outside target_builder ownership.

## 15. Target Builder

### 15.1 Purpose

`target_builder` is the only FastAPI component allowed to construct executable
target weights.

It takes structured inputs and produces deterministic targets.

### 15.2 Inputs

Inputs can include:

- current holdings
- quant baseline
- market scorecard
- strategy evidence
- ETF evidence cards
- alpha decision profiles
- alpha decision policy context
- evidence caps
- portfolio construction candidate
- position governance output
- risk policy caps
- turnover constraints
- cash policy

### 15.3 Outputs

Outputs include:

- target weights
- build diagnostics
- per-ticker rationale
- evidence cap diagnostics
- target construction mode
- whether PC was used
- whether fallbacks were used

### 15.4 Ownership Contract

Target builder may construct and adjust target weights within its authority.

Downstream layers may validate or tighten but should not create a new,
unattributed portfolio.

## 16. Risk Manager

### 16.1 Purpose

`risk_manager` validates the deterministic target-builder output.

It checks:

- hard exposure constraints
- critical alert exposure
- scorecard compliance
- role and policy consistency
- risk concentration
- whether target-builder output is usable

### 16.2 Validate-Only Contract

Risk manager should not mutate target weights.

If it detects a problem, it should:

- reject
- warn
- emit diagnostics
- pass a validated target through

It should not silently construct a new portfolio.

## 17. Position Manager and Post-Risk Controls

### 17.1 Purpose

Position manager applies tighten-only execution controls after risk validation.

Examples:

- reduce excessive turnover
- cap single-cycle trade deltas
- defer sells due to minimum hold rules
- trim decay-risk ETF exposure
- apply regime tighten-only constraints
- enforce loss-review reductions

### 17.2 Tighten-Only Contract

Position manager can reduce risk or slow execution. It should not add new
exposure that target_builder did not construct.

Allowed post-risk mutations must be classified and visible to final risk
validation.

### 17.3 Mutation Ownership

Known mutation types should be registered and audited.

Unclassified mutation paths are not acceptable as permanent architecture. If a
new mutation appears, it should either be:

- moved into target_builder if it constructs exposure
- registered as tighten-only if it reduces risk
- kept in observe diagnostics with a deadline for classification
- removed if it is obsolete

## 18. Final Risk Validation

### 18.1 Purpose

Final risk validation is the last deterministic risk gate before execution
preflight.

It validates the final target after target_builder, risk manager, and position
manager outputs have been combined.

### 18.2 Checks

It checks:

- unknown tickers
- role caps
- group caps
- watchlist/no-add restrictions
- policy version expectations
- post-risk target drift
- allowed mutation types
- material drift threshold
- cash and total exposure sanity
- evidence cap enforcement mode
- ETF decay/hold constraints when applicable

### 18.3 Modes

The effective mode can differ by authorization mode.

Current intended behavior:

- FULL_AUTO requires blocking final risk validation.
- SEMI_AUTO may run observe mode depending on config.
- If required safety mode is not satisfied, FULL_AUTO should not proceed.

## 19. Transaction Cost and Execution Throttle

### 19.1 Transaction Cost Diagnostics

The system can estimate cost using an IBKR-style proxy.

Current purpose:

- label expected cost
- make turnover visible
- support later optimization
- avoid hidden cost blindness

Cost diagnostics are not necessarily hard blockers today unless configured.

### 19.2 Execution Throttle

Execution throttle constrains how much the system can change in one cycle or
one day.

Typical limits:

- max buy delta
- max sell delta
- max daily commands
- max gross turnover per day

If target changes exceed throttle, the system can clip executable delta and
record the deferred portion.

### 19.3 Deferred Execution Ledger

Deferred execution ledger tracks deltas that were not executed due to throttle.

It helps prevent the system from repeatedly rediscovering the same deferred
trade without audit.

Deferred deltas should be revalidated in future cycles. They are not guaranteed
orders.

## 20. Execution Preflight

### 20.1 Purpose

Execution preflight checks whether a command is allowed to be sent to QC.

It does not decide investment merit. It decides command safety.

### 20.2 Checks

Preflight checks:

- command id exists
- command is not duplicate
- daily command count
- daily gross turnover
- buy delta
- sell delta
- recent account guard policy alignment
- final risk validation passed
- target weights are serializable and valid
- circuit/pause state permits execution

### 20.3 Policy Alignment

Preflight no longer requires same-cycle PolicySync ACK.

It requires recent account state guard confirmation that QC policy version is
aligned. This keeps data-plane execution separate from manual/diagnostic
PolicySync.

If policy is not aligned, the correct response is not to trade and not to send
an automatic policy repair. The correct operational response is to deploy/sync
the QC compiled policy and verify heartbeat alignment.

## 21. Executor

### 21.1 Purpose

Executor sends approved commands to QC.

It should not:

- construct weights
- repair policy
- override preflight
- override risk validation

It should:

- assert required preconditions are satisfied
- send the command
- persist submitted state
- emit lifecycle events
- surface QC response

### 21.2 SetWeights Command

The executable command typically contains:

- command id
- target weights
- policy version
- command metadata
- analysis id
- execution mode
- optional diagnostics

CASH is usually represented in FastAPI targets but not necessarily sent as a
tradable QC symbol.

### 21.3 Command Lifecycle

The lifecycle ledger records:

- created
- preflight blocked or passed
- submitted
- QC accepted
- QC rejected
- timeout
- filled
- partial
- reconciled
- reconciliation drift

The current command lifecycle implementation is strongest around created,
submitted, accepted/rejected, and timeout. Richer fill and reconciliation detail
depends on QC-side payloads and can continue to improve.

## 22. QC Command Execution

### 22.1 Independent Validation

When QC receives a SetWeights command, it independently validates:

- policy version
- reduce-only exception if mismatch
- ticker allowlist
- role caps
- group caps
- total exposure
- malformed payloads
- duplicate command id
- forbidden buy/increase under mismatch

This validation is deliberate duplication. FastAPI validation protects the
system before sending; QC validation protects the account at execution boundary.

### 22.2 Rejection Examples

QC can reject for:

- policy version mismatch with buy exposure
- unknown ticker
- role cap exceeded
- invalid payload
- duplicate command
- command target not supported
- malformed weights

QC rejection should flow back into:

- execution log
- command lifecycle
- Telegram
- auto pause evaluation
- dashboard health

### 22.3 ACK and Account Feedback

After accepting or rejecting, QC should send ACK information.

After orders are placed or holdings change, subsequent heartbeats and account
snapshots become the source of truth for the next cycle.

The system should not assume intended target equals actual holdings until QC
state confirms it.

## 23. Review, Dashboard, and Telegram

### 23.1 Dashboard Role

Dashboard should make system state inspectable.

Important panels include:

- system health
- QC/account freshness
- yfinance ETF health
- policy alignment
- circuit and pause state
- account guard result
- auto pause triggers
- command lifecycle
- portfolio construction objective
- evidence cards and vote status
- evidence cap would-clip or enforced-clip diagnostics
- live signal conviction
- performance attribution
- strategy independence
- regime gap analysis
- Alpha Decision Policy
- Alpha Decision Review Surface
- AlphaDecisionProfile table
- raw strategy count versus effective independent alpha count
- net alpha / IBKR cost proxy view
- deferred execution ledger

### 23.2 Telegram Role

Telegram is operational control and notification.

It can report:

- health summary
- command submitted
- command rejected
- preflight blocked
- circuit state
- data degradation
- account stale
- policy mismatch
- auto pause trigger

Manual confirmation must follow the same safety boundary:

- it requires recent account guard policy alignment
- it does not send automatic PolicySync
- it does not bypass final risk or preflight checks

### 23.3 Review Artifacts

A professional review should be able to answer:

- What data was used?
- Was the data fresh?
- Which strategies voted?
- Which strategies abstained and why?
- What did the LLM say, and was it advisory only?
- Who constructed the final weights?
- Which risk checks passed?
- Which mutations occurred after risk validation?
- Which alpha decision profile supported or weakened the strategy?
- Was the recommendation based on statistical status, residual alpha,
  independence, and net cost?
- Did PC use raw structural diversification or alpha-decision-adjusted
  diagnostics?
- Was final validation blocking or observe?
- What was throttled or deferred?
- What command was sent?
- Did QC accept or reject it?
- What did the account actually hold afterward?

## 24. Scheduled Jobs and Automation

### 24.1 Important Jobs

Scheduled jobs may include:

- hourly analysis pipeline
- yfinance backfill
- daily feature refresh
- QC/yfinance feature audit
- daily signal validation refresh
- signal outcome labeling
- conviction profile refresh
- alpha decision profile refresh
- performance attribution refresh
- health monitor heartbeat
- dashboard data refresh

### 24.2 Job Health

Job failures should be visible by class:

- data degraded
- research degraded
- execution degraded
- dashboard degraded
- QC stale
- account stale
- yfinance stale coverage
- command lifecycle stale

Not every degraded research job should block trading, but degradation must be
visible and should not silently contaminate execution inputs.

## 25. Failure Handling by Stage

### 25.1 QC Heartbeat Stale

Expected behavior:

- account guard fails or warns depending mode
- FULL_AUTO should block if freshness is required
- Telegram/dashboard show stale account risk
- no SetWeights if account truth is stale

### 25.2 Policy Version Mismatch

Expected behavior:

- FastAPI blocks normal buy/increase commands before execution
- QC rejects buy/increase commands if they arrive anyway
- reduce-only exception may be allowed by QC
- automatic PolicySync is not part of normal trading
- operator deploys/syncs QC compiled policy and verifies heartbeat alignment

### 25.3 yfinance Missing Feature

Expected behavior:

- affected ticker/strategy pair abstains
- mature ETF missing required feature is flagged
- young ETF missing long-history feature is not globally blocked
- missing data is not converted to score zero
- cron/backfill is responsible for repair where repair is possible

### 25.4 Strategy Weak Evidence

Expected behavior:

- strategy use may become watch-only or ignored
- evidence quality cap may shrink maximum usable weight
- conviction status and sample count remain visible
- weak evidence does not become hidden cash fallback without explanation

### 25.5 Risk Validation Failure

Expected behavior:

- final risk validation blocks in FULL_AUTO
- diagnostics explain which rule failed
- no command is sent to QC

### 25.6 Execution Throttle

Expected behavior:

- excessive buy/sell delta is clipped or blocked according to config
- deferred portion is recorded
- next cycle revalidates the deferred delta
- throttle reason is visible in Telegram/dashboard

### 25.7 QC Reject

Expected behavior:

- command lifecycle records rejection
- execution log records reason
- Telegram reports rejection
- auto pause considers reject count
- next cycle uses unchanged QC holdings as truth

## 26. Authority Matrix

| Layer | Main Owner | Can Construct Target? | Can Mutate Target? | Can Block? | Notes |
|---|---|---:|---:|---:|---|
| QC heartbeat/account | QC | No | No | Indirectly | Source of live account truth |
| yfinance feature refresh | Data jobs | No | No | Indirectly | Research features only |
| StrategyInputBuilder | FastAPI services | No | No | No | Produces scorable/excluded universe |
| Evidence cards | FastAPI services | No | No | No | Strategy evidence and vote status |
| AlphaDecisionProfile / Policy | FastAPI services | No | No | No | Interprets alpha maturity, residual alpha, cost, and independence; can affect recommendation/PC credit only by config |
| LLM agents | Agents | No | No | No | Advisory/explanatory only |
| Portfolio Construction | FastAPI services | Candidate only | No direct execution | No | Must flow through target_builder |
| Target Builder | FastAPI services | Yes | Yes, within ownership | No | Only executable target constructor |
| Risk Manager | Agent/service boundary | No | No | Yes | Validate-only |
| Position Manager | FastAPI services | No | Tighten-only | Yes/No depending rule | Must classify mutations |
| Final Risk Validation | FastAPI services | No | No | Yes | Hard gate in FULL_AUTO |
| Execution Preflight | FastAPI services | No | No | Yes | Command safety gate |
| Executor | FastAPI agent | No | No | No, should refuse if gates missing | Sends approved command |
| QC callback | QC | No | Executes after validation | Yes | Final independent account-side gate |

## 27. Current Strengths

The system is strong in:

- deterministic execution boundaries
- QC-side independent validation
- policy alignment guard
- account state guard
- final risk validation
- command lifecycle audit
- auto pause and circuit concepts
- selective per-ticker strategy readiness
- evidence cards and abstain semantics
- alpha decision profiles and deterministic policy interpretation
- statistical independence consumption in recommendation/PC diagnostics
- performance attribution feeding promotion/degradation recommendations
- IBKR cost proxy visible in net-edge review
- dashboard Alpha Decision Review Surface

These make the system more professional than a typical single-script trading
bot. It is not simply "generate a signal and trade."

## 28. Current Known Weaknesses and Watch Items

The remaining major risks are not primarily execution mechanics. They are
alpha evidence quality, data accumulation, and model validation:

1. Alpha quality and statistical independence.
   The system now penalizes duplicate correlated signals, but that does not
   create independent alpha. It only prevents duplicate credit. The actual
   strategy pool still needs enough truly independent sources.

2. Conviction sample size.
   Early live samples no longer receive full decision credit, but they still
   need time to accumulate into statistically meaningful evidence.

3. Portfolio Construction objective maturity.
   PC now reports alpha-decision-adjusted diagnostics, but it is still not a
   full expected-return / covariance optimizer and should not be interpreted as
   alpha proof.

4. Fill and reconciliation detail.
   Command lifecycle is strong pre-submit and ACK-side, but richer fill and
   reconciliation data depends on QC-side event payloads.

5. Operational discipline around QC deployment.
   Since runtime automatic PolicySync is disabled, QC compiled policy must be
   kept aligned through deployment and CI checks.

6. Cost model maturity.
   IBKR-style cost proxy is visible and consumed in alpha decision review, but
   it is still a proxy until fill-level calibration is mature.

7. Data degradation visibility.
   yfinance and QC feature roles are now clearer, but dashboards must continue
   to distinguish mature ETF feature failure from young ETF insufficient
   history.

8. Attribution model quality.
   Promotion/degradation now consumes residual alpha, so the attribution model
   itself must be periodically reviewed for omitted factors, unstable beta, and
   regime-specific weakness.

   Minimum validation checklist:

   - residual returns should not show obvious non-random structure
   - residual autocorrelation should be low
   - residuals should not strongly correlate with known omitted factors such as
     size, value, rates, volatility, or broad momentum
   - regime-specific beta should be reasonably stable within each regime
   - attribution rows should carry an explicit model version

## 29. Professional Review Checklist

A reviewer should inspect the system in this order:

1. QC heartbeat freshness and policy version.
2. Account state snapshot freshness and holdings accuracy.
3. yfinance ETF health and feature completeness.
4. QC/yfinance feature audit contract errors.
5. Authorization mode, circuit state, and trading pause state.
6. FULL_AUTO safety preconditions.
7. Account state guard latest result.
8. Auto pause triggers and recent QC rejects/timeouts.
9. Strategy input readiness and abstain matrix.
10. Evidence cards and mapping errors.
11. Evidence cap observe/gated mode and calibration freshness.
12. Strategy conviction sample count and statistical status.
13. AlphaDecisionProfile status, residual alpha, cost-adjusted edge, epoch,
    independence cluster, and redundancy multiplier.
14. Effective independent alpha count and regime gap diagnostics.
15. AlphaDecisionPolicy mode and gated blockers.
16. Portfolio Construction raw versus alpha-decision-adjusted objective diagnostics.
17. Target builder output and target construction mode.
18. Risk manager validation.
19. Position manager post-risk mutations.
20. Final risk validation result.
21. Execution throttle and deferred execution ledger.
22. Execution preflight result.
23. Command lifecycle after submission.
24. QC accept/reject reason.
25. Post-command account snapshot reconciliation.

## 30. One-Sentence System Description

This system collects live account truth from QC, research features from
yfinance, and contextual evidence from news; then it runs deterministic
strategy, alpha-decision, evidence, risk, and execution gates with LLMs limited
to advisory review, constructs final targets only through target_builder,
validates them through multiple FastAPI hard gates, sends commands only after
preflight, and requires QC to independently validate every executable command
before orders can affect the account.
