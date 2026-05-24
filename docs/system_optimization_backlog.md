# System Optimization Backlog

This document tracks the main open issues found in the current trading system
review. Use it as the working backlog for step-by-step optimization.

## Current System Assessment

The system has evolved into a runnable research-oriented trading pipeline:

1. QuantConnect sends live portfolio snapshots and richer daily feature snapshots.
2. The backend stores QC snapshots, holdings factors, yfinance research features,
   news cache, analysis records, execution logs, and memory records.
3. The main pipeline builds a market brief, runs quant baselines and strategy
   playground, asks agents to analyze and arbitrate, then applies deterministic
   risk and position controls.
4. Daily and sandbox crons are beginning to create historical memory and strategy
   comparison reports.

The remaining risk is not lack of agents. The main risk is incomplete closure of
data quality, replay validity, runtime auditability, and memory feedback.

## Next Professional Execution Optimization Roadmap

Updated: 2026-05-24

The next optimization target is not paper-live versus real-money behavior. The
target is a professional execution-control system where paper and real accounts
use the same decision core, while QC remains the final account-state authority.

Core principle:

```text
FastAPI / agents create compliant intent.
QuantConnect validates the live account state and decides whether execution is safe.
```

The revised priority order is:

1. QC Account State Guard
2. Auto Pause / Circuit Rules
3. Command Lifecycle Ledger
4. Portfolio Construction objective dashboard
5. ETF / Strategy EvidenceCard dashboard
6. Live signal conviction dashboard

Auto Pause is intentionally ahead of the full command ledger. The ledger is
needed for observability, but automatic pause is the more urgent safety
mechanism during live canary operation.

### Sprint N: QC Account State Guard

Goal:
Prevent the pipeline from constructing or sending targets when the account state
is stale, inconsistent, or mid-execution.

Current implementation status:

- PR1 `account_state_snapshot` contract implemented locally on 2026-05-24.
  FastAPI now normalizes QC heartbeat/daily snapshots into
  `account_state_snapshots`, accepts account-state fields in QC ACK payloads,
  and the QC algorithm exports `account_state` plus `actual_holdings_weights`.
  This PR is observational only; it does not yet block target construction or
  execution.
- PR2 `account_state_guard` observe mode implemented locally on 2026-05-24.
  The pipeline now loads the latest account-state snapshot before market brief,
  evaluates freshness, account/data status, policy version, buying power, open
  orders, holdings presence, and account-vs-snapshot holdings consistency, then
  writes the verdict into `pipeline_context` and `risk_output`. Pipeline
  enforcement remains `observe_only`; promotion to blocking is a later PR.

This is a guard stage, not just execution preflight. It should run after the
latest QC heartbeat/account snapshot is loaded and before target construction
or command submission can proceed.

Pre-decision checks:

- latest QC heartbeat age is below threshold, default 5 minutes
- QC reported holdings and FastAPI last-known weights differ by less than a
  configured tolerance
- no blocking open orders exist
- cash and buying power are present and fresh
- account/data status is healthy
- QC policy version is present and compatible

Pre-execution checks on QC side:

- command id has not been processed before
- QC-local policy version check passes, or mismatch command is reduce-only
- actual holdings are still close to the command's assumed starting weights
- no blocking open orders or pending fills exist
- buying power is sufficient for buy targets
- daily command, turnover, buy-delta, and sell-delta caps are not exceeded
- data/account state is not stale

Post-execution checks:

- accepted/rejected/partial/filled status is reported with structured reasons
- partial fill leaves the account in a known state
- actual post-order weights are compared with intended target weights
- reconciliation drift above threshold triggers circuit alert or pause

Proposed implementation PRs:

1. `account_state_snapshot` contract
   - Extend QC heartbeat/ACK payloads with account facts: as-of timestamp,
     cash, buying power, total portfolio value, holdings weights, open order
     count, policy version, and stale/account-health flags.
   - Store the latest account snapshot in FastAPI without making it an
     execution authority.
2. FastAPI `account_state_guard`
   - Add a deterministic guard service that returns `allowed`, `blockers`,
     `warnings`, and `snapshot_age_seconds`.
   - Pipeline blocks before target construction when the guard fails.
   - Telegram/dashboard receives a concise account-state failure reason.
3. QC-local execution guard
   - Revalidate account state immediately before order placement.
   - Reject unsafe commands with machine-readable reason codes.
4. Post-execution reconciliation
   - Compare target weights, submitted orders, fills, and actual holdings.
   - Emit reconciliation status into ACK/execution audit.

Acceptance criteria:

- stale heartbeat blocks execution before target construction
- open orders block new SetWeights commands unless explicitly reduce-only and
  safe
- holdings drift above tolerance blocks or pauses the pipeline
- QC can reject a command because account state changed after FastAPI approval
- post-execution ACK includes enough information to classify filled, partial,
  rejected, or reconciliation-drift outcomes

### Sprint N P0: Auto Pause / Circuit Rules

Goal:
Make the system pause itself when execution trust is degraded.

Current implementation status:

- PR3 `auto_pause` structured triggers implemented locally on 2026-05-24.
  The service evaluates consecutive QC rejects, stale policy mismatch, and
  account-state guard failures. The pipeline records the verdict in
  `pipeline_context` and `risk_output`. Default mode is `observe`; `mode=active`
  sets `circuit_state=ALERT` and skips FULL_AUTO runs when a trigger fires.

Auto-pause rules should be config driven, with conservative defaults:

```json
{
  "auto_pause_after_consecutive_qc_rejects": 2,
  "policy_mismatch_alert_after_minutes": 5,
  "heartbeat_stale_pause_after_minutes": 5,
  "open_order_stale_pause_after_minutes": 10,
  "max_reconciliation_drift": 0.01,
  "pause_on_account_state_guard_failure": true
}
```

Trigger conditions:

- consecutive QC rejects exceed threshold
- policy sync mismatch persists beyond tolerance
- QC heartbeat/account snapshot is stale
- open orders remain unresolved beyond tolerance
- actual holdings diverge from expected holdings beyond tolerance
- daily command, turnover, buy-delta, or sell-delta cap is exceeded
- QC reports broker/data/account unhealthy state

Acceptance criteria:

- auto pause writes a structured circuit event with reason code and evidence
- pipeline refuses new automatic execution while paused
- Telegram shows the exact pause reason and reset requirement
- `/reset_circuit` or equivalent operator action is required after hard pause
- tests cover consecutive rejects, stale heartbeat, policy mismatch timeout,
  open-order timeout, and reconciliation drift

### Sprint N+1: Command Lifecycle Ledger

Goal:
Turn execution audit from a single-row outcome into a full command lifecycle.

Target lifecycle:

```text
created
-> preflight_passed
-> submitted_to_qc
-> qc_accepted / qc_rejected
-> order_submitted
-> filled / partial / expired / canceled
-> reconciled / reconciliation_drift
```

Implementation direction:

- Either extend `execution_log` with event-style rows or add a
  `command_lifecycle_events` table.
- Use `command_id` as the stable join key across FastAPI, QC ACK, order events,
  and reconciliation.
- Store assumed starting weights, intended target weights, submitted order
  details, fill details, actual ending weights, and final status.

Acceptance criteria:

- every command has an ordered event trail
- partial fills are visible as first-class states, not collapsed into success
- daily report can summarize created/submitted/rejected/filled/reconciled
  counts
- auto-pause can read lifecycle history instead of parsing text logs

### Sprint N+2: Observability Dashboards

Goal:
Expose the professional controls in operator-facing surfaces.

Portfolio Construction dashboard:

- objective: `maximize_effective_n`
- subject-to constraints: factor cap, active basket cap, turnover budget,
  no-add constraints, ETF-specific limits
- before/after effective N
- factor and basket exposure before/after
- reason each weight changed

ETF / Strategy Evidence dashboard:

- EvidenceCards by strategy, ticker, role, action, confidence, and mapping
  reason
- missing mapping and safety-field warnings
- conviction fields shown only as diagnostics with sample count/status/source

Live Signal Conviction dashboard:

- FrozenSignal and SignalOutcome counts by strategy/ticker/branch
- insufficient, early, and calibrated conviction status
- data-lag filtering status
- recent degradation flags

Acceptance criteria:

- operator can answer why a ticker changed weight without reading logs
- conviction is never displayed as a naked number without sample count and
  source/status
- dashboard separates execution-blocking issues from research-quality issues

### CI/CD Hardening

Add policy-sync checks to prevent FastAPI and QC fallback policy drift.

Minimum CI check:

- generate FastAPI `policy_snapshot()`
- parse or import the QC fallback policy
- compare ticker roles, role caps, and key execution limits
- fail CI if policy version or role/cap contracts diverge unexpectedly

Acceptance criteria:

- changes to `services/execution_policy.py` cannot merge without updating the
  QC fallback policy or explicitly documenting why no QC fallback change is
  needed
- CI failure message identifies the exact ticker/role/cap mismatch

## P0 Issues

### 1. Synthesizer market_judgment schema mismatch

Status: done

Problem:
The Synthesizer prompt schema describes `market_judgment` as a string enum, but
the parser expects a dict with `regime`, `adjusted_confidence`, and
`uncertainty_flag`. If the LLM follows the schema literally, the parser falls
back to neutral values and may weaken downstream Risk Manager regime overlays.

Optimization:
Unify the prompt schema and parser contract. `market_judgment` should be a
structured object.

Acceptance criteria:
- Synthesizer output schema requires `market_judgment.regime`,
  `market_judgment.adjusted_confidence`, and
  `market_judgment.uncertainty_flag`.
- Unit test covers string-style malformed output and valid dict-style output.
- Risk Manager receives the intended regime instead of defaulting to neutral.

Resolution:
- Updated Synthesizer output schema to require structured `market_judgment`.
- Added validation that rejects string-style `market_judgment`.
- Added contract tests in `tests/test_synthesizer_contract.py`.

### 2. Strategy feature contract

Status: done

Problem:
Strategies declare `required_fields`, but the system does not yet provide a
single feature-contract verdict showing which fields are available, missing,
stale, or yfinance-filled for each strategy.

Optimization:
Create a strategy feature contract layer that reports field coverage, freshness,
source, and readiness before strategy scoring.

Acceptance criteria:
- Each strategy exposes required and optional fields in a machine-readable
  contract.
- Playground and main pipeline can produce a per-strategy readiness verdict.
- Agent-facing bundles include data source and freshness notes.
- Strategies with insufficient required data cannot influence allocation.

Resolution:
- Added `services/strategy_feature_contract.py`.
- Playground now attaches `feature_contract` to every strategy result.
- Strategies with missing or stale required fields are forced to CASH and cannot
  influence allocation.
- Synthesizer compact Playground input includes feature-contract verdict and
  `can_influence_allocation`.
- Added tests in `tests/test_strategy_feature_contract.py`.

### 3. Playground replay metric guardrails

Status: done

Problem:
Playground reports can overstate performance when sample size is low or forward
return proxies are sparse. Sharpe can look unrealistically high and mislead the
LLM even when the strategy is not statistically validated.

Optimization:
Add strict sample gates and conservative report language for Sharpe, IC,
hit-rate, and strategy selection.

Acceptance criteria:
- Sharpe, IC, and hit-rate are suppressed below the configured sample threshold.
- Report prompt explicitly forbids selecting a strategy based on low-sample
  metrics.
- Strategy comparison bundle exposes `n_forward_return_samples` and
  `metric_reliability`.
- Telegram report clearly labels low-confidence metrics.

Resolution:
- Added replay sample thresholds and structured `metric_reliability`.
- Added `selection_guardrail` to each strategy replay metric block.
- Playground LLM review rules now treat non-high reliability metrics as weak
  evidence.
- Fallback Telegram report now includes metric reliability.
- Added tests for replay metric suppression and reliability boundaries.

### 4. Cron run audit log

Status: done

Problem:
There are many crons, but there is no unified table showing which jobs ran,
whether they succeeded, how many rows they wrote, and the latest healthy time.

Optimization:
Add a `cron_run_log` table and a small helper used by all important cron jobs.

Acceptance criteria:
- Each important cron writes job name, start time, finish time, status, duration,
  rows written, and error message.
- Morning or daily health report can summarize job health.
- Failed jobs are visible without reading Railway logs.

Resolution:
- Added `cron_run_log` ORM model and manual migration SQL.
- Added `services/cron_audit.py` with `audit_cron_run`.
- Integrated audit logging into hourly analysis, daily analyst, playground
  analysis, yfinance backfill, morning health, post-market report, and position
  monitor.
- Morning health now includes recent cron failures.

### 5. Memory feedback must affect behavior

Status: done

Problem:
`memory_daily`, decision context, DQS, and calibration exist, but memory is still
closer to an enriched log than a complete behavior-calibration system.

Optimization:
Make decision quality affect at least Researcher confidence and Playground
strategy weighting before it affects execution.

Acceptance criteria:
- DQS updates `researcher_confidence_bias` once enough samples exist.
- Researcher prompt includes calibration only when sample size is sufficient.
- Playground or Synthesizer discounts strategies that historically underperform
  in similar regimes.
- Memory feedback is advisory first and cannot bypass Risk Manager.

Resolution:
- Added `services/memory_feedback.py` to convert same-regime historical DQS into
  conservative per-strategy advisory discounts.
- Playground now attaches `memory_feedback` to each strategy result and weights
  advisory consensus by each strategy's memory discount multiplier.
- Synthesizer compact Playground input includes memory feedback, while retaining
  Risk Manager as the hard execution gate.
- Daily decision memory now stores Playground strategy assessment and extracted
  selected strategy names for future feedback.
- Fixed calibration bookkeeping to report insufficient sample counts accurately
  and improved researcher-confidence extraction from the current synthesizer
  schema.
- Added tests in `tests/test_memory_feedback.py` and updated Playground tests.

## P1 Issues

### 6. Field provenance and freshness

Status: done

Problem:
Downstream agents need to know whether fields came from QC live snapshots, QC
daily snapshots, yfinance backfill, or stale caches.

Optimization:
Add provenance metadata to feature merging and agent bundles.

Acceptance criteria:
- Key fields include source and as-of date where practical.
- yfinance-filled fields are visible in Playground data quality.
- Stale fields are tagged and reduce readiness/confidence.

Resolution:
- Added `services/feature_provenance.py` for source/as-of annotations and
  compact freshness summaries.
- Market Brief now annotates QC heartbeat and QC daily snapshot fields with
  provenance and exposes `feature_provenance` to downstream agents.
- Researcher prompt includes a data provenance and freshness section.
- Playground data quality includes provenance summaries, keeping yfinance-filled
  and stale fields visible alongside existing strategy feature-contract gates.
- Added tests in `tests/test_feature_provenance.py` and updated snapshot merge
  provenance coverage.

### 7. Execution audit depth

Status: done

Problem:
Risk-approved target weights and actual execution outcome are not fully tied
together at order level. Position Manager max daily trades is based on proposed
actions, not actual same-day order count.

Optimization:
Improve execution audit and connect it to Position Manager.

Acceptance criteria:
- Execution log can distinguish proposed, sent, accepted, rejected, and filled
  actions where data is available.
- Position Manager can read today's actual execution count.
- Daily Analyst can compare intended vs executed weights.

Resolution:
- Added `services/execution_audit.py` for structured execution audit payloads
  and same-day actual action counting from `execution_log`.
- Semi-auto proposals now write `proposed` audit rows with intended weights,
  rebalance actions, and estimated cost.
- Full-auto executor now returns structured `accepted`, `rejected`, `failed`,
  or `skipped` audit payloads with QC response details where available.
- Position Manager now subtracts actual same-day execution actions before
  applying `max_daily_trades`.
- Daily Analyst treats accepted/sent/filled execution log rows as actual
  execution activity.
- Added tests in `tests/test_execution_audit.py` and expanded Position Manager
  daily-trade coverage.

### 8. Strategy health and decay detection

Status: done

Problem:
Strategies can generate weights, but the system lacks a stable health profile
per strategy across regimes.

Optimization:
Track strategy health by regime and detect performance decay.

Acceptance criteria:
- Store rolling IC, hit-rate, turnover, drawdown, and sample size per strategy.
- Decay detector flags strategy/regime pairs with worsening behavior.
- Parameter adjustment suggestions remain approval-only.

Resolution:
- Added `services/strategy_health.py` to maintain rolling per-strategy/per-regime
  health profiles in `system_config.strategy_health_profiles`.
- Playground replay metrics now include max drawdown when sample gates pass.
- Playground analysis cron persists rolling IC, hit-rate, turnover, drawdown,
  sample size, and reliability for each strategy/regime profile.
- Decay Detector now reads strategy health profiles and flags decaying
  strategy/regime pairs.
- Parameter suggestions are stored as approval-only recommendations and are not
  auto-applied.

## P2 Issues

### 9. Market Brief responsibility boundaries

Status: done

Problem:
Market Brief currently merges snapshots, news, memory, sector rotation, and
scenario analysis. It works, but can become hard to reason about.

Optimization:
Keep Market Brief as the assembler, but move specialized feature/provenance
logic into smaller helpers.

Acceptance criteria:
- Market Brief remains readable and mostly orchestration-oriented.
- Snapshot merge, feature provenance, memory context, and scenario context each
  have isolated helpers.

Resolution:
- Added `services/market_snapshot_merge.py` for daily feature snapshot
  normalization and heartbeat/daily snapshot merging.
- Added `services/market_brief_contexts.py` for scenario and memory context
  assembly with graceful degradation.
- Market Brief now imports specialized helpers and remains focused on
  orchestrating snapshot, news, quant facts, rotation, provenance, scenario, and
  memory assembly.
- Feature provenance remains isolated in `services/feature_provenance.py`.
- Added tests in `tests/test_market_brief_contexts.py` and updated snapshot
  merge tests to target the helper module directly.

### 10. Operational health report

Status: done

Problem:
The system sends several reports, but there is not yet a concise daily
operations report covering data freshness, cron health, feature coverage, and
pipeline status.

Optimization:
Add one operational report that summarizes whether the system is healthy enough
to trust today's analysis.

Acceptance criteria:
- Report includes latest QC heartbeat, latest daily feature snapshot, latest
  yfinance backfill, latest news cache, latest memory write, and failed crons.
- Report is short enough for Telegram.
- Report distinguishes research degradation from execution-blocking issues.

Resolution:
- Added `services/operational_health.py` to collect freshness and cron health
  into one operational snapshot.
- Morning health now appends a concise operational health report covering QC
  heartbeat, daily feature snapshot, yfinance backfill, news cache, memory
  write, pipeline status, and recent failed crons.
- The report classifies stale/missing execution-critical data separately from
  research degradation.
- Added tests in `tests/test_operational_health.py`.

## Suggested Execution Order

1. Fix Synthesizer `market_judgment` schema mismatch.
2. Add strategy feature contract and readiness verdict.
3. Harden Playground replay metric guardrails.
4. Add cron run audit log.
5. Connect DQS/calibration to agent behavior more explicitly.
6. Improve execution audit and Position Manager actual daily trade awareness.
7. Build strategy health and decay detection.
