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
