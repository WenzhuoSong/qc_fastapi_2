# Weekend Trading Review Development Plan

> Goal: turn the newly versioned decision/execution logs into a weekend
> learning loop without giving the review loop any trading authority.
>
> Core rule:
>
> ```text
> Python computes scores.
> LLM explains scores.
> Reports may recommend review.
> Reports may not mutate targets, policies, thresholds, or execution state.
> ```

---

## 1. Scope

This plan adds an off-hours review loop:

```text
validation_observations
execution_logs
diagnostic_artifacts
account snapshots
market feature rows
        |
        v
deterministic weekly metrics
        |
        v
LLM narrative summary over fixed metrics
        |
        v
versioned append-only weekly reports
        |
        v
Telegram/operator review
```

The loop is designed for weekends and market-closed windows. It is not part of
the live execution path.

### Required Invariants

- `execution_authority = none`
- `target_weight_mutation = none`
- no QC command submission
- no direct config mutation
- no strategy promotion
- no threshold changes
- no target-builder input writes
- all reports are versioned and append-only
- all headline metrics are computed by deterministic Python code
- LLM can only consume computed metrics and produce explanatory text

---

## 2. Non-Goals

This plan must not:

- make weekend reports trade-authoritative
- loosen risk validation
- change `execution_policy`
- promote any strategy from advisory/shadow to gated
- adjust hedge thresholds automatically
- decide to buy inverse ETFs
- read legacy raw JSON as training-authoritative data
- evaluate immature labels as if they were mature
- use historical replay as proof that a strategy should be promoted

Historical replay has only veto power:

```text
bad replay  -> may reject a strategy candidate
good replay -> may only allow live shadow observation
promotion   -> requires point-in-time live shadow samples
```

---

## 3. Data Inputs

Use only authoritative or explicitly limited data sources.

| Source | Use | Training Authority |
|---|---|---|
| `validation_observations` | intent/execution, hedge, basket, execution truth | yes, if versioned |
| `execution_logs` | command lifecycle and QC response | yes, if reconciled/typed |
| `diagnostic_artifacts` | feature snapshots, candidate/ranking/mix/debate artifacts | yes, if versioned |
| `account_state_snapshots` | account and holdings truth | yes, for truth/reconciliation only |
| `market_daily_features` | future labels and price paths | yes, source-tagged |
| legacy `AgentStepLog` raw JSON | debugging only | no |
| raw LLM output blobs | debugging only | no |

Training/review dataset builders must call `training_data_authority` or an
equivalent authority check before consuming structured data.

---

## 4. Deterministic Metrics Contract

All metrics below are computed by Python. LLM summaries must not invent or
override them.

### 4.0 Shared Metric Rules

#### Rate Guard

Every outcome-based rate must carry sample-size status.

```json
{
  "metric": "debate_changed_ticker_outcome_win_rate",
  "value": null,
  "status": "insufficient_sample",
  "sample_n": 3,
  "min_sample_n": 20
}
```

Rules:

- if mature sample count `< min_sample_n`, return `status=insufficient_sample`
- do not return a headline rate value for insufficient samples
- count insufficient samples separately
- LLM summaries must say "insufficient sample" rather than interpreting the
  numeric direction

Default minimums:

| Metric Family | Default `min_sample_n` |
|---|---:|
| execution truth counts | 1 |
| blocker distribution | 1 |
| hedge outcome rates | 20 |
| debate outcome rates | 20 |
| regime/risk hit rates | 30 |
| basket outcome comparisons | 20 |
| weekly self-assessment | 5 |

#### Weekly Attribution Window

Weekly execution metrics are assigned by command creation/submission week:

```text
week_bucket = command.created_at/submitted_at week
state       = latest lifecycle state observed at review time
```

Example:

```text
Command sent Friday and reconciled Monday:
  - belongs to Friday's week bucket
  - Friday review may show pending/in-flight
  - next review self-assessment updates whether that pending command resolved
```

Do not double-count the command in both weeks.

#### Authority Gate First

Metric builders must consume already-authorized/versioned inputs. The authority
gate is a prerequisite for PR1 metric implementation, not a later polish step.

For any structured input:

```text
if not training_data_authority.allowed:
    exclude from metric numerator/denominator
    count under excluded_non_authoritative_source
```

### 4.1 Execution Truth Metrics

Purpose: verify whether commands actually changed account state.

Inputs:

- `validation_observations` where `observation_type = execution_truth`
- `execution_logs`
- latest `account_state_snapshots`
- reconciliation guard summaries

Metrics:

| Metric | Definition |
|---|---|
| `commands_sent` | count of commands with execution authority |
| `accepted_count` | QC accepted ownership |
| `filled_count` | filled / reconciled commands |
| `noop_count` | no-op reconciled commands |
| `partial_count` | partial execution commands |
| `rejected_count` | QC or preflight rejected commands |
| `duplicate_target_count` | same-target dedupe events |
| `reconciliation_divergence_count` | expected vs actual drift above tolerance |
| `stuck_in_flight_count` | in-flight commands older than timeout |

Output artifact:

```json
{
  "schema_version": "weekly_execution_truth_review_v1",
  "execution_authority": "none",
  "target_weight_mutation": "none",
  "metrics": {},
  "evidence_refs": [],
  "llm_summary": null
}
```

### 4.2 Intent vs Execution Metrics

Purpose: explain why desired actions did or did not become execution.

Inputs:

- `validation_observations` where `observation_type = intent_vs_execution`
- `blocker_events`

Metrics:

| Metric | Definition |
|---|---|
| `risk_block_count` | blocked by risk validation |
| `final_validation_block_count` | blocked by final validation |
| `execution_preflight_block_count` | blocked by command preflight |
| `daily_command_cap_block_count` | blocked by daily command cap |
| `daily_turnover_cap_block_count` | blocked by daily turnover cap |
| `dedupe_count` | not sent because recent target was identical |
| `approved_not_sent_count` | approved targets that did not reach QC |
| `hedge_triggered_not_added_count` | hedge intent triggered but inverse ETF not added |

Output artifact:

```json
{
  "schema_version": "weekly_intent_execution_review_v1",
  "blocker_distribution": {},
  "unexecuted_intents": [],
  "execution_authority": "none"
}
```

### 4.3 Label Maturity Metrics

Purpose: prevent premature outcome conclusions.

Inputs:

- `OutcomeLabel` records when available
- `validation_observations`
- market feature rows

Metrics:

| Metric | Definition |
|---|---|
| `label_1d_mature_count` | mature 1d labels |
| `label_5d_mature_count` | mature 5d labels |
| `label_20d_mature_count` | mature 20d labels |
| `label_1d_pending_count` | not yet mature |
| `label_5d_pending_count` | not yet mature |
| `label_20d_pending_count` | not yet mature |
| `eligible_label_count` | labels with training authority |
| `fallback_label_count` | source-limited fallback labels |
| `excluded_immature_count` | labels explicitly excluded due to immaturity |

Hard rule:

```text
Immature labels are counted and excluded.
LLM may not use them as supporting evidence.
```

### 4.4 Hedge Review Metrics

Purpose: evaluate inverse ETF decision quality without selection bias.

Inputs:

- `hedge_intent` observations
- `intent_vs_execution` observations
- market feature rows for SPY, QQQ, and candidate hedge ETFs

Metrics:

| Metric | Definition |
|---|---|
| `hedge_trigger_count` | triggered hedge intent |
| `hedge_added_count` | inverse ETF actually added |
| `triggered_not_added_count` | triggered but no inverse ETF |
| `false_negative_count` | no hedge trigger, later market drop exceeded threshold |
| `triggered_no_drop_count` | hedge triggered, but market did not drop beyond threshold |
| `triggered_hedge_would_hurt_count` | hedge triggered, and candidate hedge ETF would have hurt |
| `missed_protection_count` | no inverse ETF and later drawdown exceeded threshold |
| `hedge_would_have_helped_count` | candidate inverse ETF return would improve outcome |
| `hedge_would_have_hurt_count` | candidate inverse ETF return would hurt outcome |

False-negative sample definition:

```text
hedge_triggered = false
and SPY or QQQ forward 5d return <= configured drawdown threshold
```

This is mandatory. Do not evaluate hedge thresholds only on triggered samples.

#### Hedge Counterfactual Contract

`hedge_would_have_helped_count` and `hedge_would_have_hurt_count` are
counterfactual metrics. They must be reproducible.

Inputs:

```text
decision_time
candidate_hedge_instrument  # e.g. SH, PSQ, RWM
severity
hedge_weight_policy_version
decision-time feature snapshot
candidate hedge ETF real historical prices
portfolio return over horizon, if available
```

Required algorithm:

```python
hedge_weight = hedge_weight_from_severity(severity, policy_version)
entry_price  = candidate ETF price observed at or before decision_time
exit_price   = candidate ETF price at the mature horizon
hedge_return = exit_price / entry_price - 1
hedge_contribution = hedge_weight * hedge_return
```

Rules:

- use the candidate inverse ETF's real price path
- do not approximate hedge return as `-1 * underlying_return`
- record `hedge_weight`, `entry_price_source`, `exit_price_source`,
  `price_source`, and `policy_version`
- if price data is fallback/yfinance, mark source-limited
- if entry/exit price is unavailable, return `status=insufficient_data`
- if severity-to-weight mapping is unavailable, return
  `status=missing_counterfactual_policy`

This avoids overstating inverse ETF protection by ignoring real ETF path,
tracking error, fees, and daily rebalance effects.

### 4.5 Debate Impact Metrics

Purpose: decide whether bull/bear/cross-exam adds value.

Inputs:

- `debate_impact_v1` artifacts
- target builder diagnostics
- final target
- mature outcome labels

Metrics:

| Metric | Definition |
|---|---|
| `debate_available_count` | decisions with debate artifacts |
| `disagreement_count_total` | total disagreement tickers |
| `debate_changed_target_count` | disagreements that changed target/final result |
| `debate_change_rate` | changed decisions / debate decisions |
| `changed_ticker_outcome_win_rate` | outcome quality where debate changed result |
| `unchanged_ticker_outcome_baseline` | outcome quality where debate did not change result |
| `debate_failure_count` | bull/bear/cross-exam failed or missing |

LLM may explain why debate was or was not useful, but cannot compute the rate.
Outcome win rates must use the shared Rate Guard. If mature changed-sample
count is below `min_sample_n`, return `insufficient_sample`, not a percentage.

### 4.6 Regime / Risk Call Metrics

Purpose: evaluate whether market/risk interpretation matches future outcomes.

Inputs:

- `market_risk_assessment_v1`
- `decision_feature_snapshot_v1`
- mature outcome labels
- SPY/QQQ/IWM/sector ETF feature rows

Metrics:

| Metric | Definition |
|---|---|
| `regime_direction_hit_rate_1d` | regime risk direction vs 1d market return |
| `regime_direction_hit_rate_5d` | regime risk direction vs 5d market return |
| `regime_direction_hit_rate_20d` | regime risk direction vs 20d market return |
| `risk_off_precision` | risk_off calls followed by negative market outcome |
| `risk_off_recall_proxy` | negative market outcomes preceded by risk_off/defensive call |
| `hard_risk_outcome_count` | hard-risk ticker calls with mature outcomes |

`risk_off_recall_proxy` is intentionally a proxy, not true recall.

Definition:

```text
denominator = observable windows where forward return crossed the configured
              negative-outcome threshold for the selected horizon
numerator   = denominator windows preceded by risk_off/defensive call
```

Limitations that must travel with the metric:

- true "should have been risk_off" is not directly observable
- metric is horizon-specific
- a 5d non-event that becomes a 20d drawdown belongs to the 20d proxy, not the
  5d proxy
- use alongside precision; never interpret proxy recall alone

LLM summaries must include the proxy caveat when citing this metric.

### 4.7 Basket / Portfolio Structure Metrics

Purpose: evaluate active basket constraints without changing execution.

Inputs:

- `active_basket` observations
- `portfolio_mix_event_v1`
- final target weights
- account holdings snapshots

Metrics:

| Metric | Definition |
|---|---|
| `active_count_avg` | average non-cash active count |
| `active_count_out_of_range_count` | outside basket policy range |
| `subscale_position_count` | below role min but above floor |
| `floor_cleared_count` | min floor removals |
| `cash_avg` | average cash target/actual |
| `effective_n_avg` | weight diversification where available |

### 4.8 Weekly Review Self-Assessment

Purpose: make the review loop review itself.

Inputs:

- previous weekly review artifacts
- current mature labels / outcomes

Metrics:

| Metric | Definition |
|---|---|
| `prior_recommendation_count` | review suggestions last week |
| `prior_recommendation_mature_count` | suggestions now evaluable |
| `prior_recommendation_supported_count` | subsequent data supports suggestion |
| `prior_recommendation_contradicted_count` | subsequent data contradicts suggestion |
| `prior_recommendation_pending_count` | still immature |

This prevents the review loop from producing recommendations that are never
audited.

---

## 5. LLM Contract

The weekend LLM receives:

- deterministic metric payloads
- evidence references
- mature-label counts
- explicit exclusions
- previous-week recommendation outcomes

The weekend LLM may output:

- operator-readable summary
- hypotheses to investigate
- candidate follow-up tasks
- "review suggested" flags
- questions for the operator

The weekend LLM must not output:

- executable target weights
- policy changes
- threshold changes
- strategy promotion decisions
- hedge buy/sell instructions
- claims based on immature labels
- claims based on unversioned raw JSON

Required summary footer:

```text
This report has execution_authority=none and target_weight_mutation=none.
All quantitative conclusions are computed by deterministic metrics.
LLM text is explanatory only.
```

---

## 6. Cron Boundary

Cron entry:

```bash
python -m cron.weekend_trading_review
```

This cron must:

- run only in off-hours or weekends unless manually invoked
- read existing data only
- not call `run_full_pipeline`
- not call `run_executor_async`
- not call QC command routes
- not mutate system configs
- not create pending approvals
- write only versioned review artifacts
- optionally send a Telegram summary

The review cron is physically separate from the execution pipeline. It should
not import execution entrypoints except read-only helpers.

---

## 7. PR Plan

### PR0: Review Data Authority Gate

Before metrics can be trusted, the review loop must reject non-authoritative
inputs.

Files likely involved:

- `services/weekend_review_loader.py`
- `tests/test_weekend_review_loader.py`

Scope:

- load only versioned `validation_observations`
- load only versioned `diagnostic_artifacts`
- load typed `execution_logs`
- load source-tagged account snapshots / feature rows
- reject legacy raw JSON as review/training authority
- use `training_data_authority` for structured inputs

Definition of Done:

- legacy raw JSON cannot enter metric builders as authoritative data
- fallback label sources are counted separately
- mixed feature authority is scope-limited
- excluded inputs are counted with reasons
- PR1 manual metric runs can only use loader-approved inputs

### PR1: Deterministic Weekly Metrics Foundation

Implement pure metric builders.

Files likely involved:

- `services/weekend_review_metrics.py`
- `tests/test_weekend_review_metrics.py`

Scope:

- input normalization helpers
- label maturity counting
- blocker distribution
- execution truth metrics
- hedge false positive / false negative metrics
- debate impact metrics
- review self-assessment metric skeleton
- shared Rate Guard helper
- hedge counterfactual helper using real candidate ETF prices

No LLM call in PR1.

Definition of Done:

- metrics are deterministic
- immature labels are excluded and counted
- insufficient samples return `status=insufficient_sample`, not a headline rate
- hedge review includes false negatives
- hedge review separates `triggered_no_drop` from `triggered_hedge_would_hurt`
- hedge counterfactual uses real candidate inverse ETF price path, not `-1x`
  underlying approximation
- weekly command attribution uses command created/submitted week and latest state
- no execution imports
- all outputs include `execution_authority=none`

### PR2: Versioned Weekly Review Artifacts

Add Pydantic models for weekly reports.

Files likely involved:

- `services/weekend_review_artifacts.py`
- `tests/test_weekend_review_artifacts.py`

Schemas:

- `weekly_execution_truth_review_v1`
- `weekly_intent_execution_review_v1`
- `weekly_label_maturity_review_v1`
- `weekly_hedge_review_v1`
- `weekly_debate_impact_review_v1`
- `weekly_regime_risk_review_v1`
- `weekly_strategy_basket_review_v1`
- `weekly_review_self_assessment_v1`

Definition of Done:

- every artifact has `schema_version`
- every artifact has `execution_authority=none`
- every artifact has `target_weight_mutation=none`
- artifacts are JSONB-safe
- artifacts are append-only
- artifacts include metric payload and evidence refs

### PR3: Weekend Review LLM Summary

Add LLM narrative over deterministic metrics only.

Files likely involved:

- `services/weekend_review_summarizer.py`
- `tests/test_weekend_review_summarizer.py`

Scope:

- build prompt from metric payloads only
- include explicit immature-label exclusions
- include prior recommendation outcome metrics
- enforce no execution instructions
- parse summary as text/report metadata only

Definition of Done:

- prompt contains no target-builder inputs
- prompt contains no permission to change policy/config
- tests assert forbidden phrases/actions are rejected or stripped
- summary carries `execution_authority=none`

### PR4: Weekend Cron

Create cron entry.

Files likely involved:

- `cron/weekend_trading_review.py`
- `tests/test_weekend_trading_review_cron.py`

Scope:

- run metrics
- build artifacts
- call summarizer
- persist artifacts
- send compact Telegram summary

Definition of Done:

- cron does not import or call execution pipeline
- cron is safe when market is open but should default to off-hours schedule
- failures alert as ops failures, not trading failures
- output is append-only

### PR5: Dashboard / Operator View

Add a simple view for review artifacts.

Files likely involved:

- dashboard read-only surface
- Telegram summary formatter

Scope:

- latest weekly review
- blocker distribution
- label maturity
- hedge false negatives / false positives
- debate value metrics
- prior recommendation self-assessment

Definition of Done:

- data first, styling second
- no execution controls in the review view
- every recommendation is visibly "review-only"

---

## 8. Rollout

1. Implement PR0 authority-gated loader.
2. Implement PR1 metrics only.
3. Run metrics manually on recent loader-approved data.
4. Confirm no LLM narrative is needed to calculate conclusions.
5. Implement PR2 artifacts.
6. Persist one manual review artifact set.
7. Add LLM summary in PR3.
8. Add weekend cron in PR4.
9. Add dashboard/Telegram view in PR5.

Do not enable automated strategy/policy changes from this report at any point.

---

## 9. Acceptance Criteria

The weekend learning loop is acceptable when it can answer:

```text
1. What did the system try to do this week?
2. What did it actually send to QC?
3. What did QC actually execute?
4. Why did approved targets fail to execute?
5. Which blockers dominated?
6. Which labels are mature enough to evaluate?
7. Were hedge thresholds too conservative or too aggressive?
8. Did bull/bear debate materially change outcomes?
9. Did active basket constraints reduce noise without hiding risk?
10. Did last week's review recommendations age well?
```

And when all answers are backed by deterministic metrics rather than LLM-only
judgment.

---

## 10. Final Positioning

This is not an alpha engine.

It is the off-hours learning layer that turns logs into reviewable evidence.
Its job is to reduce self-deception:

- no LLM self-grading
- no immature-label conclusions
- no replay-based promotion
- no triggered-sample-only hedge analysis
- no legacy raw JSON as authority

The correct endpoint is a system that can say:

```text
Here is what happened.
Here is how we measured it.
Here is what the data suggests reviewing.
No execution authority is granted by this report.
```
