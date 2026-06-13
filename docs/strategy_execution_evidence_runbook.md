# Strategy Execution Evidence Runbook

Last updated: 2026-06-12

This runbook covers the execution-grade strategy evidence gate. The gate does
not create a new trading path and does not change small-add sizing limits. It
only decides whether certified strategy evidence may release the scorecard
strategy-evidence no-add boundary.

## Normal Behavior

- Strategies with insufficient execution evidence remain `research_only`.
- The scorecard emits `insufficient_execution_evidence` and blocks add.
- Existing holdings are not force-liquidated because a strategy downgrades.
- Risk-reducing trims remain allowed.
- A strategy with enough deterministic evidence becomes
  `execution_grade_validated` and may use the existing small-add constraints.

## Kill Switch

System config key:

```json
{
  "strategy_execution_evidence_config": {
    "enabled": true,
    "force_advisory_only": true,
    "min_live_samples_for_execution": 5,
    "state_scope": "strategy_level"
  }
}
```

Use the kill switch when:

- a strategy appears to be wrongly certified,
- certification evidence checks look stale or inconsistent,
- the certification logic itself is suspected to have a bug,
- production behavior differs from the frozen funnel evidence.

Expected behavior after enabling `force_advisory_only=true`:

- all strategies with otherwise actionable evidence return to
  `insufficient_execution_evidence`,
- add is blocked by scorecard semantics,
- existing holdings are preserved,
- risk-reducing trims remain available,
- operator/weekend review evidence shows the failed
  `execution_evidence_enabled` check.

Expected behavior after setting `force_advisory_only=false`:

- certification is recomputed from current frozen evidence,
- strategies that pass every evidence check return to
  `execution_grade_validated`,
- no persisted promotion state needs manual cleanup.

## Timing

The gate is evaluated when the pipeline builds the evidence bundle and
scorecard. It affects new decisions from the next pipeline run or signal freeze.
It does not rewrite already frozen decision artifacts.

## First Production Certification Checklist

When the first strategy flips to `execution_grade_validated`:

1. Inspect the frozen `evidence_checks` snapshot.
2. Confirm `live_samples_min`, data quality, historical sample, live fit,
   walk-forward, and turnover checks all passed.
3. Confirm the first small add uses the existing sizing limits.
4. Confirm the command goes through lifecycle, fingerprint, QC ACK, and
   reconciliation.
5. Confirm the next outcome label is created on schedule.

The pipeline sends a Telegram notification when a strategy newly enters
`execution_grade_validated` from the frozen decision-funnel artifact. The
notification is observability-only: it does not change target weights, sizing
limits, execution authority, or scorecard thresholds.

## Flip Monitoring

Weekend review/operator pack reports:

```text
Strategy evidence flips: 7d=<count> alerts=<count>
Strategy evidence readiness: strategies=<count> insufficient=<count> live_min_failed=<count> live0=<count> stalled=<count> closest=<strategy>(live=<actual>/<threshold>, failed=<count>)
```

If any strategy flips three or more times in seven days, treat it as an input
to a future hysteresis sprint. The flip count is observability-only and must
not mutate thresholds.

## Readiness Distance

The readiness line answers why a strategy is still blocked from execution-grade
small adds. It is diagnostic-only and is computed from the frozen
`evidence_checks` snapshot in the decision-funnel artifact.

- `live_min_failed` counts strategies whose latest frozen check still lacks the
  required live samples.
- `live0` counts strategies that are blocked by live-sample evidence and have
  zero live samples in the frozen snapshot.
- `stalled` counts strategies whose latest frozen evidence has remained at
  zero live samples for the configured observation window. Treat this as a
  data/replay coverage investigation trigger, not as a reason to lower the
  execution evidence gate.
- `closest` shows the nearest strategy by failed-check count and live-sample
  gap. It is an operator review cue, not an automatic promotion queue.
- `signal_weighted_effective_n` remains an alpha observability trend and is not
  a certification input.

Use this section to distinguish "waiting for live evidence" from data-quality,
walk-forward, turnover, or suggested-use blockers before changing any rule.

## Bootstrap Deadlock Check

After deploying readiness-distance artifacts, inspect the next production
decision-funnel artifact first. It should include actual/threshold values for
the live-sample check. If all execution candidates remain blocked by
`live_samples_min` for three consecutive weekly reviews and the live-sample
actuals do not improve, treat it as a possible bootstrap deadlock and open a
human review. Do not silently keep waiting.

The first question in that review is where the live samples come from:

- If replay/shadow/out-of-sample evidence can feed the certification samples,
  continue observing and fix data coverage if the sample counter is not moving.
- If certification samples require real-money execution, the system may need a
  separate, human-approved seeding decision. See the seeding boundary below.

## Blocked-Buy Counterfactuals

Weekend review also reports blocked-buy counterfactual outcomes:

```text
Blocked-buy counterfactual: 1d=<outperformed>/<mature> 5d=<outperformed>/<mature> 20d=<outperformed>/<mature> review_only=true
```

The source of truth is the frozen decision-funnel `buy_intents` artifact when
available. Older decision-style blocked positions are used only as a legacy
fallback. These outcomes answer whether blocked buy candidates later beat the
benchmark. They are review inputs only and must not automatically change
scorecard, certification, or sizing thresholds.

Read 1d results as an early diagnostic only. Threshold review should wait for
the intended 5d/20d horizons to mature and should account for trading costs,
slippage, and candidate-selection bias.

## Seeding Boundary

Do not create an automatic seeding path for insufficient strategies. If review
later proves that replay/shadow evidence cannot produce certification samples,
the only acceptable first step is a separate human-reviewed seeding proposal
with a tiny bounded size, explicit `seeding` labeling, and normal lifecycle,
fingerprint, QC ACK, and reconciliation. That path does not exist by default.
