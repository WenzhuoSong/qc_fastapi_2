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
```

If any strategy flips three or more times in seven days, treat it as an input
to a future hysteresis sprint. The flip count is observability-only and must
not mutate thresholds.
