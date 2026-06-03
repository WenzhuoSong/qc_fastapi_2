# Post-Execution Stabilization: Mapping and Policy Sync Plan

> Goal: stabilize the system after the first successful end-to-end FULL_AUTO
> execution by cleaning alpha input semantics, enforcing QC fallback policy
> consistency, and removing stale operator-facing language.
>
> This is not a new-guard plan. It is a semantic cleanup plan.

---

## Current Verified State

The execution path is no longer the primary blocker.

Recent live-paper evidence:

```text
analysis_234
  FastAPI -> QC SetWeights
  QC policy stage passed
  QC executed all target SetHoldings calls
  QC ACK accepted http=200
  lifecycle: qc_accepted -> orders_submitted -> filled -> reconciled
  heartbeat: last_command_id=analysis_234, active_execution_status=filled,
             processed_command_count=1, open_order_count=0
```

Recent post-execution fixes:

```text
QC inline policy parsing disabled for SetWeights
  Reason: PythonNet/QC dictionary objects can expose non-callable members.
  Result: QC uses compiled fallback / PolicySync policy and no longer crashes
          with NoneType callable in the policy stage.

Execution policy turnover excludes CASH
  Reason: CASH is a residual balance, not an executable ticker. QC current
          holdings snapshots may omit CASH, which previously inflated turnover.
  Result: risk-reducing trims that raise CASH no longer fail with fake
          execution_policy_violation.
```

The system is now in this phase:

```text
Execution chain: usable
Risk contract: active and mostly converged
Alpha input layer: still noisy
Operator display: still contains old semantic labels
QC fallback policy deployment: still manually synchronized
```

---

## Priority Order

The next work must happen in this order:

```text
PR1  Mapping Error Audit and Classification
PR2  Strategy/ETF Mapping Cleanup
PR3  QC Fallback Policy Generation and Sync Check
PR4  Telegram/Dashboard Semantic Cleanup
PR5  Alpha Attribution Readiness Handoff
```

Rationale:

1. Mapping cleanup comes before display cleanup because mapping errors affect
   EvidenceCard vote coverage, evidence quality multipliers, and any future
   attribution input quality.
2. QC fallback policy sync is close to P0.5 because FastAPI and QC policy drift
   can cause policy mismatch, rejected commands, or silent deployment skew.
3. Telegram/dashboard cleanup comes after mapping cleanup so the display does
   not merely hide real input-layer issues.
4. Alpha attribution should not become trade-authoritative until voted signals
   and mapping coverage are clean enough to trust.

---

## Non-Goals

- Do not add another risk guard.
- Do not loosen execution policy caps.
- Do not bypass `account_state_guard`, `final_risk_validation`,
  `target_envelope`, `auto_pause`, or QC validation.
- Do not make attribution or independence directly trade-authoritative in this
  stabilization phase.
- Do not treat `mapping_error` as a dashboard-only cosmetic issue.
- Do not let QC fallback policy remain a manually edited shadow copy without a
  check.

---

## PR1: Mapping Error Audit and Classification

### Problem

Current reports show high mapping noise, for example:

```text
mapping_error=47
missing_strategy_profile
missing_compatibility_mapping
asset_profile warning
```

These are not all equivalent.

```text
missing_strategy_profile
  The strategy knowledge profile is absent or not loaded.
  This is usually a knowledge/config problem.

missing_compatibility_mapping
  The strategy profile exists, but lacks a mapping for the ETF role/action.
  This affects whether a score can become a voted EvidenceCard.

missing_asset_profile
  The ETF profile is absent or stale.
  This affects role, allowed actions, decay/liquidity rules, and diagnostics.

action_not_allowed_by_asset_profile
  Not a bug by itself. This should be watch, not mapping_error.
```

### Scope

Create a structured mapping audit report.

Expected function shape:

```python
def build_strategy_mapping_audit(
    *,
    strategy_ids: list[str],
    tickers: list[str],
    strategy_profiles: dict,
    asset_profiles: dict,
) -> dict:
    ...
```

Output schema:

```python
{
    "total_rows": int,
    "by_reason": {
        "missing_strategy_profile": int,
        "missing_compatibility_mapping": int,
        "missing_asset_profile": int,
        "action_not_allowed_by_asset_profile": int,
    },
    "hard_mapping_errors": list[dict],
    "normal_watch_rows": list[dict],
    "strategy_coverage": {
        "momentum_lite_v1": {
            "voted_or_watch_rows": int,
            "mapping_error_rows": int,
            "eligible_ticker_count": int,
            "coverage_pct": float,
        }
    },
    "ticker_coverage": {
        "DRAM": {
            "eligible_strategy_count": int,
            "mapping_error_count": int,
            "watch_count": int,
            "abstain_count": int,
        }
    },
}
```

Coverage definition:

```text
eligible_ticker_count
  Denominator: tickers in the strategy's declared universe after role/universe
  filtering.

coverage_pct
  Numerator: voted rows + watch rows.
  Denominator: eligible_ticker_count.

abstain rows
  Not counted in coverage_pct. Abstain means "no voting right for this
  strategy/ticker at this time", not "covered but constrained".

mapping_error rows
  Counted separately as hard coverage failures.
```

### Important Semantic Rule

`mapping_error` must mean "configuration/knowledge is missing or invalid".

It must not include:

- normal watch behavior
- action blocked by asset profile safety rules
- insufficient history
- field not applicable

### Acceptance Criteria

```text
□ Audit report separates missing_strategy_profile, missing_compatibility_mapping,
  missing_asset_profile, and normal watch rows.
□ Existing EvidenceCard vote statuses are not changed yet.
□ Report can list the top 20 hard mapping errors by frequency.
□ Report can answer: "Which strategy profiles are missing?".
□ Report can answer: "Which ETF asset profiles are missing?".
□ `coverage_pct` uses voted+watch over eligible_ticker_count, excluding
  abstain rows from the numerator and denominator.
□ Unit tests cover all four reason classes.
```

### Baseline Artifact

After PR1 is implemented, save the first audit output as a baseline artifact:

```text
docs/mapping_audit_baseline_YYYYMMDD.json
```

PR1 command:

```bash
python tools/run_strategy_mapping_audit.py \
  --output docs/mapping_audit_baseline_YYYYMMDD.json \
  --summary-only
```

This baseline is required for PR2 review. PR2 should report both:

```text
absolute target: hard mapping errors within threshold
relative progress: hard mapping errors reduced vs baseline
```

---

## PR2: Strategy/ETF Mapping Cleanup

### Problem

Evidence quality and future attribution depend on clean voted/watch/abstain
semantics. If a strategy/ETF pair is missing compatibility mapping, it cannot be
trusted as a real abstain or real vote.

### Scope

Use PR1 audit output to fix knowledge files, not runtime guards.

Likely files:

```text
knowledge/strategies/*.yaml
knowledge/asset_profiles*.yaml or equivalent asset profile source
services/strategy_evidence.py
services/evidence_vote_aggregation.py
tests/test_strategy_evidence.py
tests/test_alpha_strategy_plugins.py
tests/test_evidence_vote_aggregation.py
```

### Cleanup Rules

1. Missing strategy profile:
   - Add or register the profile.
   - If the strategy should not participate, mark it explicitly disabled or
     research-only. Do not leave it as missing.

2. Missing compatibility mapping:
   - Add a compatibility mapping for each intended ETF role.
   - If the strategy should not cover a role, emit
     `strategy_universe_mismatch`, not `mapping_error`.

3. Missing asset profile:
   - Add asset profile for the ETF.
   - If the ticker is intentionally outside execution universe, make that
     explicit in execution policy / universe config.

4. Watch behavior:
   - Keep `vote_status=watch`.
   - Do not alert as knowledge mapping error.

### Target Metrics

Initial target:

```text
hard mapping_error_count: <= 5
missing_strategy_profile: 0 for active or advisory strategies
missing_asset_profile: 0 for tickers in execution_policy.TICKER_ROLES
normal watch rows: allowed and visible, not alerting
```

Longer-term target:

```text
hard mapping_error_count: 0 in normal pipeline runs
```

Waiver rule:

```text
Active/advisory strategies:
  hard mapping_error_count must be 0.

Disabled/research-only strategies:
  hard mapping_error_count may remain <= 5 only if each row has an explicit
  waiver reason and does not enter evidence-quality or attribution authority.
```

### Acceptance Criteria

```text
□ Active/advisory strategies have strategy profiles.
□ All execution-policy tickers have asset profiles or explicit non-coverage
  reason.
□ Strategy/ETF non-coverage is represented as strategy_universe_mismatch or
  watch, not mapping_error.
□ Evidence cap observe no longer reports dozens of mapping_error rows during
  normal operation.
□ Tests prove mapping_error does not include normal watch rows.
□ Tests prove abstain still does not count as score=0.
```

### PR2 Progress Artifact

After PR2 cleanup, save the new audit output separately from the baseline:

```text
docs/mapping_audit_after_pr2_YYYYMMDD.json
```

The expected PR2 comparison is:

```text
baseline hard_mapping_error_count: 264
after PR2 hard_mapping_error_count: 0
normal watch rows remain allowed and non-alerting
```

---

## PR3: QC Fallback Policy Generation and Sync Check

### Problem

QC fallback policy currently lives outside the FastAPI git repo:

```text
../quantconnect_files/test1.py
```

This creates deployment skew risk:

```text
FastAPI execution_policy.py updated
QC fallback TICKER_ROLES / ROLE_CAPS not updated
  -> policy mismatch
  -> QC reject
  -> or subtle cap inconsistency
```

There is already a useful baseline test:

```text
tests/test_qc_fallback_policy_contract.py
```

But the next step is to make policy sync harder to forget.

### Scope

Add a generator/checker that derives QC fallback policy snippets from
`services.execution_policy`.

Expected tool:

```text
tools/generate_qc_fallback_policy.py
```

Expected outputs:

```text
TICKER_ROLES = {...}
ROLE_CAPS = {...}
POLICY_VERSION = "sprint8a"
```

The tool should support:

```bash
python tools/generate_qc_fallback_policy.py --print
python tools/generate_qc_fallback_policy.py --check ../quantconnect_files/test1.py
```

### Design Constraints

- Do not automatically rewrite QC `main.py` in this PR.
- Do not increase QC file size unnecessarily.
- Do not introduce a dependency on QC deployment APIs.
- The checker must fail if FastAPI policy and QC fallback policy differ.

### Acceptance Criteria

```text
□ Generator emits deterministic policy snippets from execution_policy.py.
□ Checker compares generated snippets against ../quantconnect_files/test1.py.
□ Existing test_qc_fallback_policy_contract.py is preserved or strengthened.
□ CI/local test fails if TICKER_ROLES, ROLE_CAPS, or POLICY_VERSION diverge.
□ Documentation explains the manual QC deployment step.
```

### Operator Workflow

Before deploying QC:

```bash
python tools/generate_qc_fallback_policy.py --check ../quantconnect_files/test1.py
```

If it fails:

```text
1. Update QC fallback policy snippet.
2. Ensure QC file remains below 64,000 character limit.
3. Deploy QC.
4. Verify heartbeat policy_version == FastAPI policy_snapshot()["version"].
```

### CI and Review Workflow

The checker must run in two places:

```text
CI / PR review
  Purpose: block merge when services/execution_policy.py changes without a
  matching QC fallback policy update.

Local QC deployment checklist
  Purpose: verify that the file about to be pasted/deployed to QuantConnect is
  consistent with the FastAPI policy currently being deployed.
```

Add this instruction to the developer deployment docs or README:

```text
Before merging any services/execution_policy.py change:
  Run: python tools/generate_qc_fallback_policy.py --check ../quantconnect_files/test1.py
  Must pass before PR approval.
```

---

## PR4: Telegram and Dashboard Semantic Cleanup

### Problem

The system still exposes stale terms from the SEMI_AUTO era:

```text
scorecard_human_required
Market scorecard requires human confirmation
human_required
execution_log.status=accepted while lifecycle terminal state is reconciled
```

This confuses operator interpretation. In FULL_AUTO, many of these are not
manual-confirmation requirements; they are automatic tightening rules.

### Scope

Clean external-facing language without changing trade logic.

Likely files:

```text
agents/communicator.py
services/decision_ledger.py
services/execution_gateway.py
dashboard/app.py
tests/test_communicator_scorecard.py
tests/test_execution_gateway.py
tests/test_dashboard.py
```

### Semantic Replacement

Use these terms externally:

```text
scorecard_tightened
  The scorecard reduced permission or delta, but did not stop FULL_AUTO.

hard_risk_block
  A safety/policy/account condition stopped execution.

review_flag
  Human review is useful, but not required for automatic risk-reducing flow.

lifecycle_terminal_state
  The latest terminal command lifecycle state: reconciled, drift, failed,
  superseded, timeout_no_execution_confirmed.
```

Avoid these externally except in historical/debug payloads:

```text
human_required
scorecard_human_required
requires human confirmation
```

### Execution Status Display Rule

Dashboard and Telegram should prefer lifecycle state over `execution_log.status`.

Priority:

```text
1. latest terminal lifecycle event
   reconciled / reconciliation_drift / failed_no_fill / superseded /
   timeout_no_execution_confirmed
2. latest active lifecycle event
   orders_submitted / partial / accepted
3. execution_log.qc_status
4. execution_log.status
```

Example:

```text
Wrong:
  analysis_234 status=accepted

Right:
  analysis_234 lifecycle=reconciled
  ack=accepted
  accepted is not reconciled, but this command has reconciled.
```

### Acceptance Criteria

```text
□ Telegram no longer says "Market scorecard requires human confirmation" for
  FULL_AUTO scorecard tightening.
□ Scorecard tightening is displayed as tightened, with classes/reasons.
□ Hard blockers remain visually distinct from tightening.
□ Dashboard recent command status uses lifecycle terminal state when available.
□ analysis_234-like commands display as reconciled, not merely accepted.
□ Tests cover old payload compatibility but new user-facing text.
```

### Evidence Matrix Display Rule

Default visible rows:

```text
voted
mapping_error
```

Default collapsed rows:

```text
watch
abstain
```

Reason:

```text
watch
  Normal safety behavior. For example, a strategy can score UVXY but asset
  policy can correctly force action=watch.

abstain
  Normal non-voting state. Causes include insufficient_history,
  field_not_applicable, or strategy_universe_mismatch.

mapping_error
  Configuration/knowledge issue. This should remain visible by default.
```

---

## PR5: Alpha Attribution Readiness Handoff

### Purpose

This PR does not make attribution trade-authoritative yet. It defines what must
be true before attribution and independence can influence strategy weights.

### Preconditions

```text
□ hard mapping_error_count is near zero in normal runs
□ voted/watch/abstain/mapping_error semantics are stable
□ strategy profile coverage is complete for active/advisory strategies
□ live signal ledger has enough samples to distinguish early_signal from noise
□ attribution model quality checks exist
```

### Readiness Report

Add or update an alpha readiness report that shows:

```text
strategy_id
  mapping_coverage_pct
  voted_signal_count
  abstain_count by reason
  mapping_error_count
  live_sample_count
  residual_alpha_latest
  residual_alpha_regime_specific
  redundancy_cluster
  suggested authority: disabled / advisory / candidate / gated
```

### Suggested Authority Criteria

These criteria are not trading gates in this PR. They make the readiness report
deterministic and reviewable.

```text
disabled
  Strategy is explicitly disabled, missing profile, or has recurring hard
  mapping errors in active/advisory coverage.

advisory
  Strategy has a valid profile and can emit EvidenceCards, but does not meet
  candidate criteria.

candidate
  live_sample_count >= 30
  mapping_coverage_pct >= 0.80
  residual_alpha_latest >= 0.0
  no recurring hard mapping_error in the last 10 cycles

gated
  Out of scope for this stabilization plan. Later promotion should require:
    live_sample_count >= 100
    residual_alpha_latest > 0.0
    independence cluster correlation <= 0.65 vs existing gated strategies
```

### Acceptance Criteria

```text
□ Attribution remains diagnostic-only.
□ Report explicitly states why a strategy is not ready for authority.
□ Report consumes cleaned mapping statuses from PR1/PR2.
□ No strategy is promoted solely because of historical/yfinance evidence.
□ suggested authority follows the disabled/advisory/candidate criteria above.
```

---

## Rollout Sequence

```text
Step 1  Implement PR1 audit report.
Step 2  Run audit on current universe and save baseline counts.
Step 3  Implement PR2 mapping/profile cleanup until hard mapping errors are
        within target.
Step 4  Implement PR3 QC fallback policy generator/checker.
Step 5  Implement PR4 display semantic cleanup.
Step 6  Implement PR5 alpha readiness report.
Step 7  Run the Global DoD check before deployment/review.
```

Recommended deployment checkpoints:

```text
After PR1:
  No behavior change expected. Only diagnostics improve.

After PR2:
  Evidence cap observe should show fewer hard mapping errors.
  Strategy coverage should improve or become explicitly non-covered.

After PR3:
  Local/CI check should catch FastAPI/QC fallback policy drift before deploy.

After PR4:
  Operator messages should become shorter and less misleading.

After PR5:
  Alpha attribution has a clean input-readiness gate but remains diagnostic.

After Step 7:
  The stabilization track has a single read-only verification report covering
  mapping health, QC fallback policy sync, display semantics, lifecycle status
  usage, and alpha readiness authority.
```

---

## Global Definition of Done

This stabilization track is complete when:

```text
□ Normal pipeline runs do not report large hard mapping_error counts.
□ Every active/advisory strategy has a profile.
□ Every execution-policy ticker has an asset profile or explicit exclusion.
□ Watch/abstain/mapping_error are semantically distinct in reports.
□ QC fallback policy can be checked against FastAPI execution_policy.py.
□ Operator-facing output uses scorecard_tightened instead of human_required
  for automatic tightening.
□ Dashboard command status uses lifecycle terminal state when available.
□ Alpha attribution remains diagnostic until mapping and sample-readiness
  gates are satisfied.
□ No new risk guard was added to solve a semantic cleanup problem.
```

The checklist above must be verified with:

```bash
python3 tools/run_post_execution_stabilization_check.py --summary-only
```

Expected current result:

```text
status=passed
passed_count=9
failed_count=0
hard_mapping_error_count=0
qc_fallback_policy.ok=true
```

---

## Review Questions

Before starting implementation, review these decisions:

1. Should PR3 checker be CI-only, or should it also run in the normal local
   deployment checklist?
   - Decision: both.
2. Should missing asset profiles for non-held tickers alert immediately, or only
   when they appear in candidate targets?
   - Decision: alert only when they appear in candidate targets; otherwise keep
     visible in the audit report.
3. What is the acceptable hard mapping error threshold after PR2: zero, or
   fewer than five known/waived rows?
   - Decision: zero for active/advisory strategies; <= 5 waived rows allowed
     only for disabled/research-only strategies.
4. Should dashboard hide normal watch rows by default and show them only in an
   expandable evidence matrix?
   - Decision: yes. Show voted + mapping_error by default; collapse watch +
     abstain.
5. When PR5 is complete, what minimum live sample count should allow a strategy
   to move from advisory to candidate?
   - Decision: 30 live samples, plus mapping_coverage_pct >= 0.80,
     residual_alpha_latest >= 0.0, and no recurring hard mapping_error in the
     last 10 cycles.
