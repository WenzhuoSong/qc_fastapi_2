# Decision Information Utilization Review Plan

Last updated: 2026-05-19

## Purpose

This document has three separate jobs:

1. Define the intended information-source contract.
2. Record what is implemented in code and covered by tests.
3. Record what still requires Railway live validation.

These are deliberately separate. A feature marked as code-implemented is not
considered live-proven until Railway pipeline output or Telegram output has been
validated.

This document reviews whether the trading system is designed and wired to use
the four major information sources in the ETF decision path:

- QC live and daily data
- yfinance historical data
- base knowledge
- news evidence

The immediate goal is not to add more data sources. The goal is to prove, per
ticker and per action, which evidence affected proposal shaping, scorecard
permission, risk clipping, position governance, manual review, final action, and
execution status.

## Status Semantics

```text
Code status      = files/tests/contracts exist locally.
Railway status   = behavior has been observed in real deployed pipeline output.
Live validation  = read-only validator or manual review confirms Railway output.
```

Do not treat `code implemented` as equivalent to `live validated`.

## Information-Source Contract

```text
QC data              -> current state and execution readiness
yfinance data        -> historical evidence and empirical behavior
base knowledge       -> asset/strategy/regime/risk context
news evidence        -> macro/ticker event risk and thesis pressure

all sources
  -> evidence_bundle
  -> market_scorecard
  -> strategy confidence / certification / calibration
  -> proposal_shaper
  -> risk_manager
  -> position_governance
  -> decision_ledger
  -> communicator / execution audit / daily memory
```

This is a specification. The implementation and live validation status are
tracked below.

## Implementation And Validation Matrix

| Capability | Code status | Railway status |
| --- | --- | --- |
| Resolver receives current `news_evidence` | implemented and unit-tested | pending live validation |
| Resolver marks unavailable scorecard/governance facts | implemented and unit-tested | pending live validation |
| `source_effects` in decision ledger | implemented and unit-tested | pending live validation |
| Telegram compact `sources=...` | implemented and unit-tested | pending live validation |
| `thesis_status` owner/validator contract | implemented and unit-tested | pending live validation |
| Explanation correctness for hard-risk/basket/advisory | implemented and unit-tested | pending live validation |
| `Data quality detail` display | implemented and unit-tested with source-specific labels | pending live validation |
| Phase 4 validator service | implemented and unit-tested | not yet run against Railway output |
| Pipeline `8b_live_validation` step log | implemented and unit-tested by compile/import checks | pending live validation |
| Warn/fail validation summary in Telegram | implemented | pending live validation |

## Live Validation Required Before Claiming Resolution

The following statements must not be treated as proven until Railway output is
validated:

- "Proposal shaping solved repeated invalid add proposals."
- "Telegram always shows source-specific data quality."
- "FTXL/PSI/SOXX basket losers reliably appear as manual trim review."
- "Hard-risk holdings never use stale safe-hold wording."
- "Decision ledger always shows `sources=...` for relevant rows."

Allowed wording before live validation:

```text
Code-side rule implemented and unit-tested; Railway behavior pending validation.
```

## Review Findings

### Finding 1: Knowledge Resolver Input Timing Is Incomplete

Code status: **implemented and unit-tested**

Railway status: **pending live validation**

Code reviewed:

- `services/pipeline.py`
- `services/evidence_bundle.py`
- `services/knowledge_resolver.py`

Code behavior before the Phase 1 fix:

- `pipeline` builds empirical profiles first.
- `pipeline` builds `news_evidence`.
- `build_evidence_bundle(...)` receives `news_evidence`.
- `build_evidence_bundle(...)` calls `_build_knowledge_section(...)`.
- `_build_knowledge_section(...)` passes computed facts to
  `resolve_knowledge(...)`.

Problem:

`_build_knowledge_section(...)` currently passes:

```python
"news_evidence": brief.get("news_evidence") or {}
"scorecard": brief.get("market_scorecard") or {}
"position_governance": brief.get("position_governance") or {}
```

At that point in the pipeline, `market_scorecard` and `position_governance` are
not yet produced, and `news_evidence` may not yet be written into `brief`.
This means the resolver can receive empty computed facts even though the
pipeline has already computed `news_evidence` locally.

Impact:

- Knowledge output may look present but not actually reflect news/scorecard or
  governance facts.
- Confidence adjustments may be based mostly on static context and empirical
  profiles.
- The system can overstate how fully "knowledge and computed data" are working
  together.

Code-side fix:

- Change `build_evidence_bundle(...)` and `_build_knowledge_section(...)` so
  structured `news_evidence` is passed directly into resolver computed facts.
- Do not pass unavailable `market_scorecard` or `position_governance` during the
  first evidence-bundle build.
- Add an explicit computed-facts availability block:

```json
{
  "computed_facts_available": {
    "news_evidence": true,
    "scorecard": false,
    "position_governance": false,
    "empirical_profiles": true
  }
}
```

Acceptance checks:

- Unit test proves resolver receives current `news_evidence` from the local
  `build_evidence_bundle(...)` argument.
- Resolver output records unavailable downstream facts explicitly instead of
  silently receiving `{}`.

Implemented in:

- `services/evidence_bundle.py`
- `services/knowledge_resolver.py`
- `tests/test_evidence_bundle.py`
- `tests/test_knowledge_resolver.py`

### Finding 2: Thesis Status Owner Is Implemented But Needs Contract Hardening

Code status: **implemented and unit-tested**

Railway status: **pending live validation**

Code reviewed:

- `agents/synthesizer.py`
- `services/position_governance.py`
- `tests/test_position_governance.py`

Code behavior:

- Synthesizer may propose `position_advisory_proposals[].thesis_status`.
- `position_governance` owns `_validate_thesis_status(...)`.
- Python validator accepts, overrides, or rejects the LLM thesis status.
- Output includes `execution_authority = none`.

Risk:

The owner is correct in code, but the contract must stay explicit because this
field appears in multiple docs and Telegram output.

Implemented hardening:

- Added contract tests that raw LLM `broken` cannot override deterministic
  `intact` evidence without validator override.
- Added contract tests that LLM thesis-only proposals cannot change target
  weights, force trims, or alter the position decision.
- Added documentation note:

```text
Owner: position_governance
LLM role: advisory proposal only
Execution authority: none
```

Acceptance checks:

- Existing and new tests prove thesis status cannot directly trigger execution.
- Docs name the owner and execution authority in one place.

Implemented in:

- `tests/test_position_governance.py`
- `docs/decision_information_utilization_review_plan.md`
- `docs/position_lifecycle_proposal_shaping_plan.md`

### Finding 3: Decision Ledger Aggregates Evidence But Needs Source-Effect Trace

Code status: **implemented and unit-tested**

Railway status: **pending live validation**

Code reviewed:

- `services/decision_ledger.py`
- `agents/communicator.py`
- `tests/test_decision_ledger.py`

Code behavior before the Phase 2 fix:

- Ledger includes current holdings/intraday evidence from holding metadata.
- Ledger includes historical evidence from empirical profiles.
- Ledger includes position governance decisions and explanations.
- Ledger includes proposed versus final action and execution status.

Gap:

The ledger can show evidence was available, but does not yet clearly answer:

```text
Which evidence source changed the decision?
```

For example:

- QC data caused `unrealized_loss_review`
- yfinance empirical profile provided historical context
- base knowledge caused `basket_review`
- news caused `hard_risk`
- scorecard caused `scorecard_human_required`

Code-side fix:

- Add a derived `source_effects` block per ticker.
- The mapping must be a static deterministic lookup table in code. It must not
  be inferred by an LLM, learned from runtime data, or vary between runs for the
  same `reason_code`.

```json
{
  "source_effects": {
    "qc": ["unrealized_loss_review", "high_atr"],
    "yfinance": ["empirical_profile_available"],
    "knowledge": ["basket_review", "satellite_loss_threshold"],
    "news": ["hard_risk"],
    "scorecard": ["scorecard_human_required"],
    "risk": ["risk_rejected"]
  }
}
```

Acceptance checks:

- Same input reason codes produce the same `source_effects` across repeated
  runs.
- Ledger tests cover hard-risk, basket-review, loss-review, and risk-rejected
  rows.
- Telegram displays compact source names for top decision ledger rows.

Implemented in:

- `services/decision_ledger.py`
- `agents/communicator.py`
- `tests/test_decision_ledger.py`
- `tests/test_communicator_scorecard.py`

### Finding 4: Explanation Layer Was Recently Fixed But Needs Live Validation

Code status: **implemented and unit-tested**

Railway status: **pending live validation**

Validation tooling: **implemented and unit-tested**

Code-side fixes:

- Hard-risk rows no longer fall back to "no deterministic rule requires
  reduction".
- Basket-review rows include correlated basket context.
- Advisory support is displayed as weak-positive support in manual trim review.

Remaining live risk:

This must be validated on Railway output with real positions such as
FTXL/PSI/SOXX/XLRE/XLV. Until then, explanation correctness should be marked as
recently fixed, not mature.

Acceptance checks:

- Next live Telegram shows hard-risk holdings with manual review language.
- Semiconductor basket losers show basket context.
- Advisory basket-loss manual review shows `advisory=weak-positive`.

Validation support:

- `services/decision_live_validation.py` validates the latest pipeline artifacts
  or supplied stage outputs.
- `tests/test_decision_live_validation.py` covers pass/fail/skipped validation
  cases.

### Finding 5: Data Quality Labels Need Source-Specific Meaning

Code status: **implemented and unit-tested**

Railway status: **pending live validation**

Code behavior:

- Operational health tracks QC heartbeat, daily feature snapshot, yfinance
  backfill, news cache, and memory write separately.
- Playground separates historical evidence from live fit.
- Communicator has `Data quality detail`.
- Communicator labels data-quality fields source-specifically:
  - `QC live snapshots=...`
  - `QC live fit=...`
  - `yfinance history=...`
  - `yfinance evidence=...`

Risk:

User-facing messages can still be interpreted as "data is bad" even when the
actual meaning is "heartbeat is fresh but live-fit samples are insufficient".

Required display behavior:

- Keep data-quality display source-specific:

```text
QC heartbeat: fresh/stale
Daily snapshot: fresh/stale/limited
yfinance history: strong/medium/weak/missing
QC live fit: aligned/conflicted/insufficient
News: fresh/stale/missing
Execution permission: allowed/advisory/human_required/blocked
```

Acceptance checks:

- Telegram never collapses these into only `data=limited`.
- When QC heartbeat is fresh but QC live fit is insufficient, Telegram must
  display both facts separately, for example:

```text
QC heartbeat: fresh
QC live fit: insufficient
```

- Tests cover at least one case where QC heartbeat is fresh but live fit is
  insufficient.

## Development Sequence

### Phase 1: Resolver Computed-Facts Wiring

Priority: **highest**

Code status: **implemented and unit-tested**

Railway status: **pending live validation**

Tasks:

- Pass local `structured_news_evidence` into `_build_knowledge_section(...)`.
- Add resolver availability metadata for computed facts.
- Avoid implying scorecard/governance facts are present during the first
  evidence-bundle build.
- Add tests in `tests/test_evidence_bundle.py` or
  `tests/test_knowledge_resolver.py`.

Expected impact:

- Knowledge output becomes honest about which computed facts it is wired to use.
- News and empirical profile integration become auditable.

### Phase 2: Ledger Source-Effect Trace

Priority: **high**

Code status: **implemented and unit-tested**

Railway status: **pending live validation**

Tasks:

- Add deterministic mapping from reason codes/evidence fields to source effects.
- Store `source_effects` in each ticker ledger row.
- Add tests in `tests/test_decision_ledger.py`.
- Show compact source effects in Telegram for top rejected/manual review rows.

Expected impact:

- Each ledger row can show which source category contributed to the recorded
  reason codes.

### Phase 3: Thesis Status Contract Hardening

Priority: **high**

Code status: **implemented and unit-tested**

Railway status: **pending live validation**

Tasks:

- Add owner/execution-authority note to docs.
- Add or strengthen tests proving LLM thesis cannot directly execute or bypass
  deterministic validator.

Expected impact:

- Prevents future drift where raw LLM thesis status becomes action authority.

This phase can run in parallel with Phase 2 because it is mostly contract and
test hardening.

### Phase 4: Live Validation Checklist

Priority: **deployment validation**

Code status: **read-only validation tooling implemented and unit-tested**

Railway status: **not yet run against live Railway output**

Tasks:

- Validate Railway Telegram output for:
  - `Data quality detail`
  - `Proposal shaping`
  - `manual trim review`
  - `advisory=weak-positive`
  - hard-risk explanation wording
  - decision ledger proposed/final/execution distinction

Expected impact:

- Provides a repeatable way to confirm whether code-side fixes are visible in
  real operator output.

Implemented behavior:

- Validates `Data quality detail` visibility.
- Validates `Proposal shaping` visibility only when the shaper clipped.
- Validates `manual trim review` visibility only when manual hints exist.
- Validates `advisory=weak-positive` when
  `advisory_basket_loss_review` exists.
- Validates hard-risk explanations do not regress to stale safe-hold wording.
- Validates Decision ledger proposed/final distinction.
- Validates compact `sources=` output when ledger `source_effects` are present.
- Pipeline writes an `8b_live_validation` step log after communicator.
- Pipeline stores validation output in `risk_out["live_validation"]`.
- Pipeline appends compact validation summary to Telegram only when validation
  is `warn` or `fail`.

Implemented in:

- `services/decision_live_validation.py`
- `services/pipeline.py`
- `agents/communicator.py`
- `tests/test_decision_live_validation.py`
- `tests/test_communicator_scorecard.py`

## Non-Goals

- Do not add a new LLM agent.
- Do not let news alone trigger forced selling.
- Do not let yfinance alone authorize buying.
- Do not let raw LLM `thesis_status` execute trades.
- Do not recompute governance decisions inside `decision_ledger`.
- Do not let `decision_ledger` or communicator explanations create fallback
  governance decisions when `position_governance` output is missing.
- Do not collapse source-specific data quality into one vague label.
