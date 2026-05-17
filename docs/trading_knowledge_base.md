# Trading Knowledge Base v1

## Goal

The knowledge base gives agents compact, auditable trading context. It is not a
decision engine. Deterministic scorecard, position governance, and execution
validators remain the referee; the knowledge base improves explanations,
research prompts, and risk interpretation.

The intended module boundary is:

```text
computed data
  yfinance history, QC snapshots, Playground replay, scorecard, news, positions
        |
        v
knowledge module
  static knowledge + empirical profiles + strategy certification + resolver
        |
        v
downstream users
  researcher, synthesizer, risk manager, communicator, position explanations
```

Core rule:

```text
Knowledge does not replace computation.
Knowledge interprets, constrains, and explains computed facts.
```

## v1 Scope

The first version is intentionally small:

| Direction | Count | Why This Count Is Useful |
| --- | ---: | --- |
| Strategies | 3 | Covers the strategies currently used by Playground: momentum, mean reversion, low volatility. |
| Assets | 20 | Covers core holdings, defensive bond/cash ETFs, semiconductor ETFs, and key thematic ETFs. |
| Regimes | 5 | Covers the regime labels already used by market scoring and strategy replay. |
| Risk principles | 8 | Covers the hard governance rules we already enforce or plan to enforce. |
| Sources | 7 | Separates academic, issuer, and internal-policy evidence from runtime decisions. |

This is enough to make every current alert and strategy result explainable
without loading a broad trading encyclopedia into the prompt.

## Knowledge Directions

### Asset Basic Knowledge

Files: `knowledge/assets/*.yaml`

Each ETF knowledge card should answer:

- What is this ETF?
- What asset class, sector, theme, or exposure does it represent?
- Is it leveraged, inverse, daily reset, or otherwise unsuitable for long holds?
- What are the primary risk drivers?
- Which regimes or macro contexts are usually supportive or dangerous?
- What governance notes should position management remember?

Current v1 fields:

- `asset_class`
- `sector_group`
- `summary`
- `risk_drivers`
- `positive_regimes`
- `weak_regimes`
- `holding_policy`
- `governance_notes`
- `sources`

Derived v1.1 fields, stored outside YAML:

```yaml
empirical_behavior:
  lookback_days: 756
  data_source: yfinance
  samples: 756
  best_regimes: []
  weak_regimes: []
  avg_return_by_regime: {}
  volatility_by_regime: {}
  max_drawdown: null
  correlation_top: {}
  macro_sensitivity:
    rate: unknown
    vix: unknown
    dollar: unknown
    oil: unknown
```

Important distinction:

- Static facts can be manually curated from issuer and internal policy sources.
- Historical behavior should be generated from yfinance/QC data, not invented by
  LLM prose.
- Historical behavior should not be written into static YAML. Store it in a
  derived DB table or JSON sidecar with `generated_at`, `source`, and staleness
  metadata, then let the resolver merge it at runtime.

### Strategies

Files: `knowledge/strategies/*.yaml`

Each strategy knowledge card should represent a validated algorithmic idea, not
just a narrative trading opinion.

Current v1 fields:

- best and weak regimes
- required features
- failure modes
- governance implications
- evidence sources

Planned v1.1 certification fields, stored in a derived layer:

```yaml
certification:
  status: experimental
  approved_use: watch_only
  certified_at: null
  historical:
    samples: 0
    sharpe: null
    hit_rate: null
    max_drawdown: null
    avg_turnover: null
  live:
    qc_snapshots: 0
    forward_samples: 0
    fit: insufficient
  promotion_requirements:
    min_historical_samples: 120
    min_live_samples: 30
    max_turnover: 0.50
```

Expected use:

- explain why a strategy is `primary`, `advisory`, or `watch_only`
- explain conflicts such as “momentum looks good historically but live consensus is defensive”
- help LLM reports avoid overclaiming when live samples are limited

Certification source:

- Playground historical replay should write historical validation.
- QC live snapshots should write live validation.
- Manual YAML should describe the algorithm idea and risk assumptions.
- First-stage states should be realistic:
  - `experimental`
  - `research_supported`
  - `advisory`
  - `disabled`
- `certified` is a later target after enough live samples and stable turnover
  evidence exist.

### Regimes

Files: `knowledge/regimes/*.yaml`

Each regime records:

- strategies it supports
- strategies it weakens
- risk notes

Expected use:

- ground strategy selection in regime logic
- explain live consensus/regime conflicts
- keep Telegram reports consistent across reruns

### Risk Principles

Files: `knowledge/risk_principles/*.yaml`

Each principle records:

- trigger conditions
- governance action
- assets it applies to
- what cannot override it

Expected use:

- explain hard limits like no add under high ATR
- make LLM advisory boundaries explicit
- support position-level explanations without letting prose bypass validators

### Sources

File: `knowledge/sources/registry.yaml`

Sources are references, not live data. They tell agents whether a knowledge item
comes from academic research, issuer factsheets, or internal governance policy.

## Runtime Usage

`services.knowledge_base.build_knowledge_context(...)` loads the YAML files and
returns only the relevant subset:

- current holdings and consensus tickers
- active Playground strategy names
- current regime
- reason codes from strategy confidence and execution permission

`services.evidence_bundle.build_evidence_bundle(...)` now includes this compact
context under `bundle["knowledge"]`.

## Knowledge Module Plug

The next development stage should introduce a resolver interface instead of
letting each agent interpret raw YAML independently.

### Input Contract

```python
KnowledgeQuery(
    purpose="research|risk|position_explain|telegram|scorecard",
    tickers=["QQQ", "TLT", "SOXL"],
    strategies=["momentum_lite_v1"],
    regime="trending_bull",
    computed_facts={
        "market": {},
        "strategies": {},
        "positions": {},
        "news_evidence": {},
        "scorecard": {},
        "position_governance": {},
    },
)
```

### Output Contract

```python
KnowledgeResolution(
    advisory_context=[],
    hard_constraints=[],
    conflicts=[],
    interpretation_hints=[],
    confidence_adjustments={
        "intended_consumer": "strategy_confidence_calibrator",
        "items": [],
    },
    missing_knowledge=[],
    source_trace=[],
)
```

### Output Semantics

`advisory_context` is for LLM agents:

- ETF profile summaries
- strategy assumptions
- regime interpretation
- explanation hints

`hard_constraints` is for deterministic Python layers:

- high ATR blocks adds
- leveraged ETF long-hold warnings
- human confirmation cannot be bypassed
- no strategy certification means no strategy-driven add

`conflicts` are explicit facts that need attention:

- regime says trending bull, but live consensus is defensive
- strategy historical evidence is strong, but live QC samples are insufficient
- ETF has positive momentum, but position governance blocks add due to ATR

`confidence_adjustments` are bounded suggestions, not final decisions. They have
exactly one intended consumer:

```text
strategy_confidence_calibrator
```

Risk manager, position governance, researcher, synthesizer, and communicator
must not directly apply `confidence_adjustments`. They may display the final
calibrated strategy confidence after the calibrator accepts or rejects an item,
but they must not independently subtract the same delta.

```json
{
  "intended_consumer": "strategy_confidence_calibrator",
  "items": [
    {
      "target_type": "strategy",
      "target": "momentum_lite_v1",
      "delta": -0.10,
      "reason": "live_consensus_conflict",
      "max_abs_delta": 0.15
    }
  ]
}
```

Rejected adjustments must be recorded as observability events and must not block
the pipeline. Accepted adjustments must be applied once before scorecard/risk
layers consume strategy confidence.

`missing_knowledge` records absent or stale knowledge dependencies:

```json
{
  "kind": "empirical_profile",
  "id": "SOXL",
  "severity": "warning",
  "reason": "profile_stale",
  "fallback": "static_asset_profile_only"
}
```

Handling rules:

- `info`: log only; do not alter output.
- `warning`: keep static advisory context, add warning, suppress derived claims
  that depend on the missing data.
- `blocking`: suppress the affected advisory context and mark any related
  confidence adjustment as rejected.

Blocking examples:

- A strategy knowledge card is missing a required field such as
  `required_features`, so the resolver cannot know whether computed features
  satisfy the strategy contract.
- A derived empirical profile has an incompatible schema version and cannot be
  safely parsed.
- A strategy certification record says `advisory`, but required validation
  metrics are absent or malformed.

Missing knowledge must not silently upgrade confidence. When empirical behavior
is missing or stale, resolver output should say it used static knowledge only.

## Proposed Components

### Static Knowledge Provider

Current implementation:

- `services/knowledge_base.py`
- `knowledge/**/*.yaml`

Purpose:

- Load manually curated static knowledge.
- Return compact relevant subsets.
- Validate counts and missing items.

### Knowledge Resolver

Planned implementation:

- `services/knowledge_resolver.py`

Purpose:

- Combine computed data with static knowledge.
- Emit conflicts, constraints, and interpretation hints.
- Normalize knowledge output for all downstream agents.
- Propose confidence adjustments for the single calibrator consumer only.

### Strategy Confidence Calibrator

Planned implementation:

- `services/strategy_confidence_calibrator.py`

Purpose:

- Consume `KnowledgeResolution.confidence_adjustments`.
- Apply accepted deltas exactly once to strategy confidence.
- Reject adjustments that exceed `max_abs_delta`, target unknown strategies, or
  depend on blocking missing knowledge.
- Record accepted/rejected adjustments for observability.

Pipeline position:

```text
Playground strategy confidence
  -> Knowledge Resolver
  -> Strategy Confidence Calibrator
  -> Scorecard / Risk Manager / Position Governance
```

Scorecard, risk manager, position governance, and agents consume only
post-calibration strategy confidence.

### Empirical Profile Provider

Planned implementation:

- `services/empirical_profiles.py`

Purpose:

- Generate ETF empirical behavior from yfinance/QC history.
- Update a derived DB table or JSON sidecar with regime-conditioned behavior.
- Keep `knowledge/assets/*.yaml` static and manually reviewable.
- Keep historical behavior data-driven.

### Strategy Certification Provider

Planned implementation:

- `services/strategy_certification.py`

Purpose:

- Read Playground replay and live QC fit.
- Generate `experimental / research_supported / advisory / disabled`.
- Reserve `certified` for a later phase after enough live evidence exists.
- Prevent unverified strategy ideas from influencing real allocation.

Promotion/demotion rules should be symmetric enough to avoid one-way upgrades:

- Promote `experimental -> research_supported` when historical samples are
  sufficient and replay quality is positive.
- Promote `research_supported -> advisory` when live fit has enough samples,
  turnover is within limits, and current regime fit is not persistently
  conflicted.
- Demote `advisory -> research_supported` when live fit is conflicted for a
  configured rolling window or turnover repeatedly breaches limits.
- Demote any state to `disabled` on schema failure, missing required features,
  unrecoverable data quality, or explicit operator override.

## Guardrails

- Knowledge items do not create trades.
- Knowledge items do not override scorecard, risk manager, or position governance.
- LLM can use this context to explain or propose advisory changes.
- Python validators decide whether an advisory proposal is accepted.

## Expansion Rules

Add new knowledge only when it has a runtime use:

- A new strategy enters Playground or pipeline.
- A new ETF becomes tradable or appears in holdings.
- A new regime label appears in scorecard/playground.
- A repeated risk reason needs consistent explanation.

Avoid adding broad market theory unless it changes one of:

- confidence scoring
- suggested use
- risk validation
- position explanation
- Telegram/final-report observability

## Development Plan

### Phase 1: Knowledge Resolver Plug

Implement:

- `services/knowledge_resolver.py`
- `services/strategy_confidence_calibrator.py`
- `KnowledgeQuery` and `KnowledgeResolution` typed contracts
- MVP deterministic resolver rules only:
  - `high_atr_no_add`
  - `leveraged_etf_caution`
  - `regime_strategy_conflict`

Acceptance criteria:

- `evidence_bundle["knowledge"]` remains compact.
- resolver output separates `advisory_context` from `hard_constraints`.
- calibrator applies accepted confidence adjustments exactly once before
  scorecard/risk layers read strategy confidence.
- rejected confidence adjustments are recorded and do not block the pipeline.
- blocking `missing_knowledge` rejects related confidence adjustments and
  suppresses affected advisory context.
- tests show the same input always returns the same conflicts and constraints.
- no downstream agent is blocked by a large resolver rollout.

Deferred resolver rules:

- human-required execution boundary
- historical strong but live insufficient
- live consensus conflict
- strategy certification gate

Confidence adjustment consumption:

- only `strategy_confidence_calibrator` may apply adjustment deltas.
- scorecard, risk manager, position governance, and LLM agents consume the
  post-calibration confidence only.
- rejected adjustments are logged and ignored without blocking execution.

### Phase 2: ETF Empirical Behavior

Implement:

- yfinance/QC profile builder for tracked ETF universe
- regime-conditioned return/volatility/drawdown profile
- correlation and sensitivity summaries
- derived storage with `generated_at`, `lookback_days`, and data quality status

Acceptance criteria:

- historical behavior is data-generated, not LLM-written.
- historical behavior is not stored in static YAML.
- missing or stale data produces explicit warnings.
- ETF profile can explain “usually works under X, weak under Y”.

### Phase 3: Strategy Certification

Implement:

- certification builder from Playground replay metrics
- live QC validation status
- promotion/demotion rules

Acceptance criteria:

- first-stage states are `experimental`, `research_supported`, `advisory`, and
  `disabled`.
- only `advisory` strategies may materially affect allocation.
- `research_supported` can improve explanation and monitoring but cannot create
  large target changes.
- demotion rules exist before promotion rules affect execution.
- Telegram can say why a strategy is advisory or watch-only.
- unverified strategies remain research-only.

Deferred:

- `certified` strategy status.
- adaptive use of advisory-quality feedback.
- counterfactual scoring more advanced than next-day return vs SPY.

### Phase 4: Downstream Wiring

Integrate resolver output into:

- researcher prompt
- synthesizer prompt
- risk manager context
- communicator explanation
- position governance explanation hints

Acceptance criteria:

- all agents consume the same resolved knowledge contract.
- each agent test uses mock resolver output and does not depend on YAML fixtures.
- integration tests verify no downstream component directly imports
  `services.knowledge_base` except resolver/calibrator tests.
- hard constraints are consumed by Python validators, not trusted to LLM memory.
