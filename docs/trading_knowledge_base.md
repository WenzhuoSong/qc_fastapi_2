# Trading Knowledge Base v1

## Goal

The knowledge base gives agents compact, auditable trading context. It is not a
decision engine. Deterministic scorecard, position governance, and execution
validators remain the referee; the knowledge base improves explanations,
research prompts, and risk interpretation.

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

### Strategies

Files: `knowledge/strategies/*.yaml`

Each strategy records:

- best and weak regimes
- required features
- failure modes
- governance implications
- evidence sources

Expected use:

- explain why a strategy is `primary`, `advisory`, or `watch_only`
- explain conflicts such as “momentum looks good historically but live consensus is defensive”
- help LLM reports avoid overclaiming when live samples are limited

### Assets

Files: `knowledge/assets/*.yaml`

Each asset records:

- asset class and sector group
- primary risk drivers
- positive and weak regimes
- holding policy
- governance notes

Expected use:

- explain why losing positions are still held, trimmed, or reviewed
- distinguish broad market ETF risk from leveraged ETF risk
- connect position governance to sector concentration and ATR alerts

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
