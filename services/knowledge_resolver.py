"""
Knowledge resolver MVP.

This module merges compact static knowledge with computed facts and emits a
single downstream contract. It does not decide trades and it does not apply
confidence adjustments; that is delegated to strategy_confidence_calibrator.
"""
from __future__ import annotations

from typing import Any


CONFIDENCE_ADJUSTMENT_CONSUMER = "strategy_confidence_calibrator"
LEVERAGED_ASSET_CLASSES = {"leveraged_etf", "inverse_etf", "leveraged_inverse_etf"}


def resolve_knowledge(
    *,
    knowledge_context: dict[str, Any] | None,
    computed_facts: dict[str, Any] | None = None,
    purpose: str = "evidence_bundle",
) -> dict[str, Any]:
    """Resolve compact knowledge into advisory context, constraints, and conflicts."""
    context = knowledge_context or {}
    facts = computed_facts or {}
    assets = context.get("assets") or []
    strategies = context.get("strategies") or []
    regimes = context.get("regimes") or []
    risk_principles = context.get("risk_principles") or []
    current_regime = (
        ((facts.get("market") or {}).get("regime"))
        or ((context.get("selection") or {}).get("regime"))
        or ""
    )

    empirical_profiles = facts.get("empirical_profiles") or {}
    computed_facts_available = _computed_facts_available(facts)
    advisory_context = _asset_advisory_context(
        assets=assets,
        empirical_profiles=empirical_profiles,
    )
    advisory_context.extend(_strategy_advisory_context(strategies))
    hard_constraints = _hard_constraints(
        assets=assets,
        risk_principles=risk_principles,
    )
    conflicts = _strategy_regime_conflicts(
        strategies=strategies,
        regime=str(current_regime),
    )
    interpretation_hints = _interpretation_hints(
        assets=assets,
        regimes=regimes,
        conflicts=conflicts,
    )
    confidence_adjustments = _confidence_adjustments(conflicts)
    missing_knowledge = _missing_knowledge(
        context=context,
        empirical_profiles=empirical_profiles,
    )

    if _has_blocking_missing_knowledge(missing_knowledge):
        confidence_adjustments["items"] = [
            {
                **item,
                "status": "rejected",
                "rejection_reason": "blocking_missing_knowledge",
            }
            for item in confidence_adjustments["items"]
        ]

    return {
        "available": bool(context.get("available", True)),
        "purpose": purpose,
        "advisory_context": advisory_context,
        "hard_constraints": hard_constraints,
        "conflicts": conflicts,
        "interpretation_hints": interpretation_hints,
        "confidence_adjustments": confidence_adjustments,
        "missing_knowledge": missing_knowledge,
        "computed_facts_available": computed_facts_available,
        "computed_facts_summary": _computed_facts_summary(facts),
        "source_trace": _source_trace(
            assets=assets,
            strategies=strategies,
            regimes=regimes,
            risk_principles=risk_principles,
        ),
        "warnings": list(context.get("warnings") or []),
    }


def _computed_facts_available(facts: dict[str, Any]) -> dict[str, bool]:
    explicit = facts.get("computed_facts_available")
    if isinstance(explicit, dict):
        return {
            "news_evidence": bool(explicit.get("news_evidence")),
            "scorecard": bool(explicit.get("scorecard")),
            "position_governance": bool(explicit.get("position_governance")),
            "empirical_profiles": bool(explicit.get("empirical_profiles")),
        }
    return {
        "news_evidence": bool(facts.get("news_evidence")),
        "scorecard": bool(facts.get("scorecard")),
        "position_governance": bool(facts.get("position_governance")),
        "empirical_profiles": bool(facts.get("empirical_profiles")),
    }


def _computed_facts_summary(facts: dict[str, Any]) -> dict[str, Any]:
    news = facts.get("news_evidence") or {}
    macro = news.get("macro_news_score") or {}
    hard_risk = news.get("hard_risk_events") or {}
    return {
        "news_evidence": {
            "overall_bias": macro.get("overall_bias"),
            "data_quality": macro.get("data_quality"),
            "hard_risk_tickers": sorted(str(ticker) for ticker in hard_risk.keys()),
        },
        "empirical_profiles": {
            "count": len(facts.get("empirical_profiles") or {}),
        },
    }


def _asset_advisory_context(
    assets: list[dict[str, Any]],
    empirical_profiles: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for asset in assets:
        ticker = str(asset.get("id") or "")
        profile = empirical_profiles.get(ticker) or {}
        out.append(
            {
                "type": "asset_profile",
                "id": ticker,
                "summary": asset.get("summary"),
                "asset_class": asset.get("asset_class"),
                "sector_group": asset.get("sector_group"),
                "risk_drivers": asset.get("risk_drivers") or [],
                "holding_policy": asset.get("holding_policy"),
                "empirical_behavior": _compact_empirical_profile(profile),
            }
        )
    return out


def _strategy_advisory_context(strategies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for strategy in strategies:
        out.append(
            {
                "type": "strategy_profile",
                "id": strategy.get("id"),
                "category": strategy.get("category"),
                "summary": strategy.get("summary"),
                "best_regimes": strategy.get("best_regimes") or [],
                "weak_regimes": strategy.get("weak_regimes") or [],
                "failure_modes": strategy.get("failure_modes") or [],
            }
        )
    return out


def _hard_constraints(
    *,
    assets: list[dict[str, Any]],
    risk_principles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for principle in risk_principles:
        if principle.get("id") == "high_atr_no_add":
            out.append(
                {
                    "id": "high_atr_no_add",
                    "type": "position_action_constraint",
                    "action": "block_add",
                    "applies_to": principle.get("applies_to") or ["all_assets"],
                    "reason": principle.get("summary"),
                    "source": "risk_principle",
                }
            )

    for asset in assets:
        asset_class = str(asset.get("asset_class") or "")
        if asset_class in LEVERAGED_ASSET_CLASSES:
            out.append(
                {
                    "id": "leveraged_etf_caution",
                    "type": "holding_policy_constraint",
                    "ticker": asset.get("id"),
                    "action": "prefer_short_term_or_review",
                    "reason": asset.get("summary"),
                    "source": "asset_profile",
                }
            )
    return _unique_dicts(out)


def _strategy_regime_conflicts(
    *,
    strategies: list[dict[str, Any]],
    regime: str,
) -> list[dict[str, Any]]:
    if not regime:
        return []
    out: list[dict[str, Any]] = []
    for strategy in strategies:
        weak_regimes = set(strategy.get("weak_regimes") or [])
        if regime in weak_regimes:
            out.append(
                {
                    "id": "regime_strategy_conflict",
                    "type": "strategy_regime_conflict",
                    "strategy": strategy.get("id"),
                    "regime": regime,
                    "severity": "warning",
                    "reason": f"{strategy.get('id')} lists {regime} as a weak regime",
                }
            )
    return out


def _interpretation_hints(
    *,
    assets: list[dict[str, Any]],
    regimes: list[dict[str, Any]],
    conflicts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for conflict in conflicts:
        out.append(
            {
                "id": "explain_regime_strategy_conflict",
                "target": conflict.get("strategy"),
                "hint": (
                    "Treat historical strategy evidence cautiously when the current "
                    "regime is listed as weak for that strategy."
                ),
            }
        )
    for asset in assets:
        asset_class = str(asset.get("asset_class") or "")
        if asset_class in LEVERAGED_ASSET_CLASSES:
            out.append(
                {
                    "id": "explain_leveraged_etf_caution",
                    "target": asset.get("id"),
                    "hint": "Explain leverage and daily reset risk before suggesting add/hold increases.",
                }
            )
    for regime in regimes:
        out.append(
            {
                "id": "explain_regime_context",
                "target": regime.get("id"),
                "hint": regime.get("summary"),
            }
        )
    return out


def _confidence_adjustments(conflicts: list[dict[str, Any]]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for conflict in conflicts:
        if conflict.get("id") != "regime_strategy_conflict":
            continue
        strategy = conflict.get("strategy")
        if not strategy:
            continue
        items.append(
            {
                "target_type": "strategy",
                "target": strategy,
                "delta": -0.10,
                "max_abs_delta": 0.15,
                "reason": "regime_strategy_conflict",
                "source_conflict": conflict.get("id"),
                "status": "proposed",
            }
        )
    return {
        "intended_consumer": CONFIDENCE_ADJUSTMENT_CONSUMER,
        "items": items,
    }


def _missing_knowledge(
    *,
    context: dict[str, Any],
    empirical_profiles: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    selection = context.get("selection") or {}
    selected_assets = set(selection.get("tickers") or [])
    available_assets = {str(asset.get("id")) for asset in context.get("assets") or []}
    for ticker in sorted(selected_assets - available_assets):
        out.append(
            {
                "kind": "asset_profile",
                "id": ticker,
                "severity": "warning",
                "reason": "asset_profile_missing",
                "fallback": "omit_asset_advisory_context",
            }
        )
    for ticker in sorted(available_assets):
        profile = empirical_profiles.get(ticker)
        if not profile:
            out.append(
                {
                    "kind": "empirical_profile",
                    "id": ticker,
                    "severity": "warning",
                    "reason": "empirical_profile_missing",
                    "fallback": "static_asset_profile_only",
                }
            )
            continue
        quality = str(profile.get("data_quality") or "")
        if quality in {"missing", "stale"}:
            out.append(
                {
                    "kind": "empirical_profile",
                    "id": ticker,
                    "severity": "warning",
                    "reason": f"empirical_profile_{quality}",
                    "fallback": "static_asset_profile_only",
                }
            )

    selected_strategies = set(selection.get("strategies") or [])
    available_strategies = {str(strategy.get("id")) for strategy in context.get("strategies") or []}
    for strategy in sorted(selected_strategies - available_strategies):
        out.append(
            {
                "kind": "strategy_profile",
                "id": strategy,
                "severity": "blocking",
                "reason": "strategy_profile_missing",
                "fallback": "reject_related_confidence_adjustments",
            }
        )
    return out


def _compact_empirical_profile(profile: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(profile, dict) or not profile:
        return None
    return {
        "source": profile.get("source"),
        "generated_at": profile.get("generated_at"),
        "lookback_days": profile.get("lookback_days"),
        "samples": profile.get("samples"),
        "latest_date": profile.get("latest_date"),
        "avg_return": profile.get("avg_return"),
        "volatility": profile.get("volatility"),
        "max_drawdown": profile.get("max_drawdown"),
        "correlation_top": profile.get("correlation_top") or {},
        "benchmark_correlation": profile.get("benchmark_correlation"),
        "data_quality": profile.get("data_quality"),
    }


def _has_blocking_missing_knowledge(items: list[dict[str, Any]]) -> bool:
    return any(str(item.get("severity")) == "blocking" for item in items)


def _source_trace(
    *,
    assets: list[dict[str, Any]],
    strategies: list[dict[str, Any]],
    regimes: list[dict[str, Any]],
    risk_principles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for kind, rows in (
        ("asset", assets),
        ("strategy", strategies),
        ("regime", regimes),
        ("risk_principle", risk_principles),
    ):
        for row in rows:
            out.append(
                {
                    "kind": kind,
                    "id": row.get("id"),
                    "sources": row.get("sources") or [],
                }
            )
    return out


def _unique_dicts(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple] = set()
    out: list[dict[str, Any]] = []
    for value in values:
        key = tuple(sorted((str(k), str(v)) for k, v in value.items()))
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out
