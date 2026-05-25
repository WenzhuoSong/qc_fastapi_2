"""Strategy family normalization and diversity diagnostics.

This layer prevents multiple variants of the same idea from being counted as
independent alpha sources. It is diagnostics-only.
"""
from __future__ import annotations

from typing import Any


ALPHA_FAMILIES = (
    "momentum",
    "low_vol_defensive",
    "mean_reversion",
    "carry_or_cash_proxy",
    "seasonality_flow",
    "event_risk_avoidance",
    "volatility_hedge",
)
NON_ALPHA_FAMILIES = ("benchmark",)
CANONICAL_FAMILIES = ALPHA_FAMILIES + NON_ALPHA_FAMILIES

FAMILY_ALIASES = {
    "trend_following": "momentum",
    "dual_momentum": "momentum",
    "leveraged_rotation": "momentum",
    "sector_theme_rotation": "momentum",
    "macro_rate": "carry_or_cash_proxy",
    "defensive_factor": "low_vol_defensive",
    "risk_budgeting": "low_vol_defensive",
    "benchmark": "benchmark",
}

NON_ALPHA_STRATEGIES = {
    "equal_weight_benchmark",
    "risk_parity_lite",
}

ACTIONABLE_USES = {"primary", "advisory"}


def canonical_strategy_family(family: str | None) -> str:
    raw = str(family or "").strip().lower()
    if not raw:
        return "unknown"
    return FAMILY_ALIASES.get(raw, raw)


def is_alpha_family(family: str | None) -> bool:
    return canonical_strategy_family(family) in ALPHA_FAMILIES


def is_strategy_alpha_source(strategy_name: str | None, family: str | None, alpha_source: Any = None) -> bool:
    if alpha_source is not None:
        if isinstance(alpha_source, str):
            return alpha_source.strip().lower() not in {"", "0", "false", "no", "none"}
        return bool(alpha_source)
    name = str(strategy_name or "").strip().lower()
    if name in NON_ALPHA_STRATEGIES:
        return False
    return is_alpha_family(family)


def build_strategy_diversity_summary(strategy_results: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [_normalize_strategy_row(row) for row in strategy_results if isinstance(row, dict)]
    families: dict[str, dict[str, Any]] = {}
    for row in rows:
        family = row["canonical_family"]
        group = families.setdefault(
            family,
            {
                "family": family,
                "strategy_names": [],
                "actionable_strategy_names": [],
                "alpha_source_strategy_names": [],
                "actionable_alpha_strategy_names": [],
                "suggested_uses": {},
            },
        )
        group["strategy_names"].append(row["strategy_name"])
        use = row["suggested_use"]
        group["suggested_uses"][use] = int(group["suggested_uses"].get(use, 0)) + 1
        if row["alpha_source"]:
            group["alpha_source_strategy_names"].append(row["strategy_name"])
        if row["actionable"]:
            group["actionable_strategy_names"].append(row["strategy_name"])
        if row["actionable"] and row["alpha_source"]:
            group["actionable_alpha_strategy_names"].append(row["strategy_name"])

    family_rows = []
    actionable_alpha_families = []
    warnings = []
    for family, group in sorted(families.items()):
        strategy_names = sorted(set(group["strategy_names"]))
        actionable_alpha = sorted(set(group["actionable_alpha_strategy_names"]))
        alpha_source_names = sorted(set(group["alpha_source_strategy_names"]))
        independent_alpha_counted = bool(actionable_alpha and family in ALPHA_FAMILIES)
        if independent_alpha_counted:
            actionable_alpha_families.append(family)
        if len(actionable_alpha) > 1:
            warnings.append(f"same_family_not_independent:{family}:{','.join(actionable_alpha)}")
        family_rows.append({
            "family": family,
            "strategy_count": len(strategy_names),
            "alpha_source_strategy_count": len(alpha_source_names),
            "actionable_strategy_count": len(set(group["actionable_strategy_names"])),
            "actionable_alpha_strategy_count": len(actionable_alpha),
            "independent_alpha_counted": independent_alpha_counted,
            "strategy_names": strategy_names,
            "actionable_alpha_strategy_names": actionable_alpha,
            "suggested_uses": dict(sorted(group["suggested_uses"].items())),
        })

    actionable_alpha_families = sorted(set(actionable_alpha_families))
    return {
        "contract_version": "strategy_diversity_v1",
        "canonical_alpha_families": list(ALPHA_FAMILIES),
        "non_alpha_families": list(NON_ALPHA_FAMILIES),
        "same_family_not_independent": True,
        "strategy_count": len(rows),
        "alpha_source_strategy_count": sum(1 for row in rows if row["alpha_source"]),
        "actionable_strategy_count": sum(1 for row in rows if row["actionable"]),
        "actionable_alpha_strategy_count": sum(
            1 for row in rows if row["actionable"] and row["alpha_source"]
        ),
        "independent_alpha_family_count": len(actionable_alpha_families),
        "families_present": sorted({row["canonical_family"] for row in rows}),
        "actionable_alpha_families": actionable_alpha_families,
        "family_rows": family_rows,
        "strategy_rows": rows,
        "warnings": warnings,
        "execution_authority": "none",
    }


def _normalize_strategy_row(row: dict[str, Any]) -> dict[str, Any]:
    card = row.get("strategy_card") if isinstance(row.get("strategy_card"), dict) else {}
    strategy_name = str(row.get("strategy_name") or card.get("name") or "").strip()
    raw_family = (
        row.get("raw_family")
        or row.get("family")
        or card.get("family")
        or row.get("strategy_family")
    )
    canonical = canonical_strategy_family(row.get("canonical_family") or raw_family)
    alpha_source = is_strategy_alpha_source(
        strategy_name,
        canonical,
        row.get("alpha_source", card.get("alpha_source")),
    )
    suggested_use = str(row.get("suggested_use") or "watch_only")
    actionable = bool(suggested_use in ACTIONABLE_USES and alpha_source)
    return {
        "strategy_name": strategy_name,
        "raw_family": str(raw_family or "unknown"),
        "canonical_family": canonical,
        "alpha_source": bool(alpha_source),
        "suggested_use": suggested_use,
        "actionable": actionable,
        "confidence_score": row.get("confidence_score"),
        "data_ready": bool(row.get("data_ready")),
        "can_influence_allocation": bool(row.get("can_influence_allocation")),
    }
