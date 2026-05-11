"""
services/scenario_analyst.py

Scenario stress-test module for the trading system.
Reuses the 6 canonical transmission patterns from transmission.py to evaluate
portfolio impact under different macro scenarios.

P2-2: SCENARIO_ANALYST

Usage:
    result = await run_scenario_analysis(current_weights, scenario="all")
    scenario_context = build_scenario_context(result)
"""
from __future__ import annotations

import logging
from typing import Optional

from services.transmission import (
    CANONICAL_TRANSMISSIONS,
    generate_transmission_vector,
    apply_transmission,
)

logger = logging.getLogger("qc_fastapi_2.scenario_analyst")

# Default max position for scenario tilt calculation
DEFAULT_MAX_POS = 0.20

# Scenario human-readable labels
SCENARIO_LABELS: dict[str, str] = {
    "supply_shock_oil":        "Oil/Energy Supply Shock",
    "war_geopolitical":        "War / Geopolitical Conflict",
    "rate_shock_hawkish":      "Rate Shock (Hawkish Fed)",
    "risk_off_credit_stress":  "Risk-Off / Credit Stress",
    "recession_demand_collapse": "Recession / Demand Collapse",
    "fed_dovish_easing":       "Fed Dovish Easing",
}


async def run_scenario_analysis(
    current_weights: dict[str, float],
    scenario: str = "all",
    max_pos: float = DEFAULT_MAX_POS,
) -> dict:
    """
    Run scenario stress-test on current portfolio weights.

    Args:
        current_weights: current portfolio weights (e.g. {"SPY": 0.2, "CASH": 0.1, ...})
        scenario: "all" (analyze all 6 patterns) or one specific pattern name
        max_pos: max single position for tilt clipping

    Returns:
        {
            "scenario_name": str,
            "results": {
                "<pattern>": {
                    "estimated_impact_pct": float,   # portfolio impact estimate
                    "affected_tickers": [...],       # tickers with non-zero tilt
                    "tilt_vector": {...},            # raw transmission vector
                    "confidence": str,
                }
            },
            "most_severe": str,    # pattern name with largest negative impact
            "total_scenarios": int,
        }
    """
    if not current_weights:
        return {"scenario_name": scenario, "results": {}, "most_severe": None, "total_scenarios": 0}

    scenarios_to_run = (
        list(CANONICAL_TRANSMISSIONS.keys()) if scenario == "all" else [scenario]
    )

    results: dict = {}
    most_severe: Optional[tuple[str, float]] = None

    for pattern_name in scenarios_to_run:
        if pattern_name not in CANONICAL_TRANSMISSIONS:
            continue

        result = _analyze_single_scenario(
            current_weights, pattern_name, max_pos
        )
        results[pattern_name] = result

        impact = result.get("estimated_impact_pct", 0.0)
        if most_severe is None or impact < most_severe[1]:
            most_severe = (pattern_name, impact)

    return {
        "scenario_name":   scenario,
        "results":         results,
        "most_severe":     most_severe[0] if most_severe else None,
        "total_scenarios": len(results),
    }


def _analyze_single_scenario(
    weights: dict[str, float],
    pattern_name: str,
    max_pos: float,
) -> dict:
    """Analyze a single scenario pattern."""
    vector = generate_transmission_vector(pattern_name)
    if not vector:
        return {
            "estimated_impact_pct": 0.0,
            "affected_tickers": [],
            "tilt_vector": {},
            "confidence": "low",
        }

    # Apply tilt to get new weights under this scenario
    tilted = apply_transmission(weights, vector, max_pos)

    # Calculate estimated portfolio impact
    # Compare original equity sum to tilted equity sum
    original_equity = sum(w for t, w in weights.items() if t != "CASH" and w > 0)
    tilted_equity = sum(w for t, w in tilted.items() if t != "CASH" and w > 0)

    if original_equity > 0:
        impact_pct = (tilted_equity - original_equity) / original_equity
    else:
        impact_pct = 0.0

    # Find affected tickers (those with non-zero tilt)
    affected = [
        t for t in vector.keys()
        if abs(vector.get(t, 0.0)) > 0.05
    ]

    # Confidence based on how strong the vector signals are
    avg_strength = sum(abs(v) for v in vector.values()) / max(len(vector), 1)
    if avg_strength > 0.6:
        confidence = "high"
    elif avg_strength > 0.3:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "estimated_impact_pct": round(impact_pct, 4),
        "affected_tickers":     affected,
        "tilt_vector":          {t: round(v, 3) for t, v in vector.items()},
        "confidence":           confidence,
    }


def build_scenario_context(scenario_result: dict) -> str:
    """
    Build a prose section summarizing scenario analysis for the RESEARCHER prompt.
    """
    if not scenario_result or not scenario_result.get("results"):
        return ""

    lines = ["\n## SCENARIO STRESS-TEST RESULTS\n"]
    results = scenario_result.get("results", {})

    for pattern, data in results.items():
        label = SCENARIO_LABELS.get(pattern, pattern)
        impact = data.get("estimated_impact_pct", 0)
        affected = data.get("affected_tickers", [])
        confidence = data.get("confidence", "medium")

        impact_str = f"{impact:+.1%}" if isinstance(impact, float) else str(impact)
        lines.append(
            f"- **{label}** ({confidence} confidence): "
            f"Estimated impact {impact_str}. "
            f"Affected: {', '.join(affected) if affected else 'none'}."
        )

    if scenario_result.get("most_severe"):
        most_severe_label = SCENARIO_LABELS.get(
            scenario_result["most_severe"],
            scenario_result["most_severe"]
        )
        lines.append(
            f"\n**Most Severe Scenario**: {most_severe_label} "
            f"(portfolio impact {scenario_result['results'][scenario_result['most_severe']]['estimated_impact_pct']:+.1%})"
        )

    return "\n".join(lines)


def get_active_scenarios(scenario_result: dict) -> list[dict]:
    """
    Return a list of active (non-zero impact) scenarios for pipeline use.
    """
    active = []
    results = scenario_result.get("results", {})
    for pattern, data in results.items():
        impact = data.get("estimated_impact_pct", 0)
        if abs(impact) > 0.001:  # non-zero impact
            active.append({
                "pattern": pattern,
                "label": SCENARIO_LABELS.get(pattern, pattern),
                "impact_pct": impact,
                "confidence": data.get("confidence", "medium"),
                "affected_tickers": data.get("affected_tickers", []),
            })
    return active