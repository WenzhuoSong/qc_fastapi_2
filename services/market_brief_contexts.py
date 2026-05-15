"""Specialized context builders used by Market Brief."""
from __future__ import annotations

import logging

logger = logging.getLogger("qc_fastapi_2.market_brief_contexts")


async def build_scenario_context(
    current_weights: dict[str, float],
    scenario: str = "all",
) -> dict | None:
    """Run scenario stress-test if there are meaningful equity positions."""
    try:
        equity_sum = sum(w for t, w in current_weights.items() if t != "CASH" and w > 0)
        if equity_sum < 0.10:
            return None
        from services.scenario_analyst import run_scenario_analysis

        return await run_scenario_analysis(current_weights, scenario=scenario)
    except Exception as exc:
        logger.warning("[market_brief_contexts] scenario analysis failed: %s", exc)
        return None


async def build_memory_context() -> dict:
    """Read historical memory context for downstream Researcher prompt injection."""
    try:
        from services.context_assembler import assemble_memory_context

        return await assemble_memory_context()
    except Exception as exc:
        logger.warning("[market_brief_contexts] memory context failed: %s", exc)
        return {
            "has_memory": False,
            "memory_prose": "",
            "data_gaps": [f"memory context unavailable: {type(exc).__name__}"],
        }
