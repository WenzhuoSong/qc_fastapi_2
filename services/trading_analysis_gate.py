"""Shared preflight gate for trading-analysis pipeline entrypoints."""
from __future__ import annotations

from typing import Any

from services.market_calendar import us_equity_market_status
from services.operational_health import build_operational_health_snapshot


async def evaluate_trading_analysis_gate() -> dict[str, Any]:
    """Return whether a trading-analysis pipeline run may start.

    This gate is intentionally shared by scheduled and event-driven entrypoints.
    If trading analysis consumes news evidence, every path into the pipeline must
    enforce the same freshness contract before calling run_full_pipeline().
    """
    market_status = us_equity_market_status()
    market_payload = market_status.to_dict()
    if not market_status.is_trading_day:
        return {
            "allowed": False,
            "reason": f"market_closed:{market_status.reason}",
            "market_status": market_payload,
            "news_cache": None,
            "operational_health": None,
        }

    health = await build_operational_health_snapshot()
    news_check = (health.get("checks") or {}).get("news_cache") or {}
    if news_check.get("state") != "ok":
        return {
            "allowed": False,
            "reason": f"news_cache_not_ready:{news_check.get('reason') or news_check.get('state')}",
            "market_status": market_payload,
            "news_cache": news_check,
            "operational_health": health.get("overall"),
        }

    return {
        "allowed": True,
        "reason": "ok",
        "market_status": market_payload,
        "news_cache": news_check,
        "operational_health": health.get("overall"),
    }
