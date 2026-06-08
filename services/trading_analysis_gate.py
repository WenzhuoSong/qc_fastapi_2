"""Shared preflight gate for trading-analysis pipeline entrypoints."""
from __future__ import annotations

from typing import Any

from services.market_calendar import us_equity_market_status
from services.operational_health import build_operational_health_snapshot


async def evaluate_trading_analysis_gate(*, require_market_open: bool = True) -> dict[str, Any]:
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
    if require_market_open and not market_status.is_open:
        return {
            "allowed": False,
            "reason": f"market_not_open:{market_status.reason}",
            "market_status": market_payload,
            "news_cache": None,
            "operational_health": None,
        }

    health = await build_operational_health_snapshot()
    news_check = (health.get("checks") or {}).get("news_cache") or {}
    if news_check.get("state") != "ok":
        reason = news_check.get("reason") or news_check.get("state")
        return {
            "allowed": True,
            "reason": f"news_cache_degraded:{reason}",
            "market_status": market_payload,
            "news_cache": news_check,
            "operational_health": health.get("overall"),
            "news_degraded_mode": True,
            "degraded_mode": "news_stale_reduce_only",
            "risk_increase_allowed": False,
            "reduce_only_allowed": True,
        }

    return {
        "allowed": True,
        "reason": "ok",
        "market_status": market_payload,
        "news_cache": news_check,
        "operational_health": health.get("overall"),
        "news_degraded_mode": False,
        "degraded_mode": None,
        "risk_increase_allowed": True,
        "reduce_only_allowed": True,
    }
