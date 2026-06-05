"""
cron/validation_observation_refresh.py

Observe-only validation data loop refresh. It backfills durable observations
from AgentAnalysis / ExecutionLog rows and labels mature hedge T+5 outcomes
from market_daily_features. It never writes target weights or commands.

Usage: python -m cron.validation_observation_refresh
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any

from db.queries import get_system_config
from db.session import AsyncSessionLocal
from services.cron_audit import audit_cron_run
from services.validation_observation_loop import refresh_validation_observation_loop
from tools.notify_tools import tool_send_telegram


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.validation_observation_refresh")


async def main() -> None:
    try:
        async with audit_cron_run("validation_observation_refresh") as audit:
            config = await _read_config()
            if not config.get("enabled", True):
                audit.mark_skipped("disabled_by_config")
                logger.info("Validation observation refresh disabled by config")
                return

            as_of = _parse_date(config.get("as_of_date"))
            analysis_limit = int(config.get("analysis_limit", 300))
            execution_limit = int(config.get("execution_limit", 300))
            feature_source = str(config.get("feature_source") or "yfinance")
            notify = _bool_value(config.get("notify", False))

            async with AsyncSessionLocal() as db:
                result = await refresh_validation_observation_loop(
                    db,
                    as_of_date=as_of,
                    analysis_limit=analysis_limit,
                    execution_limit=execution_limit,
                    feature_source=feature_source,
                )

            audit.add_rows(result.observations_written)
            audit.set_summary(**result.to_dict())
            logger.info(
                "Validation observation refresh analyses=%s executions=%s written=%s hedge_completed=%s pending=%s",
                result.analyses_seen,
                result.execution_logs_seen,
                result.observations_written,
                result.hedge_outcomes_completed,
                result.hedge_outcomes_pending,
            )
            if notify:
                await tool_send_telegram({
                    "text": _format_summary(result.to_dict()),
                    "parse_mode": "",
                })
    except Exception as exc:
        logger.exception("Validation observation refresh FAILED")
        try:
            await tool_send_telegram({
                "text": f"Validation observation refresh failed: {exc}",
                "parse_mode": "",
            })
        except Exception:
            pass
        raise


async def _read_config() -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "validation_observation_refresh_config")
    return (cfg.value if cfg else {}) or {"enabled": True}


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _bool_value(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _format_summary(result: dict[str, Any]) -> str:
    summary = result.get("summary") or {}
    hedge = summary.get("hedge_threshold_summary") or {}
    return (
        "Validation observation refresh\n"
        f"Observations written: {result.get('observations_written')}\n"
        f"Hedge completed: {result.get('hedge_outcomes_completed')}\n"
        f"Hedge pending: {result.get('hedge_outcomes_pending')}\n"
        f"Assessment counts: {hedge.get('assessment_counts') or {}}"
    )


if __name__ == "__main__":
    asyncio.run(main())
