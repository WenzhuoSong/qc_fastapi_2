"""
cron/weekly_performance_attribution.py

Weekly performance attribution job. It decomposes portfolio return into
SPY beta, QQQ/growth beta, momentum proxy, and residual alpha candidate.
This job is analytics-only and never writes target weights or commands.

Usage: python -m cron.weekly_performance_attribution
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any

from db.queries import get_system_config
from db.session import AsyncSessionLocal
from services.cron_audit import audit_cron_run
from services.performance_attribution import (
    MIN_ATTRIBUTION_SAMPLES,
    build_and_persist_weekly_attribution,
)
from tools.notify_tools import tool_send_telegram


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.weekly_performance_attribution")


async def main() -> None:
    try:
        async with audit_cron_run("weekly_performance_attribution") as audit:
            config = await _read_config()
            if not config.get("enabled", True):
                audit.mark_skipped("disabled_by_config")
                logger.info("Weekly performance attribution disabled by config")
                return

            period_end = _parse_date(config.get("period_end"))
            lookback_days = int(config.get("lookback_days", 7))
            min_samples = int(config.get("min_samples", MIN_ATTRIBUTION_SAMPLES))
            notify = _bool_value(config.get("notify", False))

            async with AsyncSessionLocal() as db:
                result = await build_and_persist_weekly_attribution(
                    db,
                    period_end=period_end,
                    lookback_days=lookback_days,
                    min_samples=min_samples,
                )

            audit.add_rows(1)
            audit.set_summary(
                period_key=result.period_key,
                status=result.status,
                sample_count=result.sample_count,
                data_quality=result.data_quality,
                portfolio_return=result.portfolio_return,
                residual_alpha_candidate=result.residual_alpha_candidate,
                r_squared=result.r_squared,
                benchmark_source=result.benchmark_source,
                source_tickers=result.source_tickers,
            )
            logger.info(
                "Weekly attribution period=%s status=%s samples=%s residual=%s r2=%s",
                result.period_key,
                result.status,
                result.sample_count,
                result.residual_alpha_candidate,
                result.r_squared,
            )

            if notify:
                await tool_send_telegram({
                    "text": _format_summary(result.to_dict()),
                    "parse_mode": "",
                })
    except Exception as exc:
        logger.exception("Weekly performance attribution FAILED")
        try:
            await tool_send_telegram({
                "text": f"Weekly performance attribution failed: {exc}",
                "parse_mode": "",
            })
        except Exception:
            pass
        raise


async def _read_config() -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "weekly_performance_attribution_config")
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
    return (
        "Weekly performance attribution\n"
        f"Period: {result.get('period_key')}\n"
        f"Status: {result.get('status')}\n"
        f"Samples: {result.get('sample_count')}\n"
        f"Portfolio return: {result.get('portfolio_return')}\n"
        f"Residual alpha candidate: {result.get('residual_alpha_candidate')}\n"
        f"R-squared: {result.get('r_squared')}"
    )


if __name__ == "__main__":
    asyncio.run(main())
