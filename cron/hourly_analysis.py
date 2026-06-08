"""
cron/hourly_analysis.py

Railway cron entry: 每小时运行 agent pipeline。
使用方式：python -m cron.hourly_analysis
"""
import asyncio
import logging
from datetime import date, timedelta

from services.cron_audit import audit_cron_run
from services.market_calendar import us_equity_market_status
from services.operational_health import build_operational_health_snapshot
from services.pipeline import run_full_pipeline
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.hourly")


async def _resolve_trigger() -> str:
    """
    Return a trigger label enriched with event context if we are within
    2h of a high-impact macro event (FOMC, CPI). Falls back to 'scheduled_hourly'.
    """
    EVENT_WINDOW_HOURS = 2

    try:
        from db.session import AsyncSessionLocal
        from db.models import MacroEventsCache
        from sqlalchemy import select

        today = date.today()
        window = today + timedelta(hours=EVENT_WINDOW_HOURS / 24)

        async with AsyncSessionLocal() as db:
            row = (await db.execute(
                select(MacroEventsCache).where(MacroEventsCache.id == 1)
            )).scalar_one_or_none()

        if row:
            if row.next_fomc and today <= row.next_fomc <= window:
                return "scheduled_hourly_pre_fomc"
            if row.next_cpi and today <= row.next_cpi <= window:
                return "scheduled_hourly_pre_cpi"
    except Exception as e:
        logger.warning(f"[hourly] Could not resolve event context: {e}")

    return "scheduled_hourly"


async def main() -> None:
    try:
        async with audit_cron_run("hourly_analysis") as audit:
            market_status = us_equity_market_status()
            if not market_status.is_trading_day:
                reason = f"market_closed:{market_status.reason}"
                audit.mark_skipped(reason)
                audit.set_summary(market_status=market_status.to_dict())
                logger.info("[hourly] Skipping pipeline: %s", reason)
                return

            health = await build_operational_health_snapshot()
            news_check = (health.get("checks") or {}).get("news_cache") or {}
            if news_check.get("state") != "ok":
                reason = f"news_cache_not_ready:{news_check.get('reason') or news_check.get('state')}"
                audit.mark_skipped(reason)
                audit.set_summary(
                    market_status=market_status.to_dict(),
                    news_cache=news_check,
                    operational_health=health.get("overall"),
                )
                logger.warning("[hourly] Skipping pipeline: %s", reason)
                return

            trigger = await _resolve_trigger()
            result = await run_full_pipeline(trigger=trigger)
            if result.get("status", "").startswith("skipped"):
                audit.mark_skipped(result.get("status"))
            audit.set_summary(trigger=trigger, result=result)
            logger.info(f"Pipeline result: {result}")
    except Exception as e:
        logger.exception("Hourly analysis FAILED")
        try:
            await tool_send_telegram(
                {"text": f"🚨 Hourly analysis failed: {e}", "parse_mode": ""}
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    asyncio.run(main())
