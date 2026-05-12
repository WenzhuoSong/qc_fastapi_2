# cron/dynamic_scheduler.py
"""
Railway cron entry: dynamic event-driven pipeline trigger.

Run as a Railway cron every 30 min during market hours (9:30am–4pm ET, Mon–Fri).
Schedule in Railway UI: */30 14-20 * * 1-5

Usage: python -m cron.dynamic_scheduler
"""
import asyncio
import logging

from services.dynamic_scheduler import check_and_trigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.dynamic_scheduler")


async def main() -> None:
    try:
        result = await check_and_trigger()
        if result.get("actions"):
            logger.info(f"Dynamic scheduler triggered: {result['actions']}")
        else:
            logger.debug("Dynamic scheduler: no events triggered")
    except Exception as e:
        logger.exception("Dynamic scheduler FAILED")
        raise


if __name__ == "__main__":
    asyncio.run(main())
