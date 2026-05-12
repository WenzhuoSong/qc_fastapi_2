# cron/quarterly_analyst.py
"""
Railway cron entry: QUARTERLY_ANALYST.

Runs on the first trading day of each quarter at 10:00 ET.
Calls run_quarterly_analyst() which reviews the previous quarter's performance,
evaluates MomentumLiteV1 effectiveness, and outputs strategy_revision_v1
to system_config with status "pending_approval".

Usage:
    python -m cron.quarterly_analyst

Schedule (Railway cron):
    0 10 1 1,4,7,10 *   # 10:00 ET on first trading day of Jan/Apr/Jul/Oct
    (adjusts to actual first trading day — code checks calendar)
"""
import asyncio
import logging
from datetime import date

from agents.quarterly_analyst import run_quarterly_analyst

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.quarterly_analyst")


def is_first_trading_day_of_quarter() -> bool:
    """
    Return True if today is the first trading day of the quarter.
    Checks: month is first month of a quarter (1, 4, 7, 10)
    and day is 1-3 and weekday is Mon-Fri (approx — holidays ignored).
    """
    today = date.today()
    quarter_start_month = (today.month - 1) // 3 * 3 + 1
    if today.month != quarter_start_month:
        return False
    # First 3 days of the month, Mon-Fri
    if today.day > 3:
        return False
    if today.weekday() >= 5:  # Sat/Sun
        return False
    return True


async def main() -> None:
    if not is_first_trading_day_of_quarter():
        logger.info(
            f"[quarterly_analyst] Today ({date.today()}) is not the first trading "
            f"day of the quarter — skipping."
        )
        return

    logger.info(f"[quarterly_analyst] Starting quarterly review for {date.today()}")
    try:
        result = await run_quarterly_analyst()
        logger.info(
            f"[quarterly_analyst] Complete: "
            f"changes={result.get('strategy_revision_v1', {}).get('changes_recommended')}, "
            f"confidence={result.get('strategy_revision_v1', {}).get('confidence')}"
        )
    except Exception:
        logger.exception("[quarterly_analyst] FAILED")
        raise


if __name__ == "__main__":
    asyncio.run(main())
