"""
cron/post_market_report.py

Railway cron entry: 收盘后生成日报。
使用方式：python -m cron.post_market_report
"""
import asyncio
import logging

from agents.reporter    import run_reporter_async
from services.cron_audit import audit_cron_run
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.report")


async def main() -> None:
    try:
        async with audit_cron_run("post_market_report") as audit:
            result = await run_reporter_async()
            audit.set_summary(result=result)
            logger.info(f"Reporter result: {result}")
    except Exception as e:
        logger.exception("Post market report FAILED")
        try:
            await tool_send_telegram(
                {"text": f"🚨 Post-market report failed: {e}", "parse_mode": ""}
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    asyncio.run(main())
