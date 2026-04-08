"""
cron/hourly_analysis.py

Railway cron entry: 每小时运行 agent pipeline。
使用方式：python -m cron.hourly_analysis
"""
import asyncio
import logging

from services.pipeline import run_full_pipeline
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.hourly")


async def main() -> None:
    try:
        result = await run_full_pipeline(trigger="scheduled_hourly")
        logger.info(f"Pipeline result: {result}")
    except Exception as e:
        logger.exception("Hourly analysis FAILED")
        try:
            await tool_send_telegram(
                {"text": f"🚨 小时分析异常: {e}", "parse_mode": ""}
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    asyncio.run(main())
