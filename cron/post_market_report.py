"""
cron/post_market_report.py

Railway cron entry: 收盘后生成日报。
使用方式：python -m cron.post_market_report
"""
import asyncio
import logging

from agents.reporter    import run_reporter_async
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.report")


async def main() -> None:
    try:
        result = await run_reporter_async()
        logger.info(f"Reporter result: {result}")
    except Exception as e:
        logger.exception("Post market report FAILED")
        try:
            await tool_send_telegram(
                {"text": f"🚨 日报生成异常: {e}", "parse_mode": ""}
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    asyncio.run(main())
