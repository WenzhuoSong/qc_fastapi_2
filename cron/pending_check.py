"""
cron/pending_check.py

Railway cron entry: 每分钟运行一次，检查 SEMI_AUTO 待确认建议是否超时。
超时后按策略自动执行或跳过。
使用方式：python -m cron.pending_check
"""
import asyncio
import logging

from services.proposal import check_and_handle_timeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.pending")


async def main() -> None:
    try:
        result = await check_and_handle_timeout()
        if result.get("action") != "none":
            logger.info(f"Pending check: {result}")
    except Exception:
        logger.exception("Pending check FAILED")
        raise


if __name__ == "__main__":
    asyncio.run(main())
