"""
cron/morning_health.py

Railway cron entry: 开盘前系统健康播报。
使用方式：python -m cron.morning_health
"""
import asyncio
import logging
from datetime import datetime

from db.session         import AsyncSessionLocal
from db.queries         import get_system_config
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.morning")


async def main() -> None:
    try:
        async with AsyncSessionLocal() as db:
            auth_cfg    = await get_system_config(db, "authorization_mode")
            circuit_cfg = await get_system_config(db, "circuit_state")

        mode    = (auth_cfg.value    if auth_cfg    else {}).get("value", "SEMI_AUTO")
        circuit = (circuit_cfg.value if circuit_cfg else {}).get("value", "CLOSED")

        await tool_send_telegram({
            "text": (
                f"🧩 系统健康摘要 | {datetime.utcnow().strftime('%Y-%m-%d')}\n"
                f"  授权模式: {mode}\n"
                f"  熔断状态: {circuit}\n"
                f"  市场即将开盘 🚀"
            )
        })
    except Exception:
        logger.exception("Morning health check FAILED")
        raise


if __name__ == "__main__":
    asyncio.run(main())
