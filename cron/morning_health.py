"""
cron/morning_health.py

Railway cron entry: 开盘前系统健康播报 + P1-3 数据更新。
使用方式：python -m cron.morning_health
"""
import asyncio
import logging
from datetime import datetime

from db.session         import AsyncSessionLocal
from db.queries         import get_system_config
from tools.notify_tools import tool_send_telegram
from services.earnings_tracker import update_earnings_calendar
from services.macro_watcher import update_macro_events_cache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.morning")


async def main() -> None:
    try:
        # P1-3: Update earnings calendar + macro events cache first
        logger.info("[morning_health] Updating earnings calendar...")
        earnings_result = await update_earnings_calendar()
        logger.info(f"[morning_health] Earnings calendar updated: {earnings_result}")

        logger.info("[morning_health] Updating macro events cache...")
        macro_result = await update_macro_events_cache()
        logger.info(f"[morning_health] Macro events cache updated: {macro_result}")

        # System status check
        async with AsyncSessionLocal() as db:
            auth_cfg    = await get_system_config(db, "authorization_mode")
            circuit_cfg = await get_system_config(db, "circuit_state")

        mode    = (auth_cfg.value    if auth_cfg    else {}).get("value", "SEMI_AUTO")
        circuit = (circuit_cfg.value if circuit_cfg else {}).get("value", "CLOSED")

        # Phase 3: Circuit breaker health check
        try:
            from services.circuit_breaker import CircuitBreakerMonitor
            monitor = CircuitBreakerMonitor()
            health = await monitor.run_health_check()
        except Exception as e:
            logger.warning(f"[morning_health] Circuit health check failed: {e}")
            health = None

        # Build summary text
        summary_lines = [
            f"🧩 系统健康摘要 | {datetime.utcnow().strftime('%Y-%m-%d')}",
            f"  授权模式: {mode}",
            f"  熔断状态: {circuit}",
        ]

        # Phase 3: Add circuit health issues to summary
        if health and health.has_issues:
            emoji_map = {"ALERT": "🟡", "DEFENSIVE": "🔴"}
            circuit_emoji = emoji_map.get(health.current_state, "⚪")
            summary_lines.append(f"{circuit_emoji} 熔断健康警告:")
            for issue in health.issues:
                summary_lines.append(f"  - {issue}")

        if macro_result.get("next_fomc"):
            summary_lines.append(f"  下次FOMC: {macro_result['next_fomc']}")
        if macro_result.get("next_cpi"):
            summary_lines.append(f"  下次CPI: {macro_result['next_cpi']}")

        summary_lines.append("  市场即将开盘 🚀")

        await tool_send_telegram({
            "text": "\n".join(summary_lines)
        })
        logger.info("[morning_health] Done")
    except Exception:
        logger.exception("Morning health check FAILED")
        raise


if __name__ == "__main__":
    asyncio.run(main())
