"""
cron/position_monitor.py

Railway cron entry: 每 30 分钟检查持仓健康状态（drift / holding period / ATR）。
使用方式：python -m cron.position_monitor

P1-2: POSITION_MANAGER
"""
import asyncio
import logging
from datetime import datetime

from services.position_manager import (
    run_position_health_check,
    persist_position_alerts,
)
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.position_monitor")


async def main() -> None:
    try:
        logger.info("[position_monitor] Starting position health check...")
        result = await run_position_health_check()

        total = result["total_alerts"]
        if total == 0:
            logger.info("[position_monitor] All positions healthy")
            return

        # Persist to AlertLog
        all_alerts = (
            result["drift_alerts"]
            + result["holding_period_alerts"]
            + result["intraday_alerts"]
        )
        await persist_position_alerts(all_alerts)

        # Build Telegram message
        lines = [f"📊 持仓健康报告 | {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n"]

        if result["drift_alerts"]:
            lines.append(f"🚨 Drift 告警 ({len(result['drift_alerts'])}):")
            for a in result["drift_alerts"][:3]:
                lines.append(f"  {a['ticker']}: {a['message']}")

        if result["holding_period_alerts"]:
            lines.append(f"⏰ 持仓期限告警 ({len(result['holding_period_alerts'])}):")
            for a in result["holding_period_alerts"][:3]:
                lines.append(f"  {a['ticker']}: {a['message']}")

        if result["intraday_alerts"]:
            lines.append(f"📈 高波动告警 ({len(result['intraday_alerts'])}):")
            for a in result["intraday_alerts"][:3]:
                lines.append(f"  {a['ticker']}: {a['message']}")

        if total > 10:
            lines.append(f"\n...共 {total} 项告警，详见 AlertLog")

        await tool_send_telegram({"text": "\n".join(lines)})
        logger.info(f"[position_monitor] Done, {total} alerts")

    except Exception:
        logger.exception("[position_monitor] FAILED")
        raise


if __name__ == "__main__":
    asyncio.run(main())