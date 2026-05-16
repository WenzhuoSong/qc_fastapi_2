"""
cron/position_monitor.py

Railway cron entry: 每 30 分钟检查持仓健康状态（drift / holding period / ATR）。
使用方式：python -m cron.position_monitor

P1-2: POSITION_MANAGER
"""
import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from services.cron_audit import audit_cron_run
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
MARKET_TZ = ZoneInfo("America/New_York")
NOTIFY_HEALTHY = os.getenv("POSITION_MONITOR_NOTIFY_HEALTHY", "").lower() in {"1", "true", "yes"}


def _healthy_message(result: dict) -> str:
    report_time = datetime.now(MARKET_TZ).strftime("%Y-%m-%d %H:%M %Z")
    diagnostics = result.get("diagnostics") or {}
    diag_line = _diagnostic_line(diagnostics)
    return (
        f"📊 Position health report | {report_time}\n\n"
        "No actionable position alerts.\n"
        f"Drift: {len(result.get('drift_alerts') or [])}, "
        f"Holding-period: {len(result.get('holding_period_alerts') or [])}, "
        f"Volatility: {len(result.get('intraday_alerts') or [])}"
        f"{diag_line}"
    )


def _diagnostic_line(diagnostics: dict) -> str:
    if not diagnostics:
        return ""
    schema = diagnostics.get("heartbeat_schema_version") or "unknown"
    trusted = "trusted" if diagnostics.get("holding_days_trusted") else "untrusted"
    held = diagnostics.get("held_positions", 0)
    filtered = diagnostics.get("unheld_high_atr_filtered", 0)
    max_days = diagnostics.get("max_observed_holding_days")
    max_days_text = "n/a" if max_days is None else str(max_days)
    return (
        "\nDiagnostics: "
        f"schema={schema} ({trusted}), held={held}, "
        f"max_holding_days={max_days_text}, "
        f"unheld_high_atr_filtered={filtered}"
    )


async def main() -> None:
    try:
        async with audit_cron_run("position_monitor") as audit:
            logger.info("[position_monitor] Starting position health check...")
            result = await run_position_health_check()
            audit.set_summary(
                total_alerts=result.get("total_alerts"),
                drift_alerts=len(result.get("drift_alerts") or []),
                holding_period_alerts=len(result.get("holding_period_alerts") or []),
                intraday_alerts=len(result.get("intraday_alerts") or []),
                diagnostics=result.get("diagnostics") or {},
            )

            total = result["total_alerts"]
            if total == 0:
                logger.info("[position_monitor] All positions healthy")
                if NOTIFY_HEALTHY:
                    await tool_send_telegram({"text": _healthy_message(result)})
                return

            # Persist to AlertLog
            all_alerts = (
                result["drift_alerts"]
                + result["holding_period_alerts"]
                + result["intraday_alerts"]
            )
            await persist_position_alerts(all_alerts)
            audit.add_rows(len(all_alerts))

            # Build Telegram message — only for warning/critical alerts
            report_time = datetime.now(MARKET_TZ).strftime("%Y-%m-%d %H:%M %Z")
            lines = [f"📊 Position health report | {report_time}\n"]
            diagnostic_line = _diagnostic_line(result.get("diagnostics") or {})
            if diagnostic_line:
                lines.append(diagnostic_line.strip())

            if result["drift_alerts"]:
                lines.append(f"🚨 Drift alerts ({len(result['drift_alerts'])}):")
                for a in result["drift_alerts"][:3]:
                    lines.append(f"  {a['ticker']}: {a['message']}")

            if result["holding_period_alerts"]:
                lines.append(f"⏰ Holding-period alerts ({len(result['holding_period_alerts'])}):")
                for a in result["holding_period_alerts"][:3]:
                    lines.append(f"  {a['ticker']}: {a['message']}")

            # Intraday/ATR alerts only send to Telegram if severity >= warning
            intraday_warn = [a for a in result["intraday_alerts"] if a.get("severity") in ("warning", "critical")]
            if intraday_warn:
                lines.append(f"📈 High-volatility alerts ({len(intraday_warn)}):")
                for a in intraday_warn[:3]:
                    lines.append(f"  {a['ticker']}: {a['message']}")

            # Only send if we have warning/critical content
            has_warn = any(
                result["drift_alerts"] or result["holding_period_alerts"] or intraday_warn
            )
            if not has_warn:
                logger.info("[position_monitor] Only info-level alerts, skipping Telegram notification")
                if NOTIFY_HEALTHY:
                    await tool_send_telegram({"text": _healthy_message(result)})
                return

            if total > 10:
                lines.append(f"\n...{total} total alerts; see AlertLog")

            await tool_send_telegram({"text": "\n".join(lines)})
            logger.info(f"[position_monitor] Done, {total} alerts")

    except Exception:
        logger.exception("[position_monitor] FAILED")
        raise


if __name__ == "__main__":
    asyncio.run(main())
