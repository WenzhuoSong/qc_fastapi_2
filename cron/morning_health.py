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
from services.cron_audit import audit_cron_run, read_recent_cron_runs
from services.earnings_tracker import update_earnings_calendar
from services.macro_watcher import update_macro_events_cache
from services.operational_health import (
    build_operational_health_snapshot,
    format_operational_health_report,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.morning")


async def main() -> None:
    try:
        async with audit_cron_run("morning_health") as audit:
            # P1-3: Update earnings calendar + macro events cache first
            logger.info("[morning_health] Updating earnings calendar...")
            earnings_result = await update_earnings_calendar()
            logger.info(f"[morning_health] Earnings calendar updated: {earnings_result}")

            logger.info("[morning_health] Updating macro events cache...")
            macro_result = await update_macro_events_cache()
            logger.info(f"[morning_health] Macro events cache updated: {macro_result}")
            audit.set_summary(
                earnings_status=earnings_result.get("status") if isinstance(earnings_result, dict) else None,
                macro_status=macro_result.get("status") if isinstance(macro_result, dict) else None,
            )

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

            recent_crons = await read_recent_cron_runs(limit=8)
            failed_crons = [
                row for row in recent_crons
                if row.get("status") == "failed" and row.get("job_name") != "morning_health"
            ][:3]

            # Build summary text
            summary_lines = [
                f"🧩 System health summary | {datetime.utcnow().strftime('%Y-%m-%d')}",
                f"  Authorization mode: {mode}",
                f"  Circuit state: {circuit}",
            ]

            # Phase 3: Add circuit health issues to summary
            if health and health.has_issues:
                emoji_map = {"ALERT": "🟡", "DEFENSIVE": "🔴"}
                circuit_emoji = emoji_map.get(health.current_state, "⚪")
                summary_lines.append(f"{circuit_emoji} Circuit health warning:")
                for issue in health.issues:
                    summary_lines.append(f"  - {issue}")

            if failed_crons:
                summary_lines.append("  Recent cron failures:")
                for row in failed_crons:
                    summary_lines.append(f"  - {row['job_name']}: {row.get('error_message', '')[:80]}")

            if macro_result.get("next_fomc"):
                summary_lines.append(f"  Next FOMC: {macro_result['next_fomc']}")
            if macro_result.get("next_cpi"):
                summary_lines.append(f"  Next CPI: {macro_result['next_cpi']}")

            try:
                ops_snapshot = await build_operational_health_snapshot()
                summary_lines.append("")
                summary_lines.append(format_operational_health_report(ops_snapshot))
                audit.set_summary(
                    **(audit.summary or {}),
                    ops_overall=ops_snapshot.get("overall"),
                    ops_execution_blockers=len(ops_snapshot.get("execution_blockers") or []),
                    ops_research_degradations=len(ops_snapshot.get("research_degradations") or []),
                )
            except Exception as e:
                logger.warning(f"[morning_health] Operational health report failed: {e}")

            summary_lines.append("  Market opens soon 🚀")

            await tool_send_telegram({
                "text": "\n".join(summary_lines)
            })
            logger.info("[morning_health] Done")
    except Exception:
        logger.exception("Morning health check FAILED")
        raise


if __name__ == "__main__":
    asyncio.run(main())
