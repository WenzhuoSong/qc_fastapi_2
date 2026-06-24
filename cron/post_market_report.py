"""
cron/post_market_report.py

Railway cron entry: 收盘后生成日报。
使用方式：python -m cron.post_market_report
"""
import asyncio
import logging

from agents.reporter    import run_reporter_async
from services.cron_audit import audit_cron_run
from services.newbase_monitoring import (
    format_newbase_operator_snapshot_text,
    is_active_newbase_observer,
    load_latest_newbase_operator_snapshot,
)
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.report")


async def main() -> None:
    try:
        async with audit_cron_run("post_market_report") as audit:
            if await is_active_newbase_observer():
                snapshot = await load_latest_newbase_operator_snapshot(limit=90)
                if snapshot is None:
                    audit.mark_skipped("no_newbase_live_snapshots")
                    logger.warning("No newBase live snapshots found; post-market report skipped")
                    return
                text = format_newbase_operator_snapshot_text(snapshot)
                result = await tool_send_telegram({"text": text, "parse_mode": ""})
                audit.set_summary(
                    mode="newbase_observer_only",
                    sent=result.get("sent"),
                    status=snapshot.get("status"),
                    as_of_snapshot_uid=snapshot.get("as_of_snapshot_uid"),
                    as_of_recorded_at=snapshot.get("as_of_recorded_at"),
                    execution_authority="none",
                    target_weight_mutation="none",
                )
                logger.info("newBase post-market report sent=%s", result.get("sent"))
                return

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
