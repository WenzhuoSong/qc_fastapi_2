"""Railway cron entry for newBase observer-only monitoring.

This job has no execution authority. It checks whether the expected
QC/newBase telemetry has arrived and records the result in cron_run_log.
"""
from __future__ import annotations

import asyncio
import logging

from services.cron_audit import audit_cron_run
from services.newbase_monitoring import is_active_newbase_observer
from services.qc_observer_health import load_current_observer_summary


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.newbase_monitor")


async def main() -> None:
    async with audit_cron_run("newbase_monitor") as audit:
        if not await is_active_newbase_observer():
            audit.mark_skipped("active_strategy_not_newbase")
            logger.info("[newbase_monitor] active_strategy is not newBase; skipping")
            return

        result = await load_current_observer_summary()
        audit.set_summary(
            status=result.get("status"),
            reason=result.get("reason"),
            should_alert=bool(result.get("should_alert")),
            event_stream_id=result.get("event_stream_id"),
            deployment_id=result.get("deployment_id"),
            strategy_id=result.get("strategy_id"),
            latest_event=result.get("last_event_type"),
            latest_event_time=result.get("last_event_time"),
            execution_authority="none",
            target_weight_mutation="none",
        )

        if not result.get("should_alert"):
            logger.info("[newbase_monitor] status=%s reason=%s", result.get("status"), result.get("reason"))
            return

        logger.warning(
            "[newbase_monitor] observer attention required reason=%s; circuit unchanged",
            result.get("reason"),
        )


if __name__ == "__main__":
    asyncio.run(main())
