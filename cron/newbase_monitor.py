"""Railway cron entry for newBase observer-only monitoring.

This job has no execution authority. It checks whether the expected
QC/newBase telemetry has arrived and records the result in cron_run_log.
"""
from __future__ import annotations

import asyncio
import logging

from services.circuit_breaker import CircuitBreakerMonitor, CircuitState
from services.cron_audit import audit_cron_run
from services.newbase_monitoring import (
    is_active_newbase_observer,
    run_newbase_full_auto_monitor,
)


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

        result = await run_newbase_full_auto_monitor()
        audit.set_summary(
            status=result.get("status"),
            reason=result.get("reason"),
            should_alert=bool(result.get("should_alert")),
            expected_snapshot_trading_date=result.get("expected_snapshot_trading_date"),
            as_of_snapshot_uid=result.get("as_of_snapshot_uid"),
            as_of_recorded_at=result.get("as_of_recorded_at"),
            execution_authority="none",
            target_weight_mutation="none",
        )

        if not result.get("should_alert"):
            logger.info("[newbase_monitor] status=%s reason=%s", result.get("status"), result.get("reason"))
            return

        reason = str(result.get("reason") or "newbase_monitor_alert")
        await CircuitBreakerMonitor().update_circuit_state(
            CircuitState.ALERT,
            reason=reason,
            primary_trigger="newbase_live_snapshot_monitor",
        )
        logger.warning("[newbase_monitor] alert reason=%s", reason)


if __name__ == "__main__":
    asyncio.run(main())
