"""
cron/playground_analysis.py

Research-only strategy sandbox. Runs after market close, compares multiple
strategies on recent QC snapshots, and sends a Telegram report. It never writes
execution commands or pending proposals.

Usage: python -m cron.playground_analysis
"""
from __future__ import annotations

import asyncio
import logging

from db.queries import get_system_config
from db.session import AsyncSessionLocal
from services.cron_audit import audit_cron_run
from services.newbase_monitoring import is_active_newbase_observer
from services.playground import generate_playground_report, run_playground_analysis
from services.strategy_health import persist_strategy_health_profiles
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.playground")


async def main() -> None:
    try:
        async with audit_cron_run("playground_analysis") as audit:
            config = await _read_config()
            if not config.get("enabled", True):
                audit.mark_skipped("disabled_by_config")
                logger.info("Playground analysis disabled by config")
                return
            if await is_active_newbase_observer() and not config.get("run_in_newbase_observer_mode", False):
                audit.mark_skipped("newbase_observer_legacy_playground_disabled")
                audit.set_summary(
                    mode="newbase_observer_only",
                    execution_authority="none",
                    target_weight_mutation="none",
                )
                logger.info("Playground analysis skipped in newBase observer-only mode")
                return

            days = int(config.get("lookback_days", 30))
            strategies = config.get("strategies") or None
            bundle = await run_playground_analysis(days=days, strategy_names=strategies)
            health = await persist_strategy_health_profiles(bundle.to_dict())
            report = await generate_playground_report(bundle)
            result = await tool_send_telegram({"text": report, "parse_mode": "HTML"})
            audit.set_summary(
                sent=result.get("sent"),
                snapshots=bundle.snapshot_count,
                strategies=len(bundle.strategies),
                data_gaps=len(bundle.data_gaps),
                strategy_health_flags=len(health.get("decay_flags") or []),
            )
            logger.info(
                f"Playground sent={result.get('sent')} snapshots={bundle.snapshot_count} "
                f"strategies={len(bundle.strategies)}"
            )
    except Exception as exc:
        logger.exception("Playground analysis FAILED")
        try:
            await tool_send_telegram({"text": f"Playground analysis failed: {exc}", "parse_mode": ""})
        except Exception:
            pass
        raise


async def _read_config() -> dict:
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "playground_config")
    return (cfg.value if cfg else {}) or {}


if __name__ == "__main__":
    asyncio.run(main())
