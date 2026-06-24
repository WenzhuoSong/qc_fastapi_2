"""
cron/daily_signal_freeze.py

Observe-only signal freezer. Runs Playground, extracts EvidenceCards, and stores
immutable FrozenSignal rows for live/paper validation. It never writes target
weights or execution commands.

Usage: python -m cron.daily_signal_freeze
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from db.queries import get_system_config
from db.session import AsyncSessionLocal
from services.cron_audit import audit_cron_run
from services.execution_policy import policy_snapshot
from services.newbase_monitoring import is_active_newbase_observer
from services.playground import run_playground_analysis
from services.signal_ledger import freeze_playground_bundle, persist_frozen_signals
from tools.notify_tools import tool_send_telegram


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.signal_freeze")


async def main() -> None:
    try:
        async with audit_cron_run("daily_signal_freeze") as audit:
            config = await _read_config()
            if not config.get("enabled", True):
                audit.mark_skipped("disabled_by_config")
                logger.info("Daily signal freeze disabled by config")
                return
            if await is_active_newbase_observer() and not config.get("run_in_newbase_observer_mode", False):
                audit.mark_skipped("newbase_observer_legacy_signal_freeze_disabled")
                audit.set_summary(
                    mode="newbase_observer_only",
                    execution_authority="none",
                    target_weight_mutation="none",
                )
                logger.info("Daily signal freeze skipped in newBase observer-only mode")
                return

            playground_cfg = await _read_playground_config()
            pc_config = await _read_pc_promotion_config()
            days = int(playground_cfg.get("lookback_days", 30))
            strategies = playground_cfg.get("strategies") or None
            bundle = await run_playground_analysis(days=days, strategy_names=strategies)
            generated_at = datetime.now(timezone.utc)
            signals = freeze_playground_bundle(
                bundle.to_dict(),
                generated_at=generated_at,
                signal_date=generated_at.date(),
                feature_data_date=None,
                feature_source="playground_bundle",
                feature_authority="mixed",
                qc_context={
                    "execution_authority": "none",
                    "source": "daily_signal_freeze",
                    "snapshot_count": bundle.snapshot_count,
                    "historical_snapshot_count": bundle.historical_snapshot_count,
                    "policy_version": policy_snapshot()["version"],
                    "portfolio_construction_mode": pc_config.get("portfolio_construction_mode"),
                },
                portfolio_construction_config=pc_config,
            )
            async with AsyncSessionLocal() as db:
                result = await persist_frozen_signals(db, signals)
            audit.set_summary(
                signals_seen=len(signals),
                inserted=result.inserted,
                duplicates=result.duplicates,
                conflicts=len(result.conflicts),
                strategies=len(bundle.strategies),
            )
            logger.info(
                "Daily signal freeze inserted=%s duplicates=%s conflicts=%s",
                result.inserted,
                result.duplicates,
                len(result.conflicts),
            )
            if result.conflicts:
                await tool_send_telegram({
                    "text": f"Daily signal freeze conflicts: {len(result.conflicts)}",
                    "parse_mode": "",
                })
    except Exception as exc:
        logger.exception("Daily signal freeze FAILED")
        try:
            await tool_send_telegram({"text": f"Daily signal freeze failed: {exc}", "parse_mode": ""})
        except Exception:
            pass
        raise


async def _read_config() -> dict:
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "daily_signal_freeze_config")
    return (cfg.value if cfg else {}) or {"enabled": True}


async def _read_playground_config() -> dict:
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "playground_config")
    return (cfg.value if cfg else {}) or {}


async def _read_pc_promotion_config() -> dict:
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "portfolio_construction_promotion_config")
    return (cfg.value if cfg else {}) or {}


if __name__ == "__main__":
    asyncio.run(main())
