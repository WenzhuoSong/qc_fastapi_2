"""
cron/daily_signal_validation_refresh.py

Observe-only validation refresh. Labels mature yfinance outcomes for frozen
signals and recomputes derived conviction profiles. It never writes target
weights or execution commands.

Usage: python -m cron.daily_signal_validation_refresh
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date

from db.queries import get_system_config
from db.session import AsyncSessionLocal
from services.cron_audit import audit_cron_run
from services.historical_signal_replay import DEFAULT_HORIZONS
from services.signal_validation_refresh import refresh_signal_validation
from tools.notify_tools import tool_send_telegram


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.signal_validation")


async def main() -> None:
    try:
        async with audit_cron_run("daily_signal_validation_refresh") as audit:
            config = await _read_config()
            if not config.get("enabled", True):
                audit.mark_skipped("disabled_by_config")
                logger.info("Daily signal validation refresh disabled by config")
                return

            horizons = _parse_horizons(config.get("horizons") or DEFAULT_HORIZONS)
            row_limit = int(config.get("signal_row_limit", 5000))
            as_of = _parse_date(config.get("as_of_date"))
            async with AsyncSessionLocal() as db:
                result = await refresh_signal_validation(
                    db,
                    as_of_date=as_of,
                    horizons=horizons,
                    signal_row_limit=row_limit,
                    feature_source=str(config.get("feature_source") or "yfinance"),
                )
            audit.add_rows(result.outcomes_inserted + result.profiles_inserted + result.profiles_updated)
            audit.set_summary(**result.to_dict())
            logger.info(
                "Signal validation refresh outcomes=%s/%s profiles=%s inserted=%s updated=%s",
                result.outcomes_inserted,
                result.candidate_outcomes,
                result.profiles_generated,
                result.profiles_inserted,
                result.profiles_updated,
            )
            if result.outcome_conflicts:
                await tool_send_telegram({
                    "text": f"Signal validation outcome conflicts: {len(result.outcome_conflicts)}",
                    "parse_mode": "",
                })
    except Exception as exc:
        logger.exception("Daily signal validation refresh FAILED")
        try:
            await tool_send_telegram({
                "text": f"Daily signal validation refresh failed: {exc}",
                "parse_mode": "",
            })
        except Exception:
            pass
        raise


async def _read_config() -> dict:
    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "daily_signal_validation_config")
    return (cfg.value if cfg else {}) or {"enabled": True}


def _parse_horizons(value) -> tuple[int, ...]:
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    else:
        items = list(value or [])
    parsed = sorted({int(item) for item in items if str(item).strip()})
    return tuple(parsed) or DEFAULT_HORIZONS


def _parse_date(value) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


if __name__ == "__main__":
    asyncio.run(main())
