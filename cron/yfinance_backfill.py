"""
Railway cron entrypoint for yfinance research/backfill features.

Suggested cadence: once daily after QC daily_feature_snapshot and after market
close. This job has no execution authority.
"""
from __future__ import annotations

import asyncio
import logging
import os

from constants import resolve_universe
from services.cron_audit import audit_cron_run
from services.yfinance_backfill import LOOKBACK_DAYS_DEFAULT, run_yfinance_backfill
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.yfinance_backfill")


async def main() -> None:
    async with audit_cron_run("yfinance_backfill") as audit:
        lookback_days = int(os.getenv("YFINANCE_BACKFILL_DAYS", str(LOOKBACK_DAYS_DEFAULT)))
        batch_size = int(os.getenv("YFINANCE_BATCH_SIZE", "30"))
        tickers_env = os.getenv("YFINANCE_TICKERS", "").strip()
        tickers = [t.strip().upper() for t in tickers_env.split(",") if t.strip()] if tickers_env else await resolve_universe()

        logger.info(
            "[YFINANCE_BACKFILL] start tickers=%s lookback_days=%s batch_size=%s",
            len(tickers),
            lookback_days,
            batch_size,
        )
        result = await run_yfinance_backfill(
            tickers=tickers,
            lookback_days=lookback_days,
            batch_size=batch_size,
        )
        audit.add_rows(result.get("rows_upserted"))
        audit.set_summary(
            status=result.get("status"),
            tickers=result.get("tickers"),
            failures=len(result.get("failures") or {}),
        )
        logger.info("[YFINANCE_BACKFILL] done result=%s", result)

        failures = result.get("failures") or {}
        text = (
            "📚 <b>yfinance backfill</b>\n"
            f"Status: {result.get('status')}\n"
            f"Tickers: {result.get('tickers', 0)}\n"
            f"Rows upserted: {result.get('rows_upserted', 0)}\n"
            f"Failures: {len(failures)}"
        )
        if failures:
            sample = list(failures.items())[:5]
            text += "\n" + "\n".join(f"{ticker}: {error[:80]}" for ticker, error in sample)
        await tool_send_telegram({"text": text})


if __name__ == "__main__":
    asyncio.run(main())
