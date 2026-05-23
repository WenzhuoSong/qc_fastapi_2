"""
Railway cron entrypoint for QC vs yfinance feature parity audit.

Suggested cadence: once daily after yfinance_backfill and QC daily snapshots.
This job is read-only against source data and writes only compact audit
summaries to data_quality_audit plus cron_run_log telemetry.

Usage: python -m cron.qc_yfinance_feature_audit
"""
from __future__ import annotations

import asyncio
import logging
import os

from services.cron_audit import audit_cron_run
from tools.audit_qc_yfinance_features import AUDIT_NAME, run_audit
from tools.notify_tools import tool_send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.qc_yfinance_feature_audit")


async def main() -> None:
    async with audit_cron_run("qc_yfinance_feature_audit") as audit:
        lookback_days = int(os.getenv("QC_YFINANCE_AUDIT_LOOKBACK_DAYS", "45"))
        sample_limit = int(os.getenv("QC_YFINANCE_AUDIT_SAMPLE_LIMIT", "12"))
        notify = _env_bool(os.getenv("QC_YFINANCE_AUDIT_NOTIFY", "0"))
        fail_on_unit_risk = _env_bool(os.getenv("QC_YFINANCE_AUDIT_FAIL_ON_UNIT_RISK", "0"))

        logger.info(
            "[QC_YFINANCE_AUDIT] start lookback_days=%s sample_limit=%s",
            lookback_days,
            sample_limit,
        )
        summary = await run_audit(
            lookback_days=lookback_days,
            sample_limit=sample_limit,
            write_db=True,
        )
        joined_rows = sum(int(v or 0) for v in (summary.get("packet_totals") or {}).values())
        high_drift_count = len(summary.get("high_drift_classes") or [])
        audit.add_rows(1)
        audit.set_summary(
            audit_name=AUDIT_NAME,
            status=summary.get("status"),
            lookback_days=summary.get("lookback_days"),
            joined_rows=joined_rows,
            unit_risk_count=summary.get("unit_risk_count"),
            high_drift_classes=high_drift_count,
            max_raw_momentum_error=summary.get("max_raw_momentum_error"),
            max_normalized_momentum_error=summary.get("max_normalized_momentum_error"),
        )
        logger.info(
            "[QC_YFINANCE_AUDIT] done status=%s joined_rows=%s unit_risks=%s high_drift_classes=%s",
            summary.get("status"),
            joined_rows,
            summary.get("unit_risk_count"),
            high_drift_count,
        )

        if notify:
            await tool_send_telegram({"text": _format_telegram_summary(summary), "parse_mode": "HTML"})
        if fail_on_unit_risk and summary.get("unit_risk_count"):
            raise RuntimeError(f"QC/yfinance feature audit unit risk detected: {summary.get('unit_risk_count')}")


def _format_telegram_summary(summary: dict) -> str:
    packet_totals = summary.get("packet_totals") or {}
    joined_rows = sum(int(v or 0) for v in packet_totals.values())
    return (
        "🧪 <b>QC/yfinance feature audit</b>\n"
        f"Status: {summary.get('status')}\n"
        f"Lookback: {summary.get('lookback_days')}d\n"
        f"Joined rows: {joined_rows}\n"
        f"Unit risks: {summary.get('unit_risk_count')}\n"
        f"High-drift classes: {len(summary.get('high_drift_classes') or [])}\n"
        f"Max raw momentum error: {summary.get('max_raw_momentum_error')}\n"
        f"Max normalized momentum error: {summary.get('max_normalized_momentum_error')}"
    )


def _env_bool(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


if __name__ == "__main__":
    asyncio.run(main())
