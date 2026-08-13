"""Minimal post-market account and decision report."""
from __future__ import annotations

import asyncio

from sqlalchemy import desc, select

from db.models import (
    QCAccountSnapshotHoldingProjection,
    QCAccountSnapshotProjection,
    QCDecisionManifestProjection,
)
from db.session import AsyncSessionLocal
from services.cron_audit import audit_cron_run
from services.qc_observer_health import load_current_observer_summary
from tools.notify_tools import tool_send_telegram


async def main() -> None:
    async with audit_cron_run("post_market_report") as audit:
        observer = await load_current_observer_summary()
        stream = observer.get("event_stream_id")
        if not stream:
            audit.mark_skipped("no_current_observer_stream")
            return
        async with AsyncSessionLocal() as db:
            snapshot = await db.scalar(
                select(QCAccountSnapshotProjection)
                .where(QCAccountSnapshotProjection.event_stream_id == stream)
                .order_by(desc(QCAccountSnapshotProjection.event_time))
                .limit(1)
            )
            decision = await db.scalar(
                select(QCDecisionManifestProjection)
                .where(QCDecisionManifestProjection.event_stream_id == stream)
                .order_by(desc(QCDecisionManifestProjection.event_time))
                .limit(1)
            )
            if snapshot is None:
                audit.mark_skipped("no_account_snapshot")
                return
            holdings = list(
                (
                    await db.scalars(
                        select(QCAccountSnapshotHoldingProjection)
                        .where(QCAccountSnapshotHoldingProjection.account_snapshot_id == snapshot.id)
                        .order_by(desc(QCAccountSnapshotHoldingProjection.holdings_value))
                    )
                ).all()
            )
        equity = float(snapshot.equity)
        cash = float(snapshot.cash)
        targets = dict(decision.final_target_weights or {}) if decision else {}
        holding_text = ", ".join(
            f"{row.ticker} {float(row.holdings_value or 0) / equity:.1%}" for row in holdings
        )
        text = "\n".join(
            [
                "noble-flow post-market",
                f"Observer: {observer.get('status')}",
                f"Equity: ${equity:,.2f}",
                f"Cash: ${cash:,.2f} ({cash / equity:.1%})",
                f"Holdings: {holding_text or 'none'}",
                f"Target exposure: {sum(float(value) for value in targets.values()):.1%}",
                f"Pending/deferred: {snapshot.pending_order_count}/{snapshot.deferred_target_count}",
                "Execution authority: NONE",
            ]
        )
        await tool_send_telegram({"text": text, "parse_mode": ""})
        audit.set_summary(
            observer_status=observer.get("status"),
            equity=equity,
            cash=cash,
            cash_pct=cash / equity,
            target_exposure=sum(float(value) for value in targets.values()),
            execution_authority="none",
        )


if __name__ == "__main__":
    asyncio.run(main())
