# agents/reporter.py
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, desc

from db.session import AsyncSessionLocal
from db.models import PortfolioTimeseries, ExecutionLog
from services.execution_log_store import summarize_execution_activity_rows
from tools.notify_tools import tool_send_telegram

logger = logging.getLogger("qc_fastapi_2.reporter")


async def run_reporter_async() -> dict:
    """Phase 1: query DB for headline stats, no LLM."""
    stats  = await _gather_stats()
    msg    = _format_daily_report(stats)
    result = await tool_send_telegram({"text": msg, "parse_mode": "HTML"})
    return {"reported": result.get("sent"), "stats": stats}


async def _gather_stats() -> dict:
    async with AsyncSessionLocal() as db:
        now    = datetime.utcnow()
        today  = now.date()

        latest_q = await db.execute(
            select(PortfolioTimeseries)
            .order_by(desc(PortfolioTimeseries.recorded_at))
            .limit(1)
        )
        latest = latest_q.scalar_one_or_none()

        exec_q = await db.execute(
            select(ExecutionLog)
            .where(ExecutionLog.executed_at >= datetime.combine(today, datetime.min.time()))
            .order_by(desc(ExecutionLog.executed_at))
        )
        today_executions = exec_q.scalars().all()
        cap_summary = summarize_execution_activity_rows(today_executions)
        preflight_blocked_today = sum(
            1 for row in today_executions
            if _is_preflight_blocked_row(row)
        )

        days30_q = await db.execute(
            select(PortfolioTimeseries)
            .where(PortfolioTimeseries.recorded_at >= now - timedelta(days=30))
            .order_by(PortfolioTimeseries.recorded_at)
        )
        rows30 = days30_q.scalars().all()

        win_days   = sum(1 for r in rows30 if (r.daily_pnl_pct or 0) > 0)
        total_days = max(len(rows30), 1)

        return {
            "total_value":      float(latest.total_value)               if latest else 0,
            "daily_pnl_pct":    float(latest.daily_pnl_pct or 0)        if latest else 0,
            "drawdown":         float(latest.current_drawdown_pct or 0) if latest else 0,
            "regime_label":     latest.regime_label                     if latest else "N/A",
            "win_rate_30d":     win_days / total_days,
            "commands_used_today": int(cap_summary.get("command_count") or 0),
            "gross_turnover_today": float(cap_summary.get("gross_turnover") or 0.0),
            "ordinary_commands_today": int(cap_summary.get("ordinary_command_count") or 0),
            "risk_reduce_commands_today": int(cap_summary.get("risk_reduce_command_count") or 0),
            "risk_reduce_turnover_today": float(cap_summary.get("risk_reduce_gross_turnover") or 0.0),
            "execution_log_rows_today": len(today_executions),
            "preflight_blocked_today": preflight_blocked_today,
        }


def _format_daily_report(s: dict) -> str:
    pnl_sign = "+" if s["daily_pnl_pct"] >= 0 else ""
    return (
        f"📊 <b>Daily strategy report</b> | {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
        f"――――――――――――――――\n"
        f"💰 NAV  <b>${s['total_value']:,.0f}</b>\n"
        f"📈 Day PnL  {pnl_sign}{s['daily_pnl_pct']:.2%}\n"
        f"📉 Drawdown  -{s['drawdown']:.2%}\n"
        f"\nRegime  {s['regime_label']} 🟢\n"
        f"Win rate (30d) {s['win_rate_30d']:.0%}\n"
        f"Commands used for cap  {s['commands_used_today']} "
        f"(ordinary {s['ordinary_commands_today']}, risk-reduce {s['risk_reduce_commands_today']})\n"
        f"Turnover used for cap  {s['gross_turnover_today']:.2%}\n"
        f"Risk-reduce turnover  {s['risk_reduce_turnover_today']:.2%}\n"
        f"Execution log rows  {s['execution_log_rows_today']} "
        f"({s['preflight_blocked_today']} preflight blocked)"
    )


def _is_preflight_blocked_row(row: ExecutionLog) -> bool:
    payload = getattr(row, "command_payload", None) or {}
    return (
        str(getattr(row, "qc_status", "") or "").lower() == "not_sent"
        and str(getattr(row, "status", "") or "").lower() == "rejected"
        and payload.get("reason") == "blocked_by_command_preflight"
    )
