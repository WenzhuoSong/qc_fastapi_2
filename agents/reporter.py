# agents/reporter.py
import logging
from datetime import datetime, timedelta
from sqlalchemy import select, desc
from db.session import run_async_isolated
from db.models import PortfolioTimeseries, ExecutionLog
from tools.notify_tools import tool_send_telegram

logger = logging.getLogger("qc_fastapi_2.reporter")


def run_reporter() -> dict:
    """Phase 1: 直接查库计算主要指标，不经过 LLM。"""
    stats  = run_async_isolated(_gather_stats)
    msg    = _format_daily_report(stats)
    result = tool_send_telegram({"text": msg, "parse_mode": "HTML"})
    return {"reported": result.get("sent"), "stats": stats}


async def _gather_stats(session_factory) -> dict:
    async with session_factory() as db:
        now    = datetime.utcnow()
        today  = now.date()

        # 最新快照
        latest_q = await db.execute(
            select(PortfolioTimeseries)
            .order_by(desc(PortfolioTimeseries.recorded_at))
            .limit(1)
        )
        latest = latest_q.scalar_one_or_none()

        # 今日执行记录
        exec_q = await db.execute(
            select(ExecutionLog)
            .where(ExecutionLog.executed_at >= datetime.combine(today, datetime.min.time()))
            .order_by(desc(ExecutionLog.executed_at))
        )
        today_executions = exec_q.scalars().all()

        # 过去 30 天日收益序列
        days30_q = await db.execute(
            select(PortfolioTimeseries)
            .where(PortfolioTimeseries.recorded_at >= now - timedelta(days=30))
            .order_by(PortfolioTimeseries.recorded_at)
        )
        rows30 = days30_q.scalars().all()

        # 计算胜率（粗略：用每天第一条 PnL 符号）
        win_days  = sum(1 for r in rows30 if (r.daily_pnl_pct or 0) > 0)
        total_days = max(len(rows30), 1)

        return {
            "total_value":     float(latest.total_value)          if latest else 0,
            "daily_pnl_pct":  float(latest.daily_pnl_pct or 0)   if latest else 0,
            "drawdown":        float(latest.current_drawdown_pct or 0) if latest else 0,
            "regime_label":    latest.regime_label                if latest else "N/A",
            "win_rate_30d":    win_days / total_days,
            "executions_today": len(today_executions),
        }


def _format_daily_report(s: dict) -> str:
    pnl_sign = "+" if s["daily_pnl_pct"] >= 0 else ""
    return (
        f"📊 <b>每日策略日报</b> | {datetime.utcnow().strftime('%Y-%m-%d')}\n"
        f"――――――――――――――――\n"
        f"💰 净值  <b>${s['total_value']:,.0f}</b>\n"
        f"📈 日盈亏  {pnl_sign}{s['daily_pnl_pct']:.2%}\n"
        f"📉 回撤  -{s['drawdown']:.2%}\n"
        f"\n市场制度  {s['regime_label']} 🟢\n"
        f"胜率(30d) {s['win_rate_30d']:.0%}\n"
        f"今日执行  {s['executions_today']} 笔"
    )
