"""Strategy screener: every screen states its criteria in plain words.

Rule screens list all of today's hits of a rule (live or candidate) with the
rule's measured history. Signal screens are descriptive filters over the daily
snapshot — useful context, no measured edge, and labelled as such.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable

from scout import base_rates, db
from scout.detectors import Detector, registry

MIN_DOLLAR_VOLUME = 20_000_000
MAX_ROWS = 40


@dataclass(frozen=True)
class Screen:
    id: str
    title: str
    group: str                                     # 规则 | 技术信号 | 事件
    criteria: tuple[str, ...]
    detector: Detector | None = None
    keep: Callable[[dict], bool] | None = None     # snapshot filter
    sort_key: str = "ret_20d"
    descending: bool = True
    note: str = ""
    tags: tuple[str, ...] = field(default_factory=tuple)


def _v(row: dict, key: str, default=None):
    v = row.get(key)
    return default if v is None else v


SIGNAL_SCREENS: tuple[Screen, ...] = (
    Screen("new_high", "接近 52 周新高", "技术信号", ("收盘距 52 周高点 0.5% 以内",),
           keep=lambda r: _v(r, "dist_52w_high", -1) >= -0.005, sort_key="ret_20d",
           note="历史回放显示：规则信号在贴近高点时整体跑输持有，追高前先看「为什么入选」里的位置统计。"),
    Screen("top_gainers", "今日涨幅榜", "技术信号", ("今日涨幅 ≥ 3%",),
           keep=lambda r: _v(r, "ret_1d", 0) >= 0.03, sort_key="ret_1d"),
    Screen("top_losers", "今日跌幅榜", "技术信号", ("今日跌幅 ≥ 3%",),
           keep=lambda r: _v(r, "ret_1d", 0) <= -0.03, sort_key="ret_1d", descending=False),
    Screen("unusual_volume", "异常放量", "技术信号", ("今日成交量 ≥ 50 日均量的 2 倍",),
           keep=lambda r: _v(r, "vol_ratio", 0) >= 2.0, sort_key="vol_ratio"),
    Screen("rsi_oversold", "RSI(14) 超卖", "技术信号", ("RSI(14) ≤ 30",),
           keep=lambda r: _v(r, "rsi14", 50) <= 30, sort_key="rsi14", descending=False,
           note="下降趋势里的超卖往往是趋势延续，结合「收盘在 200 日线上方」一起看更有意义。"),
    Screen("rsi_overbought", "RSI(14) 超买", "技术信号", ("RSI(14) ≥ 70",),
           keep=lambda r: _v(r, "rsi14", 50) >= 70, sort_key="rsi14",
           note="强趋势里 RSI 可以长期停留在 70 以上，超买本身不是卖出理由。"),
    Screen("swing_breakout", "突破最近波段高点", "技术信号",
           ("收盘突破最近一个已确认的波段高点（道氏理论：结构转强）",),
           keep=lambda r: "转强" in (r.get("swing_break") or ""), sort_key="vol_ratio"),
    Screen("swing_breakdown", "跌破最近波段低点", "技术信号",
           ("收盘跌破最近一个已确认的波段低点（道氏理论：结构转弱）",),
           keep=lambda r: "转弱" in (r.get("swing_break") or ""), sort_key="ret_20d", descending=False),
    Screen("support_in_uptrend", "上升格局中回踩支撑", "技术信号",
           ("收盘在 200 日线上方", "距最近主要支撑位 2% 以内"),
           keep=lambda r: _v(r, "dist_support", -1) >= -0.02 and _v(r, "close", 0) > _v(r, "sma200", 1e18),
           sort_key="rs_pct",
           note="《股市趋势技术分析》：上升趋势中回踩前期支撑且缩量企稳是较好的观察位置；有效跌破则支撑失效。"),
    Screen("near_resistance", "逼近阻力位", "技术信号", ("距最近主要阻力位 2% 以内",),
           keep=lambda r: _v(r, "dist_resistance", 1) <= 0.02, sort_key="vol_ratio",
           note="放量收盘站上阻力才算有效突破；缩量冲击阻力容易失败。"),
)


def rule_screens() -> tuple[Screen, ...]:
    return tuple(
        Screen(f"rule_{d.name}", d.label + ("（候选）" if not d.live else ""), "规则",
               tuple(c.label for c in d.conditions), detector=d)
        for d in registry(include_candidates=True)
    )


EARNINGS_SCREEN = Screen("earnings_week", "7 天内发财报", "事件", ("未来 7 天内有财报（Finnhub 日历）",),
                         sort_key="days_to_earnings", descending=False,
                         note="财报前持仓要承担跳空风险；财报后放量跳空可关注「放量跳空」规则。")


def all_screens() -> tuple[Screen, ...]:
    return rule_screens() + SIGNAL_SCREENS + (EARNINGS_SCREEN,)


async def snapshot() -> tuple[object, list[dict]]:
    """Latest daily snapshot of liquid stocks, joined with names and sectors."""
    d = await db.fetch_one("SELECT max(date) AS d FROM scout.features_daily")
    if not d or not d["d"]:
        return None, []
    rows = await db.fetch_all(
        "SELECT f.ticker, i.name, i.sector, f.payload FROM scout.features_daily f JOIN scout.instruments i USING (ticker) "
        "WHERE f.date = :d AND i.kind = 'stock'", {"d": d["d"]},
    )
    out = []
    for r in rows:
        p = r["payload"] or {}
        if _v(p, "dollar_vol20", 0) >= MIN_DOLLAR_VOLUME and _v(p, "close", 0) >= 5:
            out.append({"ticker": r["ticker"], "name": r["name"], "sector": r["sector"], **p})
    return d["d"], out


async def run(screen: Screen, asof, rows: list[dict]) -> list[dict]:
    by_ticker = {r["ticker"]: r for r in rows}
    if screen.detector is not None:
        hits = await db.fetch_all(
            "SELECT ticker, score, payload FROM scout.signal_hits WHERE asof = :d AND detector = :det ORDER BY score DESC",
            {"d": asof, "det": screen.detector.name},
        )
        out = []
        for h in hits:
            base = by_ticker.get(h["ticker"], {"ticker": h["ticker"]})
            out.append({**base, "score": h["score"], "why": h["payload"].get("why") or {}})
        return out[:MAX_ROWS]
    if screen is EARNINGS_SCREEN:
        ev = await db.fetch_all(
            "SELECT ticker, event_date, payload->>'hour' AS hour FROM scout.market_events "
            "WHERE event_type = 'earnings' AND event_date BETWEEN :a AND :b", {"a": asof, "b": asof + timedelta(days=7)},
        )
        out = [{**by_ticker[e["ticker"]], "earnings_date": e["event_date"], "earnings_hour": e["hour"],
                "days_to_earnings": (e["event_date"] - asof).days} for e in ev if e["ticker"] in by_ticker]
    else:
        out = [r for r in rows if screen.keep(r)]
    out.sort(key=lambda r: _v(r, screen.sort_key, 0), reverse=screen.descending)
    return out[:MAX_ROWS]


def counts(rows: list[dict]) -> dict[str, int]:
    return {s.id: sum(1 for r in rows if s.keep(r)) for s in SIGNAL_SCREENS}


def rule_stats(rates: dict, detector: str) -> dict:
    buckets = rates.get(detector) or {}
    return {
        "all": buckets.get("all") or {},
        "positions": [(label, buckets.get(f"pos_{key}") or {}) for key, label, _, _ in base_rates.POSITION_BUCKETS],
    }
