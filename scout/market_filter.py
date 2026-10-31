"""Market filter for the user's buys: Faber's 10-month rule on SPY (the user's decision, 2026-10-31).

Risk is off while the latest month-end close of SPY is below the mean of the last 10 month-end
closes; then the plan says no new buys. Holdings are not sold for it: each keeps its own exit
(the 20 % trailing stop). The same rule as `exit_redeploy.market_off`, which the Sharadar recheck
of `exit_redeploy` (1999–2026) tested: with the filter max drawdown −22 % instead of −54 % and 2008
−7 % instead of −35 %, at about 1.5 pp a year less return, and behind holding after 2012 (SPY alone,
2003–2026: −34 % vs −55 %, 10.4 % vs 11.3 % a year; since 2013 12.1 % vs 15.0 % with the same −34 %,
the 2020 crash was too fast for a monthly rule).
"""
from __future__ import annotations

from datetime import date

from scout import db
from scout.universe import BENCHMARK

MONTHS = 10


def _month_end(dates: list[date], t: int) -> bool:
    """Whether session t is the last of its month (the last session asks the trading calendar)."""
    if t + 1 < len(dates):
        return dates[t + 1].month != dates[t].month
    from scout.market_calendar import is_us_equity_trading_day

    d = date.fromordinal(dates[t].toordinal() + 1)
    while not is_us_equity_trading_day(d):
        d = date.fromordinal(d.toordinal() + 1)
    return d.month != dates[t].month


def states(dates: list[date], closes: list[float]) -> list[dict]:
    """Per session after its close: {off, month_end close, mean} of the latest month end so far
    (off False until MONTHS month ends exist)."""
    ends: list[float] = []
    cur = {"off": False, "close": None, "mean": None, "month_end": None}
    out = []
    for t, (d, c) in enumerate(zip(dates, closes)):
        if c is not None and _month_end(dates, t):
            ends.append(float(c))
            if len(ends) >= MONTHS:
                m = sum(ends[-MONTHS:]) / MONTHS
                cur = {"off": c < m, "close": float(c), "mean": m, "month_end": d}
        out.append({"date": d, **cur})
    return out


def summary(dates: list[date], closes: list[float]) -> dict | None:
    """The state after the last close, since when it holds, and whether it changed at this close."""
    st = states(dates, closes)
    if not st or st[-1]["mean"] is None:
        return None
    last = st[-1]
    since = None                            # the last flip (state changes only at a month-end close)
    for prev, s in zip(st, st[1:]):
        if prev["mean"] is not None and s["off"] != prev["off"]:
            since = s["date"]
    return {**last, "gap": last["close"] / last["mean"] - 1, "since": since, "changed_today": since == last["date"]}


async def current(on: date | None = None) -> dict | None:
    """The filter after the close of `on` (default: the latest stored session)."""
    rows = await db.fetch_all(
        "SELECT date, close FROM scout.bars_daily WHERE ticker = :t AND (CAST(:d AS DATE) IS NULL OR date <= :d) "
        "ORDER BY date", {"t": BENCHMARK, "d": on})
    return summary([r["date"] for r in rows], [r["close"] for r in rows])


def text(s: dict | None) -> str:
    if not s:
        return "大盘过滤：SPY 历史不足 10 个月末，暂无状态"
    head = (f"大盘过滤：{'避险，按计划不新买入' if s['off'] else '正常，可以按计划买入'}"
            f"（SPY {s['month_end']:%m-%d} 月末收盘 {s['close']:.2f}，10 个月均值 {s['mean']:.2f}，{s['gap'] * 100:+.1f}%")
    return head + (f"；自 {s['since']:%Y-%m-%d} 起）" if s.get("since") else "）")
