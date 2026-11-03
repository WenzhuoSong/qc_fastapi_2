"""How far a stock has moved around its own past earnings reports (descriptive, not a forecast).

Shown before a buy and on holdings when the stock reports within 7 days: a report can gap the price past the
20 % trailing stop, which only acts on closes, so the stop does not cap the loss on that day. Report dates are
8-K filings with item 2.02 (`angles.earnings_dates`, one per quarter) plus past calendar entries in
`scout.market_events`; the filing time of day is not stored, so the move is the close before the report day to
the close after it (two sessions, covering a release before the open or after the close). Raw moves, market
included, because the stop sees the raw price.
"""
from __future__ import annotations

from bisect import bisect_left
from datetime import date

from scout import db
from scout.angles import EARNINGS_GAP_DAYS, earnings_dates

BIG_MOVE = 0.10        # a move this large (either way) counts as a big earnings reaction
MIN_REPORTS = 4        # fewer past reports than this: no summary
MAX_REPORTS = 20       # the last five years at most


def moves(report_days: list[date], closes: list[tuple[date, float]]) -> list[dict]:
    """Per past report: close of the session before the report day → close of the session after it."""
    days = [d for d, _ in closes]
    px = [c for _, c in closes]
    out = []
    for d in sorted(report_days):
        k = bisect_left(days, d)                 # the report day, or the next session
        if k < 1 or k + 1 >= len(days) or not px[k - 1] > 0:
            continue
        out.append({"date": d, "move": px[k + 1] / px[k - 1] - 1})
    return out[-MAX_REPORTS:]


def summary(mv: list[dict]) -> dict | None:
    if len(mv) < MIN_REPORTS:
        return None
    r = [m["move"] for m in mv]
    big = [x for x in r if abs(x) >= BIG_MOVE]
    absr = sorted(abs(x) for x in r)
    n = len(absr)
    median = absr[n // 2] if n % 2 else (absr[n // 2 - 1] + absr[n // 2]) / 2
    return {"n": n, "big": len(big), "big_up": sum(x > 0 for x in big), "big_down": sum(x < 0 for x in big),
            "median_abs": median, "max_up": max(r), "max_down": min(r), "since": mv[0]["date"]}


def text(s: dict, stop_gap: float | None = None) -> str:
    """One line; `stop_gap` = how far the stop is below the price (0.20 for a new buy)."""
    out = (f"过去 {s['n']} 次财报前后两日：{s['big']} 次涨跌超过 {BIG_MOVE:.0%}（涨 {s['big_up']} / 跌 {s['big_down']}），"
           f"中位数 ±{s['median_abs']:.1%}，最大 {s['max_up']:+.1%} / {s['max_down']:+.1%}")
    if stop_gap is not None and s["max_down"] <= -0.5 * stop_gap:
        out += f"；止损只看收盘，财报跳空可以直接越过（止损在 {stop_gap:.0%} 外）"
    return out


async def history(ticker: str) -> dict | None:
    """Summary of the stock's past earnings moves from Scout's own data, or None when too few."""
    filings = await db.fetch_all("SELECT ticker, filed FROM scout.sec_filings WHERE ticker = :t AND form = '8-K' "
                                 "AND items LIKE '%2.02%'", {"t": ticker})
    cal = await db.fetch_all("SELECT ticker, event_date AS filed FROM scout.market_events WHERE ticker = :t "
                             "AND event_type = 'earnings' AND event_date < current_date", {"t": ticker})
    days = report_days([r["filed"] for r in filings], [r["filed"] for r in cal])
    if len(days) < MIN_REPORTS:
        return None
    bars = await db.fetch_all("SELECT date, close FROM scout.bars_daily WHERE ticker = :t AND date >= :d ORDER BY date",
                              {"t": ticker, "d": days[max(0, len(days) - MAX_REPORTS)]})
    return summary(moves(days, [(r["date"], float(r["close"])) for r in bars if r["close"]]))


def report_days(filed: list[date], calendar: list[date]) -> list[date]:
    """One date per quarter: 8-K 2.02 filings first; a calendar date counts only when no filing is near it."""
    days = earnings_dates([("x", d) for d in filed]).get("x", [])
    for d in sorted(calendar):
        if all(abs((d - x).days) > EARNINGS_GAP_DAYS // 3 for x in days):
            days.append(d)
    return sorted(days)
