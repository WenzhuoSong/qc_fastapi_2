"""Small deterministic US equity market calendar helper.

This intentionally avoids network calls and heavy dependencies. It covers the
standard full-day NYSE/Nasdaq holidays used by cron gating and operational
freshness checks.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)


@dataclass(frozen=True)
class MarketStatus:
    is_trading_day: bool
    is_open: bool
    phase: str
    reason: str
    market_time: str
    market_date: str

    def to_dict(self) -> dict[str, object]:
        return {
            "is_trading_day": self.is_trading_day,
            "is_open": self.is_open,
            "phase": self.phase,
            "reason": self.reason,
            "market_time": self.market_time,
            "market_date": self.market_date,
        }


def us_equity_market_status(now: datetime | None = None) -> MarketStatus:
    market_now = now.astimezone(MARKET_TZ) if now else datetime.now(MARKET_TZ)
    market_day = market_now.date()
    label = market_now.strftime("%Y-%m-%d %H:%M %Z")
    holiday = us_equity_holiday_name(market_day)
    trading_day = is_us_equity_trading_day(market_day)
    if not trading_day:
        reason = holiday or "weekend"
        return MarketStatus(
            is_trading_day=False,
            is_open=False,
            phase="closed",
            reason=reason,
            market_time=label,
            market_date=market_day.isoformat(),
        )
    market_time = market_now.time()
    if market_time < MARKET_OPEN:
        phase = "opens_soon" if market_time >= time(6, 0) else "premarket"
        return MarketStatus(
            is_trading_day=True,
            is_open=False,
            phase=phase,
            reason=phase,
            market_time=label,
            market_date=market_day.isoformat(),
        )
    if market_time < MARKET_CLOSE:
        return MarketStatus(
            is_trading_day=True,
            is_open=True,
            phase="open",
            reason="regular_hours",
            market_time=label,
            market_date=market_day.isoformat(),
        )
    return MarketStatus(
        is_trading_day=True,
        is_open=False,
        phase="closed",
        reason="after_close",
        market_time=label,
        market_date=market_day.isoformat(),
    )


def is_us_equity_trading_day(day: date) -> bool:
    return day.weekday() < 5 and us_equity_holiday_name(day) is None


def previous_us_equity_trading_day(day: date) -> date:
    candidate = day - timedelta(days=1)
    while not is_us_equity_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def us_equity_holiday_name(day: date) -> str | None:
    if day.weekday() >= 5:
        return None
    for year in (day.year - 1, day.year, day.year + 1):
        holidays = _us_equity_holidays_for_year(year)
        if day in holidays:
            return holidays[day]
    return None


def _us_equity_holidays_for_year(year: int) -> dict[date, str]:
    return {
        _observed(date(year, 1, 1)): "New Year's Day",
        _nth_weekday(year, 1, 0, 3): "Martin Luther King Jr. Day",
        _nth_weekday(year, 2, 0, 3): "Washington's Birthday",
        _easter_sunday(year) - timedelta(days=2): "Good Friday",
        _last_weekday(year, 5, 0): "Memorial Day",
        _observed(date(year, 6, 19)): "Juneteenth National Independence Day",
        _observed(date(year, 7, 4)): "Independence Day",
        _nth_weekday(year, 9, 0, 1): "Labor Day",
        _nth_weekday(year, 11, 3, 4): "Thanksgiving Day",
        _observed(date(year, 12, 25)): "Christmas Day",
    }


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    candidate = date(year, month, 1)
    offset = (weekday - candidate.weekday()) % 7
    return candidate + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        candidate = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        candidate = date(year, month + 1, 1) - timedelta(days=1)
    while candidate.weekday() != weekday:
        candidate -= timedelta(days=1)
    return candidate


def _easter_sunday(year: int) -> date:
    # Anonymous Gregorian algorithm.
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)
