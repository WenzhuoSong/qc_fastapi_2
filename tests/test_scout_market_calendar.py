import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from scout.market_calendar import (
    is_us_equity_trading_day,
    previous_us_equity_trading_day,
    us_equity_holiday_name,
    us_equity_market_status,
)


class MarketCalendarTests(unittest.TestCase):
    def test_memorial_day_2026_is_closed(self):
        now = datetime(2026, 5, 25, 9, 3, tzinfo=ZoneInfo("America/New_York"))

        status = us_equity_market_status(now)

        self.assertFalse(status.is_trading_day)
        self.assertFalse(status.is_open)
        self.assertEqual(status.reason, "Memorial Day")
        self.assertEqual(us_equity_holiday_name(now.date()), "Memorial Day")
        self.assertFalse(is_us_equity_trading_day(now.date()))

    def test_previous_trading_day_skips_memorial_day_weekend(self):
        now = datetime(2026, 5, 26, 9, 0, tzinfo=ZoneInfo("America/New_York"))

        self.assertEqual(previous_us_equity_trading_day(now.date()).isoformat(), "2026-05-22")

    def test_regular_weekday_before_open_is_opens_soon(self):
        now = datetime(2026, 5, 26, 9, 3, tzinfo=ZoneInfo("America/New_York"))

        status = us_equity_market_status(now)

        self.assertTrue(status.is_trading_day)
        self.assertFalse(status.is_open)
        self.assertEqual(status.phase, "opens_soon")



class ExpectedSessionTests(unittest.TestCase):
    """EOD scan gating built on the calendar (scout.pipeline.expected_session)."""

    def _at(self, iso):
        from scout.pipeline import expected_session

        return expected_session(datetime.fromisoformat(iso).replace(tzinfo=ZoneInfo("UTC")))

    def test_after_close_scans_today(self):
        session, reason = self._at("2026-10-26T21:30")
        self.assertEqual((session.isoformat(), reason), ("2026-10-26", "after_close"))

    def test_intraday_run_targets_previous_session(self):
        session, reason = self._at("2026-10-26T15:00")
        self.assertEqual((session.isoformat(), reason), ("2026-10-23", "before_close"))

    def test_weekend_and_holidays_are_skipped(self):
        self.assertEqual(self._at("2026-10-25T21:30"), (None, "weekend"))
        self.assertEqual(self._at("2026-11-26T22:30"), (None, "Thanksgiving Day"))
