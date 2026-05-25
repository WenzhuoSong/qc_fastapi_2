import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from services.market_calendar import (
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


if __name__ == "__main__":
    unittest.main()
