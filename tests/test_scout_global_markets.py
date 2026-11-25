import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

import pandas as pd

from scout import global_markets as gm
from scout import premarket


class FillTests(unittest.TestCase):
    def test_empty_last_close_comes_from_that_sessions_last_print(self):
        daily = pd.DataFrame({"Close": [100.0, float("nan")], "High": [101.0, 103.0]},
                             index=pd.to_datetime(["2026-11-24", "2026-11-25"]))
        ticks = pd.Series([99.0, 102.5, 104.0], index=pd.DatetimeIndex(
            ["2026-11-25 14:55", "2026-11-25 15:25", "2026-11-26 09:05"]).tz_localize("Asia/Tokyo"))
        c = gm.fill_last(daily, ticks)
        self.assertEqual(c.loc[date(2026, 11, 25)], 102.5)               # not the next morning's print

    def test_no_print_drops_the_day(self):
        daily = pd.DataFrame({"Close": [100.0, float("nan")], "High": [101.0, 103.0]},
                             index=pd.to_datetime(["2026-11-24", "2026-11-25"]))
        self.assertEqual(list(gm.fill_last(daily, pd.Series(dtype=float)).index), [date(2026, 11, 24)])


class PremarketRowTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_todays_local_session_with_its_state(self):
        d0, d1 = date(2026, 11, 25), date(2026, 11, 26)
        series = {"^N225": pd.Series([100.0, 102.0], index=[d0, d1]), "^GDAXI": pd.Series([200.0, 199.0], index=[d0, d1]),
                  "^HSI": pd.Series([50.0, 51.0], index=[date(2026, 11, 24), d0])}
        with patch("scout.global_markets.closes", AsyncMock(return_value=series)):
            rows = await premarket.global_rows(d1)
        self.assertEqual([(r["名称"], r["较前收"]) for r in rows], [("日经 225（收盘）", "+2.00%"), ("德国 DAX（盘中）", "-0.50%")])


if __name__ == "__main__":
    unittest.main()
