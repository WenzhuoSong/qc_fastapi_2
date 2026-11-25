import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from scout import behavior


class LineTests(unittest.TestCase):
    def test_stop_line_states_and_cost(self):
        rec = {"stops": [{"ticker": "MU", "signal": date(2026, 11, 5), "state": "on_plan", "sessions": 1, "cost": None},
                         {"ticker": "PBF", "signal": date(2026, 11, 19), "state": "outstanding", "sessions": 4, "cost": 900.0},
                         {"ticker": "SHOP", "signal": date(2026, 11, 19), "state": "outstanding", "sessions": 4, "cost": -100.0}],
               "early": [], "over_cap": []}
        line = behavior.stop_line(rec)
        self.assertTrue(line.startswith("你的记录：止损信号 3 次：按时执行 1 次"))
        self.assertIn("还没执行 2 次（PBF 已过 4 个交易日、SHOP 已过 4 个交易日）", line)
        self.assertIn("等待多亏约 $800", line)
        self.assertIsNone(behavior.stop_line({"stops": []}))

    def test_early_and_over_cap(self):
        rec = {"early": [{"ticker": "VLO", "sold_on": date(2026, 10, 28), "price": 383.25, "since": 0.052}],
               "over_cap": [{"ticker": "XLP", "date": date(2026, 11, 6), "pct": 0.33, "since": 0.021}]}
        self.assertIn("VLO（10-28）+5.2%", behavior.early_line(rec))
        self.assertIn("XLP（11-06，买入后占账户 33%，至今 +2.1%）", behavior.over_cap_line(rec))


class OverCapTests(unittest.IsolatedAsyncioTestCase):
    async def test_satellite_counts_other_open_trades_at_that_close(self):
        a = {"id": 1, "ticker": "A", "opened_on": date(2026, 11, 1), "closed_on": None, "shares": 100, "entry_price": 100.0}
        b = {"id": 2, "ticker": "B", "opened_on": date(2026, 11, 5), "closed_on": None, "shares": 100, "entry_price": 100.0}
        bars = {"A": [{"date": date(2026, 11, 4), "close": 160.0}], "B": [{"date": date(2026, 11, 6), "close": 110.0}]}
        with patch("scout.portfolio.get_settings", AsyncMock(return_value={"account_size": 100000.0, "satellite_max_pct": 0.25})):
            out = await behavior.over_cap_buys([a, b], {1: [], 2: []}, bars)
        self.assertEqual([x["ticker"] for x in out], ["B"])             # 16,000 of A + 10,000 of B = 26 % > 25 %
        self.assertAlmostEqual(out[0]["since"], 0.10)


if __name__ == "__main__":
    unittest.main()
