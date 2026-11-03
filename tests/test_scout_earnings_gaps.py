import asyncio
import unittest
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

from scout import digest, earnings_gaps


def closes(n: int, start: date = date(2024, 1, 1)) -> list[tuple[date, float]]:
    return [(start + timedelta(days=i), 100.0) for i in range(n)]


class MovesTests(unittest.TestCase):
    def test_window_is_session_before_to_session_after(self):
        c = closes(10)
        c[4] = (c[4][0], 100.0)      # session before the report
        c[5] = (c[5][0], 90.0)       # report day
        c[6] = (c[6][0], 85.0)       # session after
        mv = earnings_gaps.moves([c[5][0]], c)
        self.assertAlmostEqual(mv[0]["move"], -0.15)

    def test_report_on_a_non_session_uses_the_next_session(self):
        c = [x for i, x in enumerate(closes(10)) if i != 5]
        c[5] = (c[5][0], 120.0)      # day 6 is the first session on or after day 5; day 7 closes the window
        mv = earnings_gaps.moves([date(2024, 1, 6)], c)
        self.assertEqual(len(mv), 1)

    def test_edges_and_cap(self):
        c = closes(400)
        self.assertEqual(earnings_gaps.moves([c[0][0], c[-1][0]], c), [])
        days = [c[i][0] for i in range(10, 390, 15)]
        self.assertEqual(len(earnings_gaps.moves(days, c)), earnings_gaps.MAX_REPORTS)

    def test_summary_and_text(self):
        mv = [{"date": date(2024, 1, i + 1), "move": m} for i, m in enumerate([0.17, -0.156, -0.067, 0.22, -0.005])]
        self.assertIsNone(earnings_gaps.summary(mv[:3]))
        s = earnings_gaps.summary(mv)
        self.assertEqual((s["n"], s["big"], s["big_up"], s["big_down"]), (5, 3, 2, 1))
        self.assertAlmostEqual(s["median_abs"], 0.156)
        self.assertAlmostEqual(s["max_down"], -0.156)
        line = earnings_gaps.text(s, 0.20)
        self.assertIn("3 次涨跌超过 10%", line)
        self.assertIn("越过", line)                       # a 15.6 % drop is more than half the 20 % stop gap
        self.assertNotIn("越过", earnings_gaps.text(s, 0.40))
        self.assertNotIn("越过", earnings_gaps.text(s))

    def test_report_days_prefer_filings(self):
        filed = [date(2025, 2, 11), date(2025, 2, 20), date(2025, 5, 8)]       # the second is the same quarter
        cal = [date(2025, 2, 11), date(2025, 8, 6)]
        self.assertEqual(earnings_gaps.report_days(filed, cal), [date(2025, 2, 11), date(2025, 5, 8), date(2025, 8, 6)])


class DigestTests(unittest.TestCase):
    def test_holding_with_earnings_soon_shows_the_history(self):
        t = {"ticker": "SHOP", "id": 1, "sector": "Technology", "entry_price": 160.0, "shares": 10, "stop": 143.38,
             "last_price": 156.0, "opened_on": date(2026, 10, 1), "plan": {}}
        s = {"n": 19, "big": 13, "big_up": 8, "big_down": 5, "median_abs": 0.15, "max_up": 0.238, "max_down": -0.186,
             "since": date(2022, 2, 16)}
        g = {"held_rows": [t], "feats": {}, "news": {}, "prop": [], "managed": {}, "earnings": {"SHOP": date(2026, 11, 3)},
             "gaps": {"SHOP": s}}
        with patch.object(digest.portfolio, "metrics", return_value={"pnl": -40.0, "pnl_pct": -0.025, "price": 156.0,
                                                                      "remaining": 10, "to_stop": -0.081}):
            items = digest.holding_items(g, date(2026, 11, 2))
        text = "\n".join(items[0]["lines"])
        self.assertIn("13 次涨跌超过 10%", text)
        self.assertIn("止损在 8% 外", text)

    def test_history_needs_enough_reports(self):
        with patch.object(earnings_gaps.db, "fetch_all", AsyncMock(side_effect=[[{"filed": date(2025, 2, 11)}], []])):
            self.assertIsNone(asyncio.run(earnings_gaps.history("SHOP")))


if __name__ == "__main__":
    unittest.main()
