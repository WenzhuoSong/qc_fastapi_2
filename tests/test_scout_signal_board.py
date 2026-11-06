import unittest
from datetime import date

from scout import signal_board as sb

DATES = [date(2026, 11, d) for d in (2, 3, 4, 5, 6, 9, 10)]
BASE = {"entry_lo": 199.43, "entry_hi": 201.51, "stop": 195.29, "target1": 210.83, "window": 10, "limit": 20,
        "asof": date(2026, 11, 4), "entry_date": None, "entry_price": None, "exit_date": None, "ret": None, "spy_ret": None}


class StageTests(unittest.TestCase):
    def test_states_map_to_three_stages(self):
        self.assertEqual([sb.stage(s) for s in ("pending", "new", "open", "triggered")],
                         ["candidate", "candidate", "triggered", "triggered"])
        for s in ("untriggered", "expired_untriggered", "hit_target", "stopped", "trailed", "expired"):
            self.assertEqual(sb.stage(s), "done", s)

    def test_sessions_after_counts_trading_days(self):
        self.assertEqual(sb.sessions_after(DATES, date(2026, 11, 4), date(2026, 11, 6)), 2)
        self.assertEqual(sb.sessions_after(DATES, date(2026, 11, 7), date(2026, 11, 10)), 2)    # a Saturday start


class DescribeTests(unittest.TestCase):
    def test_candidate_says_days_left_and_where_the_price_is(self):
        text = sb.describe({**BASE, "state": "pending"}, 213.55, DATES, date(2026, 11, 5))
        self.assertEqual(text, "等价格进入买入区，还剩 9 个交易日，收盘 213.55 高于买入区 6.0%")
        self.assertIn("已在买入区内", sb.describe({**BASE, "state": "pending"}, 200.0, DATES, date(2026, 11, 5)))

    def test_triggered_shows_fill_day_count_and_pnl(self):
        r = {**BASE, "state": "open", "entry_date": date(2026, 11, 5), "entry_price": 201.51}
        self.assertEqual(sb.describe(r, 205.0, DATES, date(2026, 11, 9)), "11-05 以 201.51 成交，持有第 3 / 20 天，浮动 +1.7%")

    def test_done_shows_outcome_against_spy_and_expired_signals(self):
        r = {**BASE, "state": "stopped", "entry_date": date(2026, 11, 5), "exit_date": date(2026, 11, 9),
             "ret": -0.031, "spy_ret": 0.004}
        self.assertEqual(sb.describe(r, 190.0, DATES, date(2026, 11, 10)), "止损：-3.1%（11-05 → 11-09），同期 SPY +0.4%")
        self.assertEqual(sb.describe({**BASE, "state": "untriggered"}, 220.0, DATES, date(2026, 11, 10)), "没回到买入区，作废")


class VerdictTests(unittest.TestCase):
    def test_registered_entry_decides_the_rule_status(self):
        entries = [
            {"key": "a", "kind": "rule", "status": "rejected", "applies": {"detectors": ["breakout_retest"]}},
            {"key": "b", "kind": "rule", "status": "forward", "spec": {"detector": "breakout_retest"}},
            {"key": "c", "kind": "rule", "status": "rejected", "applies": {"detectors": ["power_gap"]}},
        ]
        self.assertEqual(sb.verdicts(entries), {"breakout_retest": "前向验证中", "power_gap": "否定"})


if __name__ == "__main__":
    unittest.main()
