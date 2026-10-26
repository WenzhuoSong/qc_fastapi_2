import unittest

from scout import screens


class ScreenTests(unittest.TestCase):
    def test_every_screen_has_criteria_and_unique_id(self):
        all_s = screens.all_screens()
        ids = [s.id for s in all_s]
        self.assertEqual(len(ids), len(set(ids)))
        for s in all_s:
            self.assertTrue(s.criteria, s.id)
            self.assertIn(s.group, ("规则", "技术信号", "事件"))

    def test_rule_screens_cover_candidates_and_list_conditions(self):
        rules = {s.id: s for s in screens.rule_screens()}
        self.assertIn("rule_zscore_oversold", rules)
        self.assertIn("候选", rules["rule_zscore_oversold"].title)
        self.assertEqual(len(rules["rule_trend_breakout"].criteria), len(rules["rule_trend_breakout"].detector.conditions))

    def test_signal_filters(self):
        rows = [
            {"ticker": "A", "dist_52w_high": -0.002, "ret_1d": 0.05, "vol_ratio": 2.5, "rsi14": 75,
             "swing_break": "收盘突破最近波段高点 10（2026-10-01）：结构转强", "dist_resistance": 0.01,
             "close": 100, "sma50": 90, "rs_pct": 0.9},
            {"ticker": "B", "dist_52w_high": -0.30, "ret_1d": -0.04, "vol_ratio": 0.8, "rsi14": 25,
             "swing_break": "收盘跌破最近波段低点 9（2026-10-01）：结构转弱", "dist_support": -0.01, "close": 50, "sma200": 60},
            {"ticker": "C", "dist_52w_high": -0.05, "ret_1d": 0.0, "vol_ratio": 1.0, "rsi14": 50,
             "dist_support": -0.015, "close": 80, "sma200": 70},
        ]
        c = screens.counts(rows)
        self.assertEqual((c["new_high"], c["top_gainers"], c["top_losers"]), (1, 1, 1))
        self.assertEqual((c["rsi_oversold"], c["rsi_overbought"], c["unusual_volume"]), (1, 1, 1))
        self.assertEqual((c["swing_breakout"], c["swing_breakdown"]), (1, 0))   # B breaks down on 0.8× volume
        self.assertEqual(c["support_in_uptrend"], 1)      # B is below its 200-day line, C qualifies
        self.assertEqual(c["near_resistance"], 1)


if __name__ == "__main__":
    unittest.main()
