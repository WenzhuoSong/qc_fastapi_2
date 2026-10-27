import unittest
from datetime import date

from scout import levels

F_UP = {"close": 100.0, "atr14": 2.0, "sma20": 97.0, "sma50": 94.0, "sma200": 85.0}


class ReferencePlanTests(unittest.TestCase):
    def test_uptrend_gets_pullback_and_breakout(self):
        ref = levels.reference_plans(F_UP, {"support": 96.0, "resistance": 104.0})
        names = [s["name"] for s in ref["scenarios"]]
        self.assertEqual(ref["trend"], "up")
        self.assertEqual(names, ["回调买入", "突破买入"])
        pull, brk = ref["scenarios"]
        self.assertEqual((pull["entry_lo"], pull["entry_hi"], pull["stop"]), (97.0, 98.0, 95.0))   # nearest level: 20-day line
        self.assertLessEqual(pull["target1"], 104.0)                                                 # capped at resistance
        self.assertEqual((brk["entry_lo"], brk["entry_hi"], brk["stop"]), (104.0, 105.0, 102.0))
        self.assertAlmostEqual(brk["rr"], 2.0)

    def test_stop_is_at_least_one_atr_below_the_zone(self):
        for sc in levels.reference_plans(F_UP, {"support": 96.0, "resistance": 104.0})["scenarios"]:
            self.assertLessEqual(sc["stop"], sc["entry_lo"] - F_UP["atr14"] + 1e-9)

    def test_downtrend_only_offers_a_reversal_trigger(self):
        f = {"close": 50.0, "atr14": 1.5, "sma20": 52.0, "sma50": 55.0, "sma200": 60.0}
        ref = levels.reference_plans(f, {"support": 47.0, "resistance": 54.0})
        self.assertEqual([s["name"] for s in ref["scenarios"]], ["反转确认"])
        none = levels.reference_plans(f, {"support": 47.0, "resistance": 70.0})
        self.assertEqual(none["scenarios"], [])
        self.assertIn("不提供做多参考价位", none["note"])

    def test_far_levels_are_ignored(self):
        ref = levels.reference_plans(F_UP, {"support": 80.0, "resistance": 130.0})
        self.assertEqual([s["name"] for s in ref["scenarios"]], ["回调买入"])     # 20-day line, not the far support
        self.assertEqual(ref["scenarios"][0]["entry_lo"], 97.0)

    def test_trailing_stops(self):
        closes = [90 + i * 0.5 for i in range(30)]
        lows = [c - 1 for c in closes]
        stops = levels.trailing_stops(F_UP, closes, lows)
        names = [x["name"] for x in stops]
        self.assertTrue(any("吊灯" in n for n in names))
        self.assertNotIn("成本价（保本）", names)
        self.assertEqual(stops, sorted(stops, key=lambda x: -x["price"]))


class PlanPanelTests(unittest.TestCase):
    OPP = {"id": "o1", "ticker": "VLO", "primary_detector": "pullback_in_uptrend", "status": "triggered",
           "asof": date(2026, 10, 23), "triggered_on": date(2026, 10, 26), "entry_lo": 410.75, "entry_hi": 423.54,
           "stop": 383.88, "current_stop": 383.88, "target1": 467.05, "target2": 500.32, "horizon_days": 10,
           "risk_reward": 1.5, "evidence": {}, "base_rate": {}}

    def test_triggered_plan_shows_position(self):
        from scout.web.views import trade_plan

        live = {"last": 430.0, "change": 0.01, "time_et": "10-27 10:15", "session": "盘中"}
        html = trade_plan(self.OPP, 426.76, live=live, today=date(2026, 10, 27), entry_price=419.28)
        self.assertIn("规则已入场", html)
        self.assertIn("假设成交 419.28", html)
        self.assertIn("持有第 1 / 10 个交易日", html)
        self.assertIn("盘中 10:15 美东", html)
        self.assertIn("不随股价调整", html)

    def test_pending_plan_counts_down_the_window(self):
        from scout.web.views import trade_plan

        o = {**self.OPP, "status": "new", "triggered_on": None, "asof": date(2026, 10, 26)}
        html = trade_plan(o, 426.76, today=date(2026, 10, 27))
        self.assertIn("入场窗口还剩 2 个交易日", html)
        self.assertIn("最近收盘", html)

    def test_price_panel_leads_with_buying_now(self):
        from scout.web.views import price_panel

        s_ = {"support": 96.0, "resistance": 112.0}
        html = price_panel(F_UP, s_, None, None)
        self.assertLess(html.index("如果现在买入"), html.index("回调买入"))
        self.assertIn("不是经过回放检验的规则信号", html)
        self.assertNotIn("系统跟踪", html)

    def test_price_panel_with_a_rule_signal_above_its_zone(self):
        from scout.web.views import price_panel

        opp = {**self.OPP, "entry_lo": 73.81, "entry_hi": 77.35, "stop": 70.85, "target1": 92.46,
               "primary_detector": "pullback_in_uptrend"}
        f = {"close": 80.55, "atr14": 2.9, "sma20": 79.0, "sma50": 76.0, "sma200": 65.0}
        html = price_panel(f, {"support": 75.65, "resistance": 82.78}, None, None, opp=opp, today=date(2026, 10, 27),
                           tracking_html="<p>tracking</p>")
        self.assertIn("现价高于原区间上沿 4.1%，此时买入已不属于规则入场", html)
        self.assertIn("系统跟踪", html)


class BuyNowTests(unittest.TestCase):
    def test_structural_stop_outside_the_noise(self):
        now = levels.buy_now(F_UP, {"support": 96.0, "resistance": 112.0, "last_swing_low": {"price": 95.0}})
        self.assertEqual(now["entry"], 100.0)
        self.assertEqual(now["stop"], 96.5)                      # 20-day line 97 − 0.25 ATR: tightest valid level
        self.assertEqual(now["stop_basis"], "20 日线下方")
        self.assertEqual(now["target1"], 107.0)                  # resistance beyond 2R → 2R
        self.assertEqual(now["level"], "ok")

    def test_close_resistance_makes_buying_now_poor(self):
        now = levels.buy_now(F_UP, {"support": 96.0, "resistance": 101.5})
        self.assertEqual((now["target1"], now["target1_basis"], now["level"]), (101.5, "阻力位", "bad"))
        self.assertIn("太近", now["verdict"])

    def test_no_structure_falls_back_to_two_atr(self):
        f = {"close": 100.0, "atr14": 2.0, "sma200": 80.0, "sma50": 90.0}
        now = levels.buy_now(f, {})
        self.assertEqual(now["stop"], 96.0)
        self.assertIn("ATR", now["stop_basis"])

    def test_downtrend_is_flagged(self):
        f = {"close": 50.0, "atr14": 1.5, "sma20": 52.0, "sma50": 55.0, "sma200": 60.0}
        self.assertEqual(levels.buy_now(f, {"support": 47.0})["level"], "bad")


if __name__ == "__main__":
    unittest.main()
