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
        self.assertIn("已入场", html)
        self.assertIn("模拟成交 419.28", html)
        self.assertIn("持有第 1 / 10 个交易日", html)
        self.assertIn("盘中 10:15 美东", html)
        self.assertIn("不随股价调整", html)

    def test_pending_plan_counts_down_the_window(self):
        from scout.web.views import trade_plan

        o = {**self.OPP, "status": "new", "triggered_on": None, "asof": date(2026, 10, 26)}
        html = trade_plan(o, 426.76, today=date(2026, 10, 27))
        self.assertIn("入场窗口还剩 2 个交易日", html)
        self.assertIn("最近收盘", html)

    def test_reference_card_labels_itself(self):
        from scout.web.views import reference_card

        ref = levels.reference_plans(F_UP, {"support": 96.0, "resistance": 104.0})
        html = reference_card(ref, F_UP, {"support": 96.0, "resistance": 104.0}, None)
        self.assertIn("回调买入", html)
        self.assertIn("这不是规则信号，没有历史回放统计", html)


if __name__ == "__main__":
    unittest.main()
