import unittest
from datetime import date

from scout import levels

F_UP = {"close": 100.0, "atr14": 2.0, "sma20": 97.0, "sma50": 94.0, "sma200": 85.0}


S_UP = {"resistances": [{"price": 104.0}, {"price": 108.0}], "supports": [{"price": 96.0}, {"price": 90.0}]}
F_DOWN = {"close": 50.0, "atr14": 1.5, "sma20": 52.0, "sma50": 55.0, "sma200": 60.0}


class PriceMapTests(unittest.TestCase):
    def test_levels_pivot_and_entries(self):
        pm = levels.price_map({**F_UP, "dist_52w_high": -0.1}, S_UP)
        self.assertEqual([p["label"] for p in pm["resistances"]], ["阻力 1", "阻力 2", "52 周高点"])
        self.assertEqual([p["label"] for p in pm["supports"]][:3], ["20 日线", "支撑 1", "50 日线"])
        self.assertEqual(pm["pivot"], {"price": 96.0, "label": "支撑 1", "side": "below"})
        self.assertIn("收盘跌破 96.00", pm["alternative"])
        pull, brk = pm["entries"]
        self.assertEqual((pull["name"], pull["zone_lo"], pull["zone_hi"]), ("回调买点", 97.0, 98.0))   # 20-day line + 0.5 ATR
        self.assertEqual((pull["stop"], pull["stop_basis"]), (95.5, "支撑 1 下方"))
        self.assertEqual((pull["target1"], pull["target2"], pull["target2_basis"]), (101.5, 104.0, "阻力 1"))
        self.assertEqual((brk["name"], brk["zone_lo"], brk["zone_hi"], brk["stop"]), ("突破买点", 104.0, 105.0, 102.0))
        self.assertEqual((brk["target1"], brk["target1_basis"]), (108.0, "阻力 2"))
        self.assertEqual((pull["time_stop_days"], brk["time_stop_days"]), (10, 20))

    def test_zone_is_capped_at_five_percent(self):
        pm = levels.price_map({**F_UP, "atr14": 12.0}, S_UP)
        brk = next(e for e in pm["entries"] if e["name"] == "突破买点")
        self.assertAlmostEqual(brk["zone_hi"], 104.0 * 1.05)

    def test_stop_is_at_least_one_atr_below_the_zone(self):
        for e in levels.price_map(F_UP, S_UP)["entries"]:
            self.assertLessEqual(e["stop"], (e["zone_lo"] + e["zone_hi"]) / 2 - F_UP["atr14"] + 1e-9)

    def test_downtrend_only_offers_a_reversal_trigger(self):
        pm = levels.price_map(F_DOWN, {"resistances": [{"price": 54.0}], "supports": [{"price": 47.0}]})
        self.assertEqual([e["name"] for e in pm["entries"]], ["反转确认"])
        self.assertEqual(pm["pivot"]["side"], "above")
        far = levels.price_map(F_DOWN, {"resistances": [{"price": 70.0}], "supports": [{"price": 47.0}]})
        self.assertEqual(far["entries"], [])
        self.assertEqual(far["pivot"]["label"], "50 日线")                  # nearer than the far swing high
        self.assertIn("不给做多的买点", far["note"])

    def test_reality_checks(self):
        pm = levels.price_map(F_UP, S_UP, {"quality": "ok", "expiries": {"front": {"expiry": "2026-11-06", "straddle_move": 0.05}}},
                              {"price_targets": {"low": 90, "median": 120, "mean": 118, "high": 140}})
        self.assertEqual((pm["expected_move"]["low"], pm["expected_move"]["high"]), (95.0, 105.0))
        self.assertEqual(pm["analyst_targets"]["median"], 120)
        thin = levels.price_map(F_UP, S_UP, {"quality": "thin", "expiries": {"front": {"straddle_move": 0.05}}})
        self.assertNotIn("expected_move", thin)


class StatusTests(unittest.TestCase):
    PM = levels.price_map(F_UP, S_UP)

    def states(self, price):
        return [e["state"] for e in levels.status(self.PM, price)["entries"]]

    def test_live_price_only_moves_the_status(self):
        self.assertEqual(self.states(99.0), ["waiting", "waiting"])
        self.assertEqual(self.states(97.5), ["in_zone", "waiting"])
        self.assertEqual(self.states(96.5), ["below", "waiting"])
        self.assertEqual(self.states(104.5), ["waiting", "in_zone"])
        self.assertEqual(self.states(107.0), ["waiting", "extended"])
        self.assertIn("不追", levels.status(self.PM, 107.0)["entries"][1]["text"])

    def test_pivot_break(self):
        st = levels.status(self.PM, 93.0)
        self.assertTrue(st["pivot_broken"])
        self.assertEqual(st["entries"][0]["state"], "failed")
        self.assertIn("备选情景", st["pivot_text"])

    def test_locked_plan_follows_the_fill(self):
        plan = levels.locked_plan(self.PM, 97.6, date(2026, 10, 27))
        self.assertEqual((plan["from"], plan["stop"], plan["target1"], plan["in_zone"]), ("回调买点", 95.5, 101.5, True))
        off = levels.locked_plan(self.PM, 101.0, date(2026, 10, 27))       # bought outside any zone
        self.assertEqual((off["from"], off["in_zone"]), ("按成交价", False))
        self.assertEqual((off["stop"], off["stop_basis"]), (95.5, "支撑 1 下方"))   # structure first: under the support
        self.assertEqual((off["target1"], off["target1_basis"]), (104.0, "阻力 1"))
        self.assertIsNone(levels.locked_plan({}, 100.0, date(2026, 10, 27)))


class ManageTests(unittest.TestCase):
    D = [date(2026, 10, d) for d in (26, 27, 28, 29, 30)]

    def plan(self):
        return levels.holding_plan({}, 100.0, date(2026, 10, 26))

    def test_the_stop_is_80_percent_of_the_highest_close_and_only_rises(self):
        plan = self.plan()
        self.assertEqual((plan["policy"], plan["stop"]), ("trail20", 80.0))
        stop, basis, msgs = levels.manage(plan, self.D[:3], [99.0, 110.0, 105.0], self.D[2])
        self.assertEqual(stop, 88.0)                                       # 0.8 × 110, a pullback does not lower it
        self.assertIn("110.00", basis)
        self.assertNotIn("stop_hit", plan)
        self.assertEqual(levels.manage(plan, self.D[:1], [99.0], self.D[0])[0], 79.2)   # the first close counts, not the fill

    def test_a_close_below_the_stop_is_the_signal_once(self):
        plan = self.plan()
        _, _, msgs = levels.manage(plan, self.D[:4], [100.0, 125.0, 101.0, 99.0], self.D[3])
        self.assertEqual(plan["stop_hit"], "2026-10-29")                   # 101 ≥ 100, 99 < 0.8 × 125
        self.assertIn("下一个交易日开盘卖出全部", msgs[0])
        self.assertEqual(levels.manage(plan, self.D, [100.0, 125.0, 101.0, 99.0, 95.0], self.D[4])[2], [])
        due = [st for st in levels.exit_plan(plan, 100, 100.0) if st["state"] == "due"]
        self.assertEqual((due[0]["stage"], due[0]["sell_now"]), ("回落止损触发", 100))
        self.assertIn("建议卖出余下 100 股", levels.due_advice(due[0], 99.0, 100.0))

    def test_a_missed_day_is_still_caught(self):
        plan = self.plan()
        levels.manage(plan, self.D, [100.0, 130.0, 100.0, 110.0, 112.0], self.D[4])
        self.assertEqual(plan["stop_hit"], "2026-10-28")

    def test_rerun_on_the_same_bars_changes_nothing(self):
        plan = self.plan()
        levels.manage(plan, self.D[:2], [100.0, 101.0], self.D[1])
        before = dict(plan)
        self.assertEqual(levels.manage(plan, self.D[:2], [100.0, 101.0], self.D[1])[2], [])
        self.assertEqual(plan, before)

    def test_large_stop_raises_are_pushed(self):
        plan = self.plan()
        levels.manage(plan, self.D[:1], [100.0], self.D[0])
        msgs = levels.manage(plan, self.D[:2], [100.0, 120.0], self.D[1])[2]
        self.assertIn("上移到 96.00", msgs[0])

    def test_an_earlier_plan_is_moved_to_the_trailing_stop(self):
        old = {"stop": 95.5, "stop_basis": "支撑 1 下方", "target1": 101.5, "target2": 104.0, "time_stop_days": 10,
               "target1_hit": "2026-10-27", "stop_now": 97.0, "history": [{"date": "2026-10-27", "text": "x"}]}
        new = levels.adopt_trail(old, 98.0, date(2026, 10, 30))
        self.assertEqual((new["policy"], new["stop"], new["ref"]["target1"]), ("trail20", 78.4, 101.5))
        self.assertEqual(new["previous"]["stop_now"], 97.0)
        self.assertNotIn("target1_hit", new)
        self.assertEqual(len(new["history"]), 2)
        self.assertIn("回落 20%", new["history"][-1]["text"])


class StructureStopTests(unittest.TestCase):
    LV = [{"price": 97.0, "label": "20 日线", "kind": "ma"}, {"price": 96.0, "label": "支撑 1", "kind": "swing"},
          {"price": 90.0, "label": "支撑 2", "kind": "swing"}]
    FB = (95.0, "2 个 ATR（附近没有合适的结构位）")

    def test_first_level_at_least_one_atr_away(self):
        self.assertEqual(levels.stop_for(97.5, self.LV, 2.0, self.FB), (95.5, "支撑 1 下方"))

    def test_close_level_is_widened_to_one_atr(self):
        self.assertEqual(levels.stop_for(97.5, self.LV[:1], 2.0, self.FB), (95.5, "20 日线 下方（加宽到 1 个 ATR）"))

    def test_far_level_below_the_band_is_not_used(self):
        lv = [self.LV[0], self.LV[2]]                                     # 20-day line, then a support 7 % lower
        self.assertEqual(levels.stop_for(97.5, lv, 2.0, self.FB), (95.5, "20 日线 下方（加宽到 1 个 ATR）"))

    def test_percent_cap(self):
        stop, basis = levels.stop_for(90.0, [{"price": 78.0, "label": "200 日线", "kind": "ma"}], 6.0, (84.0, "突破位下方 1 个 ATR"))
        self.assertEqual(stop, 84.0)                                     # 78 − 1.5 is 15 % away: over the 10 % cap
        self.assertIn("太远", basis)

    def test_far_structure_falls_back(self):
        stop, basis = levels.stop_for(110.0, self.LV[1:], 2.0, self.FB)
        self.assertEqual(stop, 95.0)
        self.assertIn("支撑 1 96.00 太远", basis)

    def test_structural_excludes_the_20_day_line(self):
        self.assertEqual([p["label"] for p in levels.structural(self.LV)], ["支撑 1", "支撑 2"])


class ExitPlanTests(unittest.TestCase):
    def test_one_stage_for_the_whole_position(self):
        (st,) = levels.exit_plan({"policy": "trail20", "stop": 80.0}, 67, 97.0)
        self.assertEqual((st["stage"], st["price"], st["suggest"], st["state"]), ("全部仓位", 97.0, 67, "pending"))
        self.assertIn("不设持有期限", st["why"])

    def test_holding_plan_keeps_the_map_as_reference(self):
        pm = {"asof": "2026-10-23", "atr": 2.0, "entries": [
            {"name": "回调买点", "zone_lo": 97.0, "zone_hi": 98.0, "stop": 95.0, "stop_basis": "支撑 1 下方",
             "target1": 104.0, "target1_basis": "阻力 1", "target2": 108.0, "target2_basis": "阻力 2"}]}
        plan = levels.holding_plan(pm, 97.5, date(2026, 10, 27), {"off": True})
        self.assertEqual((plan["stop"], plan["ref"]["target1"], plan["market_off_at_buy"]), (78.0, 104.0, True))
        self.assertNotIn("target1", plan)


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

    def test_price_panel_from_the_stored_map(self):
        from scout.web.views import price_panel

        pm = {**levels.price_map(F_UP, S_UP), "asof": "2026-10-26"}
        html = price_panel(pm, {"last": 97.5, "change": -0.02, "time_et": "10-27 10:15", "session": "盘中"}, None, ticker="ABC")
        self.assertIn("多空分界", html)
        self.assertIn("2026-10-26 收盘计算并保存，盘中不变", html)
        self.assertIn("/trades?t=ABC&plan=%E5%9B%9E%E8%B0%83%E4%B9%B0%E7%82%B9", html)   # only the entry in its zone links
        self.assertEqual(html.count("按此计划记录买入"), 1)
        self.assertIn("不是经过回放检验的规则信号", html)
        self.assertNotIn("系统跟踪", html)

    def test_price_panel_with_a_rule_signal_above_its_zone(self):
        from scout.web.views import price_panel

        opp = {**self.OPP, "entry_lo": 73.81, "entry_hi": 77.35, "stop": 70.85, "target1": 92.46,
               "primary_detector": "pullback_in_uptrend"}
        f = {"close": 80.55, "atr14": 2.9, "sma20": 79.0, "sma50": 76.0, "sma200": 65.0}
        pm = levels.price_map(f, {"supports": [{"price": 75.65}], "resistances": [{"price": 82.78}]})
        html = price_panel(pm, None, None, opp=opp, today=date(2026, 10, 27), tracking_html="<p>tracking</p>")
        self.assertIn("现价高于原区间上沿 4.1%，此时买入已不属于规则入场", html)
        self.assertIn("系统跟踪", html)


if __name__ == "__main__":
    unittest.main()
