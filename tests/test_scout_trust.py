import unittest

from scout import base_rates, knowledge
from scout.detectors import registry
from scout.opportunities import short_trend, state_label, state_text


class RuleStatusTests(unittest.TestCase):
    def test_live_rules_are_the_ones_that_beat_holding(self):
        live = {d.name for d in registry(include_candidates=False)}
        self.assertEqual(live, {"pullback_in_uptrend", "power_gap", "volume_shock"})

    def test_rule_health_flags_both_directions(self):
        rates = {
            "volume_shock": {"all": {"n": 180, "avg_edge_vs_hold": -0.002}},
            "sector_rotation": {"all": {"n": 1708, "avg_edge_vs_hold": 0.004}},
            "pullback_in_uptrend": {"all": {"n": 50, "avg_edge_vs_hold": -0.01}},      # too few to judge
        }
        lines = "\n".join(base_rates.rule_health(rates))
        self.assertIn("放量异动：扣费后跑输持有 -0.2%", lines)
        self.assertIn("板块轮动龙头（候选）：扣费后跑赢持有 +0.4%", lines)
        self.assertNotIn("强势回调", lines)


class MarketStateTests(unittest.TestCase):
    def test_short_trend(self):
        self.assertEqual(short_trend(105, 100, 98, 0.02), "短期向上")
        self.assertEqual(short_trend(99, 100, 97, -0.01), "短期回调")
        self.assertEqual(short_trend(95, 100, 97, -0.05), "短期走弱")

    def test_label_reports_pullback_breadth_and_growth_gap(self):
        st = state_label("uptrend", "短期回调", 0.524, -0.0391)
        self.assertEqual(st["label"], "长期上升，短期回调")
        self.assertIn("宽度一般（52% 站上 50 日线）", st["detail"])
        self.assertIn("科技成长明显落后大盘（QQQ 相对 SPY 20 日 -3.9%）", st["detail"])
        self.assertNotIn("科技", state_label("uptrend", "短期向上", 0.7, 0.01)["detail"])

    def test_old_briefs_fall_back_to_regime(self):
        self.assertEqual(state_text({"regime": "uptrend"}), "长期上升")
        self.assertEqual(state_text({"regime": "uptrend", "state": state_label("uptrend", "短期回调", None, None)}),
                         "长期上升，短期回调")


class HumanizeTests(unittest.TestCase):
    def test_citations_in_brackets_are_dropped(self):
        self.assertEqual(knowledge.humanize("符合上升趋势（swing_trend）。"), "符合上升趋势。")
        self.assertEqual(knowledge.humanize("多头排列（sma50, sma200）。"), "多头排列。")

    def test_bare_keys_get_chinese_names(self):
        self.assertIn("扣费后相对持有", knowledge.humanize("avg_edge_vs_hold 为 -1.4%"))
        self.assertIn(knowledge.zh("rsi14"), knowledge.humanize("rsi14 偏低"))

    def test_leaves_ordinary_text_and_citation_fields_alone(self):
        self.assertEqual(knowledge.humanize("RSI(14) 为 38，S&P 500 与 SPY"), "RSI(14) 为 38，S&P 500 与 SPY")
        self.assertEqual(knowledge.humanize("（unknown_field）"), "（unknown_field）")
        out = knowledge.humanize_obj({"points": ["趋势向上（swing_trend）"], "evidence": ["swing_trend"]})
        self.assertEqual(out, {"points": ["趋势向上"], "evidence": ["swing_trend"]})


class CardTests(unittest.TestCase):
    OPP = {"ticker": "PBF", "primary_detector": "sector_rotation", "status": "new", "score": 0.5, "asof": "2026-10-23",
           "horizon_days": 10, "risk_reward": 3.6, "entry_lo": 73.81, "entry_hi": 77.35, "stop": 70.85,
           "target1": 92.46, "target2": 92.46, "detectors": ["sector_rotation"], "evidence": {}, "thesis": {}, "base_rate": {}}

    def test_equal_targets_show_once_and_demoted_rules_are_marked(self):
        from scout.telegram import render_card
        from scout.web.views import opp_card

        html = opp_card(self.OPP)
        self.assertIn("<span>目标 2</span>—", html)
        self.assertIn("已降为候选", html)
        text = render_card(self.OPP)
        self.assertNotIn("92.46 / 92.46", text)
        self.assertIn("已降为候选", text)


if __name__ == "__main__":
    unittest.main()
