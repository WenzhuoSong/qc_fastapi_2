import unittest

import numpy as np
import pandas as pd

from scout import knowledge, sectors
from scout.features import build_panel


def _panel(n=260, seed=3):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2025-06-02", periods=n).date
    specs = {"SPY": 0.0005, "XLE": 0.002, "XLK": -0.001, "AAA": 0.002, "BBB": 0.0015, "CCC": -0.001, "DDD": -0.002}
    bars = {}
    for t, drift in specs.items():
        close = 100 * np.exp(np.cumsum(rng.normal(drift, 0.01, n)))
        bars[t] = pd.DataFrame({"open": close, "high": close * 1.01, "low": close * 0.99, "close": close,
                                "volume": rng.integers(2_000_000, 3_000_000, n).astype(float)}, index=dates)
    inst = {"SPY": {"kind": "etf"}, "XLE": {"kind": "etf"}, "XLK": {"kind": "etf"},
            "AAA": {"kind": "stock", "sector": "Energy"}, "BBB": {"kind": "stock", "sector": "Energy"},
            "CCC": {"kind": "stock", "sector": "Information Technology"},
            "DDD": {"kind": "stock", "sector": "Information Technology"}}
    return build_panel(bars, inst)


class SectorTests(unittest.TestCase):
    def test_quadrants(self):
        self.assertEqual(sectors.quadrant(103, 102), "leading")
        self.assertEqual(sectors.quadrant(103, 98), "weakening")
        self.assertEqual(sectors.quadrant(97, 98), "lagging")
        self.assertEqual(sectors.quadrant(97, 101), "improving")

    def test_compute_ranks_strong_sector_hotter(self):
        p = _panel()
        rows = {r["key"]: r for r in sectors.compute(p, p.dates[-1]) if r["kind"] == "sector"}
        self.assertEqual(set(rows), {"XLE", "XLK"})
        self.assertGreater(rows["XLE"]["heat"], rows["XLK"]["heat"])
        self.assertGreater(rows["XLE"]["rel_20"], rows["XLK"]["rel_20"])
        for r in rows.values():
            self.assertEqual(r["members"], 2)
            self.assertTrue(0 <= r["pct_above_50"] <= 1)
            self.assertEqual(len(r["tail"]), sectors.TAIL_POINTS)
            self.assertIn(r["heat_label"], ("热", "温", "冷"))
            self.assertTrue(-1 <= r["cmf_20"] <= 1)

    def test_sector_metrics_are_in_the_dictionary(self):
        for key in ("rel_20", "rs_ratio", "rs_momentum", "pct_above_50", "net_new_highs", "cmf_20",
                    "rel_dollar_vol", "dollar_vol_share", "heat", "updown_vol_20"):
            self.assertIn(key, knowledge.features(), key)
        self.assertEqual(knowledge.fmt("cmf_20", 0.123), "+0.12")
        self.assertEqual(knowledge.fmt("rs_ratio", 103.04), "103.0")


if __name__ == "__main__":
    unittest.main()


class MarketFocusValidationTests(unittest.TestCase):
    def _pack(self):
        sec = lambda k, name, heat, flow, att: {"key": k, "name": name, "heat_label": heat, "flow_label": flow,
                                                "attention_label": att, "metrics": [], "quadrant": {"数值": "领先", "上周": "领先"}}
        return {"data_date": "2026-10-23", "market": {"key": "market.regime", "大盘状态": "上升趋势"},
                "sectors": [sec("XLE", "能源", "热", "流入", "正常"), sec("XLK", "信息技术", "冷", "信号分歧", "下降"),
                            sec("XLC", "通信服务", "冷", "信号分歧", "基线不足")],
                "themes": [], "market_news": [{"key": "wrap_1", "headline": "x"}]}

    def _focus(self, theme_text, theme_sectors, stance_xle="热"):
        from scout.market_focus import MarketFocus, SectorView, Theme

        views = [SectorView(sector="XLE", stance=stance_xle, view="领先", evidence=["XLE.quadrant"]),
                 SectorView(sector="XLK", stance="冷", view="落后", evidence=["XLK.quadrant"]),
                 SectorView(sector="XLC", stance="冷", view="落后", evidence=["XLC.quadrant"])]
        themes = [Theme(title="a", summary=theme_text, sectors=theme_sectors, evidence=["wrap_1"]),
                  Theme(title="b", summary="能源资金流入", sectors=["XLE"], evidence=["XLE.quadrant"])]
        return MarketFocus(headline="h", regime_view="r", themes=themes, sectors=views, watch=["w"], risks=["x"],
                           confidence="medium", confidence_reason="c")

    def test_clean_passes(self):
        from scout.market_focus import validate

        self.assertEqual(validate(self._focus("科技资金流向信号分歧", ["XLK"]), self._pack()), [])

    def test_flow_and_attention_misreads_are_caught_even_if_sector_not_listed(self):
        from scout.market_focus import validate

        issues = "\n".join(validate(self._focus("信息技术和通信服务资金流出，新闻关注度下降", ["XLK"]), self._pack()))
        self.assertIn("XLK 的资金流向", issues)
        self.assertIn("XLC 的新闻关注度", issues)       # XLC only named in the prose

    def test_heat_label_must_match_rule(self):
        from scout.market_focus import validate

        self.assertTrue(any("板块热度" in i for i in validate(self._focus("x", ["XLK"], stance_xle="冷"), self._pack())))

    def test_labels(self):
        self.assertEqual(sectors.flow_label(0.10, 1.4), "流入")
        self.assertEqual(sectors.flow_label(-0.05, 0.8), "流出")
        self.assertEqual(sectors.flow_label(0.03, 0.88), "信号分歧")
        self.assertEqual(sectors.attention_label(None), "基线不足")
        self.assertEqual(sectors.attention_label(2.0), "上升")


class SectorDisplayTests(unittest.TestCase):
    ROWS = [
        {"kind": "sector", "key": "XLE", "name": "能源", "heat": 0.82, "heat_label": "热", "quadrant": "leading",
         "quadrant_prev": "leading", "tail": [[103, 104, "2026-10-16"], [109, 106, "2026-10-23"]],
         "flow_label": "流入", "pct_above_50": 0.79, "rel_20": 0.13},
        {"kind": "sector", "key": "XLK", "name": "信息技术", "heat": 0.21, "heat_label": "冷", "quadrant": "lagging",
         "quadrant_prev": "weakening", "tail": [[101, 99, "2026-10-16"], [97, 94, "2026-10-23"]],
         "flow_label": "信号分歧", "pct_above_50": 0.46, "rel_20": -0.05},
    ]

    def test_rotation_line_names_hot_cold_and_moves(self):
        line = sectors.rotation_line(self.ROWS)
        self.assertIn("🔥能源", line)
        self.assertIn("🧊信息技术", line)
        self.assertIn("信息技术 转弱→落后", line)

    def test_card_context_and_text(self):
        from scout.telegram import sector_text

        ctx = sectors.card_context(self.ROWS[1])
        self.assertEqual((ctx["热度"], ctx["轮动象限"], ctx["资金流向"]), ("冷", "落后", "信号分歧"))
        self.assertIn("转弱→落后", sector_text(ctx))

    def test_rrg_svg_renders_every_sector(self):
        from scout.web.views import rrg_svg

        svg = rrg_svg(self.ROWS)
        self.assertTrue(svg.startswith("<svg"))
        self.assertIn(">XLE<", svg)
        self.assertIn(">XLK<", svg)
        self.assertIn("marker-end='url(#rrg-leading)'", svg)
        self.assertIn("class='tail'", svg)

    def test_rotation_reads_explain_direction_and_changes_first(self):
        reads = sectors.rotation_reads(self.ROWS)
        self.assertEqual([x["key"] for x in reads], ["XLK", "XLE"])
        self.assertEqual(reads[0]["text"], "落后：本周由转弱转入。弱于大盘且在走弱，仍在向左下走弱")
        self.assertEqual(reads[1]["text"], "领先：强于大盘且在加强，继续向右上加强")
        fading = {**self.ROWS[0], "tail": [[103, 104, "a"], [102, 102.5, "b"]]}
        self.assertIn("动量回落，留意转入转弱", sectors.rotation_read(fading)["text"])
        near = {**self.ROWS[0], "tail": [[100.2, 100.1, "a"], [100.5, 100.3, "b"]]}
        self.assertTrue(sectors.rotation_read(near)["near_center"])
        self.assertIsNone(sectors.rotation_read({"key": "X", "tail": []}))
        sideways = {**self.ROWS[0], "quadrant": "weakening", "tail": [[103, 99, "a"], [104, 98, "b"]]}
        self.assertIn("相对强度回升、动量减弱", sectors.rotation_read(sideways)["text"])

    def test_rrg_panel_has_controls_reads_and_guide(self):
        from scout.web.views import rrg_panel

        html = rrg_panel(self.ROWS)
        self.assertIn("data-rrg='XLK'", html)
        self.assertIn("显示全部轨迹", html)
        self.assertIn("怎么读这张图", html)
        self.assertLess(html.index("信息技术</a>"), html.index("能源</a>"))
