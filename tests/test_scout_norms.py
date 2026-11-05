import unittest

import numpy as np
import pandas as pd

from scout import norms


def _close(n=1500, drift=0.0005, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2015-01-01", periods=n).date
    return pd.Series(100 * np.exp(np.cumsum(rng.normal(drift, 0.01, n))), index=idx)


class NormsTests(unittest.TestCase):
    def test_stats_match_direct_counts(self):
        c = _close()
        s = norms.stats(c)
        r = (c.shift(-21) / c - 1).dropna()
        x = s["h"]["21"]
        self.assertAlmostEqual(x["up"], (r > 0).mean())
        self.assertAlmostEqual(x["p10"], r.quantile(0.1))
        self.assertAlmostEqual(x["drop5"], (r <= -0.05).mean())
        vol = np.log(c).diff().iloc[-63:].std() * np.sqrt(252)
        self.assertAlmostEqual(x["sigma"], vol * np.sqrt(21 / 252))
        self.assertAlmostEqual(x["hi"], c.iloc[-1] * (1 + x["sigma"]))

    def test_short_history_is_none(self):
        self.assertIsNone(norms.stats(_close(n=200)))

    def test_line_with_stop(self):
        s = norms.stats(_close())
        sig = s["h"]["21"]["sigma"]
        inside = norms.line(s, to_stop=-sig / 2)
        outside = norms.line(s, to_stop=-sig * 2)
        self.assertIn("在正常波动范围之内", inside)
        self.assertIn("超出正常波动范围", outside)
        self.assertTrue(inside.startswith("1 个月：历史上"))

    def test_history_note(self):
        s = norms.stats(_close())                               # starts 2015
        self.assertIn("没有经历 2000、2008 年那样的熊市", norms.history_note(s))
        self.assertIsNone(norms.history_note({**s, "since": "1999-01-04"}))

    def test_calm_note(self):
        self.assertIn("更窄", norms.calm_note({"vol": 0.12, "vol_long": 0.19}))
        self.assertIsNone(norms.calm_note({"vol": 0.18, "vol_long": 0.19}))

    def test_views_render(self):
        from scout.web.views import norms_block, norms_card

        s = norms.stats(_close())
        self.assertIn("历史常态", norms_block(s))
        html = norms_card({"SPY": s}, {"SPY": "S&P 500"}, 21)
        self.assertIn("S&amp;P 500", html)
        self.assertIn("不代表 Scout 认为接下来会涨", html)

    def test_agent_view(self):
        v = norms.agent_view("SPY", norms.stats(_close()))
        self.assertIn("1 个月", v["各期限"])
        self.assertIn("不是预测", v["含义"])
        self.assertIn("说明", norms.agent_view("X", None))
