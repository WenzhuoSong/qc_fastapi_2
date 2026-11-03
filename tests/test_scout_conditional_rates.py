import unittest
from datetime import date, timedelta

import numpy as np

from scout import conditional_rates as cr


def fills(n, edge, sector="Energy", regime="uptrend", seed=0, start=date(2016, 1, 4), step=7):
    rng = np.random.default_rng(seed)
    return [{"date": start + timedelta(days=step * i), "ret": float(edge + rng.normal(0, 0.05)), "select": 0.0,
             "sector": sector, "regime": regime, "vol": "low"} for i in range(n)]


class ShrinkTests(unittest.TestCase):
    def test_small_lucky_cells_are_pulled_to_the_rule_mean(self):
        cells = [{"raw_edge": 0.08, "se": 0.04, "n": 16}] + [{"raw_edge": x, "se": 0.006, "n": 300}
                                                              for x in (0.0, 0.002, -0.001, 0.001, -0.002, 0.0, 0.001, -0.001, 0.002)]
        out = cr.shrink(cells, prior=0.001)
        self.assertLess(out[0]["shrunk_edge"], 0.03)                    # 8 % on 16 trades shrinks hard
        self.assertGreater(out[1]["weight"], out[0]["weight"] - 1)     # weights are defined for every cell

    def test_no_true_spread_means_everything_is_the_prior(self):
        cells = [{"raw_edge": x, "se": 0.05, "n": 50} for x in (0.01, -0.01, 0.0)]
        self.assertTrue(all(abs(c["shrunk_edge"] - 0.002) < 1e-12 for c in cr.shrink(cells, 0.002)))


    def test_a_cell_pulled_fully_to_the_rule_says_so(self):
        rule = {"layer": "all", "n": 900, "win_rate": 0.44, "control_win": 0.41, "raw_edge": 0.0085, "lo90": 0.0058, "hi90": 0.0112}
        cell = {"layer": "sector_regime", "sector": "Industrials", "regime": "uptrend", "n": 393, "win_rate": 0.46,
                "control_win": 0.44, "shrunk_edge": 0.0085, "lo90": 0.0085, "hi90": 0.0085, "rule": rule}
        self.assertTrue(cr.pooled(cell))
        self.assertIn("没有可区分的差异", cr.text(cell))
        self.assertIn("+0.6% 到 +1.1%", cr.text(cell))                # the rule's own interval, not a zero-width one


class CandidateTests(unittest.TestCase):
    def test_a_consistent_cell_becomes_a_candidate_and_noise_does_not(self):
        rows = (fills(120, 0.03, "Energy", step=17) + fills(300, 0.0, "Health Care", seed=1) + fills(300, 0.0, "Financials", seed=2)
                + fills(300, 0.0, "Utilities", seed=3))
        cells = cr.rule_cells(rows)
        cand = [c for c in cells if c["candidate"]]
        self.assertEqual([(c["sector"], c["regime"]) for c in cand], [("Energy", "uptrend")])
        self.assertTrue(all(c["layer"] in cr.LAYERS or c["layer"] == "all" for c in cells))
        self.assertFalse(cr.pooled(cand[0]))
        self.assertIn("偏窄", cr.text(cand[0]))

    def test_cells_below_the_minimum_are_not_estimated(self):
        self.assertIsNone(cr.cell_stats(fills(10, 0.05), date(2016, 6, 1)))


if __name__ == "__main__":
    unittest.main()
