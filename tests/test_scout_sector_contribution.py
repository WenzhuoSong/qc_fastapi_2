import unittest

import numpy as np

from scout import sector_contribution as sc


class TargetTests(unittest.TestCase):
    def test_top_three_contributions_weighted_by_contribution(self):
        w = {"XLK": 0.30, "XLF": 0.13, "XLE": 0.04, "XLV": 0.12}
        r = {"XLK": 0.10, "XLF": 0.05, "XLE": 0.30, "XLV": 0.02}       # contributions 0.030, 0.0065, 0.012, 0.0024
        t = sc.targets(w, r)
        self.assertEqual(set(t), {"XLK", "XLE", "XLF"})
        self.assertAlmostEqual(sum(t.values()), 1.0)
        self.assertAlmostEqual(t["XLK"], 0.030 / 0.0485)

    def test_falling_sectors_are_not_held_and_the_rest_is_cash(self):
        w = {"XLK": 0.30, "XLF": 0.13, "XLE": 0.04}
        r = {"XLK": 0.10, "XLF": -0.05, "XLE": -0.02}
        t = sc.targets(w, r)
        self.assertEqual(list(t), ["XLK"])
        self.assertAlmostEqual(t["XLK"], 0.030 / (0.030 + 0.0065 + 0.0008))
        self.assertEqual(sc.targets(w, {k: -0.1 for k in w}), {})

    def test_random_control_weights_by_market_weight(self):
        w = {"XLK": 0.3, "XLF": 0.1, "XLE": 0.1, "XLV": 0.5}
        t = sc.random_targets(np.random.default_rng(0), w, list(w))
        self.assertEqual(len(t), 3)
        self.assertAlmostEqual(sum(t.values()), 1.0)


if __name__ == "__main__":
    unittest.main()
