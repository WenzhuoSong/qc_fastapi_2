import unittest

import numpy as np
import pandas as pd

from scout.ml import stage4, wide


def part(n, large, breadth, date="2005-01-03"):
    return pd.DataFrame({"date": pd.Timestamp(date), "ticker": [f"{'L' if large else 'S'}{k}" for k in range(n)],
                         "cap_rank": np.arange(n) + (0 if large else 1000), "breadth": breadth,
                         "sector": "Tech", "y21_raw": np.arange(n, dtype=float), "y63_raw": 0.0})


class WideTests(unittest.TestCase):
    def test_merge_recomputes_universe_dependent_columns(self):
        raw = wide.merge(part(10, True, 0.8), part(30, False, 0.4))
        self.assertAlmostEqual(raw["breadth"].iloc[0], (0.8 * 10 + 0.4 * 30) / 40)
        self.assertEqual(raw["in_large"].sum(), 10)
        self.assertAlmostEqual(raw["y21"].mean(), 0.0)
        self.assertEqual(raw.loc[raw["cap_rank"] == 0, "size_bucket"].iloc[0], 9)
        self.assertEqual(raw.loc[raw["cap_rank"] == 1029, "size_bucket"].iloc[0], 6)

    def test_residual_label_removes_factor_exposure_and_scales_by_volatility(self):
        rng = np.random.default_rng(0)
        n = 400
        df = pd.DataFrame({"date": pd.Timestamp("2005-01-03"), "sector": rng.choice(["A", "B"], n)})
        for c in wide.RES_FACTORS:
            df[c] = rng.uniform(-1, 1, n)
        noise = rng.normal(0, 0.01, n)
        df["y21_raw"] = 0.05 * df["mom_12_1"] + 0.02 * (df["sector"] == "B") + noise
        vol = pd.Series(rng.uniform(0.2, 0.6, n))
        r = wide.residual_label(df, 21, vol)
        self.assertLess(abs(np.corrcoef(r * vol, df["mom_12_1"])[0, 1]), 1e-6)
        self.assertGreater(np.corrcoef(r * vol, noise)[0, 1], 0.95)

    def test_every_combo_has_a_baseline(self):
        keys = [stage4.name(*c) for c in stage4.COMBOS]
        self.assertEqual(len(keys), 8)
        self.assertTrue(set(stage4.BASE.values()) <= set(keys))


if __name__ == "__main__":
    unittest.main()
