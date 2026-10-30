import unittest

import numpy as np
import pandas as pd

from scout.ml import gbdt


class GbdtTests(unittest.TestCase):
    def test_the_search_budget_is_fixed(self):
        a, b = gbdt.configs(), gbdt.configs()
        self.assertEqual(len(a), 12)
        self.assertEqual(a, b)
        self.assertEqual(len({tuple(sorted(c.items())) for c in a}), 12)

    def test_deciles_are_per_date(self):
        df = pd.DataFrame({"date": [1] * 10 + [2] * 10, "y": list(range(10)) + list(range(100, 110))})
        d = gbdt.decile(df, "y")
        self.assertEqual(d.tolist()[:10], d.tolist()[10:])
        self.assertEqual((d.min(), d.max()), (0, 9))

    def test_both_objectives_learn_a_planted_signal(self):
        rng = np.random.default_rng(1)
        n_dates, per = 40, 200
        dates = pd.Series(np.repeat(np.arange(n_dates), per))
        x = rng.normal(size=n_dates * per)
        y = x + rng.normal(scale=2.0, size=len(x))
        df = pd.DataFrame({"date": dates, "y": y})
        X = pd.DataFrame({"x": x.astype("float32"), "noise": rng.normal(size=len(x)).astype("float32")})
        t = df.groupby("date")["y"].rank(pct=True)
        rel = gbdt.decile(df, "y")
        for obj in gbdt.OBJECTIVES:
            m = gbdt.fit(obj, {"num_leaves": 7, "min_data_in_leaf": 50, "learning_rate": 0.1, "feature_fraction": 1.0,
                               "lambda_l2": 0.0}, X, t, rel, dates, 50, None, 2)
            self.assertGreater(np.corrcoef(m.predict(X), x)[0, 1], 0.8, obj)

    def test_2b_matrices_remove_industry_and_date_level_inputs(self):
        from scout.ml import features as F
        rng = np.random.default_rng(2)
        n = 60
        cs = F.PRICE + F.FUND + F.EVENTS
        df = pd.DataFrame({c: rng.uniform(-1, 1, n) for c in cs})
        for c in cs:
            df[f"{c}_na"] = 0
        for c in F.TS_SOURCES:
            df[f"{c}_chg"] = 0.0
        for c in F.CONTEXT:
            df[f"ctx_{c}"] = rng.normal(size=n)
        df["breakout_retest_10d"] = 0
        df["size_bucket"], df["regime"], df["vol_state"] = 3, rng.integers(0, 3, n), rng.integers(0, 2, n)
        df["sector"], df["industry"] = "Technology", "Software"
        b1 = gbdt.matrix_2b(df, gbdt.VARIANTS["b1_no_industry"])
        self.assertNotIn("industry", b1.columns)
        self.assertIn("ctx_vix", b1.columns)
        b2 = gbdt.matrix_2b(df, gbdt.VARIANTS["b2_ctx_interact"])
        self.assertFalse([c for c in b2.columns if "_x_" not in c and (c.startswith("ctx_") or c in ("regime", "vol_state"))])
        inter = [c for c in b2.columns if "_x_" in c]
        self.assertEqual(len(inter), 40)
        np.testing.assert_allclose(b2["ctx_vix_x_bm"], df["ctx_vix"] * df["bm"], rtol=1e-5)
        np.testing.assert_allclose(b2["regime_x_mom_12_1"], (df["regime"] - 1) * df["mom_12_1"], rtol=1e-5)
        self.assertIn("sector", b2.columns)

    def test_2b_spaces_change_only_where_registered(self):
        self.assertEqual(gbdt.configs(gbdt.VARIANTS["b1_no_industry"]["space"]), gbdt.configs())
        big = gbdt.configs(gbdt.SPACE_2B)
        self.assertEqual(len(big), 12)
        self.assertTrue(all(c["min_data_in_leaf"] >= 5000 for c in big))


if __name__ == "__main__":
    unittest.main()
