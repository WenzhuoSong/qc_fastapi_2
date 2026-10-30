import unittest
import warnings

import numpy as np
import pandas as pd

from scout.ml import stage5


class Stage5FeatureTests(unittest.TestCase):
    def test_own_history_uses_only_earlier_samples(self):
        dates = pd.date_range("2005-01-03", periods=40, freq="7D")
        raw = pd.DataFrame({"date": np.tile(dates, 2), "ticker": ["A"] * 40 + ["B"] * 40,
                            "x": np.r_[np.arange(40.0), np.ones(40)]})
        z = stage5.own_history(raw, ["x"])["x_own"]
        a = z[raw["ticker"] == "A"].to_numpy()
        self.assertTrue(np.isnan(a[:stage5.OWN_MIN]).all())
        prev = np.arange(20.0)
        self.assertAlmostEqual(a[20], (20 - prev.mean()) / prev.std(ddof=1))
        raw.loc[39, "x"] = 1e6                                # a later value never changes earlier outputs
        self.assertAlmostEqual(stage5.own_history(raw, ["x"])["x_own"].iloc[30], z.iloc[30])

    def test_industry_relative_needs_enough_members(self):
        raw = pd.DataFrame({"date": pd.Timestamp("2005-01-03"), "ticker": list("abcdefg"),
                            "industry": ["I"] * 5 + ["J"] * 2})
        for c in stage5.INDUSTRY_REL:
            raw[c] = np.arange(7.0)
        r = stage5.industry_relative(raw)["bm_indrel"]
        self.assertEqual(list(r[:5]), [-2.0, -1.0, 0.0, 1.0, 2.0])
        self.assertTrue(r[5:].isna().all())

    def test_future_vol_starts_after_the_sample_day(self):
        class P:
            closeadj = np.exp(np.cumsum(np.r_[0.0, np.tile([0.01, -0.01], 30)]))[:, None]
        v = stage5.future_vol(P, np.array([0, 50]), np.array([0]))
        self.assertAlmostEqual(v[0], np.std(np.tile([0.01, -0.01], 11)[:21]))
        self.assertTrue(np.isnan(v[1]))


class ShrinkTests(unittest.TestCase):
    def setUp(self):
        rng = np.random.default_rng(1)
        self.Z = rng.normal(size=(3000, 3))
        self.sec = rng.integers(0, 3, 3000)
        w = np.array([[1.0, 0, 0], [0, 1.0, 0], [0, 0, 1.0]])[self.sec]
        self.y = (self.Z * w).sum(1) + rng.normal(0, 0.1, 3000)

    def test_huge_lambda_is_the_pooled_ridge(self):
        c = stage5.shrink_fit(self.Z, self.y, self.sec, 3, 10.0, 1e12)
        pooled = np.linalg.solve(self.Z.T @ self.Z + 10.0 * np.eye(3), self.Z.T @ self.y)
        np.testing.assert_allclose(c[:3], pooled, rtol=1e-6)
        np.testing.assert_allclose(c[3:], 0, atol=1e-6)

    def test_small_lambda_recovers_sector_coefficients(self):
        c = stage5.shrink_fit(self.Z, self.y, self.sec, 3, 1.0, 1e-3)
        p = stage5.shrink_predict(c, self.Z, self.sec)
        self.assertGreater(np.corrcoef(p, self.y)[0, 1], 0.99)


class DesignTests(unittest.TestCase):
    def frame(self, n=60):
        from scout.ml import features as F
        warnings.simplefilter("ignore", pd.errors.PerformanceWarning)
        rng = np.random.default_rng(0)
        df = pd.DataFrame({"date": pd.Timestamp("2005-01-03"), "sector": rng.choice(["A", "B"], n),
                           "industry": rng.choice(["i", "j", "k"], n)})
        cs = F.PRICE + F.FUND + F.EVENTS
        for c in cs:
            df[c] = rng.uniform(-1, 1, n)
            df[f"{c}_na"] = 0
        for c in ["mom_12_1", "bm", "gp_assets", "vol_63"]:
            df[f"{c}_secn"] = rng.uniform(-1, 1, n)
        for c in F.TS_SOURCES:
            df[f"{c}_chg"] = 0.0
        for c in ["spy_ret_21", "spy_ret_63", "spy_ret_252", "spy_vol_21", "vix", "term_spread", "breadth", "sector_rel_63"]:
            df[f"ctx_{c}"] = 0.5
        for c in [*(f"disp_{d}" for d in stage5.DISPERSION), *stage5.MACRO]:
            df[f"{stage5.NEW_CTX}{c}"] = 0.1
        df[F.SIGNALS] = 0
        df["regime"], df["vol_state"], df["size_bucket"] = 2, 1, 5
        for c in stage5.PEER:
            df[f"{c}_peer"] = 0.0
        for c in stage5.INDUSTRY_REL:
            df[f"{c}_indrel"], df[f"{c}_indrel_na"] = 0.0, 0
        for c in stage5.OWN:
            df[f"{c}_own"], df[f"{c}_own_na"] = 0.0, 0
        for c in ["sec_mom_med", "sec_above200", "sec_bm_med", "sec_sales_med", "sec_ear_med", "sec_ret63_std"]:
            df[c] = 0.0
        return df

    def test_groups_add_their_columns_and_ctx_x_replaces_hand_crosses(self):
        df = self.frame()
        _, base = stage5.B.design(df)
        _, full = stage5.design(df, stage5.GROUPS)
        self.assertNotIn("mom_x_vix", full)
        crosses = [c for c in full if "_x_" in c]
        self.assertEqual(len(crosses), 16 * 4)
        self.assertEqual(len(set(full)), len(full))
        self.assertFalse(any(c.startswith(stage5.NEW_CTX) for c in base))   # new context only via crosses
        _, own = stage5.design(df, ("own",))
        self.assertEqual(len(own) - len(base), 2 * len(stage5.OWN))

    def test_network_inputs_and_masked_loss(self):
        import torch
        torch.set_num_threads(1)
        df = self.frame()
        num = stage5.numeric_columns(df)
        self.assertEqual(len(set(num)), len(num))
        self.assertFalse(any("_x_" in c or c.startswith("sector=") for c in num))
        C = stage5.category_codes(df)
        net = stage5.make_net(len(num), stage5.category_sizes(df), 3)
        out = net(torch.from_numpy(df[num].to_numpy(np.float32)), torch.from_numpy(C))
        self.assertEqual(tuple(out.shape), (len(df), 3))
        t = torch.tensor([[1.0, float("nan")], [0.0, 2.0]])
        loss = stage5.masked_mse(torch.zeros(2, 2), t)
        self.assertAlmostEqual(float(loss), (1 + 0 + 4) / 3)


if __name__ == "__main__":
    unittest.main()
