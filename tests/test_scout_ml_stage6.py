import unittest

import numpy as np
import pandas as pd

from scout.ml import stage6


class Stage6OperatorTests(unittest.TestCase):
    def test_forward_fill_is_per_stock_and_limited(self):
        raw = pd.DataFrame({"ticker": ["A", "B"] * 4, "x": [1.0, 5.0, np.nan, np.nan, np.nan, 6.0, np.nan, np.nan]})
        out = stage6.forward_fill(raw, ["x"], steps=2)["x"]
        self.assertEqual(out[raw["ticker"] == "A"].tolist()[:3], [1.0, 1.0, 1.0])
        self.assertTrue(np.isnan(out.iloc[6]))                # A's third missing sample is past the limit
        self.assertEqual(out[raw["ticker"] == "B"].tolist(), [5.0, 5.0, 6.0, 6.0])

    def test_low_coverage_dates_become_zero_without_flags(self):
        dates = pd.Series(pd.to_datetime(["2005-01-03"] * 4 + ["2005-01-10"] * 4))
        v = pd.DataFrame({"x": [np.nan, np.nan, np.nan, 1.0, 1.0, 2.0, 3.0, np.nan]})
        r = stage6.ranked(v, dates, pd.Series(["S"] * 8), "6")
        self.assertTrue((r.iloc[:4] == 0).all().all())       # 25 % coverage: column, flag, sector rank all 0
        self.assertEqual(r["6n_x"].iloc[7], 1)               # 75 % coverage: a real missing flag
        self.assertEqual(r["6g_x"].iloc[4:7].tolist(), [-1.0, 0.0, 1.0])

    def test_gram_solve_matches_direct_ridge(self):
        rng = np.random.default_rng(0)
        X, t = rng.normal(2, 3, (500, 4)), rng.normal(size=500)
        acc = stage6.Gram(4)
        acc.add(X[:200], t[:200])
        acc.add(X[200:], t[200:])
        Gz, bz, mu, sd, const = acc.standardized()
        w = stage6.solve(Gz, bz, np.full(4, 10.0), const)
        Z = (X - X.mean(0)) / X.std(0)
        direct = np.linalg.solve(Z.T @ Z + 10 * np.eye(4), Z.T @ (t - t.mean()))
        np.testing.assert_allclose(w, direct, rtol=1e-8)

    def test_fast_ic_matches_spearman(self):
        rng = np.random.default_rng(1)
        d = pd.DataFrame({"date": np.repeat(np.arange(5), 40), "s": rng.normal(size=200), "y": rng.normal(size=200)})
        codes = d["date"].to_numpy()
        ry = d.groupby("date")["y"].rank().to_numpy()
        want = d.groupby("date").apply(lambda g: g["s"].rank().corr(g["y"].rank())).mean()
        self.assertAlmostEqual(stage6.fast_ic(codes, ry, d["s"].to_numpy()), want)

    def test_coordinate_search_removes_a_useless_group(self):
        groups = np.array(["a", "a", "b", "b"])
        good = {g: a for g, a in zip("ab", (1e3, stage6.OFF))}

        def score(pen):                                      # best when a is kept and b removed
            return -sum(abs(np.log10(pen[g]) - np.log10(good[g])) for g in "ab")
        pen, _ = stage6.coordinate_search(score, groups, ("a", "b"), 1e5)
        self.assertEqual(pen, good)

    def test_beta_uses_only_labels_ended_before_the_version(self):
        dates = pd.bdate_range("2002-01-01", periods=900)[::5]
        rows = []
        for k, d in enumerate(dates):
            for s in range(30):
                zg = float(s % 3 - 1)
                rows.append({"date": d, "ticker": f"T{s}", "i": 5 * k, "zs": np.sin(s + k), "zg": zg,
                             "y": zg + 0.01 * np.cos(s * k)})
        f = pd.DataFrame(rows)
        f["end_i"] = f["i"] + 21
        out = stage6.choose_beta(f, 21)
        betas = {b["month"]: b["beta"] for b in out.attrs["beta_log"]}
        self.assertEqual(betas["2002-01"], 0.0)              # no history yet
        self.assertEqual(betas["2004-01"], max(stage6.BETAS))  # the sector score is the whole label
        f.loc[f["date"] >= pd.Timestamp("2004-06-01"), "y"] = -f["zg"]   # later labels cannot change the 2004 choice
        again = {b["month"]: b["beta"] for b in stage6.choose_beta(f, 21).attrs["beta_log"]}
        self.assertEqual(again["2004-01"], betas["2004-01"])

    def test_pbo_is_low_for_a_dominant_model(self):
        rng = np.random.default_rng(2)
        m = pd.DataFrame(rng.normal(0, 0.01, (120, 4)), columns=list("abcd"))
        m["a"] += 0.05
        self.assertEqual(stage6.pbo(m)["pbo"], 0.0)


class Stage6NetworkTests(unittest.TestCase):
    def test_context_enters_only_through_the_gate(self):
        import torch
        torch.set_num_threads(1)
        net = stage6.make_net(6, 3, [4, 11], 2)
        x, c = torch.randn(5, 6), torch.zeros(5, 2, dtype=torch.long)
        q = torch.randn(5, 3)
        out = net(x, c, q)
        self.assertEqual(tuple(out.shape), (5, 2))
        net.eval()
        torch.testing.assert_close(net(x, c, q), net(x, c, torch.randn(5, 3)))   # zero-initialised gate weights


if __name__ == "__main__":
    unittest.main()
