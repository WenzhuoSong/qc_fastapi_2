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


if __name__ == "__main__":
    unittest.main()
