import unittest

import numpy as np

from scout import gate, rankers


def _arrays(n_days=900, n=50, seed=3):
    rng = np.random.default_rng(seed)
    drift = np.linspace(-0.001, 0.002, n)                       # stock k has a known trend
    r = drift + rng.normal(0, 0.01, size=(n_days, n))
    close = 100 * np.cumprod(1 + r, axis=0)
    a = {"close": close, "open": close * 0.999, "high": close * 1.01, "low": close * 0.99,
         "rs_pct": np.tile(np.linspace(0, 1, n), (n_days, 1)), "liquid": np.ones((n_days, n), dtype=bool),
         "spy": 100 * np.cumprod(1 + rng.normal(0.0003, 0.008, n_days)), "spy_open": None, "angle": {}}
    return a


class RankerTests(unittest.TestCase):
    def test_every_spec_names_a_ranker(self):
        for path in gate.SPECS.glob("rank_*.yaml"):
            spec = gate.load_spec(path)
            self.assertEqual(gate.validate_spec(spec), [], path.name)
            self.assertIn(spec["ranker"], rankers.RANKERS)

    def test_rankers_use_no_future_data(self):
        a = _arrays()
        i = 800
        for name, fn in rankers.RANKERS.items():
            base = fn(a, i, 0.1)
            future = {k: (v.copy() if isinstance(v, np.ndarray) else v) for k, v in a.items()}
            for k in ("close", "open", "high", "low"):
                future[k][i + 1:] *= 5                          # anything after day i must not matter
            self.assertEqual(base, fn(future, i, 0.1), name)

    def test_strong_trends_are_picked(self):
        a = _arrays()
        for name in ("high_52w", "frog_in_pan", "residual_momentum"):  # price rankers; angle rankers are tested in test_scout_angles
            picks = rankers.RANKERS[name](a, 800, 0.1)
            self.assertEqual(len(picks), 5, name)
            self.assertGreater(np.mean(picks), 25, name)       # mostly from the upward-drifting half

    def test_trend_template_needs_strength(self):
        a = _arrays()
        a["rs_pct"][:] = 0.5                                    # nobody in the top 30 %
        self.assertEqual(rankers.trend_template(a, 800, 0.1), [])

    def test_residual_momentum_needs_three_years(self):
        self.assertEqual(rankers.residual_momentum(_arrays(), 36 * rankers.MONTH - 1, 0.1), [])
