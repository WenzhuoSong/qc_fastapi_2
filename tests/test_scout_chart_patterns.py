import unittest

import numpy as np

from scout import chart_patterns as cp


def path(points: list[tuple[int, float]]) -> np.ndarray:
    """A piecewise-linear price series of cp.W days through (day, price) points."""
    xs, ys = zip(*points)
    return np.interp(np.arange(cp.W), xs, ys)


def ext(*vals: float, first: int = 1, last: int = cp.L - 1, step: int = 6) -> list[tuple[int, int, float]]:
    n = len(vals)
    pos = [last - step * (n - 1 - k) for k in range(n)]
    return [(p, first * (-1) ** k, v) for k, (p, v) in enumerate(zip(pos, vals))]


class DefinitionTests(unittest.TestCase):
    def test_head_and_shoulders_and_its_tolerances(self):
        self.assertIn("HS", cp.classify(ext(100, 90, 110, 90.5, 100.5)))
        self.assertNotIn("HS", cp.classify(ext(100, 90, 110, 90.5, 104)))       # shoulders 4 % apart
        self.assertIn("IHS", cp.classify(ext(100, 110, 90, 109, 100.5, first=-1)))
        hs = cp.classify(ext(100, 90, 110, 90, 100))["HS"]
        self.assertEqual((hs["base"], hs["height"]), (90, 20))

    def test_broadening_triangle_rectangle(self):
        self.assertIn("BTOP", cp.classify(ext(100, 95, 102, 92, 105)))
        self.assertIn("TTOP", cp.classify(ext(105, 92, 102, 95, 100)))
        self.assertIn("TBOT", cp.classify(ext(92, 105, 95, 102, 97, first=-1)))
        self.assertIn("BBOT", cp.classify(ext(97, 102, 95, 105, 92, first=-1)))
        self.assertIn("RTOP", cp.classify(ext(100, 95, 100.3, 95.2, 99.9)))
        self.assertIn("RBOT", cp.classify(ext(95, 100, 95.2, 100.3, 94.9, first=-1)))
        self.assertNotIn("RTOP", cp.classify(ext(100, 95, 102, 95.2, 99.9)))   # a top 1.3 % off the mean

    def test_double_top_needs_22_days(self):
        far = [(4, 1, 100.0), (15, -1, 90.0), (34, 1, 100.5)]
        near = [(14, 1, 100.0), (24, -1, 90.0), (34, 1, 100.5)]
        self.assertEqual(cp.classify(far)["DTOP"], {"base": 90.0, "height": 100.25 - 90.0})
        self.assertNotIn("DTOP", cp.classify(near))

    def test_only_patterns_completed_on_day_l_minus_1(self):
        self.assertEqual(cp.classify(ext(100, 90, 110, 90.5, 100.5, last=cp.L - 2)), {})


class AlgorithmTests(unittest.TestCase):
    def test_smoothing_keeps_a_line_and_flat_runs_keep_their_sign(self):
        line = np.linspace(10, 20, cp.W)[None, :]
        np.testing.assert_allclose(cp.smooth(line)[0, 5:-5], line[0, 5:-5], rtol=0.02)
        s = cp.slope_signs(np.array([[1.0, 2.0, 2.0, 2.0, 1.0]]))
        self.assertEqual(s.tolist(), [[1.0, 1.0, 1.0, -1.0]])

    def test_detects_a_drawn_head_and_shoulders_at_the_window_end(self):
        p = path([(0, 90), (6, 100), (13, 90), (20, 110), (27, 90), (34, 100), (37, 95)])
        close = np.concatenate([np.full(10, 90.0), p])
        ok = np.ones(len(close), dtype=bool)
        hits = cp.detect(close, np.ones(len(close)), ok)
        self.assertIn(("HS", len(close) - 1), [(h["pattern"], h["i"]) for h in hits])
        self.assertEqual(cp.detect(close, np.ones(len(close)), np.zeros(len(close), dtype=bool)), [])

    def test_verdict_respects_the_book_direction(self):
        s = lambda t, a, b: {"all": {"t": t}, "first": {"edge": a}, "second": {"edge": b}}  # noqa: E731
        self.assertEqual(cp.verdict("HS", s(-3.5, -0.01, -0.02), 3.0), "pass")
        self.assertEqual(cp.verdict("HS", s(3.5, 0.01, 0.02), 3.0), "opposite")
        self.assertEqual(cp.verdict("TTOP", s(-3.5, -0.01, -0.02), 3.0), "pass")
        self.assertEqual(cp.verdict("IHS", s(3.5, 0.01, -0.02), 3.0), "fail")
        self.assertEqual(cp.verdict("IHS", s(2.9, 0.01, 0.02), 3.0), "fail")


if __name__ == "__main__":
    unittest.main()
