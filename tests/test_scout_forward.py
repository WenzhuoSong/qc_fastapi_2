import unittest
from datetime import date, timedelta

from scout import forward, gate


def _sig(ticker, d, ret, state="hit_target"):
    return {"role": "signal", "ticker": ticker, "asof": d, "draw": 0, "state": state, "ret": ret, "spy_ret": 0.005}


def _leg(ticker, d, draw, ret, state="stopped"):
    return {"role": "select", "ticker": ticker, "asof": d, "draw": draw, "state": state, "ret": ret}


class PairTests(unittest.TestCase):
    def test_control_is_the_mean_of_the_first_draws_that_filled(self):
        d = date(2026, 11, 2)
        rows = [_sig("AAA", d, 0.03), _leg("AAA", d, 1, None, "untriggered"), _leg("AAA", d, 2, 0.01),
                _leg("AAA", d, 3, -0.02), _leg("AAA", d, 4, 0.04), _leg("AAA", d, 5, 0.50)]
        (p,) = forward.pairs(rows)
        self.assertAlmostEqual(p["select"], (0.01 - 0.02 + 0.04) / 3)       # draw 5 is past the first DRAWS fills
        self.assertAlmostEqual(p["diff"], 0.03 - 0.01)

    def test_incomplete_signals_wait(self):
        d = date(2026, 11, 2)
        self.assertEqual(forward.pairs([_sig("A", d, None, "open"), _leg("A", d, 1, 0.01)]), [])
        self.assertEqual(forward.pairs([_sig("A", d, 0.02), _leg("A", d, 1, None, "pending")]), [])
        self.assertEqual(forward.pairs([_sig("A", d, 0.02), _leg("A", d, 1, None, "open")]), [])
        self.assertEqual(forward.pairs([_sig("A", d, None, "untriggered")]), [])


class EvaluateTests(unittest.TestCase):
    def _complete(self, months, per_month, diff):
        start = date(2026, 11, 2)
        return [{"date": start + timedelta(days=30 * m + k), "diff": diff + (0.006 if m % 2 else -0.006)}   # month-to-month noise
                for m in range(months) for k in range(per_month)]

    def test_ready_only_when_every_bar_is_met(self):
        start = date(2026, 11, 2)
        done = forward.evaluate(self._complete(7, 5, 0.008), start, start + timedelta(days=220), 0.0083)
        self.assertTrue(done["ready"], done["criteria"])
        early = forward.evaluate(self._complete(3, 5, 0.008), start, start + timedelta(days=95), 0.0083)
        self.assertFalse(early["ready"])
        self.assertEqual([c["ok"] for c in early["criteria"]][:2], [False, False])

    def test_far_below_the_backtest_is_not_ready(self):
        start = date(2026, 11, 2)
        rep = forward.evaluate(self._complete(8, 5, 0.001), start, start + timedelta(days=250), 0.0083)
        self.assertTrue(rep["criteria"][2]["ok"])
        self.assertFalse(rep["criteria"][3]["ok"])
        self.assertFalse(rep["ready"])

    def test_thresholds_are_the_agreed_ones(self):
        self.assertEqual((gate.FORWARD_MIN_MONTHS, gate.FORWARD_MIN_SAMPLES, forward.MIN_T_VS_BACKTEST), (6, 30, -2.0))


class SelectionTests(unittest.TestCase):
    def test_only_specced_select_entries_under_test(self):
        entries = [{"key": "a", "status": "backtested", "spec": {"primary_control": "select"}},
                   {"key": "b", "status": "forward", "spec": {"primary_control": "select"}},
                   {"key": "c", "status": "backtested"},
                   {"key": "d", "status": "confirmed", "spec": {"primary_control": "select"}},
                   {"key": "e", "status": "hypothesis", "spec": {"primary_control": "select"}}]
        self.assertEqual([en["key"] for en in forward.under_test(entries)], ["a", "b"])

    def test_control_draws_are_reproducible(self):
        a = forward.leg_rng("k", date(2026, 11, 2), "AAA").integers(1000, size=5)
        b = forward.leg_rng("k", date(2026, 11, 2), "AAA").integers(1000, size=5)
        c = forward.leg_rng("k", date(2026, 11, 3), "AAA").integers(1000, size=5)
        self.assertEqual(list(a), list(b))
        self.assertNotEqual(list(a), list(c))


if __name__ == "__main__":
    unittest.main()
