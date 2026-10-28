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


class _Panel:
    """Just enough of a FeaturePanel for the ranking forward record."""

    def __init__(self, frames):
        self.f = frames

    def __getitem__(self, name):
        return self.f[name]

    @property
    def dates(self):
        return self.f["close"].index


def _rank_panel(n=10):
    import pandas as pd

    idx = pd.Index([date(2026, 11, 2) + timedelta(days=k) for k in range(n)])
    op = pd.DataFrame({"AAA": [10.0 + k for k in range(n)], "SPY": [100.0] * n}, index=idx)
    cl = pd.DataFrame({"AAA": [10.5 + k for k in range(n)], "SPY": [101.0] * n}, index=idx)
    return _Panel({"open": op, "close": cl}), idx


class RankForwardTests(unittest.TestCase):
    def test_holding_runs_from_next_open_to_the_close_hold_sessions_later(self):
        p, idx = _rank_panel()
        r = forward.resolve_rank(p, {"id": 1, "asof": idx[2], "ticker": "AAA", "hold": 3})
        self.assertEqual((r["ed"], r["xd"]), (idx[3], idx[5]))
        self.assertAlmostEqual(r["ret"], 15.5 / 13.0 - 1)
        self.assertAlmostEqual(r["spy"], 0.01)
        self.assertIsNone(forward.resolve_rank(p, {"id": 1, "asof": idx[7], "ticker": "AAA", "hold": 3}))   # still running
        self.assertEqual(forward.resolve_rank(p, {"id": 1, "asof": idx[2], "ticker": "ZZZ", "hold": 3})["s"], "missing")
        self.assertIsNone(forward.resolve_rank(p, {"id": 1, "asof": idx[0] - timedelta(days=5), "ticker": "AAA", "hold": 3}))

    def test_control_is_every_other_stock_of_the_day_and_waits_for_all(self):
        d, d2 = date(2026, 11, 2), date(2026, 12, 1)
        row = lambda t, role, ret, day=d, s="closed": {"asof": day, "ticker": t, "role": role, "ret": ret, "state": s, "spy_ret": 0.0}  # noqa: E731
        rows = [row("A", "pick", 0.10), row("B", "pool", 0.02), row("C", "pool", 0.04), row("D", "pool", None, s="missing"),
                row("E", "pick", 0.05, d2), row("F", "pool", None, d2, "pending")]
        (pick,) = forward.rank_pairs(rows)
        self.assertEqual(pick["ticker"], "A")
        self.assertAlmostEqual(pick["select"], 0.03)
        self.assertAlmostEqual(pick["diff"], 0.07)

    def test_rank_entries_are_recorded_by_rebalance_not_by_detector(self):
        from unittest.mock import AsyncMock, patch

        en = {"key": "rank_x", "status": "forward", "spec": {"type": "rank", "ranker": "low_asset_growth", "every": 3, "hold": 5,
                                                              "primary_control": "select"}}
        p, idx = _rank_panel()
        with patch.object(forward.db, "fetch_one", AsyncMock(return_value={"d": idx[5]})), \
                patch.object(forward, "rank_rows", AsyncMock(return_value=[{"role": "pick"}])) as rr, \
                patch.object(forward.db, "execute_many", AsyncMock()):
            import asyncio

            self.assertEqual(asyncio.run(forward.record_rank(p, en, idx[7])), 0)      # 2 sessions since: not yet
            self.assertEqual(asyncio.run(forward.record_rank(p, en, idx[8])), 1)      # 3 sessions: rebalance
        self.assertEqual(rr.await_count, 1)
        self.assertIn(en, forward.under_test([en]))
