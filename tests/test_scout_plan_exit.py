import unittest

import numpy as np
import pandas as pd

from scout import exit_study, gate, plan_exit
from scout.features import build_panel


def _panel(n=400, seed=5):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2024-01-02", periods=n).date
    bars = {}
    for t in ("SPY", "AAA", "BBB"):
        close = 100 * np.exp(np.cumsum(rng.normal(0.0005, 0.015, n)))
        open_ = close * (1 + rng.normal(0, 0.004, n))
        bars[t] = pd.DataFrame({"open": open_, "high": np.maximum(open_, close) * 1.01, "low": np.minimum(open_, close) * 0.99,
                                "close": close, "volume": rng.integers(2_000_000, 3_000_000, n).astype(float)}, index=dates)
    inst = {"SPY": {"kind": "etf"}, "AAA": {"kind": "stock", "sector": "Energy"}, "BBB": {"kind": "stock", "sector": "Energy"}}
    return build_panel(bars, inst)


class PlanExitTests(unittest.TestCase):
    def setUp(self):
        self.p = _panel()
        self.a = plan_exit.stock_arrays(self.p, "AAA")

    def test_policy_is_the_holdings_plan(self):
        self.assertEqual(plan_exit.POLICY.trail, "chandelier")
        self.assertTrue(plan_exit.POLICY.after_t1)
        self.assertEqual(plan_exit.POLICY.scale, ())
        self.assertEqual(plan_exit.cap(63), 60)
        self.assertEqual(plan_exit.cap(21), 21)

    def test_plan_uses_bars_up_to_the_day_only(self):
        i = 300
        cut = {k: (v[: i + 1] if isinstance(v, np.ndarray) else v[: i + 1]) for k, v in self.a.items()}
        fill = float(self.a["open"][i + 1])
        self.assertEqual(plan_exit.plan_for(self.a, i, fill), plan_exit.plan_for(cut, i, fill))

    def test_shorter_windows_match_a_direct_replay(self):
        days = list(range(260, 320))
        m = plan_exit._stock_matrix((self.a, days, (20, 63)))
        for i in days:
            for h in (20, 63):
                direct = plan_exit.buy_next_open(self.a, i, h)
                if direct is None:
                    self.assertTrue(np.isnan(m[h][i]))
                else:
                    self.assertAlmostEqual(m[h][i], direct, places=12)

    def test_a_window_ends_at_its_last_close(self):
        i, h = 270, 20
        s = i + 1
        fill = float(self.a["open"][s])
        ret, held = plan_exit.outcome(self.a, s, fill, h)
        self.assertLessEqual(held, h)
        if held == h:
            plan = plan_exit.plan_for(self.a, i, fill)
            if self.a["low"][s:s + h].min() > plan["stop"]:
                self.assertAlmostEqual(ret, self.a["close"][s + h - 1] / fill - 1)

    def test_manage_honours_the_cap(self):
        path = exit_study.Path(*(np.full(100, 10.0) for _ in range(7)), start=5)
        ret, held = exit_study.manage(path, 10.0, 9.0, 12.0, 13.0, plan_exit.POLICY, max_hold=7)
        self.assertEqual((ret, held), (0.0, 7))


class SpecExitTests(unittest.TestCase):
    def test_exit_must_be_known(self):
        base = {f: "x" for f in gate.SPEC_FIELDS} | {"primary_control": "select", "type": "rank", "ranker": "high_52w",
                                                       "hold": 21, "every": 21}
        self.assertEqual(gate.validate_spec({**base, "exit": "plan"}), [])
        self.assertTrue(gate.validate_spec({**base, "exit": "ladder"}))


class RankPlanTests(unittest.TestCase):
    def test_picks_and_controls_share_the_plan_exit(self):
        from unittest import mock

        from scout import rankers

        p = _panel(n=420)
        m = gate.Market(p)
        spec = {"key": "t", "ranker": "first", "hold": 20, "every": 21, "exit": "plan"}
        with mock.patch.dict(rankers.RANKERS, {"first": lambda a, i, frac: [int(c) for c in rankers._universe(a, i)[:1]]}), \
                mock.patch.object(plan_exit, "matrix", side_effect=_serial):    # no process pool in tests
            rows = gate.rank_backtest(p, spec, m)
        self.assertTrue(rows)
        a, stocks = rankers.arrays(p)
        for r in rows:
            i = r["i"]
            uni = [j for j in rankers._universe(a, i)]
            rets = {j: plan_exit.buy_next_open(plan_exit.stock_arrays(p, stocks[j]), i, 20) for j in uni}
            rets = {j: x for j, x in rets.items() if x is not None}
            pick = stocks.index(r["ticker"])
            self.assertAlmostEqual(r["ret"], rets[pick])
            others = [x for j, x in rets.items() if j != pick]
            if others:
                self.assertAlmostEqual(r["select"], sum(others) / len(others))


class RulePlanTests(unittest.TestCase):
    def test_rule_entry_then_plan_exit_over_the_rule_window(self):
        import dataclasses

        from scout.detectors import Levels, registry

        p = _panel()
        det = next(d for d in registry(include_candidates=True) if d.name == "pullback_in_uptrend")
        det = dataclasses.replace(det, levels=lambda r: Levels(r["close"] * 0.5, r["close"] * 2, r["close"] * 0.3,
                                                               r["close"] * 3, r["close"] * 4))   # fills at the next open
        i = 300
        span = det.trigger_window + det.exit.holding_limit(det.horizon_days)
        ret, a, b = gate.trade(p, det, "AAA", i, span, plan=True)
        self.assertEqual((a, b), (i + 1, i + det.horizon_days))
        arr = plan_exit.stock_arrays(p, "AAA")
        self.assertAlmostEqual(ret, plan_exit.buy_next_open(arr, i, det.horizon_days))


def _serial(p, stocks, days, holds):
    cols = [plan_exit._stock_matrix((plan_exit.stock_arrays(p, t), days, holds)) for t in stocks]
    return {h: np.column_stack([c[h] for c in cols]) for h in holds}


if __name__ == "__main__":
    unittest.main()
