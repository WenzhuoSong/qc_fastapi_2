import unittest
from datetime import date, timedelta

import numpy as np
import pandas as pd

from scout import learned, outlook


def _panel_frames(n_days=400, n_stocks=100, seed=1):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2024-01-01", periods=n_days)
    rets = rng.normal(0.0003, 0.02, size=(n_days, n_stocks))
    close = pd.DataFrame(100 * np.cumprod(1 + rets, axis=0), index=idx, columns=[f"S{i}" for i in range(n_stocks)])
    volume = pd.DataFrame(rng.uniform(1e5, 1e7, size=(n_days, n_stocks)), index=idx, columns=close.columns)
    spy = close.mean(axis=1)
    return close, volume, spy


class DriverTests(unittest.TestCase):
    def test_quintile_spread(self):
        sig = pd.Series(range(100), dtype=float)
        fwd = pd.Series([0.1 if i >= 80 else (-0.1 if i < 20 else 0.0) for i in range(100)])
        self.assertAlmostEqual(outlook._quintile_spread(sig, fwd), 0.2)
        self.assertIsNone(outlook._quintile_spread(sig[:10], fwd[:10]))

    def test_style_portfolios_are_formed_before_the_window(self):
        close, volume, spy = _panel_frames()
        base = outlook.style_spreads(close, volume, spy, 20)
        # a jump inside the window must not change who is in the momentum portfolio
        shocked = close.copy()
        shocked.iloc[-5:, :10] *= 3
        later = outlook.style_spreads(shocked, volume, spy, 20)
        self.assertEqual(set(base), {"momentum", "low_vol", "size", "beta"})
        self.assertNotEqual(base["momentum"], later["momentum"])             # returns change...
        formed = close.iloc[-21 - 21] / close.iloc[-21 - 252] - 1
        formed_shocked = shocked.iloc[-21 - 21] / shocked.iloc[-21 - 252] - 1
        pd.testing.assert_series_equal(formed, formed_shocked)                # ...the formation does not

    def test_too_little_history_gives_nothing(self):
        close, volume, spy = _panel_frames(n_days=200)
        self.assertEqual(outlook.style_spreads(close, volume, spy, 20), {})

    def test_breadth(self):
        close, _, _ = _panel_frames()
        b = outlook.breadth(close)
        self.assertTrue(0 <= b["above_50"] <= 1 and b["n"] == 100)


class SectorDataTests(unittest.TestCase):
    def test_fundamentals_point_in_time_series(self):
        q = [date(2024, 3, 31) + timedelta(days=91 * k) for k in range(9)]
        rev = [(d, 100 * 1.02 ** k) for k, d in enumerate(q)]
        capex = [(d, -10.0 * (1.5 if k >= 5 else 1.0)) for k, d in enumerate(q)]
        f = outlook.ticker_fundamentals({"revenue": rev, "capex": capex})
        self.assertAlmostEqual(f["rev_yoy"], 1.02 ** 4 - 1, places=3)
        self.assertGreater(f["capex_ttm_yoy"], 0.3)

    def test_estimates(self):
        e = outlook.ticker_estimates({"eps_trend": {"0y": {"current": 11.0, "30daysAgo": 10.0, "90daysAgo": 10.0}},
                                      "eps_revisions": {"0y": {"upLast30days": 3, "downLast30days": 1}}})
        self.assertAlmostEqual(e["eps_rev_30d"], 0.1)
        self.assertEqual((e["up_30d"], e["down_30d"]), (3, 1))
        self.assertEqual(outlook.ticker_estimates(None), {})

    def test_sector_block(self):
        b = outlook.sector_block({
            "A": {"fund": {"rev_yoy": 0.2, "rev_yoy_prev": 0.1, "capex_ttm": 12, "capex_ttm_prev": 10}, "est": {"eps_rev_30d": 0.02, "up_30d": 3, "down_30d": 1}},
            "B": {"fund": {"rev_yoy": 0.0, "rev_yoy_prev": 0.05}, "est": {"eps_rev_30d": -0.01, "up_30d": 0, "down_30d": 2}},
        })
        self.assertEqual(b["companies"], 2)
        self.assertAlmostEqual(b["rev_accel_share"], 0.5)
        self.assertAlmostEqual(b["capex_ttm_yoy"], 0.2)
        self.assertAlmostEqual(b["revision_breadth"], 0.0)
        self.assertAlmostEqual(b["eps_up_share_30d"], 0.5)


def _pack():
    drv = {"asof": "2026-10-30", "spy": {"ret_5": 0.01, "ret_20": 0.02, "ret_60": 0.05},
           "etfs": {"XLK": {"ret_5": 0.02, "rel_5": 0.01, "ret_20": 0.04, "rel_20": 0.02, "ret_60": 0.1, "rel_60": 0.05},
                    "XLE": {"ret_5": -0.01, "rel_5": -0.02, "ret_20": 0.0, "rel_20": -0.02, "ret_60": 0.0, "rel_60": -0.05}},
           "styles": {5: {}, 20: {"momentum": 0.03}, 60: {}}, "breadth": {"above_50": 0.6, "above_200": 0.55, "near_high": 0.2, "n": 800}}
    member = {"fund": {"rev_yoy": 0.2, "rev_yoy_prev": 0.1}, "est": {"eps_rev_30d": 0.03, "up_30d": 5, "down_30d": 1}}
    blocks = {"XLK": outlook.sector_block({f"T{i}": member for i in range(5)}) | {"upgrades_30d": 4, "downgrades_30d": 1},
              "XLE": outlook.sector_block({"A": member, "B": member}) | {"upgrades_30d": 0, "downgrades_30d": 0}}
    methods = [{"key": "method_estimate_revisions", "title": "行业盈利预期修正面", "claim": "c", "status": "hypothesis"}]
    return outlook.build_pack(drv, blocks, {}, {}, methods, {"XLE": {"日期": "2026-10-01", "方向": "偏空", "持有期": 60}})


def _thesis(**over):
    base = dict(target="XLK", stance="偏多", horizon=60, method="method_estimate_revisions", title="预期上调面扩大",
                mechanism="研究池公司收入同比中位数 +20.0%，EPS 预期 30 日上调", evidence=["est_XLK", "price_XLK"],
                falsification=["预期修正转负"], confidence="medium")
    base.update(over)
    return outlook.Thesis(**base)


class ThesisTests(unittest.TestCase):
    def test_a_grounded_thesis_passes(self):
        o = outlook.Outlook(drivers="科技相对 SPY 20 日 +2.0%", theses=[_thesis()])
        self.assertEqual(outlook.validate(o, _pack()), [])

    def test_price_only_open_target_unknown_method_and_numbers(self):
        pack = _pack()
        o = outlook.Outlook(drivers="x", theses=[
            _thesis(evidence=["price_XLK"]),
            _thesis(target="XLE", stance="偏空"),
            _thesis(target="SMH", method="vibes", mechanism="上涨空间 35%"),
        ])
        issues = "\n".join(outlook.validate(o, pack))
        self.assertIn("只引用了价格类证据", issues)
        self.assertIn("持有期内", issues)
        self.assertIn("不在可选标的", issues)
        self.assertIn("vibes", issues)
        self.assertIn("35", issues)

    def test_citation_paths_resolve_to_keys(self):
        o = outlook.resolve(outlook.Outlook(drivers="x", theses=[_thesis(evidence=["targets.XLK.est_XLK", "context.macro", "nope"])]), _pack())
        self.assertEqual(o.theses[0].evidence, ["est_XLK", "macro", "nope"])

    def test_no_thesis_needs_a_reason(self):
        self.assertTrue(outlook.validate(outlook.Outlook(drivers="x", theses=[]), _pack()))
        self.assertEqual(outlook.validate(outlook.Outlook(drivers="x", theses=[], no_thesis_reason="证据不足"), _pack()), [])

    def test_thin_coverage_gives_no_sector_aggregate(self):
        xle = _pack()["targets"]["XLE"]
        self.assertNotIn("fund_XLE", xle)
        self.assertIn("coverage_XLE", xle)
        self.assertIn("fund_XLK", _pack()["targets"]["XLK"])

    def test_open_theses_expire_with_their_horizon(self):
        today = date(2026, 10, 30)
        rows = [{"ticker": "XLK", "asof": today - timedelta(days=50), "stance": 1, "payload": {"horizon": 60}},
                {"ticker": "XLE", "asof": today - timedelta(days=120), "stance": -1, "payload": {"horizon": 60}}]
        self.assertEqual(list(outlook.open_theses(rows, today)), ["XLK"])

    def test_methods_are_seeded_as_hypotheses(self):
        ms = outlook.methods([{**en, "applies": en.get("applies") or {}} for en in learned.seed_entries()])
        self.assertGreaterEqual(len(ms), 5)
        self.assertTrue(all(m["status"] == "hypothesis" and m["kind"] == "judgment" for m in ms))

    def test_render_text_reads_stored_records(self):
        rec = {"asof": "2026-10-30", "drivers": {"etfs": {"XLK": {"rel_20": 0.02}}, "styles": {"20": {"momentum": 0.03}},
                                                  "breadth": {"above_50": 0.6, "above_200": 0.55}},
               "outlook": {"drivers": "d", "theses": [_thesis().model_dump()], "no_thesis_reason": None}}
        text = outlook.render_text(rec)
        self.assertIn("动量 +3.0%", text)
        self.assertIn("偏多", text)


if __name__ == "__main__":
    unittest.main()
