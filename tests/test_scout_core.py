import ast
import unittest
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from scout import base_rates
from scout.detectors import Levels, r_targets, registry
from scout.features import build_panel
from scout.opportunities import score_candidate
from scout.pipeline import select_top
from scout.simulate import Bar, simulate
from scout.thesis import allowed_numbers, template_thesis, unsupported_numbers
from scout.universe import parse_constituents

ROOT = Path(__file__).resolve().parents[1]


def _bars(prices, start=date(2026, 1, 5)):
    out, d = [], start
    for o, h, l, c in prices:
        out.append(Bar(d, o, h, l, c))
        d += timedelta(days=1)
    return out


LV = Levels(entry_lo=99.0, entry_hi=101.0, stop=95.0, target1=110.0, target2=115.0)


class SimulateTests(unittest.TestCase):
    def test_pending_until_trigger_window_elapses(self):
        res = simulate(_bars([(105, 106, 104, 105)]), LV, horizon=5)
        self.assertEqual(res.state, "pending")

    def test_untriggered_after_window(self):
        res = simulate(_bars([(105, 106, 104, 105)] * 3), LV, horizon=5)
        self.assertEqual(res.state, "untriggered")
        self.assertIsNone(res.return_pct)

    def test_fill_is_open_clipped_into_zone(self):
        res = simulate(_bars([(103, 104, 100, 102)]), LV, horizon=5)
        self.assertEqual(res.state, "open")
        self.assertEqual(res.entry_price, 101.0)

    def test_stop_checked_before_target_on_same_bar(self):
        res = simulate(_bars([(100, 101, 99.5, 100), (100, 111, 94, 100)]), LV, horizon=5)
        self.assertEqual(res.state, "stopped")
        self.assertEqual(res.exit_price, 95.0)

    def test_gap_through_stop_fills_at_open(self):
        res = simulate(_bars([(100, 101, 99.5, 100), (90, 92, 89, 91)]), LV, horizon=5)
        self.assertEqual(res.state, "stopped")
        self.assertEqual(res.exit_price, 90.0)

    def test_target_and_expiry(self):
        hit = simulate(_bars([(100, 101, 99.5, 100), (104, 112, 103, 111)]), LV, horizon=5)
        self.assertEqual(hit.state, "hit_target")
        self.assertAlmostEqual(hit.return_pct, 0.10)
        flat = simulate(_bars([(100, 101, 99.5, 100)] + [(100, 102, 98, 101)] * 4), LV, horizon=5)
        self.assertEqual(flat.state, "expired")
        self.assertEqual(flat.days_held, 5)


class LevelsTests(unittest.TestCase):
    def test_r_targets(self):
        lv = r_targets(99, 101, 95)
        self.assertEqual((lv.target1, lv.target2), (110.0, 115.0))
        self.assertAlmostEqual(lv.risk_reward, 2.0)
        self.assertTrue(lv.valid())
        self.assertFalse(Levels(99, 101, 102, 110, 115).valid())


def _synthetic_bars(n=320, seed=7):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2025-01-01", periods=n).date
    out = {}
    for i, t in enumerate(["SPY", "XLK", "AAA", "BBB", "CCC"]):
        drift = 0.0015 if t in ("AAA", "SPY", "XLK") else -0.0005
        close = 100 * np.exp(np.cumsum(rng.normal(drift, 0.015, n)))
        open_ = close * (1 + rng.normal(0, 0.003, n))
        out[t] = pd.DataFrame({
            "open": open_, "high": np.maximum(open_, close) * 1.01, "low": np.minimum(open_, close) * 0.99,
            "close": close, "volume": rng.integers(2_000_000, 4_000_000, n).astype(float),
        }, index=dates)
    return out


class PanelAndReplayTests(unittest.TestCase):
    def setUp(self):
        inst = {"SPY": {"kind": "etf"}, "XLK": {"kind": "etf"}}
        inst.update({t: {"kind": "stock", "sector": "Information Technology"} for t in ("AAA", "BBB", "CCC")})
        self.panel = build_panel(_synthetic_bars(), inst)

    def test_panel_shapes_and_rs_only_ranks_stocks(self):
        p = self.panel
        self.assertEqual(p["close"].shape, (320, 5))
        last = p.dates[-1]
        self.assertTrue(np.isnan(p["rs_pct"].at[last, "SPY"]))
        self.assertGreater(p["rs_pct"].at[last, "AAA"], p["rs_pct"].at[last, "BBB"])
        self.assertIn(p.regime.iloc[-1], {"uptrend", "mixed", "downtrend"})

    def test_detector_masks_never_fire_on_etfs(self):
        for det in registry():
            mask = det.mask(self.panel)
            self.assertFalse(mask[["SPY", "XLK"]].to_numpy().any(), det.name)

    def test_cooldown_dedups_consecutive_hits(self):
        mask = pd.DataFrame({"A": [True] * 25})
        self.assertEqual([i for _, i in base_rates.hit_positions(mask, cooldown=10)], [0, 10, 20])

    def test_net_of_cost_and_hold_baseline(self):
        from scout.base_rates import Sample, summarize
        from scout.simulate import ROUND_TRIP_COST

        def mk(ret, hold, spy=0.0):
            return Sample("d", "AAA", date(2026, 1, 5), "uptrend", "expired", ret, spy, hold, 0.0, 0.0, 5, 0.0)

        stats = summarize([mk(0.03, 0.01), mk(0.0005, 0.0), mk(-0.02, 0.01)])
        self.assertAlmostEqual(stats["avg_net_return"], (0.03 + 0.0005 - 0.02) / 3 - ROUND_TRIP_COST, places=5)
        # A +5bp trade is a gross win but a net loss once the round trip is paid.
        self.assertAlmostEqual(stats["win_rate"], 2 / 3, places=3)
        self.assertAlmostEqual(stats["win_rate_net"], 1 / 3, places=3)
        self.assertAlmostEqual(stats["beat_hold_rate"], 1 / 3, places=3)
        self.assertLess(stats["avg_edge_vs_hold"], stats["avg_net_return"])

    def test_drift_baseline_and_edge_preference(self):
        drift = base_rates.daily_drift(self.panel)
        self.assertGreater(drift["AAA"], drift["BBB"])        # AAA was built with the higher drift
        self.assertEqual(base_rates.edge_of({"avg_edge_vs_hold": -0.01, "avg_excess": 0.02}), -0.01)
        self.assertEqual(base_rates.edge_of({"avg_excess": 0.02}), 0.02)   # rows from before costs

    def test_pick_falls_back_to_all_bucket(self):
        rates = {"d": {"all": {"n": 100, "avg_excess": 0.01}, "uptrend": {"n": 5, "avg_excess": 0.2}}}
        self.assertEqual(base_rates.pick(rates, "d", "uptrend")["regime"], "all")
        self.assertAlmostEqual(base_rates.shrunk_edge(rates["d"]["all"]), 0.01 * 100 / 150)


class RankingTests(unittest.TestCase):
    def test_negative_edge_scores_below_positive_edge(self):
        det = registry()[0]
        lv = r_targets(99, 101, 95)
        feat = {"rs_pct": 0.9}
        good, _ = score_candidate(det, [det.name], lv, feat, {"n": 500, "avg_excess": 0.01})
        bad, _ = score_candidate(det, [det.name], lv, feat, {"n": 500, "avg_excess": -0.01})
        soon, parts = score_candidate(det, [det.name], lv, feat, {"n": 500, "avg_excess": 0.01}, days_to_earnings=2)
        self.assertGreater(good, bad)
        self.assertLess(soon, good)
        self.assertIn("earnings_penalty", parts)

    def test_select_top_caps_per_detector(self):
        class C:
            def __init__(self, name):
                self.primary = type("D", (), {"name": name})()
        cands = [C("a")] * 5 + [C("b")] * 5
        picked = select_top(cands, n=8, per_detector=3)
        self.assertEqual(len(picked), 6)


class ThesisGuardTests(unittest.TestCase):
    EV = {
        "ticker": "NVDA", "asof": "2026-10-23",
        "setup": {"primary": "trend_breakout", "label": "趋势突破", "rule": "规则", "horizon_days": 20},
        "levels": {"entry_lo": 182.0, "entry_hi": 186.5, "stop": 176.8, "target1": 198.0, "target2": 210.0},
        "price": {"ret_20d": 0.0523, "rs_pct": 0.93, "vol_ratio": 2.7, "dist_52w_high": -0.04},
        "base_rate": {"n": 143, "win_rate": 0.58, "avg_excess": 0.021, "regime": "uptrend"},
        "market": {"regime": "uptrend"},
    }

    def test_numbers_from_evidence_pass_including_percent_rounding(self):
        allowed = allowed_numbers(self.EV)
        text = "20 日上涨 5.2%，RS 分位 93%，量能 2.7 倍，止损 176.8，历史 143 次胜率 58%，2026-10-23 数据。"
        self.assertEqual(unsupported_numbers(text, allowed), [])

    def test_invented_numbers_are_flagged(self):
        allowed = allowed_numbers(self.EV)
        self.assertEqual(unsupported_numbers("目标价 250，市盈率 35 倍", allowed), ["250", "35"])

    def test_llm_view_formats_fractions_and_validation_follows_it(self):
        from scout.thesis import llm_view

        view = llm_view({**self.EV, "market": {"sector_leaders_20d": [["XLE", 0.1175]]}})
        self.assertEqual(view["price"]["ret_20d"], "5.2%")
        self.assertEqual(view["price"]["dist_52w_high"], "-4.0%")
        self.assertEqual(view["market"]["sector_leaders_20d"], ["XLE 11.8%"])
        allowed = allowed_numbers(view)
        self.assertEqual(unsupported_numbers("20 日涨 5.2%，XLE 领涨 11.8%", allowed), [])
        self.assertEqual(unsupported_numbers("距离高点 -0.04", allowed), ["-0.04"])

    def test_52_week_is_a_term_not_a_number(self):
        self.assertEqual(unsupported_numbers("距离 52 周高点不远，52周新高", allowed_numbers(self.EV)), [])

    def test_template_is_self_consistent(self):
        from scout.thesis import thesis_text

        t = template_thesis(self.EV)
        self.assertEqual(unsupported_numbers(thesis_text(t), allowed_numbers(self.EV)), [])
        self.assertTrue(t.counter_evidence)


class UniverseTests(unittest.TestCase):
    def test_parse_constituents_handles_gics_and_icb_tables(self):
        rows = "".join(f"<tr><td>T{i}.B</td><td>Co {i}</td><td>Energy</td><td>Oil</td></tr>" for i in range(60))
        gics = f"<table><tr><th>Symbol</th><th>Security</th><th>GICS Sector</th><th>GICS Sub-Industry</th></tr>{rows}</table>"
        out = parse_constituents(gics, "sp500")
        self.assertEqual(out[0].ticker, "T0-B")
        self.assertEqual(out[0].sector, "Energy")
        icb = f"<table><tr><th>Ticker</th><th>Company</th><th>ICB Industry</th><th>X</th></tr>{rows}</table>"
        self.assertIsNone(parse_constituents(icb, "ndx")[0].sector)


class SecretLoggingTests(unittest.TestCase):
    def test_httpx_request_urls_are_not_logged(self):
        """Telegram/Finnhub URLs carry tokens; httpx logs URLs at INFO."""
        import logging

        import scout.telegram  # noqa: F401
        import tools.notify_tools  # noqa: F401

        self.assertGreaterEqual(logging.getLogger("httpx").level, logging.WARNING)


class ScoutFenceTests(unittest.TestCase):
    """Scout is a research product: it must never reach trading/execution code
    (the archived QC observer lives under git tag `qc-observer-final`)."""

    FORBIDDEN = ("services.execution", "services.qc_", "api.", "audit", "tools.", "main", "dashboard")

    def test_scout_imports_no_qc_or_execution_modules(self):
        for path in (ROOT / "scout").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                    names = [node.module]
                for name in names:
                    for bad in self.FORBIDDEN:
                        self.assertFalse(
                            name == bad.rstrip(".") or name.startswith(bad),
                            f"{path.relative_to(ROOT)} imports {name}",
                        )

    def test_news_pipeline_does_not_import_scout(self):
        """The shared news cron must keep running even if Scout is broken."""
        paths = [ROOT / "constants.py", ROOT / "config.py"]
        for folder in ("services", "cron", "db", "tools"):
            paths += list((ROOT / folder).rglob("*.py"))
        for path in paths:
            self.assertNotIn("from scout", path.read_text(encoding="utf-8"), str(path))


if __name__ == "__main__":
    unittest.main()
