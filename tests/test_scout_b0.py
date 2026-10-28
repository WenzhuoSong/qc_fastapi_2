import tempfile
import unittest
from datetime import date
from pathlib import Path

from scout import b0, learned
from scout.detectors import registry

# B0's own research stays out of this repository; the tests use a made-up sample.
SAMPLE = """
- key: b0_sample_positive
  kind: regularity
  polarity: positive
  status: backtested
  title: 示例：通过的结论
  claim: 示例结论
  evidence:
    - {source: b0, date: 2026-10-28, summary: 示例摘要, ref: notes/example_report.md}
  recheck_on: 2027-04-28
- key: b0_sample_negative
  kind: method
  polarity: negative
  status: rejected
  title: 示例：失败的方法
  claim: 示例结论
  evidence:
    - {source: b0, date: 2026-10-27, summary: 示例摘要}
  recheck_on: 2027-10-28
"""


def _sample() -> list[dict]:
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as f:
        f.write(SAMPLE)
    return b0.load_import(f.name)


class ImportTests(unittest.TestCase):
    def test_a_valid_file_is_importable(self):
        items = _sample()
        self.assertEqual(b0.validate_import(items), [])
        self.assertEqual(items[0]["applies"], {"topics": ["b0"]})             # default scope

    def test_an_import_cannot_confirm(self):
        item = {**_sample()[0], "status": "confirmed"}
        self.assertIn("不能直接写已确认", "".join(b0.validate_import([item])))
        self.assertTrue(learned.check_transition(None, item, "import"))
        self.assertEqual(learned.check_transition(None, {**item, "status": "rejected"}, "import"), [])

    def test_an_import_needs_b0_evidence_and_negative_polarity_when_rejected(self):
        item = {**_sample()[1]}
        bad = {**item, "polarity": "positive", "evidence": [{"source": "web", "date": "2026-10-28", "summary": "x"}]}
        issues = "".join(b0.validate_import([bad]))
        self.assertIn("negative", issues)
        self.assertIn("source 为 b0", issues)


class ExportTests(unittest.TestCase):
    def test_strategy_spec_has_the_exact_rule_and_status(self):
        det = next(d for d in registry(include_candidates=True) if d.name == "breakout_retest")
        en = {"key": "breakout_retest_selection", "version": 7, "status": "forward", "title": "突破回踩", "claim": "c",
              "mechanism": "m", "spec": {"primary_control": "select", "expected_failure": "e"}}
        run = {"backtest": {"summary": {"first": "2015-10-19", "last": "2026-09-15", "n": 2313, "split": "2021-07-28", "avg_net": 0.0026,
                                        "vs_select": {"edge": 0.0083, "t": 5.02}}}}
        fwd = {"started": "2026-10-27", "signals": 1, "n": 0, "criteria": [{"name": "记录时长", "value": "0.0 个月", "bar": "≥ 6 个月", "ok": False}]}
        md = b0.strategy_markdown(en, det, run, fwd)
        self.assertIn("不能用于实盘", md)
        self.assertIn("high_55_prior", md)
        self.assertIn("def _breakout_retest_levels", md)
        self.assertIn("+0.83%（t 5.02）", md)
        self.assertIn("记录时长", md)

    def test_readme_explains_the_import_format(self):
        text = b0.readme(date(2026, 10, 28), [], [{"status": "rejected"}], {"tilts": []})
        self.assertIn("python -m scout.jobs.b0 import", text)
        self.assertIn("不会向 QuantConnect", text)


if __name__ == "__main__":
    unittest.main()


class RankTests(unittest.TestCase):
    # A made-up configuration: the real parameters are B0's and live in the environment.
    CFG = b0.MomentumScore.from_json('{"weights": {"20": 1.0, "10": 0.5, "2": -0.1}, "trend_horizon": 10, '
                                     '"min_dollar_volume": 5e6, "per_index": 1, "indexes": ["sp500", "ndx"], '
                                     '"gate": ["IDX_A", "IDX_B"], "gate_sma": 50, "target_holdings": 2}')

    def _frames(self):
        import numpy as np
        import pandas as pd

        idx = pd.bdate_range("2025-01-01", periods=100)
        growth = {"UP": 0.002, "FLAT": 0.0, "DOWN": -0.001, "IDX_A": 0.0005, "IDX_B": -0.0008, "THIN": 0.003}
        close = pd.DataFrame({t: 100 * np.exp(g * np.arange(100)) for t, g in growth.items()}, index=idx)
        volume = pd.DataFrame({t: (1e3 if t == "THIN" else 1e6) for t in growth}, index=idx)
        return close, volume

    def test_unset_means_off(self):
        self.assertIsNone(b0.MomentumScore.from_json(""))
        with self.assertRaises(ValueError):
            b0.MomentumScore.from_json('{"weights": {"20": 1}, "trend_horizon": 10, "min_dollar_volume": 0, "per_index": 1, '
                                       '"indexes": ["sp500"], "gate": [], "gate_sma": 50, "target_holdings": 1}')

    def test_score_trend_filter_and_liquidity(self):
        close, volume = self._frames()
        rows = b0.b0_scores(close, volume, ["UP", "FLAT", "DOWN", "THIN"], self.CFG)
        self.assertEqual([r["ticker"] for r in rows], ["UP"])              # FLAT and DOWN fail the trend filter, THIN the floor
        self.assertAlmostEqual(rows[0]["returns"]["20"], 0.002 * 20, places=6)
        self.assertAlmostEqual(rows[0]["score"], 0.002 * (20 + 0.5 * 10 - 0.1 * 2), places=6)

    def test_universe_is_the_largest_per_index(self):
        close, volume = self._frames()
        volume["DOWN"] = 2e6
        uni = b0.b0_universe(close, volume, {"UP": ["sp500", "ndx"], "FLAT": ["sp400"], "DOWN": ["ndx"]}, self.CFG)
        self.assertEqual(uni, {"UP": ["sp500"], "DOWN": ["ndx"]})

    def test_market_filter(self):
        close, _ = self._frames()
        g = b0.b0_gate(close, self.CFG)
        self.assertTrue(g["tickers"]["IDX_A"]["above"])
        self.assertFalse(g["tickers"]["IDX_B"]["above"])
        self.assertFalse(g["risk_on"])

    def test_panel_renders(self):
        from scout.web import views

        close, volume = self._frames()
        rows = b0.b0_scores(close, volume, ["UP"], self.CFG)
        cfg = self.CFG
        html = views.b0_rank_panel({"asof": "2026-10-27", "gate": b0.b0_gate(close, cfg), "universe": 1, "scored": 1,
                                    "rows": [{**rows[0], "indexes": ["ndx"], "name": "Up Inc", "sector": "Information Technology"}],
                                    "weights": [list(w) for w in cfg.weights], "trend_horizon": cfg.trend_horizon,
                                    "min_dollar_volume": cfg.min_dollar_volume, "per_index": cfg.per_index,
                                    "indexes": list(cfg.indexes), "target_holdings": cfg.target_holdings})
        self.assertIn("大盘过滤未通过", html)
        self.assertIn("不是 B0 的持仓", html)
        self.assertIn("Nasdaq-100", html)
        self.assertIn("SCOUT_B0_SCORE", views.b0_rank_panel(None))


if __name__ == "__main__":
    unittest.main()
