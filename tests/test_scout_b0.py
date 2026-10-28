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
