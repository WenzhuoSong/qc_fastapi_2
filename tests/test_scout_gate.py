import unittest
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

from scout import gate, learned
from scout.detectors import registry


def _spec(**over):
    base = gate.load_spec(gate.SPECS / "breakout_retest_selection.yaml")
    base.update(over)
    return base


def _rows(edge_first: float, edge_second: float, months: int = 48) -> list[dict]:
    """Two signals a month; the primary control trails the trade by the given edge, with noise."""
    rows = []
    for k in range(months):
        d = date(2015, 1, 5) + timedelta(days=31 * k)
        edge = edge_first if k < months // 2 else edge_second
        for j, noise in enumerate((0.004, -0.004)):
            rows.append({"ticker": f"T{j}", "i": k * 21, "date": d + timedelta(days=j), "a": k * 21 + 1, "b": k * 21 + 10,
                         "ret": 0.01 + noise, "spy": 0.008, "ew": 0.009, "timing": 0.01 - edge / 2,
                         "select": 0.01 - edge + noise * (0.5 if k % 2 else -0.5), "regime": "uptrend" if k % 3 else "mixed"})
    return rows


class SpecTests(unittest.TestCase):
    def test_spec_files_are_complete(self):
        for path in gate.SPECS.glob("*.yaml"):
            self.assertEqual(gate.validate_spec(gate.load_spec(path)), [], path.name)

    def test_missing_fields_and_unknown_rule(self):
        issues = "；".join(gate.validate_spec(_spec(detector="nope", primary_control="spy", mechanism="")))
        self.assertIn("mechanism", issues)
        self.assertIn("nope", issues)
        self.assertIn("主对照", issues)

    def test_bar_rises_only_with_many_trials(self):
        self.assertEqual(gate.required_t(1), 3.0)
        self.assertEqual(gate.required_t(10), 3.0)
        self.assertGreater(gate.required_t(200), 3.5)

    def test_each_rule_has_its_own_random_stream(self):
        a, b = registry(include_candidates=True)[:2]
        self.assertEqual(gate.rng_for(a).integers(1 << 30), gate.rng_for(a).integers(1 << 30))
        self.assertNotEqual(gate.rng_for(a).integers(1 << 30), gate.rng_for(b).integers(1 << 30))


class BacktestTests(unittest.TestCase):
    def test_summary_and_criteria_pass_when_both_halves_hold(self):
        s = gate.summarize(_rows(0.008, 0.006))
        self.assertEqual(s["n"], 96)
        self.assertAlmostEqual(s["vs_select_first"]["edge"], 0.008, places=4)
        crit = gate.criteria(s, _spec(), 1, {}, set())
        self.assertTrue(all(c["ok"] for c in crit), crit)

    def test_a_negative_half_fails(self):
        crit = gate.criteria(gate.summarize(_rows(0.008, -0.002)), _spec(), 1, {}, set())
        self.assertFalse(next(c for c in crit if c["name"] == "后半段超额为正")["ok"])

    def test_duplicate_of_a_passed_rule_fails_but_of_a_failed_rule_does_not(self):
        s = gate.summarize(_rows(0.008, 0.006))
        ov = {"pullback_in_uptrend": {"shared": 0.6, "corr": 0.8, "months": 48}}
        self.assertFalse(gate.criteria(s, _spec(), 1, ov, {"pullback_in_uptrend"})[-1]["ok"])
        self.assertTrue(gate.criteria(s, _spec(), 1, ov, set())[-1]["ok"])

    def test_overlap_counts_shared_fills_and_correlates_months(self):
        rows = _rows(0.008, 0.006)
        same = gate.overlap(rows, rows)
        self.assertEqual(same["shared"], 1.0)
        self.assertGreater(same["corr"], 0.99)
        elsewhere = gate.overlap(rows, [{**r, "ticker": "ZZZ"} for r in rows])
        self.assertEqual(elsewhere["shared"], 0.0)


class CritiqueTests(unittest.TestCase):
    def _critique(self, verdicts=None, reason="后半段 +1.01%"):
        verdicts = verdicts or {}
        return gate.Critique(items=[gate.CheckItem(check=c, verdict=verdicts.get(c, "通过"), reason=reason) for c in gate.CHECKS],
                             counter_argument="可能只是样本内的运气", summary="暂可接受")

    def test_every_check_answered_and_numbers_traced(self):
        payload = {"report": {"edge": 0.0101}}
        self.assertEqual(gate.validate_critique(self._critique(), payload), [])
        self.assertTrue(gate.validate_critique(self._critique(reason="年化 37.5%"), payload))
        partial = gate.Critique(items=self._critique().items[:3], counter_argument="x", summary="y")
        self.assertIn("必须且只能", "".join(gate.validate_critique(partial, payload)))

    def test_a_failed_item_fails_the_gate_and_doubts_are_shown(self):
        c = {"source": "llm", **self._critique({"single_period": "不通过", "crowding": "有疑问"}).model_dump()}
        ok, notes = gate.critique_ok(c)
        self.assertFalse(ok)
        self.assertIn("单一时期", notes[0])
        ok, notes = gate.critique_ok({"source": "llm", **self._critique({"crowding": "有疑问"}).model_dump()})
        self.assertTrue(ok)
        self.assertIn("拥挤", notes[0])

    def test_fail_verdicts_the_report_does_not_support_are_sent_back(self):
        summary = gate.summarize(_rows(0.008, 0.006))
        report = {"summary": summary, "criteria": gate.criteria(summary, _spec(), 1, {}, set())}
        wrong = self._critique({"single_period": "不通过", "duplicate": "不通过"})
        issues = gate.critique_misreads(wrong, report, "select")
        self.assertEqual(len(issues), 2)
        weak = gate.summarize(_rows(0.008, -0.002))
        report = {"summary": weak, "criteria": gate.criteria(weak, _spec(), 1, {}, set())}
        self.assertEqual(gate.critique_misreads(self._critique({"single_period": "不通过"}), report, "select"), [])

    def test_no_model_no_pass(self):
        self.assertFalse(gate.critique_ok({"source": "none", "issues": ["没有配置 AI 模型"]})[0])


class RegisterTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_spec_becomes_a_hypothesis(self):
        with patch.object(learned, "get", AsyncMock(return_value=None)), patch.object(learned, "write", AsyncMock()) as w:
            await gate.register(_spec(key="new_idea"))
        entry, actor, note = w.await_args.args
        self.assertEqual((entry["status"], actor, note), ("hypothesis", "system", "预登记"))
        self.assertEqual(entry["applies"], {"detectors": ["breakout_retest"]})

    async def test_a_spec_cannot_be_replaced(self):
        with patch.object(learned, "get", AsyncMock(return_value={"key": "x", "spec": {"a": 1}})):
            with self.assertRaises(learned.KnowledgeError):
                await gate.register(_spec())

    def test_rule_of_prefers_the_spec(self):
        en = {"kind": "rule", "applies": {"detectors": ["breakout_retest", "trend_breakout"]}}
        self.assertEqual(learned.rule_of(en), "breakout_retest")
        self.assertEqual(learned.rule_of({**en, "spec": {"detector": "power_gap"}}), "power_gap")
        self.assertIsNone(learned.rule_of({"kind": "method", "applies": {"detectors": ["x"]}}))


if __name__ == "__main__":
    unittest.main()


class ContextTests(unittest.TestCase):
    def test_when_filters_by_market_and_sector(self):
        spec = {"when": {"regime": ["uptrend"], "vol": ["low"], "sectors": ["Energy"]}}
        self.assertTrue(gate.applies(spec, "uptrend", "low", "Energy"))
        self.assertFalse(gate.applies(spec, "uptrend", "high", "Energy"))
        self.assertFalse(gate.applies(spec, "downtrend", "low", "Energy"))
        self.assertFalse(gate.applies(spec, "uptrend", "low", "Utilities"))
        self.assertTrue(gate.applies({}, "downtrend", "high", None))
        self.assertIn("低波动", gate.when_zh(spec))

    def test_when_is_validated(self):
        base = gate.load_spec(next(gate.SPECS.glob("rank_*.yaml")))
        self.assertEqual(gate.validate_spec({**base, "when": {"vol": ["high"]}}), [])
        self.assertTrue(gate.validate_spec({**base, "when": {"vol": ["extreme"]}}))
        self.assertTrue(gate.validate_spec({**base, "when": {"weekday": ["mon"]}}))
        self.assertTrue(gate.validate_spec({**base, "when": {"sectors": ["Crypto"]}}))

    def test_volatility_state_uses_no_future_data(self):
        import numpy as np

        rng = np.random.default_rng(1)
        spy = 100 * np.cumprod(1 + rng.normal(0, 0.01, 600))
        base = gate.vol_states(spy)
        later = spy.copy()
        later[400:] *= np.cumprod(1 + rng.normal(0, 0.05, 200))
        self.assertTrue((base[:400] == gate.vol_states(later)[:400]).all())
        self.assertEqual(base[0], "")
        self.assertTrue(set(base[300:]) <= {"high", "low"})


class PeerControlTests(unittest.TestCase):
    def test_peers_exclude_the_pick_and_need_enough_of_them(self):
        self.assertAlmostEqual(gate._peers((0.10 + 0.5, 7), 0.10), 0.5 / 6)
        self.assertIsNone(gate._peers((1.0, gate.MIN_SECTOR_PEERS), 0.1))
        self.assertIsNone(gate._peers(None, 0.1))

    def test_reported_control_is_summarised_but_not_a_criterion(self):
        from datetime import date

        rows = [{"date": date(2020, 1 + k % 12, 1 + k // 12), "ret": 0.02, "spy": 0.0, "ew": 0.0, "timing": 0.0, "select": 0.0,
                 "peers": 0.01, "regime": "uptrend", "vol": "low", "sector": "Energy"} for k in range(48)]
        s = gate.summarize(rows)
        self.assertAlmostEqual(s["vs_peers"]["edge"], 0.01)
        self.assertNotIn("peers", [c["name"] for c in gate.criteria(s, {"primary_control": "select"}, 1, {}, set())])
