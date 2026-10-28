import json
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from scout import knowledge, learned
from scout.detectors import registry
from scout.insight import LensView, _rule_check, evidence_keys, sub_pack, validate_lenses, GROUP_CONTEXT, GROUPS


def _entry(**over):
    base = {"key": "x", "kind": "rule", "polarity": "positive", "status": "backtested", "title": "t", "claim": "c",
            "context": {"kind": ["stock"]}, "applies": {"detectors": ["breakout_retest"]},
            "evidence": [{"source": "rule_benchmarks", "date": "2026-10-28", "summary": "相对同日随机股票 +0.89%（t 4.97）"}]}
    base.update(over)
    return base


class SeedTests(unittest.TestCase):
    def test_seed_entries_are_valid_and_point_at_real_rules_and_lenses(self):
        entries = learned.seed_entries()
        keys = [en["key"] for en in entries]
        self.assertEqual(len(keys), len(set(keys)))
        rules = {d.name for d in registry(include_candidates=True)}
        lenses = {lens.id for lens in knowledge.lenses()}
        for en in entries:
            self.assertEqual(learned.validate(en), [], en["key"])
            self.assertTrue(set((en.get("applies") or {}).get("detectors") or []) <= rules, en["key"])
            self.assertTrue(set((en.get("applies") or {}).get("lenses") or []) <= lenses, en["key"])
            self.assertTrue(en.get("recheck_on"), en["key"])
            self.assertNotEqual(en["status"], "confirmed", "only the user confirms")
            if en["polarity"] == "negative":
                self.assertEqual(en["status"], "rejected", en["key"])

    def test_every_live_rule_has_a_verdict(self):
        covered = {d for en in learned.seed_entries() for d in (en.get("applies") or {}).get("detectors") or []}
        for d in registry(include_candidates=True):
            self.assertIn(d.name, covered)


class GateTests(unittest.TestCase):
    def test_no_skipping_steps(self):
        issues = learned.check_transition(_entry(status="hypothesis"), _entry(status="confirmed"), "user")
        self.assertTrue(issues)
        self.assertEqual(learned.check_transition(_entry(status="hypothesis"), _entry(status="backtested"), "system"), [])

    def test_only_the_user_confirms(self):
        old, new = _entry(status="forward"), _entry(status="confirmed")
        self.assertIn("只有用户", "".join(learned.check_transition(old, new, "system")))
        self.assertEqual(learned.check_transition(old, new, "user"), [])

    def test_judgments_skip_the_backtest(self):
        old = _entry(kind="judgment", status="hypothesis")
        self.assertEqual(learned.check_transition(old, _entry(kind="judgment", status="forward"), "system"), [])
        self.assertTrue(learned.check_transition(_entry(status="hypothesis"), _entry(status="forward"), "system"))

    def test_new_entries_start_as_hypotheses_except_seeds(self):
        self.assertTrue(learned.check_transition(None, _entry(), "system"))
        self.assertEqual(learned.check_transition(None, _entry(status="hypothesis"), "system"), [])
        self.assertEqual(learned.check_transition(None, _entry(), "seed"), [])

    def test_evidence_required_beyond_hypothesis(self):
        self.assertTrue(learned.validate(_entry(evidence=[])))
        self.assertEqual(learned.validate(_entry(status="hypothesis", evidence=[])), [])


class StoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_write_adds_a_version(self):
        old = {**_entry(status="forward"), "version": 2, "recheck_on": date(2027, 4, 28)}
        with patch.object(learned, "get", AsyncMock(return_value=old)), \
                patch.object(learned.db, "execute", AsyncMock()) as ex:
            out = await learned.move("x", "confirmed", "user", "前向 8 个月达标")
        self.assertEqual(out["version"], 3)
        params = ex.await_args.args[1]
        self.assertEqual((params["status"], params["changed_by"], params["note"]), ("confirmed", "user", "前向 8 个月达标"))
        self.assertEqual(json.loads(params["applies"]), {"detectors": ["breakout_retest"]})

    async def test_move_needs_a_reason(self):
        with self.assertRaises(learned.KnowledgeError):
            await learned.move("x", "rejected", "user", " ")

    async def test_seed_never_overwrites(self):
        have = [{"key": en["key"]} for en in learned.seed_entries()[1:]]
        with patch.object(learned.db, "fetch_all", AsyncMock(return_value=have)), \
                patch.object(learned, "write", AsyncMock()) as w:
            added = await learned.seed()
        self.assertEqual(added, [learned.seed_entries()[0]["key"]])
        self.assertEqual(w.await_count, 1)


class UseTests(unittest.TestCase):
    def test_relevant_by_rule_lens_context_and_status(self):
        entries = [
            _entry(key="rule_hit"),
            _entry(key="lens_hit", applies={"lenses": ["support_resistance"]}, status="rejected", polarity="negative"),
            _entry(key="other_rule", applies={"detectors": ["power_gap"]}),
            _entry(key="etf_only", context={"kind": ["etf"]}),
            _entry(key="draft", status="hypothesis"),
            _entry(key="topic_only", applies={"topics": ["exit"]}),
        ]
        got = learned.relevant(entries, detector="breakout_retest", lenses={"support_resistance"}, ctx={"kind": "stock"})
        self.assertEqual([en["key"] for en in got], ["lens_hit", "rule_hit"])     # negative before unverified

    def test_context_block_tells_the_ai_how_to_use_each_status(self):
        block, scope = learned.context_block([_entry(), _entry(key="y", status="rejected", polarity="negative")])
        self.assertEqual([b["key"] for b in block], ["kb_1", "kb_2"])
        self.assertIn("尚未验证", block[0]["用法"])
        self.assertIn("不能当作", block[1]["用法"])
        self.assertIn("+0.89%", block[0]["证据"])
        self.assertEqual(scope["kb_1"]["detectors"], ["breakout_retest"])
        self.assertEqual(_rule_check("breakout_retest", block, scope)[:4], "回测通过")
        self.assertEqual(_rule_check("power_gap", block, scope), "尚未做公平基准检验")

    def test_due_for_recheck(self):
        entries = [_entry(recheck_on="2026-10-01"), _entry(key="y", recheck_on="2027-10-01"),
                   _entry(key="z", status="rejected", recheck_on="2026-10-01")]
        self.assertEqual([en["key"] for en in learned.due(entries, date(2026, 10, 28))], ["x"])


def _pack_with_knowledge():
    lenses = [{"id": l.id, "title": l.title, "method": "", "evidence": []} for l in knowledge.lenses()]
    lenses[1]["evidence"] = [knowledge.evidence("rsi14", 38.0).as_dict()]
    block, scope = learned.context_block([
        _entry(key="sr", applies={"lenses": ["support_resistance"]}),
        _entry(key="rule", applies={"detectors": ["pullback_in_uptrend"]}, status="rejected", polarity="negative"),
    ])
    return {"ticker": "NVDA", "name": "x", "type": "个股", "data_date": "2026-10-23", "lenses": lenses,
            "context": {"scout_view": {"结论": "x", "要点": {}}, "driver": "混合", "news": "近 7 天没有可匹配的新闻",
                        "opportunity": {"规则": "强势回调"}, "knowledge": block},
            "knowledge_scope": scope, "opportunity_detector": "pullback_in_uptrend"}


class InsightTests(unittest.TestCase):
    def test_each_group_sees_only_its_knowledge(self):
        pack = _pack_with_knowledge()
        tech = sub_pack(pack, GROUPS["技术与市场"], GROUP_CONTEXT["技术与市场"])
        self.assertEqual([k["key"] for k in tech["context"]["knowledge"]], ["kb_1"])
        events = sub_pack(pack, GROUPS["事件与财报"], GROUP_CONTEXT["事件与财报"])
        self.assertEqual([k["key"] for k in events["context"]["knowledge"]], ["kb_2"])     # the opportunity's rule
        fund = sub_pack(pack, GROUPS["基本面与预期"], GROUP_CONTEXT["基本面与预期"])
        self.assertNotIn("knowledge", fund["context"])

    def test_knowledge_is_citable(self):
        pack = _pack_with_knowledge()
        self.assertIn("kb_1", evidence_keys(pack))
        sp = sub_pack(pack, GROUPS["技术与市场"], GROUP_CONTEXT["技术与市场"])
        views = [LensView(lens=b["id"], verdict="中性" if b["evidence"] else "不适用",
                          points=["回测显示相对同日随机股票 +0.89%，尚未验证"] if b["evidence"] else ["没有相关数据"],
                          evidence=["rsi14", "kb_1"] if b["evidence"] else []) for b in sp["lenses"]]
        self.assertEqual(validate_lenses(views, sp, ""), [])
        self.assertEqual(knowledge.humanize("见（kb_1）"), "见")


if __name__ == "__main__":
    unittest.main()


class PushTests(unittest.TestCase):
    def test_rules_that_failed_the_fair_test_are_not_pushed(self):
        entries = [_entry(key="a", status="rejected", polarity="negative", applies={"detectors": ["power_gap"]}),
                   _entry(key="b", status="forward", spec={"detector": "breakout_retest"}),
                   _entry(key="c", kind="method", status="rejected", polarity="negative", applies={"detectors": ["volume_shock"]}),
                   _entry(key="d", status="retired", applies={"detectors": ["pullback_in_uptrend", "x"]})]
        self.assertEqual(learned.held_back(entries), {"power_gap", "pullback_in_uptrend"})

    def test_all_three_live_rules_are_held_back_by_the_seeds(self):
        live = {d.name for d in registry(include_candidates=False)}
        self.assertEqual(learned.held_back(learned.seed_entries()) & live, live)
