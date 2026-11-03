import unittest

from scout import propagation as pp

LINKS = [{"target": "MU", "relation": "supplier", "quote": "We purchase memory from Micron", "filing_of": "NVDA"},
         {"target": "AMD", "relation": "competitor", "quote": "our competitors include AMD", "filing_of": "NVDA"}]


class ValidateTests(unittest.TestCase):
    def test_only_linked_directional_reasoned_targets(self):
        raw = {"material": True, "targets": [
            {"ticker": "MU", "impact": "利好", "mechanism": "NVDA 上调出货 → 作为存储器供应商订单可能增加", "confidence": "medium"},
            {"ticker": "AMD", "impact": "不确定", "mechanism": "竞争格局变化方向不明", "confidence": "low"},
            {"ticker": "INTC", "impact": "利空", "mechanism": "不在关系图里，应该被丢弃的条目", "confidence": "low"},
            {"ticker": "MU", "impact": "利空", "mechanism": "短", "confidence": "low"}]}
        out = pp.validate(raw, LINKS)
        self.assertEqual([(o["ticker"], o["impact"], o["link"]["relation"]) for o in out], [("MU", 1, "supplier")])

    def test_a_reason_naming_another_relation_is_dropped_and_at_most_four_kept(self):
        raw = {"material": True, "targets": [{"ticker": "MU", "impact": "利好", "mechanism": "作为竞争对手可能获得竞争机会", "confidence": "low"}]}
        self.assertEqual(pp.validate(raw, LINKS), [])
        mirror = {"material": True, "targets": [{"ticker": "MU", "impact": "利空", "mechanism": "NVDA 是它的客户，砍单会减少它的销售", "confidence": "medium"}]}
        self.assertEqual(len(pp.validate(mirror, LINKS)), 1)            # supplier of NVDA = NVDA is its customer
        many = [{"target": f"T{i}", "relation": "competitor", "quote": "q", "filing_of": "S"} for i in range(8)]
        raw = {"material": True, "targets": [{"ticker": f"T{i}", "impact": "利空", "mechanism": "同一价格环境，竞争对手同样承压",
                                              "confidence": "high" if i == 7 else "low"} for i in range(8)]}
        out = pp.validate(raw, many)
        self.assertEqual((len(out), out[0]["ticker"]), (4, "T7"))

    def test_immaterial_news_gives_nothing(self):
        raw = {"material": False, "targets": [{"ticker": "MU", "impact": "利好", "mechanism": "随便写的一个渠道说明", "confidence": "low"}]}
        self.assertEqual(pp.validate(raw, LINKS), [])


class PushTests(unittest.TestCase):
    def test_holdings_first_and_labelled_as_inference(self):
        sig = lambda t: {"ticker": t, "impact": 1, "mechanism": "渠道说明", "confidence": "medium",  # noqa: E731
                         "link": {"relation": "supplier"}, "src": {"ticker": "NVDA", "headline": "h"}, "reaction": {"own_ret_1d": 0.012}}
        low = {**sig("QCOM"), "confidence": "low"}
        txt = pp.push_text([sig("AMD"), sig("MU"), low], held={"MU"}, watch=set())
        self.assertNotIn("QCOM", txt)                                  # low confidence: recorded, not pushed
        self.assertIn("推断，未经检验", txt)
        self.assertLess(txt.index("MU"), txt.index("AMD"))
        self.assertIn("【持仓】", txt)
        self.assertIsNone(pp.push_text([], set(), set()))


if __name__ == "__main__":
    unittest.main()
