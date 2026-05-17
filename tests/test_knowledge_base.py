import unittest

from services.knowledge_base import build_knowledge_context, load_knowledge_base


class KnowledgeBaseTest(unittest.TestCase):
    def test_loads_expected_v1_scope(self):
        kb = load_knowledge_base()

        self.assertGreaterEqual(kb["object_counts"]["strategies"], 3)
        self.assertGreaterEqual(kb["object_counts"]["assets"], 20)
        self.assertGreaterEqual(kb["object_counts"]["regimes"], 5)
        self.assertGreaterEqual(kb["object_counts"]["risk_principles"], 8)
        self.assertGreaterEqual(kb["object_counts"]["sources"], 7)
        self.assertIn("momentum_lite_v1", kb["strategies"])
        self.assertIn("QQQ", kb["assets"])
        self.assertIn("trending_bull", kb["regimes"])

    def test_builds_relevant_context(self):
        ctx = build_knowledge_context(
            tickers=["QQQ", "TLT", "SOXL"],
            strategy_names=["momentum_lite_v1"],
            regime="trending_bull",
            reason_codes=["high_atr", "semiconductors_concentration_high"],
        )

        self.assertTrue(ctx["available"])
        self.assertEqual(
            [item["id"] for item in ctx["strategies"]],
            ["momentum_lite_v1"],
        )
        self.assertEqual(
            [item["id"] for item in ctx["assets"]],
            ["QQQ", "TLT", "SOXL"],
        )
        self.assertIn("trending_bull", [item["id"] for item in ctx["regimes"]])
        principle_ids = [item["id"] for item in ctx["risk_principles"]]
        self.assertIn("high_atr_no_add", principle_ids)
        self.assertIn("sector_concentration", principle_ids)
        self.assertIn("leveraged_etf", principle_ids)


if __name__ == "__main__":
    unittest.main()
