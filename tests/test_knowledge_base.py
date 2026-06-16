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

    def test_historical_prior_candidates_are_review_only_static_metadata(self):
        kb = load_knowledge_base()

        for strategy_id in (
            "momentum_lite_v1",
            "dual_momentum_rotation",
            "sector_theme_relative_strength_lite",
        ):
            candidates = kb["strategies"][strategy_id].get("historical_prior_candidates")
            self.assertIsInstance(candidates, list)
            self.assertGreaterEqual(len(candidates), 1)
            for candidate in candidates:
                self.assertEqual(candidate.get("status"), "review_only")
                self.assertEqual(candidate.get("basis"), "strategy_signal_outcomes")
                self.assertIn("defensive", candidate.get("untested_regimes") or [])
                self.assertIn("choppy", candidate.get("untested_regimes") or [])
                self.assertIn(
                    "review_only_not_execution_authority",
                    candidate.get("caveats") or [],
                )
                self.assertIn(
                    "requires_positive_excess_through_non_bull_regime",
                    candidate.get("upgrade_gate") or [],
                )

        ctx = build_knowledge_context(
            strategy_names=["momentum_lite_v1"],
            regime="trending_bull",
        )
        self.assertNotIn("historical_prior_candidates", ctx["strategies"][0])


if __name__ == "__main__":
    unittest.main()
