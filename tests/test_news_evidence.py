import time
import unittest

from services.news_evidence import build_news_evidence, score_news_item


NOW = 1_700_000_000


class NewsEvidenceTest(unittest.TestCase):
    def test_effective_credibility_uses_source_impact_relevance_and_freshness(self):
        item = score_news_item(
            {
                "headline": "Fed surprise sends yields lower",
                "source": "Reuters",
                "sentiment": "positive",
                "relevance": "direct",
                "datetime": NOW - 60,
            },
            ticker="XLK",
            now_ts=NOW,
        )

        # Reuters .95 * high impact 1.2 * direct 1.2 * fresh 1.1
        self.assertAlmostEqual(item["effective_credibility"], 1.5048, places=4)
        self.assertEqual(item["market_impact"], "high")
        self.assertEqual(item["action_bias"], "allow_overweight")

    def test_noise_or_low_effective_credibility_is_ignored(self):
        item = score_news_item(
            {
                "headline": "Generic opinion recap",
                "source": "Unknown Blog",
                "sentiment": "positive",
                "relevance": "noise",
                "datetime": NOW - 60,
            },
            ticker="SPY",
            now_ts=NOW,
        )

        self.assertEqual(item["effective_credibility"], 0.0)
        self.assertEqual(item["action_bias"], "ignore")

    def test_hard_risk_overrides_to_block_new_buy(self):
        item = score_news_item(
            {
                "headline": "Regional bank faces credit stress",
                "source": "Reuters",
                "sentiment": "neutral",
                "relevance": "direct",
                "datetime": NOW - 60,
                "is_hard_event": True,
            },
            ticker="XLF",
            now_ts=NOW,
        )

        self.assertEqual(item["action_bias"], "block_new_buy")
        self.assertTrue(item["hard_risk_types"])

    def test_build_news_evidence_filters_ignored_items_from_supporting_items(self):
        evidence = build_news_evidence(
            {
                "news_context": {"macro_signals": []},
                "per_ticker_news": {
                    "SPY": [
                        {
                            "headline": "SPY direct Reuters update",
                            "source": "Reuters",
                            "sentiment": "positive",
                            "relevance": "direct",
                            "datetime": NOW - 60,
                        },
                        {
                            "headline": "SPY noisy repost",
                            "source": "Unknown Blog",
                            "sentiment": "positive",
                            "relevance": "noise",
                            "datetime": NOW - 60,
                        },
                    ]
                },
                "hard_risks_map": {},
            },
            now_ts=NOW,
        )

        spy = evidence["ticker_news_scores"]["SPY"]
        self.assertEqual(len(spy["supporting_items"]), 1)
        self.assertEqual(spy["supporting_items"][0]["headline"], "SPY direct Reuters update")
        self.assertEqual(len(evidence["ignored_items"]), 1)

    def test_hard_risk_events_are_reported_by_ticker(self):
        evidence = build_news_evidence(
            {
                "news_context": {"macro_signals": []},
                "per_ticker_news": {
                    "XLF": [
                        {
                            "headline": "Bank crisis pressure rises",
                            "source": "Reuters",
                            "sentiment": "negative",
                            "relevance": "direct",
                            "datetime": NOW - 60,
                        }
                    ]
                },
                "hard_risks_map": {"XLF": {"credit_stress": "bank funding stress"}},
            },
            now_ts=NOW,
        )

        self.assertIn("XLF", evidence["hard_risk_events"])
        self.assertIn("credit_stress", evidence["hard_risk_events"]["XLF"])
        self.assertEqual(evidence["ticker_news_scores"]["XLF"]["action_bias"], "block_new_buy")

    def test_macro_news_score_aggregates_bias_and_themes(self):
        evidence = build_news_evidence(
            {
                "news_context": {
                    "macro_signals": [
                        {
                            "driver": "fed_hawkish",
                            "direction": "negative",
                            "impact": "high",
                            "time_horizon": "short_term",
                            "confidence": "high",
                        },
                        {
                            "driver": "ai_capex",
                            "direction": "positive",
                            "impact": "medium",
                            "time_horizon": "medium_term",
                            "confidence": "medium",
                        },
                    ]
                },
                "per_ticker_news": {},
                "hard_risks_map": {},
            },
            now_ts=NOW,
        )

        macro = evidence["macro_news_score"]
        self.assertEqual(macro["overall_bias"], "mixed")
        self.assertEqual(macro["confidence"], "high")
        self.assertEqual(macro["market_impact"], "high")
        self.assertIn("fed_hawkish", macro["dominant_themes"])

    def test_missing_news_returns_data_gaps(self):
        evidence = build_news_evidence({}, now_ts=NOW)

        self.assertEqual(evidence["macro_news_score"]["data_quality"], "limited")
        self.assertIn("no ticker news evidence available", evidence["data_gaps"])
        self.assertIn("no structured macro news signals available", evidence["data_gaps"])


if __name__ == "__main__":
    unittest.main()
