import unittest
from datetime import date

from services.feature_provenance import (
    annotate_snapshot_row_provenance,
    summarize_feature_provenance,
)


class FeatureProvenanceTests(unittest.TestCase):
    def test_summarizes_yfinance_and_stale_fields(self):
        holdings = [
            {
                "ticker": "SPY",
                "mom_252d": 0.12,
                "feature_sources": [
                    {
                        "source": "yfinance",
                        "filled_fields": ["mom_252d"],
                        "trading_date": "2026-05-01",
                    }
                ],
            },
            annotate_snapshot_row_provenance(
                {"ticker": "QQQ", "mom_60d": 0.08},
                source="qc_daily_snapshot",
                as_of="2026-05-14T20:00:00Z",
                trading_date="2026-05-14",
            ),
        ]

        summary = summarize_feature_provenance(
            holdings,
            as_of=date(2026, 5, 14),
            stale_after_days=5,
        )

        self.assertTrue(summary["has_stale_fields"])
        self.assertEqual(summary["yfinance_filled_fields"]["SPY"], ["mom_252d"])
        self.assertIn("mom_252d", summary["stale_fields"]["SPY"])
        self.assertIn("qc_daily_snapshot", summary["source_counts"])
        self.assertIn("daily_research", summary["authority_counts"])
        self.assertIn("qc_eod_audit", summary["authority_counts"])

    def test_annotates_authority_and_canonical_aliases(self):
        row = annotate_snapshot_row_provenance(
            {"ticker": "SPY", "mom_60d": 0.08, "weight_current": 0.12},
            source="qc_heartbeat",
            as_of="2026-05-14T15:00:00Z",
            trading_date="2026-05-14",
        )

        source = row["feature_sources"][0]

        self.assertEqual(source["authority_by_field"]["weight_current"], "live_state")
        self.assertEqual(source["authority_by_field"]["mom_60d"], "legacy_debug")
        self.assertEqual(source["canonical_aliases"]["mom_60d"], "return_60d")

    def test_merge_feature_sources_preserves_authority_metadata(self):
        first = annotate_snapshot_row_provenance(
            {"ticker": "SPY", "mom_60d": 0.08},
            source="qc_heartbeat",
            as_of="2026-05-14T15:00:00Z",
            trading_date="2026-05-14",
        )
        second = annotate_snapshot_row_provenance(
            {"ticker": "SPY", "weight_current": 0.12},
            source="qc_heartbeat",
            as_of="2026-05-14T15:00:00Z",
            trading_date="2026-05-14",
        )

        from services.feature_provenance import merge_feature_sources

        merged = merge_feature_sources(first, second)
        source = merged[0]

        self.assertEqual(sorted(source["filled_fields"]), ["mom_60d", "weight_current"])
        self.assertEqual(source["authority_by_field"]["mom_60d"], "legacy_debug")
        self.assertEqual(source["authority_by_field"]["weight_current"], "live_state")


if __name__ == "__main__":
    unittest.main()
