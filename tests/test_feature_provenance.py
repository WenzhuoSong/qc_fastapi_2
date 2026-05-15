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


if __name__ == "__main__":
    unittest.main()
