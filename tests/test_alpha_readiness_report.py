import unittest

from services.alpha_readiness_report import build_alpha_readiness_report


def _mapping_audit(*, coverage=1.0, errors=0):
    return {
        "strategy_coverage": {
            "momentum_lite_v1": {
                "coverage_pct": coverage,
                "eligible_ticker_count": 10,
                "voted_or_watch_rows": int(round(coverage * 10)),
                "mapped_rows": int(round(coverage * 10)),
                "watch_rows": 0,
                "mapping_error_rows": errors,
            }
        }
    }


def _evidence(*, status="advisory"):
    return {
        "strategy_rows": [
            {
                "strategy": "momentum_lite_v1",
                "suggested_use": status,
            }
        ],
        "evidence_card_rows": [
            {"strategy": "momentum_lite_v1", "ticker": "SPY", "vote_status": "voted"},
            {"strategy": "momentum_lite_v1", "ticker": "QQQ", "vote_status": "voted"},
            {
                "strategy": "momentum_lite_v1",
                "ticker": "DRAM",
                "vote_status": "abstain",
                "abstain_reason": "insufficient_history",
            },
        ],
    }


def _profiles(*, source_buckets=None, sample_count=30, residual=0.01):
    return {
        "rows": [
            {
                "strategy_id": "momentum_lite_v1",
                "regime": "trending_bull",
                "source_buckets": source_buckets or ["live_paper"],
                "sample_count": sample_count,
                "residual_alpha": residual,
                "independence_cluster_id": "momentum_cluster",
                "max_positive_correlation": 0.32,
            }
        ]
    }


class AlphaReadinessReportTests(unittest.TestCase):
    def test_candidate_requires_live_samples_clean_mapping_and_non_negative_residual(self):
        report = build_alpha_readiness_report(
            mapping_audit=_mapping_audit(),
            strategy_evidence=_evidence(),
            alpha_decision_profiles=_profiles(),
        )

        row = report["rows"][0]
        self.assertEqual(report["execution_authority"], "none")
        self.assertTrue(report["diagnostic_only"])
        self.assertEqual(row["suggested_authority"], "candidate")
        self.assertEqual(row["live_sample_count"], 30)
        self.assertEqual(row["mapping_coverage_pct"], 1.0)
        self.assertEqual(row["voted_signal_count"], 2)
        self.assertEqual(row["abstain_count_by_reason"], {"insufficient_history": 1})
        self.assertEqual(row["residual_alpha_regime_specific"], {"trending_bull": 0.01})

    def test_historical_only_samples_do_not_create_candidate(self):
        report = build_alpha_readiness_report(
            mapping_audit=_mapping_audit(),
            strategy_evidence=_evidence(),
            alpha_decision_profiles=_profiles(source_buckets=["historical_prior"], sample_count=300, residual=0.04),
        )

        row = report["rows"][0]
        self.assertEqual(row["suggested_authority"], "advisory")
        self.assertEqual(row["live_sample_count"], 0)
        self.assertIn("live_sample_count_below_30", row["readiness_reasons"])

    def test_current_mapping_errors_disable_strategy(self):
        report = build_alpha_readiness_report(
            mapping_audit=_mapping_audit(errors=1),
            strategy_evidence=_evidence(),
            alpha_decision_profiles=_profiles(),
        )

        row = report["rows"][0]
        self.assertEqual(row["suggested_authority"], "disabled")
        self.assertIn("current_hard_mapping_errors", row["authority_blockers"])

    def test_negative_residual_keeps_strategy_advisory(self):
        report = build_alpha_readiness_report(
            mapping_audit=_mapping_audit(),
            strategy_evidence=_evidence(),
            alpha_decision_profiles=_profiles(residual=-0.001),
        )

        row = report["rows"][0]
        self.assertEqual(row["suggested_authority"], "advisory")
        self.assertIn("residual_alpha_negative", row["readiness_reasons"])

    def test_explicitly_disabled_strategy_remains_disabled(self):
        report = build_alpha_readiness_report(
            mapping_audit=_mapping_audit(),
            strategy_evidence=_evidence(status="disabled"),
            alpha_decision_profiles=_profiles(),
        )

        row = report["rows"][0]
        self.assertEqual(row["suggested_authority"], "disabled")
        self.assertIn("strategy_explicitly_disabled", row["authority_blockers"])


if __name__ == "__main__":
    unittest.main()
