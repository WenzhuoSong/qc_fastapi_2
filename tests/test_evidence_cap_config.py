import unittest
from datetime import UTC, datetime, timedelta

from services.evidence_cap_config import (
    calibration_freshness,
    default_evidence_cap_config,
    resolve_evidence_cap_mode,
)


class EvidenceCapConfigTest(unittest.TestCase):
    def test_defaults_are_observe_only(self):
        cfg = default_evidence_cap_config({})

        self.assertEqual(cfg["mode"], "observe")
        self.assertEqual(cfg["min_observe_cycles"], 10)
        self.assertEqual(cfg["max_would_clip_rate"], 0.30)
        self.assertEqual(cfg["max_calibration_age_days"], 7.0)
        self.assertTrue(cfg["require_fresh_calibration"])

        resolved = resolve_evidence_cap_mode(cfg)
        self.assertEqual(resolved["configured_mode"], "observe")
        self.assertEqual(resolved["effective_mode"], "observe")
        self.assertEqual(resolved["execution_effect"], "diagnostic_only")

    def test_gated_falls_back_to_observe_without_readiness_metrics(self):
        resolved = resolve_evidence_cap_mode({"mode": "gated"})

        self.assertEqual(resolved["configured_mode"], "gated")
        self.assertEqual(resolved["effective_mode"], "observe")
        self.assertEqual(resolved["blocked_reason"], "enforcement_criteria_not_met")
        self.assertIn("insufficient_observe_cycles", resolved["gate_blockers"])
        self.assertIn("missing_would_clip_rate", resolved["gate_blockers"])

    def test_gated_enables_tighten_only_when_readiness_passes(self):
        resolved = resolve_evidence_cap_mode(
            {
                "mode": "gated",
                "observe_cycles": 12,
                "min_observe_cycles": 10,
                "would_clip_rate": 0.20,
                "max_would_clip_rate": 0.30,
                "calibration_generated_at": datetime.now(UTC).isoformat(),
            }
        )

        self.assertEqual(resolved["effective_mode"], "gated")
        self.assertTrue(resolved["criteria_met"])
        self.assertEqual(resolved["execution_effect"], "tighten_only")
        self.assertEqual(resolved["gate_blockers"], [])
        self.assertTrue(resolved["calibration_freshness"]["fresh"])

    def test_gated_falls_back_to_observe_when_calibration_missing_or_stale(self):
        fresh_base = {
            "mode": "gated",
            "observe_cycles": 12,
            "min_observe_cycles": 10,
            "would_clip_rate": 0.20,
            "max_would_clip_rate": 0.30,
        }

        missing = resolve_evidence_cap_mode(fresh_base)
        self.assertEqual(missing["effective_mode"], "observe")
        self.assertIn("missing_calibration_generated_at", missing["gate_blockers"])

        stale = resolve_evidence_cap_mode({
            **fresh_base,
            "calibration_generated_at": (datetime.now(UTC) - timedelta(days=8)).isoformat(),
        })
        self.assertEqual(stale["effective_mode"], "observe")
        self.assertIn("calibration_data_stale", stale["gate_blockers"])

    def test_calibration_freshness_reports_age(self):
        now = datetime(2026, 5, 28, tzinfo=UTC)
        result = calibration_freshness(
            generated_at=datetime(2026, 5, 25, tzinfo=UTC).isoformat(),
            max_age_days=7,
            now=now,
        )

        self.assertTrue(result["fresh"])
        self.assertEqual(result["age_days"], 3.0)

    def test_plan_named_young_etf_criterion_blocks_when_false(self):
        resolved = resolve_evidence_cap_mode(
            {
                "mode": "gated",
                "observe_cycles": 12,
                "would_clip_rate": 0.20,
                "calibration_generated_at": datetime.now(UTC).isoformat(),
                "young_etf_cap_within_expected_range": False,
            }
        )

        self.assertEqual(resolved["effective_mode"], "observe")
        self.assertEqual(resolved["blocked_reason"], "enforcement_criteria_not_met")
        self.assertIn("young_etf_cap_within_expected_range", resolved["gate_blockers"])


if __name__ == "__main__":
    unittest.main()
