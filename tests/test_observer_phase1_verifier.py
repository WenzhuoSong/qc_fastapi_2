import json
import tempfile
import unittest
from pathlib import Path

from tools.observer_phase1_verifier import (
    CapacityConfig,
    FROZEN_CASE_IDS,
    REPORT_SCHEMA_VERSION,
    build_report,
    load_frozen_observer_cases,
    main,
    verify_obs25_mixed_batch,
    verify_outage_capacity,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "docs/fastapi_qc_trading_system_case_catalog_v1.md"
INGESTION = ROOT / "services/qc_observer_ingestion.py"


class ObserverPhase1VerifierTests(unittest.TestCase):
    def test_frozen_catalog_tracks_exactly_obs_01_through_obs_39(self):
        cases = load_frozen_observer_cases(CATALOG)

        self.assertEqual(tuple(case.case_id for case in cases), FROZEN_CASE_IDS)
        self.assertEqual({case.priority for case in cases}, {"P0"})

    def test_obs25_expected_contract_tombstones_rejected_sequence(self):
        evidence = verify_obs25_mixed_batch(INGESTION)

        self.assertTrue(evidence["expected_contract_passed"])
        self.assertEqual(
            evidence["accepted_event_ids"],
            ["outage-p0-00000001", "outage-p0-00000003"],
        )
        self.assertEqual(
            evidence["rejections"],
            [
                {
                    "event_id": "outage-p0-00000002",
                    "event_channel": "P0",
                    "event_sequence": 2,
                    "code": "payload_missing_required_fields",
                }
            ],
        )
        self.assertEqual(
            evidence["channel_receipt"],
            {
                "highest_contiguous_sequence": 3,
                "highest_seen_sequence": 3,
                "missing_sequences": [],
            },
        )

    def test_obs25_runtime_probe_reports_partial_validation_support(self):
        evidence = verify_obs25_mixed_batch(INGESTION)

        self.assertTrue(evidence["runtime_contract_passed"])
        self.assertEqual(
            evidence["runtime_probe"]["reason"],
            "per_event_acceptance_shape_detected",
        )

    def test_30_day_capacity_compression_and_replay_are_proven(self):
        evidence = verify_outage_capacity(
            CapacityConfig(
                days=30,
                events_per_day=32,
                segment_events=50,
                capacity_bytes=8 * 1024 * 1024,
            )
        )

        self.assertEqual(evidence["event_count"], 960)
        self.assertEqual(evidence["first_sequence"], 1)
        self.assertEqual(evidence["last_sequence"], 960)
        self.assertLess(evidence["compressed_spool_bytes"], evidence["raw_spool_bytes"])
        self.assertTrue(evidence["raw_capacity_passed"])
        self.assertTrue(evidence["compressed_capacity_passed"])
        self.assertTrue(evidence["replay"]["passed"])
        self.assertEqual(evidence["replay"]["accepted_first_pass"], 960)
        self.assertEqual(evidence["replay"]["duplicates_second_pass"], 960)
        self.assertEqual(evidence["replay"]["conflicts"], [])

    def test_capacity_evidence_is_byte_deterministic(self):
        config = CapacityConfig(days=3, events_per_day=8, segment_events=7)

        first = verify_outage_capacity(config)
        second = verify_outage_capacity(config)

        self.assertEqual(first, second)
        self.assertEqual(first["spool_sha256"], second["spool_sha256"])

    def test_report_is_machine_readable_and_truthfully_incomplete(self):
        report = build_report(
            CATALOG,
            INGESTION,
            CapacityConfig(days=3, events_per_day=8, segment_events=7),
        )
        by_id = {result["case_id"]: result for result in report["cases"]}

        self.assertEqual(report["schema_version"], REPORT_SCHEMA_VERSION)
        self.assertEqual(report["summary"]["case_count"], 39)
        self.assertEqual(report["summary"]["overall_status"], "incomplete")
        self.assertEqual(by_id["OBS-25"]["verification_status"], "passed")
        self.assertEqual(by_id["OBS-28"]["verification_status"], "passed")
        self.assertEqual(report["summary"]["counts"], {"passed": 2, "failed": 0, "not_run": 37})

    def test_cli_writes_stable_json_and_strict_mode_fails_incomplete_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.json"
            second = Path(directory) / "second.json"
            common = [
                "--catalog",
                str(CATALOG),
                "--ingestion-source",
                str(INGESTION),
                "--events-per-day",
                "8",
                "--capacity-bytes",
                str(8 * 1024 * 1024),
            ]

            self.assertEqual(main([*common, "--output", str(first)]), 0)
            self.assertEqual(main([*common, "--output", str(second)]), 0)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(json.loads(first.read_text())["summary"]["case_count"], 39)
            self.assertEqual(main([*common, "--output", str(second), "--strict"]), 1)


if __name__ == "__main__":
    unittest.main()
