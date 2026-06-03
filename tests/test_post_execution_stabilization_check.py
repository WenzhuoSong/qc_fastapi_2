import unittest
from pathlib import Path

from services.post_execution_stabilization_check import (
    build_post_execution_stabilization_check,
)


QC_FILE = Path(__file__).resolve().parents[2] / "quantconnect_files" / "test1.py"
REPO_ROOT = Path(__file__).resolve().parents[1]


class PostExecutionStabilizationCheckTests(unittest.TestCase):
    def test_global_definition_of_done_check_passes_current_contracts(self):
        report = build_post_execution_stabilization_check(
            qc_fallback_path=QC_FILE,
            repo_root=REPO_ROOT,
        )

        self.assertEqual(report["contract_version"], "post_execution_stabilization_check_v1")
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["execution_authority"], "none")
        self.assertEqual(report["target_weight_mutation"], "none")
        self.assertTrue(report["diagnostic_only"])
        self.assertEqual(report["failed_count"], 0)
        self.assertEqual(report["mapping_summary"]["hard_mapping_error_count"], 0)
        self.assertTrue(report["qc_fallback_policy"]["ok"])

    def test_missing_qc_fallback_path_fails_policy_sync_check_only(self):
        report = build_post_execution_stabilization_check(
            qc_fallback_path=None,
            repo_root=REPO_ROOT,
        )

        failed_checks = [
            row["check"]
            for row in report["checks"]
            if not row["passed"]
        ]
        self.assertEqual(failed_checks, ["qc_fallback_policy_check_available"])
        self.assertEqual(report["qc_fallback_policy"]["reason"], "qc_fallback_path_not_provided")


if __name__ == "__main__":
    unittest.main()
