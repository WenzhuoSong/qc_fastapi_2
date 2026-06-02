import unittest

from services.weight_source_contract import (
    assert_no_forbidden_target_builder_inputs,
    classify_weight_column,
    dashboard_weight_source_labels,
    weight_source_contract_summary,
)


class WeightSourceContractTests(unittest.TestCase):
    def test_contract_labels_separate_executable_advisory_and_reference(self):
        labels = {row["column"]: row for row in dashboard_weight_source_labels()}

        self.assertEqual(labels["final_target"]["authority"], "executable")
        self.assertEqual(labels["diagnostic_llm_target"]["authority"], "advisory_only")
        self.assertEqual(labels["baseline_reference_weights"]["authority"], "reference_only")
        self.assertEqual(labels["final_target"]["visual_class"], "weight-executable")
        self.assertEqual(labels["diagnostic_llm_target"]["visual_class"], "weight-advisory")
        self.assertEqual(labels["baseline_reference_weights"]["visual_class"], "weight-reference")

    def test_forbidden_target_builder_input_scan_is_recursive(self):
        with self.assertRaisesRegex(AssertionError, "nested.llm_adjusted_weights"):
            assert_no_forbidden_target_builder_inputs(
                {"safe": {"nested": {"llm_adjusted_weights": {"SPY": 0.5}}}}
            )

    def test_target_weights_are_registered_contract_output(self):
        summary = weight_source_contract_summary()
        self.assertEqual(summary["contract_version"], "weight_source_contract_v1")
        self.assertEqual(summary["executable_target_key"], "target_weights")
        self.assertIn("pc_shadow_weights", summary["forbidden_target_builder_input_keys"])
        self.assertEqual(classify_weight_column("target_builder_target")["authority"], "executable")


if __name__ == "__main__":
    unittest.main()
