import unittest

from services.feature_authority_mode import (
    AUDIT_ONLY,
    DEFAULT_FEATURE_AUTHORITY_MODE,
    LEGACY_OVERLAY,
    YFINANCE_RESEARCH,
    feature_authority_rollback_config,
    normalize_feature_authority_mode,
)


class FeatureAuthorityModeTests(unittest.TestCase):
    def test_default_mode_is_audit_only(self):
        self.assertEqual(DEFAULT_FEATURE_AUTHORITY_MODE, AUDIT_ONLY)
        self.assertEqual(normalize_feature_authority_mode(None), AUDIT_ONLY)

    def test_accepts_supported_modes_from_dict_or_string(self):
        self.assertEqual(normalize_feature_authority_mode({"value": YFINANCE_RESEARCH}), YFINANCE_RESEARCH)
        self.assertEqual(normalize_feature_authority_mode({"mode": LEGACY_OVERLAY}), LEGACY_OVERLAY)
        self.assertEqual(normalize_feature_authority_mode("audit_only"), AUDIT_ONLY)

    def test_invalid_mode_falls_back_to_audit_only(self):
        self.assertEqual(normalize_feature_authority_mode("surprise"), AUDIT_ONLY)

    def test_rollback_config_is_non_destructive_legacy_overlay(self):
        cfg = feature_authority_rollback_config(previous_mode=YFINANCE_RESEARCH, reason="pipeline_regression")

        self.assertEqual(cfg["value"], LEGACY_OVERLAY)
        self.assertEqual(normalize_feature_authority_mode(cfg), LEGACY_OVERLAY)
        self.assertEqual(cfg["rollback"]["previous_mode"], YFINANCE_RESEARCH)
        self.assertEqual(cfg["rollback"]["reason"], "pipeline_regression")
        self.assertTrue(cfg["rollback"]["preserve_audit_report"])
        self.assertTrue(cfg["rollback"]["no_database_rollback"])
        self.assertTrue(cfg["rollback"]["preserve_provenance_fields"])


if __name__ == "__main__":
    unittest.main()
