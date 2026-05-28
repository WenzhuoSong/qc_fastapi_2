import unittest

from services.feature_authority_mode import (
    DEFAULT_FEATURE_AUTHORITY_MODE,
    YFINANCE_RESEARCH,
    normalize_feature_authority_mode,
)


class FeatureAuthorityModeTests(unittest.TestCase):
    def test_default_mode_is_yfinance_research(self):
        self.assertEqual(DEFAULT_FEATURE_AUTHORITY_MODE, YFINANCE_RESEARCH)
        self.assertEqual(normalize_feature_authority_mode(None), YFINANCE_RESEARCH)

    def test_accepts_yfinance_research_from_dict_or_string(self):
        self.assertEqual(normalize_feature_authority_mode({"value": YFINANCE_RESEARCH}), YFINANCE_RESEARCH)
        self.assertEqual(normalize_feature_authority_mode({"mode": YFINANCE_RESEARCH}), YFINANCE_RESEARCH)
        self.assertEqual(normalize_feature_authority_mode("yfinance_research"), YFINANCE_RESEARCH)

    def test_removed_modes_fall_back_to_yfinance_research(self):
        self.assertEqual(normalize_feature_authority_mode("surprise"), YFINANCE_RESEARCH)
        self.assertEqual(normalize_feature_authority_mode("audit_only"), YFINANCE_RESEARCH)
        self.assertEqual(normalize_feature_authority_mode({"value": "legacy_overlay"}), YFINANCE_RESEARCH)


if __name__ == "__main__":
    unittest.main()
