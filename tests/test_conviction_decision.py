import unittest

from services.conviction_decision import (
    STAT_STATUS_EARLY_SIGNAL,
    STAT_STATUS_INDICATIVE,
    STAT_STATUS_INSUFFICIENT,
    STAT_STATUS_MONITORING_READY,
    STAT_STATUS_STATISTICALLY_MEANINGFUL,
    decision_conviction_discount,
    decision_statistical_status,
    statistical_status_for_samples,
)


class ConvictionDecisionTests(unittest.TestCase):
    def test_sample_count_status_boundaries(self):
        self.assertEqual(statistical_status_for_samples(29), STAT_STATUS_INSUFFICIENT)
        self.assertEqual(statistical_status_for_samples(30), STAT_STATUS_MONITORING_READY)
        self.assertEqual(statistical_status_for_samples(99), STAT_STATUS_MONITORING_READY)
        self.assertEqual(statistical_status_for_samples(100), STAT_STATUS_EARLY_SIGNAL)
        self.assertEqual(statistical_status_for_samples(299), STAT_STATUS_EARLY_SIGNAL)
        self.assertEqual(statistical_status_for_samples(300), STAT_STATUS_INDICATIVE)
        self.assertEqual(statistical_status_for_samples(782), STAT_STATUS_INDICATIVE)
        self.assertEqual(statistical_status_for_samples(783), STAT_STATUS_STATISTICALLY_MEANINGFUL)

    def test_sample_count_overrides_legacy_statistical_label(self):
        self.assertEqual(
            decision_statistical_status(status="early_signal", n=45),
            STAT_STATUS_MONITORING_READY,
        )
        self.assertEqual(
            decision_statistical_status(status="statistically_meaningful", n=320),
            STAT_STATUS_INDICATIVE,
        )

    def test_monitoring_ready_has_no_more_credit_than_early_signal(self):
        self.assertLessEqual(
            decision_conviction_discount(STAT_STATUS_MONITORING_READY),
            decision_conviction_discount(STAT_STATUS_EARLY_SIGNAL),
        )


if __name__ == "__main__":
    unittest.main()
