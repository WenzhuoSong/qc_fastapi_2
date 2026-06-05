import unittest
from datetime import date, datetime

from services.validation_observation_loop import (
    OBS_ACTIVE_BASKET,
    OBS_EXECUTION_TRUTH,
    OBS_HEDGE_INTENT,
    STATUS_COMPLETED,
    STATUS_OBSERVED,
    STATUS_PENDING_OUTCOME,
    build_execution_truth_observation_record,
    build_validation_observation_records_from_analysis,
    complete_hedge_observation_if_mature,
    forward_return_from_feature_rows,
)


class ValidationObservationLoopTests(unittest.TestCase):
    def test_builds_hedge_and_basket_observations_from_analysis(self):
        records = build_validation_observation_records_from_analysis({
            "id": 42,
            "analyzed_at": datetime(2026, 6, 6, 10, 0, 0),
            "trigger_type": "scheduled",
            "execution_status": "pending",
            "risk_output": {
                "hedge_intent_outcome": {
                    "date": "2026-06-06",
                    "outcome_status": "pending_t5",
                    "triggered": True,
                    "severity": 0.52,
                    "add_hedge_etf": False,
                    "candidate_hedge_instrument": "PSQ",
                },
                "active_basket_policy": {
                    "active_count": 8,
                    "target_active_count_min": 4,
                    "target_active_count_max": 10,
                    "within_target_active_count": True,
                    "subscale_count": 0,
                    "floor_cleared_count": 1,
                },
            },
        })

        by_type = {row["observation_type"]: row for row in records}
        self.assertEqual(set(by_type), {OBS_HEDGE_INTENT, OBS_ACTIVE_BASKET})
        self.assertEqual(by_type[OBS_HEDGE_INTENT]["status"], STATUS_PENDING_OUTCOME)
        self.assertEqual(by_type[OBS_HEDGE_INTENT]["horizon_days"], 5)
        self.assertEqual(by_type[OBS_ACTIVE_BASKET]["status"], STATUS_OBSERVED)
        self.assertEqual(by_type[OBS_ACTIVE_BASKET]["metrics"]["active_count"], 8)
        self.assertEqual(by_type[OBS_ACTIVE_BASKET]["execution_authority"], "none")

    def test_forward_return_uses_future_price_path(self):
        rows = [
            {"ticker": "SPY", "trading_date": "2026-06-01", "adj_close_price": 100.0},
            {"ticker": "SPY", "trading_date": "2026-06-02", "adj_close_price": 101.0},
            {"ticker": "SPY", "trading_date": "2026-06-03", "adj_close_price": 102.0},
            {"ticker": "SPY", "trading_date": "2026-06-04", "adj_close_price": 103.0},
            {"ticker": "SPY", "trading_date": "2026-06-05", "adj_close_price": 104.0},
            {"ticker": "SPY", "trading_date": "2026-06-08", "adj_close_price": 95.0},
        ]

        result = forward_return_from_feature_rows(
            rows,
            ticker="SPY",
            observation_date=date(2026, 6, 1),
            horizon_days=5,
        )

        self.assertEqual(result["label_date"], "2026-06-08")
        self.assertAlmostEqual(result["forward_return"], -0.05)

    def test_backfills_mature_hedge_observation(self):
        records = build_validation_observation_records_from_analysis({
            "id": 43,
            "analyzed_at": datetime(2026, 6, 1, 10, 0, 0),
            "risk_output": {
                "hedge_intent_outcome": {
                    "date": "2026-06-01",
                    "outcome_status": "pending_t5",
                    "triggered": False,
                    "candidate_hedge_instrument": "SH",
                }
            },
        })
        feature_rows = [
            {"ticker": "SPY", "trading_date": "2026-06-01", "adj_close_price": 100.0},
            {"ticker": "SPY", "trading_date": "2026-06-02", "adj_close_price": 99.0},
            {"ticker": "SPY", "trading_date": "2026-06-03", "adj_close_price": 98.0},
            {"ticker": "SPY", "trading_date": "2026-06-04", "adj_close_price": 97.0},
            {"ticker": "SPY", "trading_date": "2026-06-05", "adj_close_price": 96.0},
            {"ticker": "SPY", "trading_date": "2026-06-08", "adj_close_price": 94.0},
            {"ticker": "SH", "trading_date": "2026-06-01", "adj_close_price": 20.0},
            {"ticker": "SH", "trading_date": "2026-06-02", "adj_close_price": 20.2},
            {"ticker": "SH", "trading_date": "2026-06-03", "adj_close_price": 20.4},
            {"ticker": "SH", "trading_date": "2026-06-04", "adj_close_price": 20.6},
            {"ticker": "SH", "trading_date": "2026-06-05", "adj_close_price": 20.8},
            {"ticker": "SH", "trading_date": "2026-06-08", "adj_close_price": 21.0},
        ]

        updated = complete_hedge_observation_if_mature(
            records[0],
            feature_rows,
            as_of_date=date(2026, 6, 8),
        )

        self.assertEqual(updated["status"], STATUS_COMPLETED)
        self.assertEqual(updated["outcome_payload"]["outcome_status"], "completed_t5")
        self.assertEqual(updated["outcome_payload"]["threshold_assessment"], "too_conservative")
        self.assertAlmostEqual(updated["metrics"]["spy_return_5d"], -0.06)

    def test_builds_execution_truth_observation(self):
        record = build_execution_truth_observation_record({
            "analysis_id": 44,
            "command_id": "analysis_44",
            "executed_at": datetime(2026, 6, 6, 10, 30, 0),
            "status": "deduped",
            "qc_status": "not_sent",
            "command_payload": {
                "order_summary": {
                    "execution_state": "noop_reconciled",
                    "is_noop": True,
                    "actual_order_count": 0,
                    "filled_order_count": 0,
                }
            },
        })

        self.assertEqual(record["observation_type"], OBS_EXECUTION_TRUTH)
        self.assertEqual(record["status"], STATUS_COMPLETED)
        self.assertTrue(record["outcome_payload"]["is_noop"])
        self.assertEqual(record["metrics"]["actual_order_count"], 0)
        self.assertEqual(
            record["recommendation"]["operator_action"],
            "review_dedupe_or_snapshot_freshness",
        )


if __name__ == "__main__":
    unittest.main()
