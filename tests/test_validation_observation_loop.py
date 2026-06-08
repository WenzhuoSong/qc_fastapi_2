import unittest
from datetime import date, datetime

from services.validation_observation_loop import (
    OBS_ACTIVE_BASKET,
    OBS_EXECUTION_TRUTH,
    OBS_HEDGE_INTENT,
    OBS_INTENT_EXECUTION,
    STATUS_COMPLETED,
    STATUS_OBSERVED,
    STATUS_PENDING_OUTCOME,
    build_execution_truth_observation_record,
    build_validation_observation_records_from_analysis,
    complete_hedge_observation_if_mature,
    forward_return_from_feature_rows,
    _validation_observation_market_open_verdict,
)


class ValidationObservationLoopTests(unittest.TestCase):
    def test_agent_analysis_outside_market_open_is_not_observed(self):
        records = build_validation_observation_records_from_analysis({
            "id": 41,
            "analyzed_at": datetime(2026, 6, 6, 10, 0, 0),
            "risk_output": {
                "approved": True,
                "target_weights": {"QQQ": 0.10, "CASH": 0.90},
                "final_validation": {"approved": True},
            },
        })

        self.assertEqual(records, [])

    def test_historical_agent_analysis_observation_outside_market_open_is_excluded(self):
        verdict = _validation_observation_market_open_verdict({
            "observed_at": datetime(2026, 6, 5, 22, 0, 0),
            "observation_payload": {
                "source": "agent_analysis.risk_output",
            },
        })

        self.assertFalse(verdict["allowed"])
        self.assertIn("analysis_outside_market_open", verdict["reasons"])

    def test_builds_hedge_and_basket_observations_from_analysis(self):
        records = build_validation_observation_records_from_analysis({
            "id": 42,
            "analyzed_at": datetime(2026, 6, 5, 14, 0, 0),
            "trigger_type": "scheduled",
            "execution_status": "pending",
            "risk_output": {
                "approved": False,
                "target_weights": {"QQQ": 0.12, "CASH": 0.88},
                "final_validation": {
                    "approved": False,
                    "reason": "execution_policy_violation",
                    "blockers": ["execution_policy_violation"],
                },
                "hedge_intent": {
                    "triggered": True,
                    "severity": 0.52,
                    "add_hedge_etf": False,
                    "hedge_instrument": "PSQ",
                    "reasons": ["risk_off_test"],
                },
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
        self.assertEqual(set(by_type), {OBS_HEDGE_INTENT, OBS_ACTIVE_BASKET, OBS_INTENT_EXECUTION})
        intent = by_type[OBS_INTENT_EXECUTION]
        self.assertEqual(intent["status"], STATUS_OBSERVED)
        self.assertEqual(intent["observation_payload"]["schema_version"], "intent_vs_execution_v1")
        self.assertEqual(intent["observation_payload"]["intended_action"], "blocked_by_final_validation")
        self.assertIn("execution_policy_violation", intent["observation_payload"]["blockers"])
        self.assertIn(
            "hedge_triggered_without_inverse_etf",
            intent["observation_payload"]["unexecuted_intents"],
        )
        self.assertEqual(
            intent["observation_payload"]["outcome_label_contract"]["preferred_training_source"],
            "qc_execution",
        )
        self.assertFalse(intent["outcome_payload"]["command_sent"])
        self.assertEqual(intent["metrics"]["unexecuted_intent_count"], 2)
        self.assertEqual(intent["recommendation"]["operator_action"], "review_unexecuted_intent")
        self.assertEqual(by_type[OBS_HEDGE_INTENT]["status"], STATUS_PENDING_OUTCOME)
        self.assertEqual(by_type[OBS_HEDGE_INTENT]["horizon_days"], 5)
        self.assertEqual(by_type[OBS_ACTIVE_BASKET]["status"], STATUS_OBSERVED)
        self.assertEqual(by_type[OBS_ACTIVE_BASKET]["metrics"]["active_count"], 8)
        self.assertEqual(by_type[OBS_ACTIVE_BASKET]["execution_authority"], "none")

    def test_intent_vs_execution_records_approved_target_not_sent(self):
        records = build_validation_observation_records_from_analysis({
            "id": 45,
            "analyzed_at": datetime(2026, 6, 5, 15, 0, 0),
            "trigger_type": "scheduled",
            "execution_status": "deduped",
            "risk_output": {
                "approved": True,
                "target_weights": {"SPY": 0.10, "CASH": 0.90},
                "final_validation": {"approved": True},
            },
        })

        intent = next(row for row in records if row["observation_type"] == OBS_INTENT_EXECUTION)

        self.assertEqual(intent["observation_payload"]["intended_action"], "send_qc_command")
        self.assertEqual(intent["outcome_payload"]["not_sent_reason"], "deduped")
        self.assertIn(
            "approved_target_not_sent:deduped",
            intent["observation_payload"]["unexecuted_intents"],
        )
        self.assertEqual(intent["recommendation"]["operator_action"], "review_unexecuted_intent")

    def test_intent_vs_execution_classifies_preflight_cap_blockers(self):
        records = build_validation_observation_records_from_analysis(
            {
                "id": 46,
                "analyzed_at": datetime(2026, 6, 5, 15, 30, 0),
                "trigger_type": "scheduled",
                "execution_status": "rejected",
                "risk_output": {
                    "approved": True,
                    "target_weights": {"QQQ": 0.10, "CASH": 0.90},
                    "final_validation": {"approved": True},
                },
            },
            execution_log={
                "analysis_id": 46,
                "command_id": "analysis_46",
                "status": "rejected",
                "qc_status": "not_sent",
                "qc_rejection_reason": "blocked_by_command_preflight",
                "command_payload": {
                    "reason": "blocked_by_command_preflight",
                    "command_preflight": {
                        "allowed": False,
                        "blockers": ["daily_command_count_ok", "daily_gross_turnover_ok"],
                        "checks": {
                            "daily_command_count_ok": {
                                "actual": 3,
                                "threshold": 3,
                                "base_threshold": 3,
                                "reserve_applied": 1,
                                "bucket": "ordinary",
                            },
                            "daily_gross_turnover_ok": {
                                "actual": 0.81,
                                "threshold": 0.80,
                                "base_threshold": 0.80,
                                "reserve_applied": 0.0,
                                "bucket": "ordinary",
                            },
                        },
                    },
                },
            },
        )

        intent = next(row for row in records if row["observation_type"] == OBS_INTENT_EXECUTION)
        payload = intent["observation_payload"]

        self.assertEqual(payload["blocker_events_schema_version"], "intent_blocker_events_v1")
        self.assertIn("daily_command_count_ok", payload["blockers"])
        self.assertIn("daily_gross_turnover_ok", payload["blockers"])
        categories = {event["code"]: event["category"] for event in payload["blocker_events"]}
        self.assertEqual(categories["daily_command_count_ok"], "execution_daily_cap")
        self.assertEqual(categories["daily_gross_turnover_ok"], "execution_turnover_cap")
        self.assertIn(
            "approved_target_blocked_by_execution_preflight",
            payload["unexecuted_intents"],
        )
        self.assertIn(
            "approved_target_blocked_by_daily_command_cap",
            payload["unexecuted_intents"],
        )
        self.assertIn(
            "approved_target_blocked_by_daily_turnover_cap",
            payload["unexecuted_intents"],
        )
        self.assertEqual(intent["metrics"]["blocker_categories"]["execution_daily_cap"], 1)
        self.assertEqual(intent["metrics"]["blocker_categories"]["execution_turnover_cap"], 1)

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
            "analyzed_at": datetime(2026, 6, 1, 14, 0, 0),
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

        hedge_record = next(row for row in records if row["observation_type"] == OBS_HEDGE_INTENT)
        updated = complete_hedge_observation_if_mature(
            hedge_record,
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
