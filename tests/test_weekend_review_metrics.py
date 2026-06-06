from datetime import UTC, date, datetime, timedelta
import unittest

from services.weekend_review_loader import build_weekend_review_dataset
from services.weekend_review_metrics import (
    DEFAULT_HEDGE_WEIGHT_POLICY_VERSION,
    build_weekly_review_metrics,
    hedge_counterfactual_return,
    rate_metric,
)


class WeekendReviewMetricsTests(unittest.TestCase):
    def test_rate_guard_blocks_small_outcome_samples(self):
        metric = rate_metric(
            "changed_ticker_outcome_win_rate",
            numerator=2,
            denominator=3,
            min_sample_n=20,
        )

        self.assertIsNone(metric["value"])
        self.assertEqual(metric["status"], "insufficient_sample")
        self.assertEqual(metric["sample_n"], 3)

    def test_execution_truth_counts_latest_state_and_stuck_in_flight(self):
        review_as_of = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
        dataset = build_weekend_review_dataset(
            execution_logs=[
                {
                    "command_id": "analysis_1",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "reconciled",
                    "qc_status": "filled",
                    "submitted_at": "2026-06-05T15:00:00+00:00",
                    "command_payload": {"weights": {"SPY": 0.1}},
                },
                {
                    "command_id": "analysis_2",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "noop_reconciled",
                    "submitted_at": "2026-06-05T16:00:00+00:00",
                    "command_payload": {
                        "weights": {"SPY": 0.1},
                        "order_summary": {"is_noop": True},
                    },
                },
                {
                    "command_id": "analysis_3",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "pending_ack",
                    "submitted_at": "2026-06-06T10:00:00+00:00",
                    "command_payload": {"weights": {"QQQ": 0.1}},
                },
                {
                    "command_id": "analysis_4",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "deduped",
                    "submitted_at": "2026-06-06T10:05:00+00:00",
                    "command_payload": {"reason": "recent_same_target_reconciled"},
                },
            ]
        )

        metrics = build_weekly_review_metrics(dataset, review_as_of=review_as_of)
        execution = metrics["sections"]["execution_truth"]["metrics"]

        self.assertEqual(execution["commands_sent"], 3)
        self.assertEqual(execution["filled_count"], 2)
        self.assertEqual(execution["noop_count"], 1)
        self.assertEqual(execution["duplicate_target_count"], 1)
        self.assertEqual(execution["stuck_in_flight_count"], 1)
        self.assertEqual(metrics["execution_authority"], "none")
        self.assertEqual(metrics["target_weight_mutation"], "none")
        self.assertIn("2026-06-01..2026-06-07", metrics["sections"]["execution_truth"]["week_buckets"])

    def test_execution_truth_uses_qc_status_when_legacy_lifecycle_is_created(self):
        dataset = build_weekend_review_dataset(
            execution_logs=[
                {
                    "command_id": "analysis_accepted",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "created",
                    "qc_status": "accepted",
                    "status": "accepted",
                    "executed_at": "2026-06-05T15:00:00+00:00",
                    "qc_ack_at": "2026-06-05T15:00:01+00:00",
                    "command_payload": {"weights": {"SPY": 0.1}},
                },
                {
                    "command_id": "analysis_timeout",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "created",
                    "qc_status": "timeout_no_execution_confirmed",
                    "status": "timeout_no_ack",
                    "executed_at": "2026-06-05T16:00:00+00:00",
                    "command_payload": {"weights": {"QQQ": 0.1}},
                },
                {
                    "command_id": "analysis_rejected",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "created",
                    "qc_status": "rejected",
                    "status": "rejected",
                    "executed_at": "2026-06-05T17:00:00+00:00",
                    "command_payload": {"weights": {"IWM": 0.1}},
                },
                {
                    "command_id": "analysis_not_sent",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "created",
                    "qc_status": "not_sent",
                    "status": "rejected",
                    "executed_at": "2026-06-05T18:00:00+00:00",
                    "command_payload": {"weights": {"XLE": 0.1}},
                },
                {
                    "command_id": "analysis_deduped",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "created",
                    "qc_status": "not_sent",
                    "status": "deduped",
                    "executed_at": "2026-06-05T19:00:00+00:00",
                    "command_payload": {"reason": "recent_same_target_reconciled"},
                },
            ]
        )

        metrics = build_weekly_review_metrics(
            dataset,
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )
        execution = metrics["sections"]["execution_truth"]["metrics"]

        self.assertEqual(execution["commands_sent"], 3)
        self.assertEqual(execution["accepted_count"], 1)
        self.assertEqual(execution["duplicate_target_count"], 1)
        self.assertEqual(execution["rejected_count"], 1)
        self.assertEqual(execution["true_qc_rejected_count"], 1)
        self.assertEqual(execution["not_sent_count"], 1)
        self.assertEqual(execution["timeout_no_execution_confirmed_count"], 1)

    def test_execution_truth_does_not_count_dedupe_config_mentions(self):
        dataset = build_weekend_review_dataset(
            execution_logs=[
                {
                    "command_id": "analysis_config_only",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "accepted",
                    "qc_status": "accepted",
                    "status": "accepted",
                    "executed_at": "2026-06-05T15:00:00+00:00",
                    "command_payload": {
                        "weights": {"SPY": 0.1},
                        "config": {
                            "recent_same_target_dedupe_minutes": 5,
                            "recent_same_target_dedupe_tolerance": 0.005,
                        },
                    },
                }
            ]
        )

        metrics = build_weekly_review_metrics(dataset)
        execution = metrics["sections"]["execution_truth"]["metrics"]

        self.assertEqual(execution["commands_sent"], 1)
        self.assertEqual(execution["duplicate_target_count"], 0)

    def test_execution_truth_prefers_command_lifecycle_events_over_stale_row(self):
        dataset = build_weekend_review_dataset(
            execution_logs=[
                {
                    "command_id": "analysis_242",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "created",
                    "qc_status": "accepted",
                    "status": "accepted",
                    "executed_at": "2026-06-05T15:00:00+00:00",
                    "command_payload": {"weights": {"SPY": 0.1}},
                },
            ],
            command_lifecycle_events=[
                {
                    "command_id": "analysis_242",
                    "event_type": "filled",
                    "event_status": "filled",
                    "event_time": "2026-06-05T15:00:30+00:00",
                    "source": "qc",
                    "payload": {},
                },
                {
                    "command_id": "analysis_242",
                    "event_type": "reconciled",
                    "event_status": "reconciled",
                    "event_time": "2026-06-05T15:01:00+00:00",
                    "source": "fastapi",
                    "payload": {},
                },
            ],
        )

        metrics = build_weekly_review_metrics(
            dataset,
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )
        execution_section = metrics["sections"]["execution_truth"]
        execution = execution_section["metrics"]

        self.assertEqual(execution["commands_sent"], 1)
        self.assertEqual(execution["filled_count"], 1)
        self.assertEqual(execution_section["evidence_refs"][0]["state"], "reconciled")
        self.assertIn("reconciled", execution_section["evidence_refs"][0]["event_types"])

    def test_intent_execution_counts_blocker_categories(self):
        dataset = build_weekend_review_dataset(
            validation_observations=[
                {
                    "observation_id": "intent_vs_execution:46",
                    "observation_type": "intent_vs_execution",
                    "observation_date": date(2026, 6, 6),
                    "observed_at": datetime(2026, 6, 6, 11, 30, tzinfo=UTC),
                    "status": "observed",
                    "execution_authority": "none",
                    "target_weight_mutation": "none",
                    "observation_payload": {
                        "schema_version": "intent_vs_execution_v1",
                        "risk_approved": True,
                        "blockers": ["daily_command_count_ok", "daily_gross_turnover_ok"],
                        "blocker_events": [
                            {"code": "daily_command_count_ok", "category": "execution_daily_cap"},
                            {"code": "daily_gross_turnover_ok", "category": "execution_turnover_cap"},
                            {"code": "final_validation", "category": "final_validation"},
                        ],
                        "unexecuted_intents": [
                            "approved_target_blocked_by_execution_preflight",
                            "approved_target_blocked_by_daily_command_cap",
                            "approved_target_blocked_by_daily_turnover_cap",
                            "approved_target_not_sent:deduped",
                        ],
                        "hedge_intent": {
                            "triggered": True,
                            "add_hedge_etf": False,
                        },
                    },
                    "outcome_payload": {
                        "command_sent": False,
                        "not_sent_reason": "deduped",
                    },
                }
            ]
        )

        metrics = build_weekly_review_metrics(dataset)
        intent = metrics["sections"]["intent_execution"]

        self.assertEqual(intent["metrics"]["final_validation_block_count"], 1)
        self.assertEqual(intent["metrics"]["execution_preflight_block_count"], 1)
        self.assertEqual(intent["metrics"]["daily_command_cap_block_count"], 1)
        self.assertEqual(intent["metrics"]["daily_turnover_cap_block_count"], 1)
        self.assertEqual(intent["metrics"]["dedupe_count"], 1)
        self.assertEqual(intent["metrics"]["approved_not_sent_count"], 1)
        self.assertEqual(intent["metrics"]["hedge_triggered_not_added_count"], 1)
        self.assertEqual(intent["blocker_distribution"]["execution_daily_cap"], 1)

    def test_intent_execution_uses_lifecycle_events_as_blocker_fallback(self):
        dataset = build_weekend_review_dataset(
            command_lifecycle_events=[
                {
                    "command_id": "analysis_cap",
                    "event_type": "preflight_blocked",
                    "event_status": "rejected",
                    "event_time": "2026-06-05T15:00:00+00:00",
                    "source": "fastapi",
                    "payload": {
                        "audit_payload": {
                            "blockers": ["daily_command_count_ok", "daily_gross_turnover_ok"],
                        }
                    },
                },
                {
                    "command_id": "analysis_dedupe",
                    "event_type": "execution_result",
                    "event_status": "deduped",
                    "event_time": "2026-06-05T15:01:00+00:00",
                    "source": "fastapi",
                    "payload": {},
                },
                {
                    "command_id": "analysis_timeout",
                    "event_type": "qc_timeout",
                    "event_status": "timeout_no_ack",
                    "event_time": "2026-06-05T15:02:00+00:00",
                    "source": "fastapi",
                    "payload": {},
                },
                {
                    "command_id": "analysis_reject",
                    "event_type": "qc_rejected",
                    "event_status": "rejected",
                    "event_time": "2026-06-05T15:03:00+00:00",
                    "source": "qc",
                    "payload": {},
                },
            ]
        )

        metrics = build_weekly_review_metrics(dataset)
        intent = metrics["sections"]["intent_execution"]

        self.assertEqual(intent["metrics"]["execution_preflight_block_count"], 1)
        self.assertEqual(intent["metrics"]["daily_command_cap_block_count"], 1)
        self.assertEqual(intent["metrics"]["daily_turnover_cap_block_count"], 1)
        self.assertEqual(intent["metrics"]["dedupe_count"], 1)
        self.assertEqual(intent["metrics"]["execution_timeout_count"], 1)
        self.assertEqual(intent["metrics"]["qc_reject_count"], 1)
        self.assertEqual(intent["blocker_distribution"]["execution_preflight"], 1)
        self.assertEqual(intent["blocker_distribution"]["execution_dedupe"], 1)

    def test_lifecycle_preflight_fallback_counts_only_actual_failed_blockers(self):
        dataset = build_weekend_review_dataset(
            command_lifecycle_events=[
                {
                    "command_id": "analysis_daily_only",
                    "event_type": "preflight_blocked",
                    "event_status": "rejected",
                    "event_time": "2026-06-05T15:00:00+00:00",
                    "source": "fastapi",
                    "payload": {
                        "audit_payload": {
                            "command_preflight": {
                                "blockers": ["daily_command_count_ok"],
                                "checks": {
                                    "daily_command_count_ok": {"pass": False},
                                    "daily_gross_turnover_ok": {"pass": True},
                                },
                                "config": {
                                    "recent_same_target_dedupe_tolerance": 0.005,
                                },
                            }
                        }
                    },
                }
            ]
        )

        metrics = build_weekly_review_metrics(dataset)
        intent = metrics["sections"]["intent_execution"]

        self.assertEqual(intent["metrics"]["execution_preflight_block_count"], 1)
        self.assertEqual(intent["metrics"]["daily_command_cap_block_count"], 1)
        self.assertEqual(intent["metrics"]["daily_turnover_cap_block_count"], 0)

    def test_label_maturity_counts_eligible_fallback_and_immature(self):
        dataset = build_weekend_review_dataset(
            outcome_labels=[
                {
                    "label_schema_version": "outcome_label_v1",
                    "training_authority": "eligible",
                    "horizon": "5d",
                },
                {
                    "label_schema_version": "outcome_label_v1",
                    "training_authority": "feature_scope_limited",
                    "scope_limit_reasons": ["fallback_label_source"],
                    "source_metadata": {"label_source_role": "fallback"},
                    "horizon": "5d",
                },
            ],
            validation_observations=[
                {
                    "observation_id": "label_pending:1",
                    "observation_type": "hedge_intent",
                    "observation_date": date(2026, 6, 1),
                    "observed_at": datetime(2026, 6, 1, 10, 0, tzinfo=UTC),
                    "horizon_days": 20,
                    "maturity_date": date(2026, 6, 30),
                    "status": "pending_outcome",
                    "execution_authority": "none",
                    "target_weight_mutation": "none",
                    "observation_payload": {
                        "contract_version": "validation_observation_loop_v1",
                        "hedge_intent_outcome": {"triggered": False},
                    },
                }
            ],
        )

        metrics = build_weekly_review_metrics(
            dataset,
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )
        labels = metrics["sections"]["label_maturity"]["metrics"]

        self.assertEqual(labels["eligible_label_count"], 1)
        self.assertEqual(labels["fallback_label_count"], 1)
        self.assertEqual(labels["label_5d_mature_count"], 1)
        self.assertEqual(labels["label_20d_pending_count"], 1)
        self.assertEqual(labels["excluded_immature_count"], 1)

    def test_hedge_review_includes_false_negative_and_real_etf_counterfactual(self):
        dataset = build_weekend_review_dataset(
            validation_observations=[
                _hedge_observation(
                    "hedge_intent:1",
                    date(2026, 6, 1),
                    triggered=False,
                    add=False,
                    severity=0.5,
                    candidate="SH",
                ),
                _hedge_observation(
                    "hedge_intent:2",
                    date(2026, 6, 10),
                    triggered=True,
                    add=False,
                    severity=0.6,
                    candidate="PSQ",
                ),
            ],
            market_features=[
                *_price_path("SPY", date(2026, 6, 1), [100, 99, 98, 97, 96, 94]),
                *_price_path("QQQ", date(2026, 6, 1), [100, 100, 99, 98, 97, 95]),
                *_price_path("SH", date(2026, 6, 1), [20, 20.1, 20.2, 20.4, 20.7, 21.0]),
                *_price_path("SPY", date(2026, 6, 10), [100, 101, 102, 102, 103, 104]),
                *_price_path("QQQ", date(2026, 6, 10), [100, 101, 101, 102, 103, 104]),
                *_price_path("PSQ", date(2026, 6, 10), [10, 9.9, 9.8, 9.7, 9.6, 9.5]),
            ],
        )

        metrics = build_weekly_review_metrics(dataset)
        hedge = metrics["sections"]["hedge_review"]

        self.assertEqual(hedge["metrics"]["hedge_trigger_count"], 1)
        self.assertEqual(hedge["metrics"]["triggered_not_added_count"], 1)
        self.assertEqual(hedge["metrics"]["false_negative_count"], 1)
        self.assertEqual(hedge["metrics"]["missed_protection_count"], 1)
        self.assertEqual(hedge["metrics"]["triggered_no_drop_count"], 1)
        self.assertEqual(hedge["metrics"]["triggered_hedge_would_hurt_count"], 1)
        self.assertEqual(hedge["metrics"]["hedge_would_have_helped_count"], 1)
        self.assertEqual(hedge["metrics"]["hedge_would_have_hurt_count"], 1)
        self.assertTrue(hedge["counterfactual_contract"]["uses_real_candidate_etf_price_path"])
        self.assertTrue(hedge["counterfactual_contract"]["does_not_use_negative_underlying_approximation"])
        self.assertEqual(hedge["rates"]["false_negative_rate"]["status"], "insufficient_sample")

    def test_hedge_counterfactual_uses_candidate_etf_price_path(self):
        result = hedge_counterfactual_return(
            candidate_hedge_instrument="PSQ",
            severity=0.5,
            decision_date=date(2026, 6, 1),
            feature_rows=_price_path("PSQ", date(2026, 6, 1), [10, 10.1, 10.0, 9.9, 9.8, 9.5]),
            horizon_days=5,
            policy_version=DEFAULT_HEDGE_WEIGHT_POLICY_VERSION,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["candidate_hedge_instrument"], "PSQ")
        self.assertLess(result["hedge_return"], 0)
        self.assertLess(result["hedge_contribution"], 0)
        self.assertTrue(result["uses_real_candidate_etf_price_path"])

    def test_hedge_review_does_not_double_count_intent_summary_when_dedicated_record_exists(self):
        dedicated = _hedge_observation(
            "hedge_intent:7",
            date(2026, 6, 1),
            triggered=False,
            add=False,
            severity=0.5,
            candidate="SH",
        )
        dedicated["analysis_id"] = 7
        intent_summary = {
            "observation_id": "intent_vs_execution:7",
            "observation_type": "intent_vs_execution",
            "analysis_id": 7,
            "observation_date": date(2026, 6, 1),
            "observed_at": datetime(2026, 6, 1, 10, 0, tzinfo=UTC),
            "status": "observed",
            "execution_authority": "none",
            "target_weight_mutation": "none",
            "observation_payload": {
                "schema_version": "intent_vs_execution_v1",
                "hedge_intent": {
                    "triggered": False,
                    "severity": 0.5,
                    "add_hedge_etf": False,
                    "candidate_hedge_instrument": "SH",
                },
            },
        }
        dataset = build_weekend_review_dataset(
            validation_observations=[dedicated, intent_summary],
            market_features=[
                *_price_path("SPY", date(2026, 6, 1), [100, 99, 98, 97, 96, 94]),
                *_price_path("QQQ", date(2026, 6, 1), [100, 100, 99, 98, 97, 95]),
                *_price_path("SH", date(2026, 6, 1), [20, 20.1, 20.2, 20.4, 20.7, 21.0]),
            ],
        )

        metrics = build_weekly_review_metrics(dataset)
        hedge = metrics["sections"]["hedge_review"]["metrics"]

        self.assertEqual(hedge["false_negative_count"], 1)
        self.assertEqual(hedge["missed_protection_count"], 1)

    def test_debate_metrics_use_rate_guard_for_outcomes(self):
        dataset = build_weekend_review_dataset(
            diagnostic_artifacts=[
                {
                    "schema_version": "debate_impact_v1",
                    "artifact_id": "debate_impact_v1:1",
                    "artifact_type": "debate_impact",
                    "execution_authority": "none",
                    "disagreement_count": 2,
                    "arbitration_count": 1,
                    "disagreement_tickers_changed_by_target_builder": ["QQQ"],
                    "disagreement_tickers_in_final_target": ["QQQ"],
                }
            ]
        )

        metrics = build_weekly_review_metrics(dataset)
        debate = metrics["sections"]["debate_impact"]

        self.assertEqual(debate["metrics"]["debate_available_count"], 1)
        self.assertEqual(debate["metrics"]["disagreement_count_total"], 2)
        self.assertEqual(debate["metrics"]["debate_changed_target_count"], 1)
        self.assertEqual(debate["rates"]["debate_change_rate"]["status"], "ok")
        self.assertEqual(debate["rates"]["changed_ticker_outcome_win_rate"]["status"], "insufficient_sample")

    def test_basket_metrics_are_deterministic(self):
        dataset = build_weekend_review_dataset(
            validation_observations=[
                {
                    "observation_id": "active_basket:1",
                    "observation_type": "active_basket",
                    "observation_date": date(2026, 6, 6),
                    "observed_at": datetime(2026, 6, 6, 10, 0, tzinfo=UTC),
                    "status": "observed",
                    "execution_authority": "none",
                    "target_weight_mutation": "none",
                    "metrics": {
                        "active_count": 11,
                        "subscale_count": 2,
                        "floor_cleared_count": 1,
                    },
                    "observation_payload": {
                        "contract_version": "validation_observation_loop_v1",
                        "active_basket_policy": {
                            "within_target_active_count": False,
                        },
                    },
                }
            ],
            diagnostic_artifacts=[
                {
                    "schema_version": "portfolio_mix_event_v1",
                    "artifact_id": "portfolio_mix_event_v1:1",
                    "artifact_type": "portfolio_mix_event",
                    "execution_authority": "none",
                    "active_count": 9,
                    "cash_weight": 0.4,
                    "diagnostics": {"effective_n": 6.2},
                }
            ],
        )

        metrics = build_weekly_review_metrics(dataset)
        basket = metrics["sections"]["basket_portfolio"]["metrics"]

        self.assertEqual(basket["active_count_avg"], 10.0)
        self.assertEqual(basket["active_count_out_of_range_count"], 1)
        self.assertEqual(basket["subscale_position_count"], 2)
        self.assertEqual(basket["floor_cleared_count"], 1)
        self.assertEqual(basket["cash_avg"], 0.4)
        self.assertEqual(basket["effective_n_avg"], 6.2)


def _hedge_observation(
    observation_id: str,
    observation_date: date,
    *,
    triggered: bool,
    add: bool,
    severity: float,
    candidate: str,
) -> dict:
    return {
        "observation_id": observation_id,
        "observation_type": "hedge_intent",
        "observation_date": observation_date,
        "observed_at": datetime.combine(observation_date, datetime.min.time(), tzinfo=UTC),
        "status": "pending_outcome",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "observation_payload": {
            "contract_version": "validation_observation_loop_v1",
            "hedge_intent_outcome": {
                "date": observation_date.isoformat(),
                "triggered": triggered,
                "severity": severity,
                "add_hedge_etf": add,
                "candidate_hedge_instrument": candidate,
            },
        },
    }


def _price_path(ticker: str, start: date, prices: list[float]) -> list[dict]:
    rows = []
    for idx, price in enumerate(prices):
        rows.append({
            "trading_date": start + timedelta(days=idx),
            "ticker": ticker,
            "source": "yfinance",
            "adj_close_price": price,
        })
    return rows


if __name__ == "__main__":
    unittest.main()
