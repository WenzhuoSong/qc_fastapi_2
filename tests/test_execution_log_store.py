import importlib
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


def _load_utcnow_db_naive():
    sqlalchemy = type(sys)("sqlalchemy")
    sqlalchemy.select = lambda *args, **kwargs: None
    sqlalchemy.update = lambda *args, **kwargs: None
    sqlalchemy.desc = lambda value: value

    models = type(sys)("db.models")
    models.ExecutionLog = type("ExecutionLog", (), {})
    models.AgentAnalysis = type("AgentAnalysis", (), {})
    models.AccountStateSnapshot = type("AccountStateSnapshot", (), {})
    models.CommandLifecycleEvent = type("CommandLifecycleEvent", (), {})

    session = type(sys)("db.session")
    session.AsyncSessionLocal = object

    lifecycle = type(sys)("services.command_lifecycle")
    lifecycle.append_command_lifecycle_event = None
    lifecycle.build_command_reconciliation_events = lambda **kwargs: []
    lifecycle.lifecycle_state_from_status = lambda **kwargs: "created"
    lifecycle.next_lifecycle_state = lambda current, proposed: proposed if current in (None, "") else current

    previous = sys.modules.pop("services.execution_log_store", None)
    try:
        with patch.dict(
            "sys.modules",
            {
                "sqlalchemy": sqlalchemy,
                "db": type(sys)("db"),
                "db.models": models,
                "db.session": session,
                "services.command_lifecycle": lifecycle,
            },
        ):
            return importlib.import_module("services.execution_log_store")
    finally:
        if previous is not None:
            sys.modules["services.execution_log_store"] = previous
        else:
            sys.modules.pop("services.execution_log_store", None)


execution_log_store = _load_utcnow_db_naive()
_utcnow_db_naive = execution_log_store._utcnow_db_naive


class ExecutionLogStoreTests(unittest.TestCase):
    def test_apply_command_lifecycle_skeleton_sets_shared_row_fields(self):
        row = type(
            "Row",
            (),
            {
                "command_id": None,
                "correlation_id": None,
                "analysis_id": None,
                "source_analysis_id": None,
                "command_type": None,
                "policy_version": None,
                "submitted_at": None,
                "executed_at": None,
                "latest_qc_ack_at": None,
                "lifecycle_metadata": None,
                "status": "sent",
                "qc_status": "submitted",
                "qc_response": None,
            },
        )()

        execution_log_store._apply_command_lifecycle_skeleton(
            row,
            command_id="analysis_244",
            analysis_id=244,
            command_type="weight_adjustment",
            policy_version="sprint8a",
            status="sent",
            qc_status="submitted",
            metadata={"source": "test"},
        )

        self.assertEqual(row.command_id, "analysis_244")
        self.assertEqual(row.correlation_id, "analysis_244")
        self.assertEqual(row.analysis_id, 244)
        self.assertEqual(row.source_analysis_id, 244)
        self.assertEqual(row.command_type, "weight_adjustment")
        self.assertEqual(row.policy_version, "sprint8a")
        self.assertIsNotNone(row.submitted_at)
        self.assertEqual(row.lifecycle_metadata["source"], "test")
        self.assertEqual(row.lifecycle_state, "created")

    def test_apply_command_lifecycle_skeleton_preserves_existing_correlation_id(self):
        row = type(
            "Row",
            (),
            {
                "command_id": "analysis_244",
                "correlation_id": "corr_existing",
                "analysis_id": 244,
                "source_analysis_id": 244,
                "command_type": "weight_adjustment",
                "policy_version": "sprint8a",
                "submitted_at": None,
                "executed_at": None,
                "latest_qc_ack_at": None,
                "lifecycle_metadata": {},
                "status": "sent",
                "qc_status": "submitted",
                "qc_response": None,
            },
        )()

        execution_log_store._apply_command_lifecycle_skeleton(
            row,
            command_id="analysis_244",
            status="sent",
            qc_status="submitted",
        )

        self.assertEqual(row.correlation_id, "corr_existing")

    def test_apply_command_lifecycle_skeleton_uses_monotonic_transition(self):
        with patch.object(
            execution_log_store,
            "lifecycle_state_from_status",
            return_value="accepted",
        ), patch.object(
            execution_log_store,
            "next_lifecycle_state",
            return_value="filled",
        ) as transition:
            row = type(
                "Row",
                (),
                {
                    "command_id": "analysis_244",
                    "correlation_id": "analysis_244",
                    "analysis_id": 244,
                    "source_analysis_id": 244,
                    "command_type": "weight_adjustment",
                    "policy_version": "sprint8a",
                    "submitted_at": None,
                    "executed_at": None,
                    "latest_qc_ack_at": None,
                    "lifecycle_metadata": {},
                    "lifecycle_state": "filled",
                    "status": "sent",
                    "qc_status": "accepted",
                    "qc_response": None,
                },
            )()

            execution_log_store._apply_command_lifecycle_skeleton(
                row,
                command_id="analysis_244",
                status="sent",
                qc_status="accepted",
            )

        transition.assert_called_once_with("filled", "accepted")
        self.assertEqual(row.lifecycle_state, "filled")

    def test_setweights_target_fingerprint_ignores_lifecycle_metadata(self):
        first = execution_log_store._build_setweights_target_fingerprint(
            {"QQQ": 0.3001},
            policy_version="sprint8a",
            command_id="analysis_242",
            analysis_id=242,
            correlation_id="corr_a",
            tolerance=0.005,
        )
        second = execution_log_store._build_setweights_target_fingerprint(
            {"QQQ": 0.3001},
            policy_version="sprint8a",
            command_id="analysis_243",
            analysis_id=243,
            correlation_id="corr_b",
            tolerance=0.005,
        )

        self.assertEqual(first["fingerprint"], second["fingerprint"])
        self.assertEqual(first["command_type"], "SetWeights")
        self.assertEqual(first["dedupe_tolerance"], 0.005)

    def test_target_fingerprint_from_command_payload_prefers_sent_weights(self):
        row = type(
            "Row",
            (),
            {
                "policy_version": "sprint8a",
                "command_id": "analysis_242",
                "correlation_id": "analysis_242",
                "analysis_id": 242,
            },
        )()
        payload = {
            "sent_weights": {"SPY": 0.1001},
            "proposed_weights": {"SPY": 0.20},
            "policy_version": "sprint8a",
        }

        fingerprint = execution_log_store._target_fingerprint_from_command_payload(
            payload,
            row=row,
        )

        self.assertEqual(fingerprint["normalized_weights"]["SPY"], 0.1)
        self.assertEqual(fingerprint["policy_version"], "sprint8a")

    def test_target_fingerprint_setter_does_not_overwrite_existing_value(self):
        row = type("Row", (), {"target_fingerprint": "existing"})()

        execution_log_store._set_target_fingerprint(row, {"fingerprint": "new"})

        self.assertEqual(row.target_fingerprint, "existing")

    def test_execution_log_json_payload_safety_converts_sets(self):
        payload = execution_log_store._safe_json_payload({
            "blockers": {"daily_command_count_ok", "daily_gross_turnover_ok"},
        })

        self.assertEqual(payload["blockers"], ["daily_command_count_ok", "daily_gross_turnover_ok"])

    def test_execution_log_db_status_normalizes_long_business_reasons(self):
        self.assertEqual(
            execution_log_store._execution_log_db_status("skipped_broker_order_filter"),
            "skipped",
        )
        self.assertEqual(
            execution_log_store._execution_log_db_status("deferred_by_active_execution"),
            "deferred",
        )
        self.assertLessEqual(len(execution_log_store._execution_log_db_status("skipped_broker_order_filter")), 20)
        self.assertLessEqual(len(execution_log_store._execution_log_db_status("deferred_by_active_execution")), 20)

    def test_analysis_execution_status_prefers_terminal_qc_status(self):
        row = type(
            "Row",
            (),
            {
                "qc_status": "reconciled",
                "lifecycle_state": "filled",
                "status": "accepted",
            },
        )()

        self.assertEqual(
            execution_log_store._analysis_execution_status_from_row(row),
            "reconciled",
        )

    def test_analysis_execution_status_uses_specific_not_sent_reason(self):
        row = type(
            "Row",
            (),
            {
                "qc_status": "not_sent",
                "lifecycle_state": "deduped",
                "status": "deduped",
            },
        )()

        self.assertEqual(execution_log_store._analysis_execution_status_from_row(row), "deduped")

    def test_analysis_execution_status_recovers_broker_filter_skip_reason(self):
        row = type(
            "Row",
            (),
            {
                "qc_status": "not_sent",
                "lifecycle_state": "created",
                "status": "skipped",
                "command_payload": {
                    "action_status": "skipped",
                    "reason": "broker_order_filter_no_executable_delta",
                },
            },
        )()

        self.assertEqual(
            execution_log_store._analysis_execution_status_from_row(row),
            "skipped_broker_order_filter",
        )

    def test_analysis_execution_status_recovers_active_execution_defer_reason(self):
        row = type(
            "Row",
            (),
            {
                "qc_status": "not_sent",
                "lifecycle_state": "created",
                "status": "deferred",
                "command_payload": {
                    "action_status": "skipped",
                    "reason": "active_execution_wait",
                },
            },
        )()

        self.assertEqual(
            execution_log_store._analysis_execution_status_from_row(row),
            "deferred_by_active_execution",
        )

    def test_qc_ack_timestamp_is_naive_for_db_column(self):
        value = _utcnow_db_naive()

        self.assertIsNone(value.tzinfo)

    def test_timeout_reconciliation_releases_unprocessed_command(self):
        row = type("Row", (), {"command_id": "analysis_214"})()
        snapshot = type(
            "Snapshot",
            (),
            {
                "id": 123,
                "recorded_at": "2026-06-01 16:30:00",
                "raw_snapshot": {"last_command_id": "", "processed_command_count": 0},
                "target_weights": {},
                "has_open_orders": False,
                "open_order_count": 0,
            },
        )()

        decision = execution_log_store._timeout_reconciliation_decision(row, snapshot)

        self.assertEqual(decision["status"], "timeout_no_execution_confirmed")

    def test_timeout_reconciliation_keeps_processed_command_pending(self):
        row = type("Row", (), {"command_id": "analysis_214"})()
        snapshot = type(
            "Snapshot",
            (),
            {
                "id": 123,
                "recorded_at": "2026-06-01 16:30:00",
                "raw_snapshot": {"last_command_id": "analysis_214"},
                "target_weights": {"SPY": 0.1},
                "has_open_orders": False,
                "open_order_count": 0,
            },
        )()

        decision = execution_log_store._timeout_reconciliation_decision(row, snapshot)

        self.assertEqual(decision["status"], "pending")
        self.assertEqual(decision["reason"], "account_snapshot_reports_command_processed")

    def test_timeout_reconciliation_keeps_active_command_pending(self):
        row = type("Row", (), {"command_id": "analysis_214"})()
        snapshot = type(
            "Snapshot",
            (),
            {
                "id": 123,
                "recorded_at": "2026-06-01 16:30:00",
                "active_command_id": "analysis_214",
                "raw_snapshot": {"last_command_id": "analysis_213"},
                "target_weights": {"SPY": 0.1},
                "has_open_orders": True,
                "open_order_count": 1,
            },
        )()

        decision = execution_log_store._timeout_reconciliation_decision(row, snapshot)

        self.assertEqual(decision["status"], "pending")
        self.assertEqual(decision["reason"], "account_snapshot_reports_command_processed")
        self.assertEqual(decision["active_command_id"], "analysis_214")

    def test_account_state_command_id_prefers_active_command(self):
        command_id = execution_log_store._account_state_command_id(
            {
                "last_command_id": "analysis_213",
                "active_command_id": "analysis_214",
                "raw_snapshot": {"last_command_id": "analysis_212"},
            }
        )

        self.assertEqual(command_id, "analysis_214")

    def test_qc_status_from_reconciliation_event_types(self):
        self.assertEqual(
            execution_log_store._qc_status_from_reconciliation_event_types(
                ["orders_submitted", "partial"],
                "accepted",
            ),
            "partial",
        )
        self.assertEqual(
            execution_log_store._qc_status_from_reconciliation_event_types(
                ["reconciliation_drift"],
                "partial",
            ),
            "reconciliation_drift",
        )
        self.assertEqual(
            execution_log_store._qc_status_from_reconciliation_event_types([], "timeout_no_ack"),
            "timeout_no_ack",
        )
        self.assertEqual(
            execution_log_store._qc_status_from_reconciliation_event_types(
                ["timeout_reconciled_no_execution"],
                "timeout_no_ack",
            ),
            "timeout_no_execution_confirmed",
        )

    def test_sync_execution_log_from_reconciliation_events_updates_stale_row(self):
        row = type(
            "Row",
            (),
            {
                "command_id": "analysis_242",
                "correlation_id": "analysis_242",
                "analysis_id": 242,
                "source_analysis_id": 242,
                "command_type": "weight_adjustment",
                "policy_version": "sprint8a",
                "submitted_at": None,
                "executed_at": None,
                "latest_qc_ack_at": None,
                "qc_ack_at": datetime(2026, 6, 5, 15, 0, 0),
                "lifecycle_metadata": {},
                "status": "accepted",
                "qc_status": "accepted",
                "qc_response": {},
                "command_payload": {"weights": {"SPY": 0.1}},
                "lifecycle_state": "created",
            },
        )()

        with patch.object(
            execution_log_store,
            "next_lifecycle_state",
            return_value="filled",
        ):
            changed = execution_log_store._sync_execution_log_from_reconciliation_events(
                row,
                command_id="analysis_242",
                event_types=["filled", "reconciled"],
                event_time=datetime(2026, 6, 5, 15, 1, 0),
                source="test",
            )

        self.assertTrue(changed)
        self.assertEqual(row.qc_status, "reconciled")
        self.assertEqual(row.lifecycle_state, "filled")
        self.assertEqual(row.latest_qc_ack_at, datetime(2026, 6, 5, 15, 1, 0))
        self.assertEqual(row.command_payload["reconciliation_row_cache_sync"]["source"], "test")

    def test_heartbeat_reconciliation_response_forces_accepted_contract(self):
        response = execution_log_store._qc_response_for_heartbeat_reconciliation(
            {"status": "timeout_no_ack"},
            {
                "target_weights": {"SPY": 0.2},
                "holdings_weights": {"SPY": 0.2},
                "open_order_count": 0,
                "has_open_orders": False,
                "active_execution_status": "orders_submitted",
            },
        )

        self.assertEqual(response["status"], "accepted")
        self.assertEqual(response["actual_target_weights"]["SPY"], 0.2)
        self.assertEqual(response["actual_holdings_weights"]["SPY"], 0.2)
        self.assertEqual(response["order_summary"]["open_order_count_after"], 0)

    def test_heartbeat_reconciliation_overrides_stale_ack_open_orders(self):
        response = execution_log_store._qc_response_for_heartbeat_reconciliation(
            {
                "status": "accepted",
                "order_summary": {"open_order_count_after": 3, "has_open_orders": True},
            },
            {
                "target_weights": {"SPY": 0.2},
                "holdings_weights": {"SPY": 0.2},
                "open_order_count": 0,
                "has_open_orders": False,
            },
        )

        self.assertEqual(response["order_summary"]["open_order_count_after"], 0)
        self.assertFalse(response["order_summary"]["has_open_orders"])

    def test_superseded_lifecycle_payload_from_qc_response(self):
        payload = execution_log_store._superseded_lifecycle_payload_from_qc_response(
            "analysis_230",
            {
                "status": "accepted",
                "reason": "reduce_only_override",
                "superseded_command_id": "analysis_214",
                "canceled_order_count": 3,
                "order_summary": {"canceled_order_ids": [1, 2, 3]},
            },
        )

        self.assertEqual(payload["command_id"], "analysis_214")
        self.assertEqual(payload["reason"], "reduce_only_override")
        self.assertEqual(payload["payload"]["superseded_by_command_id"], "analysis_230")
        self.assertEqual(payload["payload"]["canceled_order_count"], 3)

    def test_superseded_lifecycle_payload_ignores_missing_superseded_id(self):
        payload = execution_log_store._superseded_lifecycle_payload_from_qc_response(
            "analysis_230",
            {"status": "accepted"},
        )

        self.assertIsNone(payload)

    def test_qc_status_maps_to_lifecycle_event_types(self):
        self.assertEqual(execution_log_store._event_type_for_qc_status("accepted"), "qc_accepted")
        self.assertEqual(execution_log_store._event_type_for_qc_status("orders_submitted"), "orders_submitted")
        self.assertEqual(execution_log_store._event_type_for_qc_status("partial"), "partial")
        self.assertEqual(execution_log_store._event_type_for_qc_status("canceled"), "canceled")
        self.assertEqual(
            execution_log_store._event_type_for_qc_status("timeout_no_execution_confirmed"),
            "timeout_reconciled_no_execution",
        )

    def test_qc_status_event_source_distinguishes_fastapi_timeouts(self):
        self.assertEqual(execution_log_store._event_source_for_qc_status("partial"), "qc")
        self.assertEqual(execution_log_store._event_source_for_qc_status("reconciled"), "qc")
        self.assertEqual(execution_log_store._event_source_for_qc_status("timeout_no_ack"), "fastapi")

    def test_daily_turnover_counts_active_lifecycle_statuses(self):
        row = type(
            "Row",
            (),
            {"command_type": "weight_adjustment", "status": "sent", "qc_status": "partial"},
        )()

        self.assertTrue(execution_log_store._counts_toward_daily_command(row))
        self.assertTrue(execution_log_store._counts_toward_daily_turnover(row))

    def test_active_execution_wait_status_does_not_count_toward_daily_caps(self):
        row = type(
            "Row",
            (),
            {
                "command_type": "weight_adjustment",
                "status": "deferred_by_active_execution",
                "qc_status": "not_sent",
            },
        )()

        self.assertFalse(execution_log_store._counts_toward_daily_command(row))
        self.assertFalse(execution_log_store._counts_toward_daily_turnover(row))

    def test_noop_reconciled_does_not_count_toward_daily_caps(self):
        row = type(
            "Row",
            (),
            {
                "command_type": "weight_adjustment",
                "status": "sent",
                "qc_status": "reconciled",
                "qc_response": {
                    "execution_state": "noop_reconciled",
                    "order_summary": {
                        "action_count": 11,
                        "actual_order_count": 0,
                        "submitted_order_count": 0,
                        "filled_order_count": 0,
                        "is_noop": True,
                    },
                },
            },
        )()

        self.assertFalse(execution_log_store._counts_toward_daily_command(row))
        self.assertFalse(execution_log_store._counts_toward_daily_turnover(row))

    def test_daily_activity_summary_uses_cap_counting_rules(self):
        sent = type(
            "Row",
            (),
            {
                "command_type": "weight_adjustment",
                "status": "sent",
                "qc_status": "reconciled",
                "command_payload": {
                    "command_preflight": {"metrics": {"gross_turnover": 0.12}}
                },
            },
        )()
        blocked = type(
            "Row",
            (),
            {
                "command_type": "weight_adjustment",
                "status": "rejected",
                "qc_status": "not_sent",
                "command_payload": {
                    "command_preflight": {"metrics": {"gross_turnover": 0.99}}
                },
            },
        )()

        summary = execution_log_store.summarize_execution_activity_rows([sent, blocked])

        self.assertEqual(summary["command_count"], 1)
        self.assertEqual(summary["gross_turnover"], 0.12)
        self.assertEqual(summary["ordinary_command_count"], 1)
        self.assertEqual(summary["risk_reduce_command_count"], 0)

    def test_daily_activity_summary_counts_broker_executable_preflight_delta(self):
        """Daily buy cap is a conservative reservation over executable command delta."""
        rounded_buy = type(
            "Row",
            (),
            {
                "command_type": "weight_adjustment",
                "status": "sent",
                "qc_status": "filled",
                "qc_response": {
                    "account_state": {
                        "holdings_weights": {"SMH": 0.0141, "CASH": 0.9859},
                    },
                },
                "command_payload": {
                    "sent_weights": {"SMH": 0.019181, "CASH": 0.980819},
                    "proposed_weights": {"SMH": 0.017302, "CASH": 0.982698},
                    "command_preflight": {
                        "broker_order_filter": {
                            "rounded_orders": [
                                {
                                    "ticker": "SMH",
                                    "strategy_intent_weight": 0.017302,
                                    "broker_executable_weight": 0.019181,
                                }
                            ]
                        },
                        "metrics": {
                            "buy_delta": 0.009581,
                            "sell_delta": 0.0,
                            "gross_turnover": 0.0047905,
                        },
                    },
                },
            },
        )()

        summary = execution_log_store.summarize_execution_activity_rows([rounded_buy])

        self.assertEqual(summary["buy_delta"], 0.009581)
        self.assertEqual(summary["gross_turnover"], 0.00479)

    def test_daily_activity_summary_splits_risk_reduce_rows(self):
        risk_reduce = type(
            "Row",
            (),
            {
                "command_type": "weight_adjustment",
                "status": "sent",
                "qc_status": "reconciled",
                "command_payload": {
                    "command_preflight": {
                        "metrics": {
                            "buy_delta": 0.0,
                            "sell_delta": 0.08,
                            "gross_turnover": 0.04,
                        }
                    }
                },
            },
        )()
        ordinary = type(
            "Row",
            (),
            {
                "command_type": "weight_adjustment",
                "status": "sent",
                "qc_status": "reconciled",
                "command_payload": {
                    "command_preflight": {
                        "metrics": {
                            "buy_delta": 0.02,
                            "sell_delta": 0.04,
                            "gross_turnover": 0.03,
                        }
                    }
                },
            },
        )()

        summary = execution_log_store.summarize_execution_activity_rows([risk_reduce, ordinary])

        self.assertEqual(summary["command_count"], 2)
        self.assertEqual(summary["gross_turnover"], 0.07)
        self.assertEqual(summary["risk_reduce_command_count"], 1)
        self.assertEqual(summary["risk_reduce_gross_turnover"], 0.04)
        self.assertEqual(summary["ordinary_command_count"], 1)
        self.assertEqual(summary["ordinary_gross_turnover"], 0.03)

    def test_timeout_reconciliation_uses_earliest_conclusive_no_execution_snapshot(self):
        row = type("Row", (), {"command_id": "analysis_232"})()
        no_execution_snapshot = type(
            "Snapshot",
            (),
            {
                "id": 168,
                "recorded_at": "2026-06-03 14:30:00",
                "raw_snapshot": {"last_command_id": ""},
                "active_command_id": None,
                "target_weights": {},
                "has_open_orders": False,
                "open_order_count": 0,
            },
        )()
        later_success_snapshot = type(
            "Snapshot",
            (),
            {
                "id": 169,
                "recorded_at": "2026-06-03 14:45:00",
                "raw_snapshot": {"last_command_id": "analysis_234"},
                "active_command_id": None,
                "target_weights": {"SPY": 0.1},
                "has_open_orders": False,
                "open_order_count": 0,
            },
        )()

        decision = execution_log_store._timeout_reconciliation_decision_from_snapshots(
            row,
            [no_execution_snapshot, later_success_snapshot],
        )

        self.assertEqual(decision["status"], "timeout_no_execution_confirmed")
        self.assertEqual(decision["snapshot_id"], 168)

    def test_record_active_execution_wait_contract_exists(self):
        text = Path("services/execution_log_store.py").read_text()

        self.assertIn("async def record_active_execution_wait", text)
        self.assertIn('"action_status": "deferred_by_active_execution"', text)
        self.assertIn('row.qc_status = "not_sent"', text)
        self.assertIn('event_type="deferred_by_active_execution"', text)

    def test_operator_force_reconcile_and_cancel_request_contracts_exist(self):
        text = Path("services/execution_log_store.py").read_text()

        self.assertIn("async def force_reconcile_command", text)
        self.assertIn('event_type="force_reconciled_by_operator"', text)
        self.assertIn("actual_holdings_weights", text)
        self.assertIn("async def record_cancel_orders_requested", text)
        self.assertIn('event_type="cancel_orders_requested_by_operator"', text)
        self.assertIn("operator_cancel_orders", text)

    def test_delayed_reconciliation_account_state_is_reachable_contract(self):
        text = Path("services/execution_log_store.py").read_text()

        self.assertIn(
            "if qc_status not in RECONCILIATION_ACTIVE_QC_STATUSES:\n"
            "        return 0\n"
            "    reconciliation_account_state = {",
            text,
        )
        self.assertIn('"total_value": account_state.get("total_value")', text)
        self.assertIn('"prices": _prices_from_raw_snapshot(raw)', text)

    def test_qc_feedback_trust_is_stored_on_lifecycle_row_contract(self):
        text = Path("services/execution_log_store.py").read_text()

        self.assertIn("classify_qc_feedback_trust", text)
        self.assertIn('"feedback_trust"', text)
        self.assertIn("unknown_command_feedback", text)
        self.assertIn("_feedback_trust_allows_reconciliation_event_derivation", text)
        self.assertIn("return {", text)

    def test_force_reconcile_drift_helper_excludes_cash(self):
        diff = execution_log_store._weight_diff_for_force_reconcile(
            {"SPY": 0.2, "CASH": 0.8},
            {"SPY": 0.1, "CASH": 0.9},
        )

        self.assertEqual(diff["max_abs_diff"], 0.1)
        self.assertEqual(diff["diffs"][0]["ticker"], "SPY")

    def test_force_reconcile_uses_guard_drift_not_one_percent(self):
        snapshot = type(
            "Snapshot",
            (),
            {
                "total_value": 135_000.0,
                "raw_snapshot": {
                    "holdings_detail_rows": [
                        {"ticker": "SMH", "market_price": 650.0},
                    ],
                },
            },
        )()

        drift = execution_log_store._reconciliation_drift_for_force_reconcile(
            snapshot,
            {"SMH": 0.019181},
            {"SMH": 0.0143},
        )

        self.assertLess(drift["max_abs_diff"], 0.01)
        self.assertEqual(drift["drift_tickers"][0]["ticker"], "SMH")


if __name__ == "__main__":
    unittest.main()
