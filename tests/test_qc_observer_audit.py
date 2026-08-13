from datetime import datetime, timedelta, timezone
from decimal import Decimal

from audit.calculators import (
    first_post_execution_snapshot,
    risk_intent_candidate,
    summarize_intent,
    target_reconciliation,
)


class Snapshot:
    def __init__(self, event_time, broker_reconciled=True):
        self.event_time = event_time
        self.broker_reconciled = broker_reconciled


def test_target_reconciliation_explains_cash_and_tracking_error():
    result = target_reconciliation(
        {"AAPL": "0.25", "MSFT": "0.25"},
        equity="100000",
        cash="50000",
        holdings=[
            {"security_id": "AAPL SID", "ticker": "AAPL", "holdings_value": "24000"},
            {"security_id": "MSFT SID", "ticker": "MSFT", "holdings_value": "26000"},
        ],
    )

    assert result["target_exposure"] == Decimal("0.50")
    assert result["actual_exposure"] == Decimal("0.50")
    assert result["cash_weight"] == Decimal("0.50")
    assert result["tracking_error_l1"] == Decimal("0.02")


def test_target_reconciliation_is_blocked_without_positive_nav():
    result = target_reconciliation({"AAPL": "0.25"}, equity=0, cash=0, holdings=[])
    assert result["actual_exposure"] is None
    assert result["tracking_error_l1"] is None


def test_incremental_partial_fill_summary():
    start = datetime(2026, 8, 10, 13, 30, tzinfo=timezone.utc)
    result = summarize_intent(
        {"requested_quantity": "-100", "submitted_quantity": "-100"},
        [
            {
                "raw_event_id": 1,
                "event_time": start,
                "status": "Submitted",
                "fill_quantity": 0,
                "fill_quantity_semantics": "incremental_callback",
                "remaining_quantity": "-100",
                "fee": 0,
            },
            {
                "raw_event_id": 2,
                "event_time": start + timedelta(seconds=2),
                "status": "PartiallyFilled",
                "fill_quantity": "-40",
                "fill_quantity_semantics": "incremental_callback",
                "remaining_quantity": "-60",
                "fee": "1",
            },
        ],
    )

    assert result["fill_ratio"] == Decimal("0.4")
    assert result["remaining_quantity"] == Decimal("-60")
    assert result["partial"] is True
    assert result["terminal"] is False
    assert result["fees"] == Decimal("1")


def test_reconstructed_fill_is_not_double_counted():
    start = datetime(2026, 8, 10, 13, 30, tzinfo=timezone.utc)
    result = summarize_intent(
        {"requested_quantity": "10", "submitted_quantity": "10"},
        [
            {
                "raw_event_id": 1,
                "event_time": start,
                "status": "Filled",
                "fill_quantity": "10",
                "fill_quantity_semantics": "cumulative_reconstructed",
                "remaining_quantity": 0,
                "fee": "1.25",
            }
        ],
    )

    assert result["fill_ratio"] == Decimal("1")
    assert result["filled_quantity"] == Decimal("10")
    assert result["terminal"] is True
    assert result["partial"] is False


def test_risk_link_requires_same_stream_security_and_marker():
    now = datetime(2026, 8, 10, 13, 30, tzinfo=timezone.utc)
    risk = {"event_stream_id": "stream", "security_id": "sid", "event_time": now}
    valid = {
        "event_stream_id": "stream",
        "security_id": "sid",
        "event_time": now + timedelta(minutes=1),
        "risk_class": "risk_reducing",
        "reason": "atr_exit",
    }
    ordinary = {**valid, "risk_class": "ordinary", "reason": "weekly_rebalance"}

    assert risk_intent_candidate(risk, valid) is True
    assert risk_intent_candidate(risk, ordinary) is False


def test_snapshot_selection_waits_for_terminal_execution_and_reconciliation():
    decision = datetime(2026, 8, 10, 13, 20, tzinfo=timezone.utc)
    terminal = decision + timedelta(minutes=10)
    pre_trade = Snapshot(decision)
    unreconciled = Snapshot(terminal + timedelta(minutes=1), broker_reconciled=False)
    post_trade = Snapshot(terminal + timedelta(hours=6))

    selected = first_post_execution_snapshot(
        [pre_trade, unreconciled, post_trade],
        decision_time=decision,
        terminal_times=[terminal],
    )

    assert selected is post_trade
