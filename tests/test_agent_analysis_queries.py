from types import SimpleNamespace

from env_setup import ensure_test_settings

ensure_test_settings()

from services.agent_analysis_queries import is_review_only_analysis


def _row(**kwargs):
    defaults = {
        "trigger_type": "scheduled_hourly",
        "execution_status": "not_sent",
        "planner_output": {},
        "decision": {},
        "risk_output": {},
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_weekend_review_rows_are_review_only():
    assert is_review_only_analysis(_row(trigger_type="weekend_review"))


def test_review_only_execution_status_is_review_only():
    assert is_review_only_analysis(_row(execution_status="review_only"))


def test_review_only_payload_flag_is_review_only():
    assert is_review_only_analysis(_row(planner_output={"review_only": True}))
    assert is_review_only_analysis(_row(decision={"review_only": True}))
    assert is_review_only_analysis(_row(risk_output={"review_only": True}))


def test_normal_trade_decision_is_not_review_only():
    assert not is_review_only_analysis(
        _row(
            trigger_type="scheduled_hourly",
            execution_status="not_sent",
            risk_output={"market_scorecard": {"status": "ok"}},
        )
    )
