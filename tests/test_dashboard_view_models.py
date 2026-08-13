from datetime import datetime, timezone
from types import SimpleNamespace

from dashboard.view_models import daily_account_series, momentum_view, weekly_rebalance_logs


def row(**values):
    return SimpleNamespace(**values)


def test_daily_series_keeps_last_reconciled_snapshot_per_day():
    morning = datetime(2026, 8, 10, 13, 20, tzinfo=timezone.utc)
    close = datetime(2026, 8, 10, 19, 55, tzinfo=timezone.utc)
    result = daily_account_series([
        row(event_time=close, broker_reconciled=True, equity=105, cash=20),
        row(event_time=morning, broker_reconciled=True, equity=100, cash=25),
        row(event_time=close.replace(day=11), broker_reconciled=False, equity=999, cash=0),
    ])

    assert result == [{
        "date": "2026-08-10",
        "as_of": close.isoformat(),
        "equity": 105.0,
        "cash": 20.0,
        "cash_pct": 20 / 105,
    }]


def test_momentum_view_reports_week_over_week_held_state():
    latest = [row(ticker="MU", rank=1, score=2.0), row(ticker="WDC", rank=3, score=1.5)]
    prior = [row(ticker="MU", rank=2, score=1.8), row(ticker="WDC", rank=1, score=1.7)]
    result = momentum_view(latest, prior, ["MU", "WDC"], boundary_gap=0.06)

    assert result["held_top8_count"] == 2
    assert result["held_improving_count"] == 1
    assert result["held_weakening_count"] == 1
    assert result["holdings"][0]["rank_change"] == 1
    assert result["positive_score_pct"] == 1


def test_weekly_log_explains_ranking_risk_band_and_execution():
    decision = row(
        id=1,
        decision_id="decision-1",
        event_time=datetime(2026, 8, 10, 13, 20, tzinfo=timezone.utc),
        final_target_weights={"MU": 0.2, "WDC": 0.2},
        raw_target_weights={"MU": 0.25},
        risk_adjusted_weights={"MU": 0.2, "WDC": 0.2},
        suppressed_deltas={"MU": "inside_1pct_band"},
        payload_extra={"exchange_session": "2026-08-10"},
    )
    rankings = {1: [row(ticker="MU", rank=1, score=2.0)]}
    audit = row(
        status="complete",
        intent_count=2,
        terminal_intent_count=2,
        aggregate_fill_ratio=1,
        total_fees=2,
    )
    result = weekly_rebalance_logs([decision], rankings, {1: audit})[0]

    assert result["opened"] == ["MU", "WDC"]
    assert any("3ATR" in reason for reason in result["reasons"])
    assert any("1% no-trade band" in reason for reason in result["reasons"])
    assert any("2/2" in reason for reason in result["reasons"])
