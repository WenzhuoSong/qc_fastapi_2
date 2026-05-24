import unittest
from datetime import date, datetime, timedelta, timezone

from services.historical_signal_replay import (
    SIGNAL_SOURCE_YFINANCE_REPLAY,
    FrozenSignal,
    SignalOutcome,
)
from services.strategy_conviction import (
    SOURCE_BUCKET_COMBINED,
    SOURCE_BUCKET_HISTORICAL_PRIOR,
    ConvictionProfile,
)
from services.strategy_validation_dashboard import build_validation_dashboard_summary


def _signal(idx=0):
    signal_date = date(2020, 1, 1) + timedelta(days=idx)
    return FrozenSignal(
        signal_id=f"sig-{idx}",
        signal_source=SIGNAL_SOURCE_YFINANCE_REPLAY,
        signal_date=signal_date,
        generated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        tradable_from_date=signal_date + timedelta(days=1),
        strategy_id="leveraged_etf_momentum_allocator",
        strategy_version="1.0",
        ticker="TQQQ",
        role="leveraged_long",
        branch="branch_a",
        action="increase",
        signal_type="risk_on_amplifier",
        confidence=0.8,
        raw_score=0.8,
        normalized_score=0.8,
        max_reasonable_weight=0.08,
        risk_budget_cost=0.9,
        feature_data_date=signal_date,
        data_lag_days=0,
        feature_source="yfinance",
        feature_authority="daily_research",
        regime_at_signal="trending_bull",
        vix_at_signal=None,
        evidence_contract_version="v1",
        diagnostics={},
        created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )


def _outcome(signal, horizon=1):
    return SignalOutcome(
        outcome_id=f"out-{signal.signal_id}-{horizon}",
        signal_id=signal.signal_id,
        signal_source=signal.signal_source,
        signal_date=signal.signal_date,
        label_date=signal.signal_date + timedelta(days=horizon),
        strategy_id=signal.strategy_id,
        ticker=signal.ticker,
        branch=signal.branch,
        action=signal.action,
        horizon_days=horizon,
        forward_return=0.01,
        spy_forward_return=0.004,
        excess_vs_spy=0.006,
        drawdown_during_horizon=-0.02,
        spy_drawdown_during_horizon=-0.01,
        target_pool_drawdown=None,
        hit=True,
        hit_definition="increase:forward_return>0_and_excess_vs_spy>-0.005",
        excess_calculation_method="raw",
        outcome_source="yfinance",
        data_quality="ok",
        created_at=datetime(2020, 1, 8, tzinfo=timezone.utc),
    )


def _profile(source_bucket=SOURCE_BUCKET_HISTORICAL_PRIOR, conviction=0.42, status="early_estimate"):
    return ConvictionProfile(
        profile_id=f"profile-{source_bucket}",
        as_of_date=date(2020, 1, 8),
        strategy_id="leveraged_etf_momentum_allocator",
        ticker="TQQQ",
        branch="branch_a",
        action="increase",
        regime_at_signal="trending_bull",
        horizon_days=5,
        source_bucket=source_bucket,
        conviction=conviction,
        status=status,
        n=14,
        required_samples=30,
        hit_rate=0.57,
        avg_forward_return=0.008,
        avg_excess_vs_spy=0.006,
        ic=0.12,
        max_adverse_drawdown=-0.041,
        data_lag_filtered=3,
        requires_live_confirmation=source_bucket == SOURCE_BUCKET_COMBINED,
        hist_n=14,
        live_n=0,
        hist_weight=1.0,
        live_weight=0.0,
        source_counts={"historical_prior": 14},
        diagnostics={"naked_number_guard": True},
        created_at=datetime(2020, 1, 8, tzinfo=timezone.utc),
    )


class StrategyValidationDashboardTest(unittest.TestCase):
    def test_builds_operator_summary_with_profile_buckets(self):
        signal = _signal()

        summary = build_validation_dashboard_summary(
            signals=[signal],
            outcomes=[_outcome(signal, horizon=1)],
            profiles=[
                _profile(SOURCE_BUCKET_HISTORICAL_PRIOR, conviction=0.42),
                _profile(SOURCE_BUCKET_COMBINED, conviction=0.40, status="historical_prior_requires_live_confirmation"),
            ],
            as_of_date=date(2020, 1, 8),
            horizons=(1, 5, 20),
        )

        self.assertEqual(summary["status"], "available")
        self.assertEqual(summary["outcomes_labeled_today"], 1)
        self.assertEqual(summary["pending_outcomes"]["total"], 2)
        self.assertEqual(summary["pending_outcomes"]["mature"], 1)
        self.assertEqual(len(summary["historical_prior_profiles"]), 1)
        self.assertEqual(len(summary["combined_profiles"]), 1)
        self.assertEqual(summary["requires_live_confirmation_count"], 1)
        combined = summary["combined_profiles"][0]
        self.assertEqual(combined["last_signal_date"], "2020-01-01")
        self.assertEqual(combined["source_counts"], {"historical_prior": 14})

    def test_insufficient_conviction_renders_as_missing_not_zero_percent(self):
        signal = _signal()

        summary = build_validation_dashboard_summary(
            signals=[signal],
            outcomes=[],
            profiles=[_profile(conviction=None, status="insufficient_samples")],
            as_of_date=date(2020, 1, 8),
            horizons=(1,),
        )

        row = summary["historical_prior_profiles"][0]
        self.assertIsNone(row["conviction"])
        self.assertEqual(row["conviction_display"], "-")
        self.assertEqual(row["status"], "insufficient_samples")


if __name__ == "__main__":
    unittest.main()
