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
    SOURCE_BUCKET_LIVE_PAPER,
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


def _profile(
    source_bucket=SOURCE_BUCKET_HISTORICAL_PRIOR,
    conviction=0.42,
    status="early_estimate",
    *,
    ticker="TQQQ",
    regime_at_signal="trending_bull",
    n=14,
    hit_rate=0.57,
    avg_excess_vs_spy=0.006,
    ic=0.12,
    data_lag_filtered=3,
):
    return ConvictionProfile(
        profile_id=f"profile-{source_bucket}-{ticker}-{regime_at_signal}",
        as_of_date=date(2020, 1, 8),
        strategy_id="leveraged_etf_momentum_allocator",
        ticker=ticker,
        branch="branch_a",
        action="increase",
        regime_at_signal=regime_at_signal,
        horizon_days=5,
        source_bucket=source_bucket,
        conviction=conviction,
        status=status,
        n=n,
        required_samples=30,
        hit_rate=hit_rate,
        avg_forward_return=0.008,
        avg_excess_vs_spy=avg_excess_vs_spy,
        ic=ic,
        max_adverse_drawdown=-0.041,
        data_lag_filtered=data_lag_filtered,
        requires_live_confirmation=source_bucket == SOURCE_BUCKET_COMBINED,
        hist_n=n if source_bucket != SOURCE_BUCKET_LIVE_PAPER else 0,
        live_n=0,
        hist_weight=1.0,
        live_weight=0.0,
        source_counts=(
            {"historical_prior": n}
            if source_bucket == SOURCE_BUCKET_COMBINED
            else {source_bucket: n}
        ),
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
        self.assertEqual(summary["regime_level_profiles"][0]["regime_at_signal"], "trending_bull")
        self.assertIn("regime_summary_rows", summary)
        self.assertEqual(summary["regime_summary_rows"][0]["regime_at_signal"], "trending_bull")
        self.assertEqual(summary["regime_summary_rows"][0]["source_bucket"], SOURCE_BUCKET_COMBINED)

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

    def test_regime_summary_groups_by_regime_and_source_bucket(self):
        summary = build_validation_dashboard_summary(
            signals=[],
            outcomes=[],
            profiles=[
                _profile(
                    SOURCE_BUCKET_LIVE_PAPER,
                    conviction=0.70,
                    status="calibrated",
                    ticker="TQQQ",
                    regime_at_signal="trending_bull",
                    n=30,
                    hit_rate=0.60,
                    avg_excess_vs_spy=0.010,
                    ic=0.20,
                    data_lag_filtered=1,
                ),
                _profile(
                    SOURCE_BUCKET_LIVE_PAPER,
                    conviction=0.20,
                    status="insufficient_samples",
                    ticker="UVXY",
                    regime_at_signal="defensive",
                    n=5,
                    hit_rate=0.40,
                    avg_excess_vs_spy=-0.004,
                    ic=-0.10,
                    data_lag_filtered=2,
                ),
            ],
            as_of_date=date(2020, 1, 8),
            horizons=(5,),
        )

        rows = {
            (row["regime_at_signal"], row["source_bucket"]): row
            for row in summary["regime_summary_rows"]
        }
        bull = rows[("trending_bull", SOURCE_BUCKET_LIVE_PAPER)]
        defensive = rows[("defensive", SOURCE_BUCKET_LIVE_PAPER)]

        self.assertEqual(bull["profile_count"], 1)
        self.assertEqual(bull["total_n"], 30)
        self.assertEqual(bull["calibrated_profiles"], 1)
        self.assertEqual(bull["hit_rate"], 0.60)
        self.assertEqual(bull["avg_excess_vs_spy"], 0.010)
        self.assertEqual(bull["ic"], 0.20)
        self.assertEqual(defensive["insufficient_profiles"], 1)
        self.assertEqual(defensive["data_lag_filtered"], 2)


if __name__ == "__main__":
    unittest.main()
