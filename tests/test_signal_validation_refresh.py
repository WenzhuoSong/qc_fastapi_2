import unittest
from datetime import date, datetime, timezone

from services.historical_signal_replay import FrozenSignal
from services.signal_validation_refresh import build_signal_validation_refresh_plan


def _signal():
    return FrozenSignal(
        signal_id="sig-tqqq",
        signal_source="yfinance_replay",
        signal_date=date(2020, 1, 1),
        generated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        tradable_from_date=date(2020, 1, 2),
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
        feature_data_date=date(2020, 1, 1),
        data_lag_days=0,
        feature_source="yfinance",
        feature_authority="daily_research",
        regime_at_signal="trending_bull",
        vix_at_signal=None,
        evidence_contract_version="v1",
        diagnostics={},
        created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )


def _feature_rows():
    dates = [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)]
    tqqq = [100, 105, 110]
    spy = [100, 101, 102]
    rows = []
    for idx, trading_date in enumerate(dates):
        rows.extend([
            {
                "ticker": "TQQQ",
                "trading_date": trading_date,
                "source": "yfinance",
                "close_price": tqqq[idx],
                "adj_close_price": tqqq[idx],
            },
            {
                "ticker": "SPY",
                "trading_date": trading_date,
                "source": "yfinance",
                "close_price": spy[idx],
                "adj_close_price": spy[idx],
            },
        ])
    return rows


class SignalValidationRefreshTest(unittest.TestCase):
    def test_build_refresh_plan_labels_outcomes_and_profiles(self):
        signal = _signal()

        plan = build_signal_validation_refresh_plan(
            signals=[signal],
            existing_outcomes=[],
            feature_rows=_feature_rows(),
            as_of_date=date(2020, 1, 3),
            horizons=(1,),
            created_at=datetime(2020, 1, 3, tzinfo=timezone.utc),
        )

        self.assertEqual(len(plan.candidate_outcomes), 1)
        self.assertEqual(plan.candidate_outcomes[0].signal_id, signal.signal_id)
        self.assertEqual(plan.candidate_outcomes[0].horizon_days, 1)
        self.assertEqual(len(plan.profiles), 2)  # source bucket + combined
        self.assertEqual(plan.summary["execution_authority"], "none")
        self.assertEqual(plan.summary["candidate_outcomes"], 1)

    def test_existing_outcome_is_deduped_before_conviction(self):
        signal = _signal()
        first = build_signal_validation_refresh_plan(
            signals=[signal],
            existing_outcomes=[],
            feature_rows=_feature_rows(),
            as_of_date=date(2020, 1, 3),
            horizons=(1,),
            created_at=datetime(2020, 1, 3, tzinfo=timezone.utc),
        )
        second = build_signal_validation_refresh_plan(
            signals=[signal],
            existing_outcomes=list(first.candidate_outcomes),
            feature_rows=_feature_rows(),
            as_of_date=date(2020, 1, 3),
            horizons=(1,),
            created_at=datetime(2020, 1, 3, tzinfo=timezone.utc),
        )

        source_profile = next(
            profile
            for profile in second.profiles
            if profile.source_bucket == "historical_prior"
        )
        self.assertEqual(source_profile.n, 1)
        self.assertEqual(second.summary["candidate_outcomes"], 1)
        self.assertEqual(second.summary["existing_outcomes_seen"], 1)


if __name__ == "__main__":
    unittest.main()
