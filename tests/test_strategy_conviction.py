import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

from services.construction_epoch import build_construction_epoch
from services.historical_signal_replay import (
    SIGNAL_SOURCE_YFINANCE_REPLAY,
    FrozenSignal,
    SignalOutcome,
)
from services.signal_ledger import SIGNAL_SOURCE_FASTAPI_LIVE_FREEZE
from services.strategy_conviction import (
    SOURCE_BUCKET_COMBINED,
    SOURCE_BUCKET_HISTORICAL_PRIOR,
    SOURCE_BUCKET_LIVE_PAPER,
    STATUS_CALIBRATED,
    STATUS_EARLY_ESTIMATE,
    STATUS_HISTORICAL_REQUIRES_LIVE,
    STATUS_INSUFFICIENT_SAMPLES,
    STAT_STATUS_EARLY_SIGNAL,
    STAT_STATUS_INDICATIVE,
    STAT_STATUS_INSUFFICIENT,
    STAT_STATUS_MONITORING_READY,
    STAT_STATUS_STATISTICALLY_MEANINGFUL,
    compute_conviction_profiles,
    conviction_profile_content_hash,
    plan_conviction_profile_writes,
    statistical_status_for_samples,
    wilson_hit_rate_interval,
)


BASE_DATE = date(2020, 1, 1)


def _signal(
    idx,
    *,
    source=SIGNAL_SOURCE_YFINANCE_REPLAY,
    branch="branch_a",
    action="increase",
    confidence=0.5,
    data_lag_days=0,
    ticker="TQQQ",
    regime="trending_bull",
    diagnostics=None,
):
    signal_date = BASE_DATE + timedelta(days=idx)
    feature_date = signal_date - timedelta(days=data_lag_days) if data_lag_days is not None else None
    return FrozenSignal(
        signal_id=f"sig-{source}-{ticker}-{branch}-{idx}-{data_lag_days}",
        signal_source=source,
        signal_date=signal_date,
        generated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        tradable_from_date=signal_date + timedelta(days=1),
        strategy_id="leveraged_etf_momentum_allocator",
        strategy_version="1.0",
        ticker=ticker,
        role="leveraged_long",
        branch=branch,
        action=action,
        signal_type="risk_on_amplifier",
        confidence=confidence,
        raw_score=confidence,
        normalized_score=confidence,
        max_reasonable_weight=0.08,
        risk_budget_cost=0.9,
        feature_data_date=feature_date,
        data_lag_days=data_lag_days,
        feature_source="yfinance",
        feature_authority="daily_research",
        regime_at_signal=regime,
        vix_at_signal=None,
        evidence_contract_version="v1",
        diagnostics=dict(diagnostics or {}),
        created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )


def _outcome(signal, *, hit=True, forward_return=0.01, excess_vs_spy=0.006, horizon=5):
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
        forward_return=forward_return,
        spy_forward_return=forward_return - excess_vs_spy,
        excess_vs_spy=excess_vs_spy,
        drawdown_during_horizon=-0.02,
        spy_drawdown_during_horizon=-0.01,
        target_pool_drawdown=None,
        hit=hit,
        hit_definition="increase:forward_return>0_and_excess_vs_spy>-0.005",
        excess_calculation_method="raw",
        outcome_source="yfinance",
        data_quality="ok",
        created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )


def _samples(
    n,
    *,
    source=SIGNAL_SOURCE_YFINANCE_REPLAY,
    branch="branch_a",
    hit=True,
    data_lag_days=0,
    start_idx=0,
    forward_base=0.004,
    excess=0.006,
):
    signals = []
    outcomes = []
    denom = max(1, n - 1)
    for offset in range(n):
        confidence = offset / denom
        forward = forward_base + confidence * 0.02
        signal = _signal(
            start_idx + offset,
            source=source,
            branch=branch,
            confidence=confidence,
            data_lag_days=data_lag_days,
        )
        signals.append(signal)
        outcomes.append(_outcome(
            signal,
            hit=hit,
            forward_return=forward,
            excess_vs_spy=excess,
        ))
    return signals, outcomes


def _profile(profiles, *, source_bucket, branch="branch_a"):
    return next(
        item
        for item in profiles
        if item.source_bucket == source_bucket and item.branch == branch
    )


class StrategyConvictionTest(unittest.TestCase):
    def test_insufficient_samples_has_null_conviction_not_zero(self):
        signals, outcomes = _samples(9)

        result = compute_conviction_profiles(
            signals,
            outcomes,
            as_of_date=date(2020, 2, 1),
            include_combined=False,
        )

        profile = _profile(result.profiles, source_bucket=SOURCE_BUCKET_HISTORICAL_PRIOR)
        self.assertEqual(profile.status, STATUS_INSUFFICIENT_SAMPLES)
        self.assertIsNone(profile.conviction)
        self.assertEqual(profile.n, 9)
        self.assertEqual(profile.to_dict()["statistical_status"], STAT_STATUS_INSUFFICIENT)
        self.assertEqual(profile.required_samples, 30)
        self.assertEqual(profile.source_counts, {SOURCE_BUCKET_HISTORICAL_PRIOR: 9})
        self.assertIn("data_lag_filtered", profile.to_dict())

    def test_calibrated_profile_uses_confidence_forward_return_ic(self):
        signals, outcomes = _samples(30, excess=0.01)

        result = compute_conviction_profiles(
            signals,
            outcomes,
            as_of_date=date(2020, 2, 1),
            include_combined=False,
        )

        profile = _profile(result.profiles, source_bucket=SOURCE_BUCKET_HISTORICAL_PRIOR)
        self.assertEqual(profile.status, STATUS_CALIBRATED)
        self.assertEqual(profile.to_dict()["legacy_operational_status"], STATUS_CALIBRATED)
        self.assertEqual(profile.to_dict()["statistical_status"], STAT_STATUS_MONITORING_READY)
        self.assertEqual(profile.hit_rate, 1.0)
        self.assertGreater(profile.ic, 0.99)
        self.assertAlmostEqual(profile.conviction, 0.85, places=2)
        self.assertLess(profile.to_dict()["hit_rate_ci"]["lower"], 1.0)

    def test_statistical_status_thresholds_and_wilson_ci_are_exposed(self):
        self.assertEqual(statistical_status_for_samples(29), STAT_STATUS_INSUFFICIENT)
        self.assertEqual(statistical_status_for_samples(30), STAT_STATUS_MONITORING_READY)
        self.assertEqual(statistical_status_for_samples(99), STAT_STATUS_MONITORING_READY)
        self.assertEqual(statistical_status_for_samples(100), STAT_STATUS_EARLY_SIGNAL)
        self.assertEqual(statistical_status_for_samples(299), STAT_STATUS_EARLY_SIGNAL)
        self.assertEqual(statistical_status_for_samples(300), STAT_STATUS_INDICATIVE)
        self.assertEqual(statistical_status_for_samples(782), STAT_STATUS_INDICATIVE)
        self.assertEqual(statistical_status_for_samples(783), STAT_STATUS_STATISTICALLY_MEANINGFUL)

        ci = wilson_hit_rate_interval(hit_rate=0.60, n=100)

        self.assertEqual(ci["method"], "wilson_score")
        self.assertEqual(ci["n"], 100)
        self.assertLess(ci["lower"], 0.60)
        self.assertGreater(ci["upper"], 0.60)
        self.assertGreater(ci["width"], 0)

    def test_data_lag_filter_excludes_stale_samples_and_counts_them(self):
        good_signals, good_outcomes = _samples(10)
        stale_signals, stale_outcomes = _samples(3, data_lag_days=2, start_idx=100)

        result = compute_conviction_profiles(
            good_signals + stale_signals,
            good_outcomes + stale_outcomes,
            as_of_date=date(2020, 2, 1),
            include_combined=False,
        )

        profile = _profile(result.profiles, source_bucket=SOURCE_BUCKET_HISTORICAL_PRIOR)
        self.assertEqual(profile.status, STATUS_EARLY_ESTIMATE)
        self.assertEqual(profile.n, 10)
        self.assertEqual(profile.data_lag_filtered, 3)
        self.assertEqual(result.summary["skipped"]["data_lag_filtered"], 3)

    def test_branch_level_profiles_are_independent(self):
        a_signals, a_outcomes = _samples(10, branch="branch_a", hit=True, excess=0.01)
        b_signals, b_outcomes = _samples(10, branch="branch_b", hit=False, excess=-0.01, start_idx=100)

        result = compute_conviction_profiles(
            a_signals + b_signals,
            a_outcomes + b_outcomes,
            as_of_date=date(2020, 2, 1),
            include_combined=False,
        )

        branch_a = _profile(result.profiles, source_bucket=SOURCE_BUCKET_HISTORICAL_PRIOR, branch="branch_a")
        branch_b = _profile(result.profiles, source_bucket=SOURCE_BUCKET_HISTORICAL_PRIOR, branch="branch_b")
        self.assertEqual(branch_a.hit_rate, 1.0)
        self.assertEqual(branch_b.hit_rate, 0.0)
        self.assertGreater(branch_a.conviction, branch_b.conviction)

    def test_construction_epochs_are_separate_profile_populations(self):
        shadow_epoch = build_construction_epoch(
            pc_mode="shadow",
            policy_version="execution_policy_v1",
            promotion_config_hash="same",
        )
        gated_epoch = build_construction_epoch(
            pc_mode="gated",
            policy_version="execution_policy_v1",
            promotion_config_hash="same",
        )
        signals = []
        outcomes = []
        for idx in range(10):
            signal = _signal(
                idx,
                diagnostics={"construction_epoch": shadow_epoch},
            )
            signals.append(signal)
            outcomes.append(_outcome(signal, hit=True, excess_vs_spy=0.01))
        for idx in range(10, 20):
            signal = _signal(
                idx,
                diagnostics={"construction_epoch": gated_epoch},
            )
            signals.append(signal)
            outcomes.append(_outcome(signal, hit=False, excess_vs_spy=-0.01))

        result = compute_conviction_profiles(
            signals,
            outcomes,
            as_of_date=date(2020, 2, 1),
            include_combined=False,
        )

        self.assertEqual(len(result.profiles), 2)
        epoch_ids = {profile.to_dict()["construction_epoch_id"] for profile in result.profiles}
        self.assertEqual(epoch_ids, {shadow_epoch["epoch_id"], gated_epoch["epoch_id"]})
        hit_rates = {
            profile.to_dict()["construction_epoch"]["pc_mode"]: profile.hit_rate
            for profile in result.profiles
        }
        self.assertEqual(hit_rates["shadow"], 1.0)
        self.assertEqual(hit_rates["gated"], 0.0)

    def test_combined_requires_live_confirmation_when_live_samples_are_low(self):
        hist_signals, hist_outcomes = _samples(30, source=SIGNAL_SOURCE_YFINANCE_REPLAY, excess=0.01)
        live_signals, live_outcomes = _samples(
            5,
            source=SIGNAL_SOURCE_FASTAPI_LIVE_FREEZE,
            start_idx=100,
            excess=0.02,
        )

        result = compute_conviction_profiles(
            hist_signals + live_signals,
            hist_outcomes + live_outcomes,
            as_of_date=date(2020, 3, 1),
        )

        hist = _profile(result.profiles, source_bucket=SOURCE_BUCKET_HISTORICAL_PRIOR)
        combined = _profile(result.profiles, source_bucket=SOURCE_BUCKET_COMBINED)
        self.assertEqual(combined.status, STATUS_HISTORICAL_REQUIRES_LIVE)
        self.assertEqual(combined.conviction, hist.conviction)
        self.assertTrue(combined.requires_live_confirmation)
        self.assertEqual(combined.hist_n, 30)
        self.assertEqual(combined.live_n, 5)
        self.assertEqual(combined.source_counts, {
            SOURCE_BUCKET_HISTORICAL_PRIOR: 30,
            SOURCE_BUCKET_LIVE_PAPER: 5,
        })

    def test_combined_uses_sample_size_weighting_with_live_bonus(self):
        hist_signals, hist_outcomes = _samples(50, source=SIGNAL_SOURCE_YFINANCE_REPLAY, excess=0.006)
        live_signals, live_outcomes = _samples(
            30,
            source=SIGNAL_SOURCE_FASTAPI_LIVE_FREEZE,
            start_idx=100,
            excess=0.02,
        )

        result = compute_conviction_profiles(
            hist_signals + live_signals,
            hist_outcomes + live_outcomes,
            as_of_date=date(2020, 3, 1),
        )

        combined = _profile(result.profiles, source_bucket=SOURCE_BUCKET_COMBINED)
        self.assertEqual(combined.status, STATUS_CALIBRATED)
        self.assertEqual(combined.hist_n, 50)
        self.assertEqual(combined.live_n, 30)
        self.assertAlmostEqual(combined.live_weight, 0.5625, places=4)
        self.assertAlmostEqual(combined.hist_weight, 0.4375, places=4)

    def test_profile_write_plan_inserts_updates_and_duplicates(self):
        signals, outcomes = _samples(10)
        profile = compute_conviction_profiles(
            signals,
            outcomes,
            as_of_date=date(2020, 2, 1),
            include_combined=False,
        ).profiles[0]

        first = plan_conviction_profile_writes([profile])
        self.assertEqual(first.insert_count, 1)

        existing = {profile.profile_id: {"content_hash": conviction_profile_content_hash(profile)}}
        duplicate = plan_conviction_profile_writes([profile], existing)
        self.assertEqual(duplicate.duplicate_count, 1)

        modified = replace(profile, conviction=0.99)
        update = plan_conviction_profile_writes([modified], existing)
        self.assertEqual(update.update_count, 1)
        self.assertEqual(update.insert_count, 0)


if __name__ == "__main__":
    unittest.main()
