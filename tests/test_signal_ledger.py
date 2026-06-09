import unittest
from dataclasses import replace
from datetime import date, datetime, timezone

from services.signal_ledger import (
    freeze_evidence_cards_for_live,
    freeze_playground_bundle,
    frozen_signal_content_hash,
    frozen_signal_record,
    plan_frozen_signal_writes,
)


def _card(ticker="TQQQ", action="increase", confidence=0.8):
    return {
        "ticker": ticker,
        "strategy": "leveraged_etf_momentum_allocator",
        "strategy_version": "1.0",
        "role": "leveraged_long",
        "action": action,
        "signal_type": "risk_on_amplifier",
        "horizon": "short_tactical",
        "confidence": confidence,
        "conviction": 0.0,
        "raw_score": confidence,
        "normalized_score": confidence,
        "max_reasonable_weight": 0.08,
        "risk_budget_cost": 0.9,
        "branch": "bull_trend_to_tqqq",
        "reason": "mapped_by_compatibility_threshold",
        "diagnostics": {"contract_version": "v1"},
    }


class SignalLedgerTest(unittest.TestCase):
    def test_freeze_live_cards_preserves_unknown_feature_lag(self):
        generated_at = datetime(2026, 5, 24, 21, 0, tzinfo=timezone.utc)

        signals = freeze_evidence_cards_for_live(
            [_card()],
            signal_date=date(2026, 5, 24),
            generated_at=generated_at,
            feature_data_date=None,
            regime_at_signal="trending_bull",
            qc_context={"policy_version_match": True},
        )

        self.assertEqual(len(signals), 1)
        signal = signals[0]
        self.assertEqual(signal.signal_source, "fastapi_live_freeze")
        self.assertIsNone(signal.feature_data_date)
        self.assertIsNone(signal.data_lag_days)
        self.assertEqual(signal.diagnostics["source_bucket"], "live_paper")
        self.assertFalse(signal.diagnostics["signal_freeze"]["feature_date_known"])
        self.assertTrue(signal.diagnostics["qc_context"]["policy_version_match"])
        self.assertEqual(signal.diagnostics["construction_epoch"]["pc_mode"], "unknown")
        self.assertEqual(signal.diagnostics["construction_epoch"]["execution_authority"], "none")

    def test_freeze_live_cards_records_data_lag_when_feature_date_known(self):
        signals = freeze_evidence_cards_for_live(
            [_card()],
            signal_date=date(2026, 5, 24),
            generated_at=datetime(2026, 5, 24, tzinfo=timezone.utc),
            feature_data_date=date(2026, 5, 23),
            regime_at_signal="trending_bull",
            qc_context={"policy_version": "execution_policy_v1"},
            portfolio_construction_config={"portfolio_construction_mode": "gated"},
        )

        self.assertEqual(signals[0].data_lag_days, 1)
        self.assertEqual(signals[0].feature_data_date, date(2026, 5, 23))
        self.assertEqual(signals[0].diagnostics["construction_epoch"]["pc_mode"], "gated")
        self.assertEqual(signals[0].diagnostics["construction_epoch"]["policy_version"], "execution_policy_v1")

    def test_write_plan_is_idempotent_for_same_signal_content(self):
        signal = freeze_evidence_cards_for_live(
            [_card()],
            signal_date=date(2026, 5, 24),
            generated_at=datetime(2026, 5, 24, tzinfo=timezone.utc),
            feature_data_date=date(2026, 5, 24),
        )[0]

        first = plan_frozen_signal_writes([signal])
        self.assertEqual(first.insert_count, 1)

        existing = {signal.signal_id: {"content_hash": frozen_signal_content_hash(signal)}}
        second = plan_frozen_signal_writes([signal], existing)
        self.assertEqual(second.insert_count, 0)
        self.assertEqual(second.duplicate_count, 1)
        self.assertEqual(second.conflict_count, 0)

    def test_db_record_datetimes_are_naive_for_timestamp_columns(self):
        signal = freeze_evidence_cards_for_live(
            [_card()],
            signal_date=date(2026, 5, 24),
            generated_at=datetime(2026, 5, 24, 21, 0, tzinfo=timezone.utc),
            feature_data_date=date(2026, 5, 24),
        )[0]

        record = frozen_signal_record(signal)

        self.assertIsNone(record["generated_at"].tzinfo)
        self.assertIsNone(record["created_at"].tzinfo)

    def test_write_plan_detects_same_signal_id_with_different_content(self):
        signal = freeze_evidence_cards_for_live(
            [_card(confidence=0.8)],
            signal_date=date(2026, 5, 24),
            generated_at=datetime(2026, 5, 24, tzinfo=timezone.utc),
            feature_data_date=date(2026, 5, 24),
        )[0]
        modified = replace(signal, confidence=0.9)

        existing = {signal.signal_id: {"content_hash": frozen_signal_content_hash(signal)}}
        plan = plan_frozen_signal_writes([modified], existing)

        self.assertEqual(plan.insert_count, 0)
        self.assertEqual(plan.conflict_count, 1)
        self.assertEqual(plan.conflicts[0]["reason"], "existing_content_hash_conflict")

    def test_freeze_playground_bundle_extracts_v1_evidence_cards_only(self):
        bundle = {
            "generated_at": "2026-05-24T21:00:00+00:00",
            "regime_label": "trending_bull",
            "strategies": [
                {
                    "strategy_name": "leveraged_etf_momentum_allocator",
                    "evidence_contract_version": "v1",
                    "evidence_cards": [_card()],
                },
                {
                    "strategy_name": "legacy",
                    "evidence_cards": [_card(ticker="UVXY", action="hedge")],
                },
            ],
        }

        signals = freeze_playground_bundle(
            bundle,
            signal_date=date(2026, 5, 24),
            feature_data_date=date(2026, 5, 24),
        )

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].ticker, "TQQQ")
        self.assertEqual(signals[0].regime_at_signal, "trending_bull")


if __name__ == "__main__":
    unittest.main()
