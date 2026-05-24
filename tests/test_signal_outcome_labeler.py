import unittest
from dataclasses import replace
from datetime import date, datetime, timezone

from services.signal_ledger import freeze_evidence_cards_for_live
from services.signal_outcome_labeler import (
    frozen_signal_from_record,
    label_mature_signal_outcomes,
    plan_signal_outcome_writes,
    signal_outcome_content_hash,
)


DATES = [
    date(2020, 1, 1),
    date(2020, 1, 2),
    date(2020, 1, 3),
    date(2020, 1, 6),
    date(2020, 1, 7),
    date(2020, 1, 8),
]


def _card(ticker="TQQQ", action="increase", confidence=0.8):
    return {
        "ticker": ticker,
        "strategy": "leveraged_etf_momentum_allocator",
        "strategy_version": "1.0",
        "role": "leveraged_long",
        "action": action,
        "signal_type": "risk_on_amplifier",
        "confidence": confidence,
        "raw_score": confidence,
        "normalized_score": confidence,
        "max_reasonable_weight": 0.08,
        "risk_budget_cost": 0.9,
        "branch": "test_branch",
        "diagnostics": {"contract_version": "v1"},
    }


def _signal(ticker="TQQQ", action="increase"):
    return freeze_evidence_cards_for_live(
        [_card(ticker=ticker, action=action)],
        signal_date=date(2020, 1, 1),
        tradable_from_date=date(2020, 1, 2),
        generated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        feature_data_date=date(2020, 1, 1),
        regime_at_signal="test_regime",
    )[0]


def _rows(tqqq=None, uvxy=None, spy=None):
    tqqq = tqqq or [100, 101, 102, 103, 104, 105]
    uvxy = uvxy or [20, 19, 18, 17, 16, 15]
    spy = spy or [100, 101, 102, 103, 104, 105]
    rows = []
    for idx, trading_date in enumerate(DATES):
        for ticker, prices in {"TQQQ": tqqq, "UVXY": uvxy, "SPY": spy}.items():
            rows.append({
                "ticker": ticker,
                "trading_date": trading_date,
                "source": "yfinance",
                "close_price": prices[idx],
                "adj_close_price": prices[idx],
            })
    return rows


class SignalOutcomeLabelerTest(unittest.TestCase):
    def test_t5_outcome_is_not_generated_before_maturity(self):
        result = label_mature_signal_outcomes(
            [_signal()],
            _rows(),
            as_of_date=date(2020, 1, 3),
            horizons=(5,),
            created_at=datetime(2020, 1, 3, tzinfo=timezone.utc),
        )

        self.assertEqual(result.outcomes, [])
        self.assertEqual(result.summary["skipped"], {"h5:not_mature": 1})

    def test_t5_outcome_is_generated_when_mature(self):
        result = label_mature_signal_outcomes(
            [_signal()],
            _rows(tqqq=[100, 101, 102, 103, 104, 110]),
            as_of_date=date(2020, 1, 8),
            horizons=(5,),
            created_at=datetime(2020, 1, 8, tzinfo=timezone.utc),
        )

        self.assertEqual(len(result.outcomes), 1)
        self.assertEqual(result.outcomes[0].label_date, date(2020, 1, 8))
        self.assertEqual(result.outcomes[0].horizon_days, 5)
        self.assertEqual(result.outcomes[0].excess_calculation_method, "raw")

    def test_increase_hit_uses_forward_return_and_excess_vs_spy(self):
        result = label_mature_signal_outcomes(
            [_signal()],
            _rows(tqqq=[100, 101, 102, 103, 104, 105], spy=[100, 104, 105, 106, 107, 108]),
            as_of_date=date(2020, 1, 2),
            horizons=(1,),
            created_at=datetime(2020, 1, 2, tzinfo=timezone.utc),
        )

        outcome = result.outcomes[0]
        self.assertGreater(outcome.forward_return, 0)
        self.assertLess(outcome.excess_vs_spy, -0.005)
        self.assertFalse(outcome.hit)
        self.assertIn("excess_vs_spy", outcome.hit_definition)

    def test_hedge_hit_ignores_uvxy_return_and_uses_spy_stress(self):
        result = label_mature_signal_outcomes(
            [_signal(ticker="UVXY", action="hedge")],
            _rows(uvxy=[20, 19, 18, 17, 16, 15], spy=[100, 97, 98, 99, 100, 101]),
            as_of_date=date(2020, 1, 2),
            horizons=(1,),
            created_at=datetime(2020, 1, 2, tzinfo=timezone.utc),
        )

        outcome = result.outcomes[0]
        self.assertLess(outcome.forward_return, 0)
        self.assertTrue(outcome.hit)
        self.assertIn("spy_forward_return", outcome.hit_definition)

    def test_outcome_write_plan_is_idempotent(self):
        outcome = label_mature_signal_outcomes(
            [_signal()],
            _rows(),
            as_of_date=date(2020, 1, 2),
            horizons=(1,),
            created_at=datetime(2020, 1, 2, tzinfo=timezone.utc),
        ).outcomes[0]

        first = plan_signal_outcome_writes([outcome])
        self.assertEqual(first.insert_count, 1)

        existing = {outcome.outcome_id: {"content_hash": signal_outcome_content_hash(outcome)}}
        second = plan_signal_outcome_writes([outcome], existing)
        self.assertEqual(second.insert_count, 0)
        self.assertEqual(second.duplicate_count, 1)
        self.assertEqual(second.conflict_count, 0)

    def test_outcome_write_plan_detects_conflicting_content(self):
        outcome = label_mature_signal_outcomes(
            [_signal()],
            _rows(),
            as_of_date=date(2020, 1, 2),
            horizons=(1,),
            created_at=datetime(2020, 1, 2, tzinfo=timezone.utc),
        ).outcomes[0]
        modified = replace(outcome, forward_return=0.99)

        existing = {outcome.outcome_id: {"content_hash": signal_outcome_content_hash(outcome)}}
        plan = plan_signal_outcome_writes([modified], existing)

        self.assertEqual(plan.insert_count, 0)
        self.assertEqual(plan.conflict_count, 1)
        self.assertEqual(plan.conflicts[0]["reason"], "existing_content_hash_conflict")

    def test_frozen_signal_db_record_conversion(self):
        signal = _signal()
        converted = frozen_signal_from_record(signal.to_dict())

        self.assertIsNotNone(converted)
        self.assertEqual(converted.signal_id, signal.signal_id)
        self.assertEqual(converted.tradable_from_date, signal.tradable_from_date)
        self.assertEqual(converted.data_lag_days, 0)


if __name__ == "__main__":
    unittest.main()
