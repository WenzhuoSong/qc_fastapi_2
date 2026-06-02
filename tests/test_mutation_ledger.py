import unittest

from services.mutation_ledger import (
    CONDITIONAL_TYPES,
    TIGHTEN_ONLY_TYPES,
    MutationLedger,
    MutationLedgerError,
    TickerMutation,
    normalize_mutation_type,
)


class MutationLedgerTest(unittest.TestCase):
    def test_tighten_only_mutation_cannot_increase_weight(self):
        with self.assertRaises(MutationLedgerError):
            TickerMutation(
                mutation_type="cap_single_buy_delta",
                ticker="XLK",
                weight_before=0.10,
                weight_after=0.12,
                reason="bad tighten",
            )

    def test_conditional_mutation_can_stay_above_target(self):
        mutation = TickerMutation(
            mutation_type="sell_delta_throttle",
            ticker="SPY",
            weight_before=0.00,
            weight_after=0.15,
            reason="sell delta limited",
        )

        self.assertTrue(mutation.is_conditional)
        self.assertFalse(mutation.is_tighten_only)
        self.assertGreater(mutation.delta_vs_target, 0)

    def test_legacy_min_hold_type_is_normalized(self):
        self.assertEqual(
            normalize_mutation_type("defer_sell_due_to_min_hold_days"),
            "min_hold_defer_sell",
        )

        mutation = TickerMutation(
            mutation_type="defer_sell_due_to_min_hold_days",
            ticker="QQQ",
            weight_before=0.10,
            weight_after=0.15,
            reason="legacy alias",
        )
        self.assertEqual(mutation.mutation_type, "min_hold_defer_sell")
        self.assertTrue(mutation.is_conditional)

    def test_cash_mutation_is_rejected(self):
        with self.assertRaises(MutationLedgerError):
            TickerMutation(
                mutation_type="cash_raise_from_policy_cap",
                ticker="CASH",
                weight_before=0.70,
                weight_after=0.75,
                reason="cash accounting belongs in diagnostics",
            )

    def test_ledger_summary_tracks_types_and_affected_tickers(self):
        ledger = MutationLedger()
        ledger.record(
            mutation_type="cap_single_buy_delta",
            ticker="XLK",
            before=0.18,
            after=0.15,
            reason="policy cap",
        )
        ledger.record(
            mutation_type="sell_delta_throttle",
            ticker="SPY",
            before=0.00,
            after=0.15,
            reason="sell delta limited",
        )

        self.assertFalse(ledger.is_all_tighten_only())
        self.assertEqual(ledger.affected_tickers(), {"XLK", "SPY"})
        self.assertEqual(ledger.mutation_types(), ["cap_single_buy_delta", "sell_delta_throttle"])
        self.assertEqual([m.ticker for m in ledger.conditional_mutations()], ["SPY"])
        summary = ledger.to_dict()
        self.assertEqual(summary["contract_version"], "mutation_ledger_v1")
        self.assertEqual(summary["conditional_count"], 1)
        self.assertEqual(summary["affected_tickers"], ["SPY", "XLK"])

    def test_from_details_normalizes_aliases(self):
        ledger = MutationLedger.from_details(
            [
                {
                    "type": "defer_sell_due_to_min_hold_days",
                    "ticker": "spy",
                    "before": 0.0,
                    "after": 0.1,
                }
            ]
        )

        self.assertEqual(ledger.mutations[0].mutation_type, "min_hold_defer_sell")
        self.assertEqual(ledger.mutations[0].ticker, "SPY")

    def test_mutation_type_sets_are_disjoint(self):
        self.assertFalse(TIGHTEN_ONLY_TYPES & CONDITIONAL_TYPES)


if __name__ == "__main__":
    unittest.main()
