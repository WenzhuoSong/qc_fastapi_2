import unittest

from services.target_envelope import TargetEnvelope


class TargetEnvelopeTest(unittest.TestCase):
    def test_final_target_returns_copy(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "CASH": 0.90},
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
        )

        view = envelope.final_target
        view["SPY"] = 0.99

        self.assertAlmostEqual(envelope.final_target["SPY"], 0.20)

    def test_mutate_records_before_after_and_adjusts_cash(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "CASH": 0.90},
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
        )

        envelope.mutate(
            ticker="SPY",
            new_weight=0.15,
            mutation_type="loss_trim",
            reason="risk trim",
        )

        mutation = envelope.ledger.mutations[0]
        self.assertEqual(mutation.ticker, "SPY")
        self.assertEqual(mutation.mutation_type, "loss_trim")
        self.assertAlmostEqual(mutation.weight_before, 0.20)
        self.assertAlmostEqual(mutation.weight_after, 0.15)
        self.assertAlmostEqual(envelope.final_target["SPY"], 0.15)
        self.assertAlmostEqual(envelope.final_target["CASH"], 0.85)

    def test_replay_ledger_exactly_reconstructs_final_target(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "QQQ": 0.10, "CASH": 0.80},
            risk_approved_target={"SPY": 0.20, "QQQ": 0.10, "CASH": 0.70},
        )

        envelope.mutate("SPY", 0.15, "loss_trim", "trim SPY")
        envelope.mutate("QQQ", 0.12, "min_hold_defer_sell", "defer young sell")

        self.assertEqual(envelope.accounting_check(), [])
        self.assertEqual(envelope.replay_ledger(), envelope.final_target)

    def test_unaccounted_direct_drift_fails_accounting(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "CASH": 0.90},
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
        )

        envelope._final_target["SPY"] = 0.15

        violations = envelope.accounting_check()
        self.assertEqual(violations[0]["type"], "ledger_replay_mismatch")
        self.assertEqual(violations[0]["ticker"], "SPY")

    def test_wrong_ledger_amount_fails_accounting(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "CASH": 0.90},
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
        )

        envelope.mutate("SPY", 0.15, "loss_trim", "trim SPY")
        envelope._final_target["SPY"] = 0.14

        violations = envelope.accounting_check()
        self.assertTrue(any(row["ticker"] == "SPY" for row in violations))

    def test_cash_mismatch_is_reported_by_replay_diagnostics(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "CASH": 0.90},
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
        )

        envelope.mutate("SPY", 0.15, "loss_trim", "trim SPY")
        envelope._final_target["CASH"] = 0.80

        violations = envelope.accounting_check()
        self.assertTrue(any(row["ticker"] == "CASH" for row in violations))

    def test_apply_stage_target_imports_stage_output_through_ledger(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "QQQ": 0.16, "CASH": 0.74},
            risk_approved_target={"SPY": 0.20, "QQQ": 0.15, "CASH": 0.65},
        )

        envelope.apply_stage_target(
            new_weights={"SPY": 0.18, "QQQ": 0.14, "CASH": 0.68},
            mutation_type="loss_trim",
            reason="position governance bridge",
            stage="position_governance",
        )

        self.assertEqual(envelope.accounting_check(), [])
        self.assertEqual(envelope.replay_ledger(), envelope.final_target)
        self.assertEqual(envelope.stage_snapshots[0]["stage"], "position_governance")
        self.assertEqual(
            [m.ticker for m in envelope.ledger.mutations],
            ["QQQ", "SPY"],
        )

    def test_direct_cash_mutation_is_rejected(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "CASH": 0.90},
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
        )

        with self.assertRaises(ValueError):
            envelope.mutate("CASH", 0.90, "loss_trim", "cash direct")

    def test_apply_stage_ledger_preserves_existing_mutation_types(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "QQQ": 0.16, "CASH": 0.74},
            risk_approved_target={"SPY": 0.20, "QQQ": 0.15, "CASH": 0.65},
        )
        stage_ledger = {
            "mutations": [
                {
                    "type": "min_hold_defer_sell",
                    "ticker": "QQQ",
                    "before": 0.15,
                    "after": 0.16,
                    "reason": "young holding sell deferred",
                }
            ]
        }

        envelope.apply_stage_ledger(
            new_weights={"SPY": 0.18, "QQQ": 0.16, "CASH": 0.66},
            mutation_ledger=stage_ledger,
            fallback_mutation_type="loss_trim",
            reason="position manager bridge",
            stage="position_manager",
        )

        self.assertEqual(envelope.accounting_check(), [])
        self.assertEqual(
            [m.mutation_type for m in envelope.ledger.mutations],
            ["min_hold_defer_sell", "loss_trim"],
        )
        self.assertAlmostEqual(envelope.final_target["QQQ"], 0.16)
        self.assertAlmostEqual(envelope.final_target["SPY"], 0.18)

    def test_apply_stage_mutation_ledger_has_no_fallback_guessing(self):
        envelope = TargetEnvelope(
            current_weights={"SPY": 0.10, "QQQ": 0.16, "CASH": 0.74},
            risk_approved_target={"SPY": 0.20, "QQQ": 0.15, "CASH": 0.65},
        )
        stage_ledger = {
            "mutations": [
                {
                    "type": "min_hold_defer_sell",
                    "ticker": "QQQ",
                    "before": 0.15,
                    "after": 0.16,
                    "reason": "young holding sell deferred",
                }
            ]
        }

        envelope.apply_stage_mutation_ledger(
            mutation_ledger=stage_ledger,
            stage="position_manager",
            reason="position manager direct ledger",
        )

        self.assertEqual(envelope.accounting_check(), [])
        self.assertEqual([m.ticker for m in envelope.ledger.mutations], ["QQQ"])
        self.assertAlmostEqual(envelope.final_target["QQQ"], 0.16)
        self.assertAlmostEqual(envelope.final_target["SPY"], 0.20)
        self.assertTrue(envelope.stage_snapshots[0]["direct_mutation_ledger_only"])

    def test_safety_diagnostics_marks_restricted_reduction(self):
        envelope = TargetEnvelope(
            current_weights={"QQQ": 0.16, "CASH": 0.84},
            risk_approved_target={"QQQ": 0.15, "CASH": 0.85},
        )
        envelope.mutate("QQQ", 0.14, "loss_trim", "restricted trim")

        diagnostics = envelope.safety_diagnostics(
            {"scorecard_restricted_tickers": ["QQQ"]}
        )

        row = diagnostics["rows"][0]
        self.assertEqual(row["ticker"], "QQQ")
        self.assertEqual(row["direction"], "reduce")
        self.assertTrue(row["restricted"])


if __name__ == "__main__":
    unittest.main()
