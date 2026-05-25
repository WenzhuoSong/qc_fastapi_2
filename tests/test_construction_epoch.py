import unittest

from services.construction_epoch import (
    CONSTRUCTION_EPOCH_CONTRACT_VERSION,
    build_construction_epoch,
    build_historical_replay_construction_epoch,
    construction_epoch_from_diagnostics,
    unknown_construction_epoch,
)


class ConstructionEpochTest(unittest.TestCase):
    def test_epoch_id_is_stable_for_same_config(self):
        cfg = {
            "portfolio_construction_mode": "gated",
            "min_shadow_cycles": 5,
            "min_pass_rate": 0.7,
        }

        first = build_construction_epoch(
            policy_version="execution_policy_v1",
            promotion_config=cfg,
            source="daily_signal_freeze",
        )
        second = build_construction_epoch(
            policy_version="execution_policy_v1",
            promotion_config=dict(reversed(list(cfg.items()))),
            source="daily_signal_freeze",
        )

        self.assertEqual(first["contract_version"], CONSTRUCTION_EPOCH_CONTRACT_VERSION)
        self.assertEqual(first["epoch_id"], second["epoch_id"])
        self.assertEqual(first["pc_mode"], "gated")

    def test_epoch_changes_when_pc_mode_changes(self):
        shadow = build_construction_epoch(
            pc_mode="shadow",
            policy_version="execution_policy_v1",
            promotion_config_hash="same",
        )
        gated = build_construction_epoch(
            pc_mode="gated",
            policy_version="execution_policy_v1",
            promotion_config_hash="same",
        )

        self.assertNotEqual(shadow["epoch_id"], gated["epoch_id"])

    def test_historical_and_unknown_epoch_contracts_are_explicit(self):
        historical = build_historical_replay_construction_epoch()
        unknown = unknown_construction_epoch()

        self.assertEqual(historical["pc_mode"], "historical_replay")
        self.assertEqual(unknown["epoch_id"], "unknown")
        self.assertEqual(unknown["execution_authority"], "none")

    def test_extracts_or_falls_back_from_diagnostics(self):
        epoch = build_construction_epoch(pc_mode="candidate", promotion_config_hash="abc")

        self.assertEqual(
            construction_epoch_from_diagnostics({"construction_epoch": epoch})["epoch_id"],
            epoch["epoch_id"],
        )
        self.assertEqual(
            construction_epoch_from_diagnostics({})["epoch_id"],
            "unknown",
        )


if __name__ == "__main__":
    unittest.main()
