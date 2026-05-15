import unittest

from services.strategy_health import update_strategy_health_profiles


class StrategyHealthTests(unittest.TestCase):
    def test_updates_strategy_regime_health_profile(self):
        bundle = {
            "generated_at": "2026-05-15T20:00:00",
            "regime_label": "trending_bull",
            "strategies": [{"strategy_name": "momentum_lite_v1"}],
            "replay_metrics": {
                "momentum_lite_v1": {
                    "ic": 0.12,
                    "hit_rate": 0.58,
                    "avg_turnover": 0.11,
                    "max_drawdown_pct": 0.04,
                    "n_forward_return_samples": 30,
                    "n_ic_samples": 15,
                    "metric_reliability": {"level": "high"},
                }
            },
        }

        updated = update_strategy_health_profiles(bundle)
        profile = updated["profiles"]["momentum_lite_v1|trending_bull"]

        self.assertEqual(profile["latest"]["rolling_ic"], 0.12)
        self.assertEqual(profile["latest"]["hit_rate"], 0.58)
        self.assertEqual(profile["latest"]["max_drawdown_pct"], 0.04)
        self.assertFalse(profile["decay"]["flagged"])
        self.assertTrue(updated["parameter_adjustments"]["approval_required"])
        self.assertFalse(updated["parameter_adjustments"]["auto_apply"])

    def test_flags_strategy_decay_approval_only(self):
        existing = {"profiles": {}}
        for i, (ic, hit, dd) in enumerate(
            [(0.22, 0.62, 0.03), (0.20, 0.60, 0.04), (0.04, 0.42, 0.11)]
        ):
            existing = update_strategy_health_profiles(
                {
                    "generated_at": f"2026-05-{10 + i}T20:00:00",
                    "regime_label": "high_vol",
                    "strategies": [{"strategy_name": "mean_reversion_lite"}],
                    "replay_metrics": {
                        "mean_reversion_lite": {
                            "ic": ic,
                            "hit_rate": hit,
                            "avg_turnover": 0.18,
                            "max_drawdown_pct": dd,
                            "n_forward_return_samples": 30,
                            "n_ic_samples": 12,
                            "metric_reliability": {"level": "high"},
                        }
                    },
                },
                existing,
            )

        flags = existing["decay_flags"]

        self.assertEqual(len(flags), 1)
        self.assertEqual(flags[0]["strategy_name"], "mean_reversion_lite")
        self.assertTrue(flags[0]["approval_required"])
        self.assertEqual(
            existing["parameter_adjustments"]["suggestions"][0]["approval_required"],
            True,
        )


if __name__ == "__main__":
    unittest.main()
