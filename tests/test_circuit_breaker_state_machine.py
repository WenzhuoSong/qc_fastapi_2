from datetime import datetime, timedelta
import unittest

try:
    from services.circuit_breaker import CircuitBreakerMonitor, CircuitConfig, CircuitState, TriggerResult
except ModuleNotFoundError as exc:  # pragma: no cover - local lightweight env may omit DB deps
    if exc.name == "sqlalchemy":
        CircuitBreakerMonitor = CircuitConfig = CircuitState = TriggerResult = None
        _CIRCUIT_IMPORT_ERROR = "sqlalchemy is unavailable in this local test environment"
    else:
        raise
else:
    _CIRCUIT_IMPORT_ERROR = None


class _ConfigRow:
    def __init__(self, value):
        self.value = value


@unittest.skipIf(_CIRCUIT_IMPORT_ERROR, _CIRCUIT_IMPORT_ERROR or "")
class CircuitBreakerStateMachineTests(unittest.TestCase):
    def test_alert_closes_after_cooldown_when_escalations_clear(self):
        monitor = CircuitBreakerMonitor(
            CircuitConfig(cooldown_minutes=30, persistent_alert_hours=2)
        )
        circuit_cfg = _ConfigRow({
            "value": "ALERT",
            "updated_at": (datetime.utcnow() - timedelta(minutes=31)).isoformat(),
        })

        next_state, trigger, reason = monitor._compute_next_state(
            CircuitState.ALERT,
            [
                _clear("vix"),
                _clear("drawdown"),
                _clear("rejections"),
                _clear("llm_failure"),
                TriggerResult(
                    name="persistent_alert",
                    value=0.5,
                    threshold=2,
                    triggered=False,
                    direction="none",
                    details="ALERT for 0.5h",
                ),
            ],
            circuit_cfg,
        )

        self.assertEqual(next_state, CircuitState.CLOSED)
        self.assertEqual(trigger, "all_clear")
        self.assertIn("cooldown", reason)

    def test_alert_stays_open_when_escalation_is_still_active(self):
        monitor = CircuitBreakerMonitor(
            CircuitConfig(cooldown_minutes=30, persistent_alert_hours=2)
        )
        circuit_cfg = _ConfigRow({
            "value": "ALERT",
            "updated_at": (datetime.utcnow() - timedelta(minutes=31)).isoformat(),
        })

        next_state, trigger, reason = monitor._compute_next_state(
            CircuitState.ALERT,
            [
                _clear("vix"),
                _clear("drawdown"),
                TriggerResult(
                    name="llm_failure",
                    value=0.75,
                    threshold=0.5,
                    triggered=True,
                    direction="escalate",
                    details="LLM failure rate=75%",
                ),
                TriggerResult(
                    name="persistent_alert",
                    value=0.5,
                    threshold=2,
                    triggered=False,
                    direction="none",
                    details="ALERT for 0.5h",
                ),
            ],
            circuit_cfg,
        )

        self.assertEqual(next_state, CircuitState.ALERT)
        self.assertEqual(trigger, "")
        self.assertEqual(reason, "no state change")

    def test_persistent_alert_escalates_to_defensive(self):
        monitor = CircuitBreakerMonitor(
            CircuitConfig(cooldown_minutes=30, persistent_alert_hours=2)
        )
        circuit_cfg = _ConfigRow({
            "value": "ALERT",
            "updated_at": (datetime.utcnow() - timedelta(hours=3)).isoformat(),
            "primary_trigger": "vix",
        })

        next_state, trigger, reason = monitor._compute_next_state(
            CircuitState.ALERT,
            [
                _clear("vix"),
                _clear("drawdown"),
                TriggerResult(
                    name="persistent_alert",
                    value=3.0,
                    threshold=2,
                    triggered=True,
                    direction="escalate",
                    details="ALERT persisted 3.0h > 2h",
                ),
            ],
            circuit_cfg,
        )

        self.assertEqual(next_state, CircuitState.DEFENSIVE)
        self.assertEqual(trigger, "persistent_alert")
        self.assertIn("persisted", reason)

    def test_persistent_technical_alert_does_not_escalate_to_defensive(self):
        monitor = CircuitBreakerMonitor(
            CircuitConfig(cooldown_minutes=30, persistent_alert_hours=2)
        )
        circuit_cfg = _ConfigRow({
            "value": "ALERT",
            "updated_at": (datetime.utcnow() - timedelta(hours=3)).isoformat(),
            "primary_trigger": "llm_failure",
        })

        next_state, trigger, reason = monitor._compute_next_state(
            CircuitState.ALERT,
            [
                TriggerResult(
                    name="llm_failure",
                    value=0.75,
                    threshold=0.5,
                    triggered=True,
                    direction="escalate",
                    details="LLM failure rate=75%",
                ),
                TriggerResult(
                    name="persistent_alert",
                    value=3.0,
                    threshold=2,
                    triggered=True,
                    direction="escalate",
                    details="ALERT persisted 3.0h > 2h",
                ),
            ],
            circuit_cfg,
        )

        self.assertEqual(next_state, CircuitState.ALERT)
        self.assertEqual(trigger, "")
        self.assertEqual(reason, "no state change")

    def test_persistent_control_plane_alert_closes_when_original_trigger_clears(self):
        monitor = CircuitBreakerMonitor(
            CircuitConfig(cooldown_minutes=30, persistent_alert_hours=2)
        )
        circuit_cfg = _ConfigRow({
            "value": "ALERT",
            "updated_at": (datetime.utcnow() - timedelta(hours=3)).isoformat(),
            "primary_trigger": "policy_mismatch_timeout",
        })

        next_state, trigger, reason = monitor._compute_next_state(
            CircuitState.ALERT,
            [
                _clear("vix"),
                _clear("drawdown"),
                _clear("llm_failure"),
                TriggerResult(
                    name="persistent_alert",
                    value=3.0,
                    threshold=2,
                    triggered=True,
                    direction="escalate",
                    details="ALERT persisted 3.0h > 2h",
                ),
            ],
            circuit_cfg,
        )

        self.assertEqual(next_state, CircuitState.CLOSED)
        self.assertEqual(trigger, "all_clear")
        self.assertIn("cooldown", reason)


def _clear(name: str) -> TriggerResult:
    return TriggerResult(
        name=name,
        value=0,
        threshold=1,
        triggered=False,
        direction="clear",
        details=f"{name} clear",
    )


if __name__ == "__main__":
    unittest.main()
