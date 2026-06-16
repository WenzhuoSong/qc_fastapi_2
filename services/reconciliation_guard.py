"""Read-only reconciliation guard for expected target vs QC account truth."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from services.broker_order_filter import reconciliation_target_weights_from_command_payload
from services.operator_messages import format_reconciliation_guard_alert_message


DEFAULT_RECONCILIATION_GUARD_CONFIG: dict[str, Any] = {
    "enabled": True,
    "mode": "blocking",
    "relative_weight_tolerance": 0.0025,
    "absolute_notional_tolerance_usd": 100.0,
    "ignore_cash": True,
    "cash_tolerance_mode": "residual",
    "market_closed_behavior": "skip",
    "auto_set_reconciliation_halt": False,
    "auto_halt_min_clean_market_runs": 20,
    "auto_halt_min_clean_market_days": 5,
    "max_pending_ack_age_seconds": 300,
    "max_in_flight_age_seconds": 900,
    "whole_share_rounding_tolerance_enabled": True,
    "whole_share_rounding_tolerance_multiplier": 1.0,
    "max_rounding_tolerance_weight": 0.01,
}

IN_FLIGHT_STATES = {"pending_ack", "accepted", "orders_submitted", "partial"}
UNTRUSTED_STATES = {"pending_reconcile"}
SETTLED_STATES = {"filled", "noop_reconciled", "diverged"}
NO_EXECUTION_STATES = {"created", "rejected"}


def default_reconciliation_guard_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    out = dict(DEFAULT_RECONCILIATION_GUARD_CONFIG)
    out.update(config or {})
    mode = str(out.get("mode") or "blocking").lower().strip()
    out["mode"] = mode if mode in {"blocking", "observe", "off"} else "blocking"
    out["enabled"] = bool(out.get("enabled", True))
    out["relative_weight_tolerance"] = _positive_float(out.get("relative_weight_tolerance"), 0.0025)
    out["absolute_notional_tolerance_usd"] = max(
        _float_or_none(out.get("absolute_notional_tolerance_usd")) or 0.0,
        0.0,
    )
    out["ignore_cash"] = bool(out.get("ignore_cash", True))
    out["cash_tolerance_mode"] = str(out.get("cash_tolerance_mode") or "residual")
    out["market_closed_behavior"] = str(out.get("market_closed_behavior") or "skip").lower().strip()
    out["auto_set_reconciliation_halt"] = bool(out.get("auto_set_reconciliation_halt", False))
    out["auto_halt_min_clean_market_runs"] = max(int(_float_or_none(out.get("auto_halt_min_clean_market_runs")) or 20), 0)
    out["auto_halt_min_clean_market_days"] = max(int(_float_or_none(out.get("auto_halt_min_clean_market_days")) or 5), 0)
    out["max_pending_ack_age_seconds"] = max(int(_float_or_none(out.get("max_pending_ack_age_seconds")) or 300), 1)
    out["max_in_flight_age_seconds"] = max(int(_float_or_none(out.get("max_in_flight_age_seconds")) or 900), 1)
    out["whole_share_rounding_tolerance_enabled"] = bool(out.get("whole_share_rounding_tolerance_enabled", True))
    out["whole_share_rounding_tolerance_multiplier"] = _positive_float(
        out.get("whole_share_rounding_tolerance_multiplier"),
        1.0,
    )
    out["max_rounding_tolerance_weight"] = _positive_float(out.get("max_rounding_tolerance_weight"), 0.01)
    return out


async def load_reconciliation_guard(
    *,
    config: dict[str, Any] | None = None,
    account_state_guard: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load latest DB facts and evaluate the read-only reconciliation guard."""
    from sqlalchemy import desc, select

    from db.models import AccountStateSnapshot, ExecutionLog
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        snapshot = (
            await db.execute(
                select(AccountStateSnapshot)
                .order_by(desc(AccountStateSnapshot.recorded_at), desc(AccountStateSnapshot.id))
                .limit(1)
            )
        ).scalar_one_or_none()
        command = (
            await db.execute(
                select(ExecutionLog)
                .where(ExecutionLog.command_type == "weight_adjustment")
                .order_by(desc(ExecutionLog.executed_at), desc(ExecutionLog.id))
                .limit(1)
            )
        ).scalar_one_or_none()

    return evaluate_reconciliation_guard(
        snapshot=_snapshot_to_dict(snapshot) if snapshot else None,
        command=_command_to_dict(command) if command else None,
        account_state_guard=account_state_guard,
        config=config,
        now=now,
    )


def evaluate_reconciliation_guard(
    *,
    snapshot: dict[str, Any] | None,
    command: dict[str, Any] | None,
    account_state_guard: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    cfg = default_reconciliation_guard_config(config)
    checked_at = _strip_tz(now or datetime.now(UTC))
    base = {
        "enabled": cfg["enabled"],
        "mode": cfg["mode"],
        "checked_at": checked_at.isoformat(),
        "execution_effect": _execution_effect(cfg, False),
        "should_block_current_run": False,
        "should_set_reconciliation_halt": False,
        "max_drift": 0.0,
        "drift_tickers": [],
        "config": _public_config(cfg),
    }

    if not cfg["enabled"] or cfg["mode"] == "off":
        return {**base, "status": "disabled", "reason": "reconciliation_guard_disabled"}

    if account_state_guard and account_state_guard.get("should_block_pipeline"):
        return {
            **base,
            "status": "insufficient_data",
            "reason": "account_state_guard_not_passed",
        }

    if not snapshot:
        return _blockable_result(base, cfg, "insufficient_data", "missing_account_state_snapshot")

    if snapshot.get("is_market_open") is False and cfg["market_closed_behavior"] == "skip":
        return {
            **base,
            "status": "skipped_market_closed",
            "reason": "market_closed_reconciliation_skipped",
            "snapshot": _snapshot_summary(snapshot),
        }

    if not command:
        return {
            **base,
            "status": "insufficient_data",
            "reason": "missing_execution_command",
            "snapshot": _snapshot_summary(snapshot),
        }

    lifecycle_state = str(command.get("lifecycle_state") or "").lower().strip() or _state_from_qc_status(command)
    command_age = _command_age_seconds(command, checked_at)
    command_summary = _command_summary(command, lifecycle_state, command_age)
    feedback_trust = command.get("feedback_trust") if isinstance(command.get("feedback_trust"), dict) else {}

    if lifecycle_state in IN_FLIGHT_STATES:
        return _in_flight_result(base, cfg, command_summary, command_age, lifecycle_state)

    if lifecycle_state in UNTRUSTED_STATES:
        if _is_stuck(command_age, cfg["max_in_flight_age_seconds"]):
            return _stuck_result(base, cfg, command_summary, command_age, "execution_in_flight_timeout")
        return _blockable_result(
            base,
            cfg,
            "untrusted_feedback",
            (feedback_trust.get("reason") or "execution_feedback_pending_reconcile"),
            command=command_summary,
            feedback_trust=feedback_trust,
        )

    if lifecycle_state in NO_EXECUTION_STATES:
        return {
            **base,
            "status": "pass",
            "reason": "latest_command_has_no_execution_authority",
            "snapshot": _snapshot_summary(snapshot),
            "command": command_summary,
            "feedback_trust": feedback_trust,
        }

    expected = _clean_weights(command.get("target_weights") or {})
    actual = _clean_weights(snapshot.get("holdings_weights") or {})
    if not expected or not actual:
        return _blockable_result(
            base,
            cfg,
            "insufficient_data",
            "missing_expected_or_actual_holdings",
            snapshot=_snapshot_summary(snapshot),
            command=command_summary,
            feedback_trust=feedback_trust,
        )

    drift = calculate_reconciliation_drift(
        expected,
        actual,
        total_value=_float_or_none(snapshot.get("total_value")),
        prices=snapshot.get("prices") or {},
        config=cfg,
    )
    if drift["max_drift"] > 0:
        should_set_halt = bool(cfg["auto_set_reconciliation_halt"]) and bool(
            (feedback_trust or {}).get("trusted_for_reconciliation", False)
        )
        return _blockable_result(
            base,
            cfg,
            "diverged",
            "holdings_reconciliation_divergence",
            max_drift=drift["max_drift"],
            drift_tickers=drift["drift_tickers"],
            snapshot=_snapshot_summary(snapshot),
            command=command_summary,
            feedback_trust=feedback_trust,
            should_set_reconciliation_halt=should_set_halt,
        )

    return {
        **base,
        "status": "pass",
        "reason": "actual_holdings_match_expected_target",
        "snapshot": _snapshot_summary(snapshot),
        "command": command_summary,
        "feedback_trust": feedback_trust,
        "max_drift": drift["max_drift"],
        "drift_tickers": drift["drift_tickers"],
    }


def calculate_reconciliation_drift(
    expected: dict[str, Any],
    actual: dict[str, Any],
    *,
    total_value: float | None,
    prices: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = default_reconciliation_guard_config(config)
    left = _clean_weights(expected)
    right = _clean_weights(actual)
    tickers = sorted(set(left) | set(right))
    if cfg["ignore_cash"]:
        tickers = [ticker for ticker in tickers if ticker != "CASH"]
    total = float(total_value or 0.0)
    absolute_weight_floor = (
        float(cfg["absolute_notional_tolerance_usd"]) / total
        if total > 0
        else 0.0
    )
    base_threshold = max(float(cfg["relative_weight_tolerance"]), absolute_weight_floor)
    price_map = _clean_prices(prices or {})
    drift_tickers = []
    max_drift = 0.0
    for ticker in tickers:
        expected_w = float(left.get(ticker, 0.0) or 0.0)
        actual_w = float(right.get(ticker, 0.0) or 0.0)
        diff = actual_w - expected_w
        max_drift = max(max_drift, abs(diff))
        threshold = _ticker_reconciliation_threshold(
            ticker=ticker,
            base_threshold=base_threshold,
            total_value=total,
            prices=price_map,
            config=cfg,
        )
        if abs(diff) > threshold:
            drift_tickers.append({
                "ticker": ticker,
                "expected": round(expected_w, 6),
                "actual": round(actual_w, 6),
                "diff": round(diff, 6),
                "threshold": round(threshold, 6),
                "base_threshold": round(base_threshold, 6),
                "whole_share_tolerance": round(max(threshold - base_threshold, 0.0), 6),
            })
    drift_tickers.sort(key=lambda row: (-abs(float(row["diff"])), row["ticker"]))
    return {
        "max_drift": round(max((abs(float(row["diff"])) for row in drift_tickers), default=0.0), 6),
        "raw_max_abs_diff": round(max_drift, 6),
        "threshold": round(base_threshold, 6),
        "drift_tickers": drift_tickers,
    }


def format_reconciliation_guard_alert(verdict: dict[str, Any]) -> str:
    return format_reconciliation_guard_alert_message(verdict)


def _in_flight_result(
    base: dict[str, Any],
    cfg: dict[str, Any],
    command_summary: dict[str, Any],
    command_age: float | None,
    lifecycle_state: str,
) -> dict[str, Any]:
    threshold = cfg["max_pending_ack_age_seconds"] if lifecycle_state == "pending_ack" else cfg["max_in_flight_age_seconds"]
    if _is_stuck(command_age, threshold):
        return _stuck_result(base, cfg, {**command_summary, "timeout_threshold_seconds": threshold}, command_age, "execution_in_flight_timeout")
    return _blockable_result(
        base,
        cfg,
        "in_flight",
        "execution_in_flight_wait_for_settlement",
        command={**command_summary, "timeout_threshold_seconds": threshold},
    )


def _stuck_result(
    base: dict[str, Any],
    cfg: dict[str, Any],
    command_summary: dict[str, Any],
    command_age: float | None,
    reason: str,
) -> dict[str, Any]:
    return _blockable_result(
        base,
        cfg,
        "stuck_in_flight",
        reason,
        command={**command_summary, "age_seconds": command_age},
    )


def _blockable_result(
    base: dict[str, Any],
    cfg: dict[str, Any],
    status: str,
    reason: str,
    *,
    max_drift: float = 0.0,
    drift_tickers: list[dict[str, Any]] | None = None,
    snapshot: dict[str, Any] | None = None,
    command: dict[str, Any] | None = None,
    feedback_trust: dict[str, Any] | None = None,
    should_set_reconciliation_halt: bool = False,
) -> dict[str, Any]:
    should_block = cfg["mode"] == "blocking"
    return {
        **base,
        "status": status,
        "reason": reason,
        "execution_effect": _execution_effect(cfg, should_block),
        "should_block_current_run": should_block,
        "should_set_reconciliation_halt": bool(should_set_reconciliation_halt and should_block),
        "max_drift": round(float(max_drift or 0.0), 6),
        "drift_tickers": drift_tickers or [],
        "snapshot": snapshot,
        "command": command,
        "feedback_trust": feedback_trust or {},
    }


def _execution_effect(cfg: dict[str, Any], blocked: bool) -> str:
    if not cfg.get("enabled") or cfg.get("mode") == "off":
        return "none"
    if cfg.get("mode") == "blocking":
        return "blocking" if blocked else "blocking_ready"
    return "diagnostic_only"


def _is_stuck(age_seconds: float | None, threshold_seconds: int) -> bool:
    return age_seconds is not None and age_seconds > float(threshold_seconds)


def _command_age_seconds(command: dict[str, Any], now: datetime) -> float | None:
    started = _parse_datetime(
        command.get("latest_qc_ack_at")
        or command.get("qc_ack_at")
        or command.get("submitted_at")
        or command.get("executed_at")
    )
    if started is None:
        return None
    return max((now - started).total_seconds(), 0.0)


def _state_from_qc_status(command: dict[str, Any]) -> str:
    status = str(command.get("qc_status") or "").lower().strip()
    return {
        "submitted": "pending_ack",
        "accepted": "accepted",
        "orders_submitted": "orders_submitted",
        "partial": "partial",
        "timeout_no_ack": "pending_reconcile",
        "reconciled": "filled",
        "filled": "filled",
        "reconciliation_drift": "diverged",
        "not_sent": "created",
        "rejected": "rejected",
    }.get(status, "created")


def _command_summary(command: dict[str, Any], lifecycle_state: str, age_seconds: float | None) -> dict[str, Any]:
    return {
        "command_id": command.get("command_id"),
        "correlation_id": command.get("correlation_id"),
        "lifecycle_state": lifecycle_state,
        "qc_status": command.get("qc_status"),
        "policy_version": command.get("policy_version"),
        "age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
        "submitted_at": _iso_or_none(command.get("submitted_at")),
        "latest_qc_ack_at": _iso_or_none(command.get("latest_qc_ack_at") or command.get("qc_ack_at")),
    }


def _snapshot_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": snapshot.get("id"),
        "recorded_at": _iso_or_none(snapshot.get("recorded_at")),
        "account_timestamp": _iso_or_none(snapshot.get("account_timestamp")),
        "source_packet_type": snapshot.get("source_packet_type"),
        "is_market_open": snapshot.get("is_market_open"),
        "total_value": _float_or_none(snapshot.get("total_value")),
        "holdings_count": len(_clean_weights(snapshot.get("holdings_weights") or {})),
    }


def _snapshot_to_dict(row: Any) -> dict[str, Any]:
    raw_snapshot = getattr(row, "raw_snapshot", None) or {}
    return {
        "id": getattr(row, "id", None),
        "recorded_at": getattr(row, "recorded_at", None),
        "account_timestamp": getattr(row, "account_timestamp", None),
        "source_packet_type": getattr(row, "source_packet_type", None),
        "is_market_open": getattr(row, "is_market_open", None),
        "total_value": getattr(row, "total_value", None),
        "holdings_weights": getattr(row, "holdings_weights", None) or {},
        "target_weights": getattr(row, "target_weights", None) or {},
        "prices": _prices_from_raw_snapshot(raw_snapshot),
        "raw_snapshot": raw_snapshot,
    }


def _command_to_dict(row: Any) -> dict[str, Any]:
    payload = getattr(row, "command_payload", None) or {}
    qc_response = getattr(row, "qc_response", None) or {}
    lifecycle_metadata = getattr(row, "lifecycle_metadata", None) or {}
    return {
        "id": getattr(row, "id", None),
        "command_id": getattr(row, "command_id", None),
        "correlation_id": getattr(row, "correlation_id", None),
        "command_type": getattr(row, "command_type", None),
        "policy_version": getattr(row, "policy_version", None),
        "lifecycle_state": getattr(row, "lifecycle_state", None),
        "qc_status": getattr(row, "qc_status", None),
        "executed_at": getattr(row, "executed_at", None),
        "submitted_at": getattr(row, "submitted_at", None),
        "qc_ack_at": getattr(row, "qc_ack_at", None),
        "latest_qc_ack_at": getattr(row, "latest_qc_ack_at", None),
        "target_weights": _target_weights(payload, qc_response),
        "command_payload": payload,
        "feedback_trust": lifecycle_metadata.get("feedback_trust") if isinstance(lifecycle_metadata, dict) else {},
    }


def _target_weights(payload: dict[str, Any], qc_response: dict[str, Any]) -> dict[str, Any]:
    account_state = qc_response.get("account_state") if isinstance(qc_response.get("account_state"), dict) else {}
    reconciliation_target = reconciliation_target_weights_from_command_payload(payload)
    if reconciliation_target:
        return reconciliation_target
    for value in (
        qc_response.get("actual_target_weights"),
        account_state.get("target_weights"),
        payload.get("sent_weights"),
        payload.get("proposed_weights"),
    ):
        if isinstance(value, dict) and value:
            return value
    return {}


def _clean_weights(value: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(value, dict):
        return out
    for ticker, raw in value.items():
        key = str(ticker or "").upper().strip()
        number = _float_or_none(raw)
        if key and number is not None:
            out[key] = max(number, 0.0)
    return out


def _public_config(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "enabled": cfg.get("enabled"),
        "mode": cfg.get("mode"),
        "relative_weight_tolerance": cfg.get("relative_weight_tolerance"),
        "absolute_notional_tolerance_usd": cfg.get("absolute_notional_tolerance_usd"),
        "ignore_cash": cfg.get("ignore_cash"),
        "cash_tolerance_mode": cfg.get("cash_tolerance_mode"),
        "market_closed_behavior": cfg.get("market_closed_behavior"),
        "auto_set_reconciliation_halt": cfg.get("auto_set_reconciliation_halt"),
        "max_pending_ack_age_seconds": cfg.get("max_pending_ack_age_seconds"),
        "max_in_flight_age_seconds": cfg.get("max_in_flight_age_seconds"),
        "whole_share_rounding_tolerance_enabled": cfg.get("whole_share_rounding_tolerance_enabled"),
        "whole_share_rounding_tolerance_multiplier": cfg.get("whole_share_rounding_tolerance_multiplier"),
        "max_rounding_tolerance_weight": cfg.get("max_rounding_tolerance_weight"),
    }


def _ticker_reconciliation_threshold(
    *,
    ticker: str,
    base_threshold: float,
    total_value: float,
    prices: dict[str, float],
    config: dict[str, Any],
) -> float:
    threshold = float(base_threshold)
    if not bool(config.get("whole_share_rounding_tolerance_enabled", True)):
        return threshold
    price = prices.get(str(ticker or "").upper().strip())
    if not price or price <= 0 or total_value <= 0:
        return threshold
    share_tolerance = (
        price
        * float(config.get("whole_share_rounding_tolerance_multiplier") or 1.0)
        / total_value
    )
    share_tolerance = min(
        max(share_tolerance, 0.0),
        float(config.get("max_rounding_tolerance_weight") or 0.01),
    )
    return max(threshold, share_tolerance)


def _prices_from_raw_snapshot(raw_snapshot: dict[str, Any]) -> dict[str, float]:
    prices: dict[str, float] = {}
    for row in raw_snapshot.get("holdings_detail_rows") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        price = _float_or_none(row.get("market_price") or row.get("price"))
        if ticker and price and price > 0:
            prices[ticker] = price
    return prices


def _clean_prices(value: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(value, dict):
        return out
    for ticker, raw in value.items():
        key = str(ticker or "").upper().strip()
        price = _float_or_none(raw)
        if key and price and price > 0:
            out[key] = price
    return out


def _positive_float(value: Any, fallback: float) -> float:
    number = _float_or_none(value)
    return number if number is not None and number > 0 else fallback


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _strip_tz(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _strip_tz(datetime.fromisoformat(text))
    except ValueError:
        return None


def _strip_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _iso_or_none(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else None
