"""Diagnostic guard for QC account-state freshness and consistency."""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from services.market_calendar import us_equity_market_status


DEFAULT_ACCOUNT_STATE_GUARD_CONFIG: dict[str, Any] = {
    "enabled": True,
    "mode": "observe",
    "max_snapshot_age_seconds": 300,
    "max_market_closed_stale_seconds": 259200,
    "max_reference_weight_diff": 0.01,
    "require_no_open_orders": True,
    "require_buying_power": True,
    "require_policy_version": True,
    "expected_policy_version": None,
    "require_explicit_account_state": False,
    "ok_account_statuses": ["ok"],
    "ok_data_statuses": ["ok"],
}


def default_account_state_guard_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a normalized guard config with conservative observe defaults."""
    merged = dict(DEFAULT_ACCOUNT_STATE_GUARD_CONFIG)
    merged.update(config or {})
    mode = str(merged.get("mode") or "observe").lower().strip()
    merged["mode"] = mode if mode in {"observe", "blocking", "off"} else "observe"
    merged["max_snapshot_age_seconds"] = _positive_float(merged.get("max_snapshot_age_seconds"), 300.0)
    merged["max_market_closed_stale_seconds"] = _positive_float(
        merged.get("max_market_closed_stale_seconds"),
        259200.0,
    )
    merged["max_reference_weight_diff"] = _positive_float(merged.get("max_reference_weight_diff"), 0.01)
    merged["ok_account_statuses"] = sorted(_string_set(merged.get("ok_account_statuses"), {"ok"}))
    merged["ok_data_statuses"] = sorted(_string_set(merged.get("ok_data_statuses"), {"ok"}))
    return merged


def account_state_guard_pipeline_effect(verdict: dict[str, Any] | None) -> dict[str, Any]:
    """Translate a guard verdict into pipeline enforcement behavior."""
    verdict = verdict or {}
    mode = str(verdict.get("mode") or "observe").lower().strip()
    enabled = bool(verdict.get("enabled", True))
    if not enabled or mode == "off":
        return {
            "pipeline_enforcement": "none",
            "should_block_pipeline": False,
            "pipeline_effect_status": "disabled",
        }
    if mode == "blocking":
        should_block = not bool(verdict.get("allowed", True))
        return {
            "pipeline_enforcement": "blocking",
            "should_block_pipeline": should_block,
            "pipeline_effect_status": "blocked" if should_block else "pass",
        }
    return {
        "pipeline_enforcement": "observe_only",
        "should_block_pipeline": False,
        "pipeline_effect_status": "observe",
    }


async def load_latest_account_state_guard(
    *,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load the latest normalized account snapshot and evaluate it."""
    cfg = default_account_state_guard_config(config)
    if not bool(cfg.get("enabled", True)):
        return evaluate_account_state_guard(None, config=cfg, now=now)

    from sqlalchemy import desc, select

    from db.models import AccountStateSnapshot, HoldingsFactor
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(
                select(AccountStateSnapshot)
                .order_by(desc(AccountStateSnapshot.recorded_at))
                .limit(1)
            )
        ).scalar_one_or_none()
        reference_weights: dict[str, float] | None = None
        if row and row.qc_snapshot_id:
            holdings = (
                await db.execute(
                    select(HoldingsFactor).where(HoldingsFactor.snapshot_id == row.qc_snapshot_id)
                )
            ).scalars().all()
            reference_weights = {
                str(item.ticker).upper(): _float_or_zero(item.weight_current)
                for item in holdings
                if item.ticker
            }

    return evaluate_account_state_guard(
        _row_to_snapshot(row) if row else None,
        config=cfg,
        now=now,
        reference_weights=reference_weights,
    )


def evaluate_account_state_guard(
    snapshot: dict[str, Any] | None,
    *,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
    reference_weights: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate whether the latest QC account state is trustworthy.

    In observe mode this never blocks execution; it reports whether the same
    facts would block once the guard is promoted.
    """
    cfg = default_account_state_guard_config(config)
    mode = cfg["mode"]
    now = _strip_tz(now or datetime.now(UTC))

    if mode == "off" or not bool(cfg.get("enabled", True)):
        return {
            "enabled": False,
            "mode": mode,
            "status": "disabled",
            "allowed": True,
            "would_block": False,
            "execution_effect": "none",
            "blockers": [],
            "warnings": [],
            "checks": {},
            "snapshot": None,
            "config": _public_config(cfg),
        }

    checks: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    blockers: list[str] = []

    if not snapshot:
        _add_check(checks, blockers, "snapshot_available", False, reason="missing_account_state_snapshot")
        return _build_result(mode, blockers, warnings, checks, None, cfg)

    recorded_at = _parse_datetime(snapshot.get("recorded_at"))
    age_seconds = (now - recorded_at).total_seconds() if recorded_at else None
    freshness = classify_account_snapshot_freshness(
        age_seconds=age_seconds,
        now=now,
        config=cfg,
    )
    _add_check(
        checks,
        blockers,
        "snapshot_fresh",
        bool(freshness["pass"]),
        actual=round(age_seconds, 3) if age_seconds is not None else None,
        threshold=cfg["max_snapshot_age_seconds"],
        reason=str(freshness["blocker_reason"] or "account_state_snapshot_stale_or_missing_time"),
        blocker=bool(freshness["blocking"]),
        extra={
            "classification": freshness["classification"],
            "market_status": freshness["market_status"],
            "max_market_closed_stale_seconds": cfg["max_market_closed_stale_seconds"],
        },
    )
    if freshness.get("warning"):
        warnings.append(str(freshness["warning"]))

    raw = snapshot.get("raw_snapshot") if isinstance(snapshot.get("raw_snapshot"), dict) else {}
    explicit = bool(raw.get("explicit_account_state"))
    require_explicit = bool(cfg.get("require_explicit_account_state"))
    _add_check(
        checks,
        blockers,
        "explicit_account_state",
        explicit or not require_explicit,
        actual=explicit,
        threshold=require_explicit,
        reason="missing_explicit_account_state",
    )
    if not explicit and not require_explicit:
        warnings.append("legacy_or_derived_account_state")

    account_status = str(snapshot.get("account_status") or "unknown").lower()
    _add_check(
        checks,
        blockers,
        "account_status_ok",
        account_status in cfg["ok_account_statuses"],
        actual=account_status,
        threshold=sorted(cfg["ok_account_statuses"]),
        reason="account_status_not_ok",
    )

    data_status = str(snapshot.get("data_status") or "unknown").lower()
    _add_check(
        checks,
        blockers,
        "data_status_ok",
        data_status in cfg["ok_data_statuses"],
        actual=data_status,
        threshold=sorted(cfg["ok_data_statuses"]),
        reason="data_status_not_ok",
    )

    policy_version = str(snapshot.get("policy_version") or "").strip()
    policy_ok = bool(policy_version) and policy_version.lower() not in {"unknown", "none", "null"}
    _add_check(
        checks,
        blockers,
        "policy_version_present",
        policy_ok or not bool(cfg.get("require_policy_version", True)),
        actual=policy_version or None,
        threshold="known policy_version",
        reason="missing_policy_version",
    )
    expected_policy_version = str(cfg.get("expected_policy_version") or "").strip()
    if expected_policy_version:
        _add_check(
            checks,
            blockers,
            "policy_version_matches_expected",
            policy_version == expected_policy_version,
            actual=policy_version or None,
            threshold=expected_policy_version,
            reason="policy_version_mismatch",
        )

    open_order_count = _int_or_none(snapshot.get("open_order_count"))
    has_open_orders = _bool_or_none(snapshot.get("has_open_orders"))
    no_open_orders = (
        open_order_count == 0
        if open_order_count is not None
        else has_open_orders is False
    )
    _add_check(
        checks,
        blockers,
        "no_open_orders",
        no_open_orders or not bool(cfg.get("require_no_open_orders", True)),
        actual={"open_order_count": open_order_count, "has_open_orders": has_open_orders},
        threshold="0 blocking open orders",
        reason="open_orders_present_or_unknown",
    )

    buying_power = _float_or_none(snapshot.get("buying_power"))
    _add_check(
        checks,
        blockers,
        "buying_power_present",
        buying_power is not None or not bool(cfg.get("require_buying_power", True)),
        actual=buying_power,
        threshold="known buying_power",
        reason="missing_buying_power",
    )

    holdings_weights = _clean_weights(snapshot.get("holdings_weights") or {})
    _add_check(
        checks,
        blockers,
        "holdings_weights_present",
        bool(holdings_weights),
        actual=len(holdings_weights),
        threshold="non-empty holdings_weights",
        reason="missing_holdings_weights",
    )

    reference = _clean_weights(reference_weights or {})
    if reference:
        max_diff = _max_weight_diff(holdings_weights, reference)
        _add_check(
            checks,
            blockers,
            "holdings_match_snapshot_rows",
            max_diff <= float(cfg["max_reference_weight_diff"]),
            actual=round(max_diff, 6),
            threshold=cfg["max_reference_weight_diff"],
            reason="account_holdings_mismatch_snapshot_rows",
        )
    else:
        warnings.append("reference_holdings_unavailable")

    for warning in raw.get("warnings") or []:
        if isinstance(warning, str) and warning not in warnings:
            warnings.append(warning)

    return _build_result(mode, blockers, warnings, checks, _snapshot_summary(snapshot, age_seconds), cfg, freshness=freshness)


def classify_account_snapshot_freshness(
    *,
    age_seconds: float | None,
    now: datetime | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify snapshot staleness with market-closed semantics.

    Market-open stale account truth is dangerous and remains blocking. Market
    closed stale account truth is expected up to a generous closed-window
    threshold and should not panic the operator or stop diagnostic analysis.
    """
    cfg = default_account_state_guard_config(config)
    current = _strip_tz(now or datetime.now(UTC))
    market_status = _market_status_dict(current)
    if age_seconds is None:
        return {
            "classification": "missing_snapshot_time",
            "pass": False,
            "blocking": True,
            "blocker_reason": "account_state_snapshot_stale_or_missing_time",
            "warning": None,
            "market_status": market_status,
        }
    age = max(float(age_seconds), 0.0)
    if age <= float(cfg["max_snapshot_age_seconds"]):
        return {
            "classification": "fresh",
            "pass": True,
            "blocking": False,
            "blocker_reason": None,
            "warning": None,
            "market_status": market_status,
        }
    if bool(market_status.get("is_open")):
        return {
            "classification": "unexpected_market_open_stale",
            "pass": False,
            "blocking": True,
            "blocker_reason": "account_state_snapshot_stale_or_missing_time",
            "warning": None,
            "market_status": market_status,
        }
    if age <= float(cfg["max_market_closed_stale_seconds"]):
        return {
            "classification": "expected_market_closed_stale",
            "pass": True,
            "blocking": False,
            "blocker_reason": None,
            "warning": None,
            "market_status": market_status,
        }
    return {
        "classification": "extended_closed_stale",
        "pass": False,
        "blocking": False,
        "blocker_reason": "extended_closed_stale",
        "warning": "extended_closed_stale",
        "market_status": market_status,
    }


def _build_result(
    mode: str,
    blockers: list[str],
    warnings: list[str],
    checks: dict[str, dict[str, Any]],
    snapshot: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    freshness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    would_block = bool(blockers)
    allowed = not would_block if mode == "blocking" else True
    if would_block:
        status = "blocked" if mode == "blocking" else "would_block"
    else:
        status = "pass"
    return {
        "enabled": True,
        "mode": mode,
        "status": status,
        "allowed": allowed,
        "would_block": would_block,
        "execution_effect": "blocking" if mode == "blocking" else "diagnostic_only",
        "blockers": blockers,
        "warnings": warnings,
        "checks": checks,
        "snapshot": snapshot,
        "freshness": freshness or {},
        "config": _public_config(cfg),
    }


def _row_to_snapshot(row: Any) -> dict[str, Any]:
    return {
        "id": row.id,
        "qc_snapshot_id": row.qc_snapshot_id,
        "recorded_at": row.recorded_at,
        "account_timestamp": row.account_timestamp,
        "source_packet_type": row.source_packet_type,
        "contract_version": row.contract_version,
        "account_status": row.account_status,
        "data_status": row.data_status,
        "policy_version": row.policy_version,
        "total_value": row.total_value,
        "cash": row.cash,
        "cash_pct": row.cash_pct,
        "buying_power": row.buying_power,
        "open_order_count": row.open_order_count,
        "has_open_orders": row.has_open_orders,
        "is_market_open": row.is_market_open,
        "holdings_weights": row.holdings_weights or {},
        "target_weights": row.target_weights or {},
        "raw_snapshot": row.raw_snapshot or {},
    }


def _snapshot_summary(snapshot: dict[str, Any], age_seconds: float | None) -> dict[str, Any]:
    holdings = _clean_weights(snapshot.get("holdings_weights") or {})
    return {
        "id": snapshot.get("id"),
        "qc_snapshot_id": snapshot.get("qc_snapshot_id"),
        "recorded_at": _iso_or_none(snapshot.get("recorded_at")),
        "account_timestamp": _iso_or_none(snapshot.get("account_timestamp")),
        "age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
        "source_packet_type": snapshot.get("source_packet_type"),
        "contract_version": snapshot.get("contract_version"),
        "account_status": snapshot.get("account_status"),
        "data_status": snapshot.get("data_status"),
        "policy_version": snapshot.get("policy_version"),
        "open_order_count": snapshot.get("open_order_count"),
        "has_open_orders": snapshot.get("has_open_orders"),
        "is_market_open": snapshot.get("is_market_open"),
        "holdings_count": len(holdings),
    }


def _add_check(
    checks: dict[str, dict[str, Any]],
    blockers: list[str],
    name: str,
    passed: bool,
    *,
    actual: Any = None,
    threshold: Any = None,
    reason: str,
    blocker: bool = True,
    extra: dict[str, Any] | None = None,
) -> None:
    checks[name] = {
        "pass": bool(passed),
        "actual": actual,
        "threshold": threshold,
        "reason": None if passed else reason,
    }
    if extra:
        checks[name].update(extra)
    if not passed and blocker:
        blockers.append(reason)


def _max_weight_diff(left: dict[str, float], right: dict[str, float]) -> float:
    tickers = set(left) | set(right)
    if not tickers:
        return 0.0
    return max(abs(float(left.get(t, 0.0)) - float(right.get(t, 0.0))) for t in tickers)


def _clean_weights(value: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, raw in (value or {}).items():
        key = str(ticker or "").upper().strip()
        number = _float_or_none(raw)
        if key and number is not None:
            out[key] = number
    return out


def _public_config(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "mode": cfg.get("mode"),
        "max_snapshot_age_seconds": cfg.get("max_snapshot_age_seconds"),
        "max_market_closed_stale_seconds": cfg.get("max_market_closed_stale_seconds"),
        "max_reference_weight_diff": cfg.get("max_reference_weight_diff"),
        "require_no_open_orders": cfg.get("require_no_open_orders"),
        "require_buying_power": cfg.get("require_buying_power"),
        "require_policy_version": cfg.get("require_policy_version"),
        "expected_policy_version": cfg.get("expected_policy_version"),
        "require_explicit_account_state": cfg.get("require_explicit_account_state"),
    }


def _positive_float(value: Any, fallback: float) -> float:
    number = _float_or_none(value)
    return number if number is not None and number > 0 else fallback


def _string_set(value: Any, fallback: set[str]) -> set[str]:
    if not isinstance(value, (list, tuple, set)):
        return set(fallback)
    out = {str(item).lower().strip() for item in value if str(item).strip()}
    return out or set(fallback)


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _float_or_zero(value: Any) -> float:
    return _float_or_none(value) or 0.0


def _int_or_none(value: Any) -> int | None:
    number = _float_or_none(value)
    return int(number) if number is not None else None


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return bool(value)


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


def _market_status_dict(now: datetime) -> dict[str, Any]:
    aware = _strip_tz(now).replace(tzinfo=UTC)
    return us_equity_market_status(aware).to_dict()


def _iso_or_none(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else None
