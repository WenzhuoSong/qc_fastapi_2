"""Deterministic staged execution throttle.

This module converts a desired target into the target allowed for this command.
It is an explicit post-risk mutation layer, so executor preflight remains a
read-only final safety check.
"""
from __future__ import annotations

from typing import Any

from services.execution_preflight import command_weight_delta_metrics
from services.mutation_ledger import MutationLedger


EXECUTION_THROTTLE_CONTRACT_VERSION = "v1"
DEFAULT_EXECUTION_THROTTLE_CONFIG = {
    "enabled": True,
    "max_buy_delta": None,
}


def apply_execution_throttle(
    *,
    target_weights: dict[str, Any],
    current_weights: dict[str, Any] | None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Stage positive buy deltas to the configured per-command buy limit.

    Sell deltas are intentionally not throttled here because delaying de-risking
    can increase portfolio risk. The existing executor preflight still enforces
    ``max_sell_delta`` as a hard safety limit.
    """
    cfg = _throttle_config(config)
    desired = _normalize_cash_first(_clean_weights(target_weights))
    current = _normalize_cash_first(_clean_weights(current_weights or {}))
    metrics_before = command_weight_delta_metrics(desired, current)

    if not cfg["enabled"]:
        return _result(
            applied=False,
            desired=desired,
            staged=desired,
            current=current,
            metrics_before=metrics_before,
            metrics_after=metrics_before,
            limits=cfg,
            buy_scale=1.0,
            deferred_delta={},
            violations=[],
            mutation_types=[],
            mutation_ledger=MutationLedger().to_dict(),
            reason="disabled",
        )

    max_buy_delta = _optional_float(cfg.get("max_buy_delta"))
    if max_buy_delta is None or metrics_before["buy_delta"] <= max_buy_delta + 1e-12:
        return _result(
            applied=False,
            desired=desired,
            staged=desired,
            current=current,
            metrics_before=metrics_before,
            metrics_after=metrics_before,
            limits=cfg,
            buy_scale=1.0,
            deferred_delta={},
            violations=[],
            mutation_types=[],
            mutation_ledger=MutationLedger().to_dict(),
            reason="within_limits",
        )

    buy_delta = float(metrics_before["buy_delta"] or 0.0)
    buy_scale = max(min(max_buy_delta / buy_delta, 1.0), 0.0) if buy_delta > 0 else 1.0
    staged: dict[str, float] = {}
    deferred_delta: dict[str, float] = {}
    for ticker in sorted((set(desired) | set(current)) - {"CASH"}):
        desired_w = float(desired.get(ticker, 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        delta = desired_w - current_w
        staged_w = current_w + delta * buy_scale if delta > 0 else desired_w
        if staged_w > 1e-9:
            staged[ticker] = staged_w
        deferred = desired_w - staged_w
        if abs(deferred) > 1e-9:
            deferred_delta[ticker] = round(deferred, 6)

    staged["CASH"] = max(1.0 - sum(staged.values()), 0.0)
    staged = _normalize_cash_first(staged)
    metrics_after = command_weight_delta_metrics(staged, current)
    mutation_ledger = _mutation_ledger_for_execution_throttle(
        desired=desired,
        staged=staged,
        deferred_delta=deferred_delta,
        buy_scale=buy_scale,
    )
    return _result(
        applied=True,
        desired=desired,
        staged=staged,
        current=current,
        metrics_before=metrics_before,
        metrics_after=metrics_after,
        limits=cfg,
        buy_scale=buy_scale,
        deferred_delta=deferred_delta,
        violations=[f"buy_delta_throttled:{buy_delta:.2%}->{max_buy_delta:.2%}"],
        mutation_types=["execution_buy_delta_throttle"],
        mutation_ledger=mutation_ledger,
        reason="buy_delta_exceeds_limit",
    )


def _result(
    *,
    applied: bool,
    desired: dict[str, float],
    staged: dict[str, float],
    current: dict[str, float],
    metrics_before: dict[str, float],
    metrics_after: dict[str, float],
    limits: dict[str, Any],
    buy_scale: float,
    deferred_delta: dict[str, float],
    violations: list[str],
    mutation_types: list[str],
    mutation_ledger: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    return {
        "contract_version": EXECUTION_THROTTLE_CONTRACT_VERSION,
        "enabled": bool(limits.get("enabled")),
        "applied": bool(applied),
        "reason": reason,
        "execution_effect": "target_weight_mutation" if applied else "none",
        "desired_target_weights": desired,
        "staged_target_weights": staged,
        "current_weights": current,
        "metrics_before": metrics_before,
        "metrics_after": metrics_after,
        "limits": {
            "max_buy_delta": _optional_float(limits.get("max_buy_delta")),
        },
        "buy_scale": round(float(buy_scale), 6),
        "deferred_delta": deferred_delta,
        "deferred_buy_delta": round(
            sum(max(float(value or 0.0), 0.0) for value in deferred_delta.values()),
            6,
        ),
        "violations": violations,
        "mutation_types": mutation_types,
        "mutation_ledger": mutation_ledger,
    }


def _mutation_ledger_for_execution_throttle(
    *,
    desired: dict[str, float],
    staged: dict[str, float],
    deferred_delta: dict[str, float],
    buy_scale: float,
) -> dict[str, Any]:
    ledger = MutationLedger()
    for ticker in sorted(deferred_delta):
        before = float(desired.get(ticker, 0.0) or 0.0)
        after = float(staged.get(ticker, 0.0) or 0.0)
        if after < before - 1e-9:
            ledger.record(
                mutation_type="execution_buy_delta_throttle",
                ticker=ticker,
                before=before,
                after=after,
                reason=f"execution buy delta throttled with buy_scale={buy_scale:.6f}",
            )
    return ledger.to_dict()


def _throttle_config(config: dict[str, Any] | None) -> dict[str, Any]:
    cfg = dict(DEFAULT_EXECUTION_THROTTLE_CONFIG)
    raw = config or {}
    if "enabled" in raw:
        cfg["enabled"] = bool(raw.get("enabled"))
    if "max_buy_delta" in raw:
        cfg["max_buy_delta"] = _optional_float(raw.get("max_buy_delta"))
    return cfg


def _clean_weights(weights: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (weights or {}).items():
        key = str(ticker or "").upper().strip()
        if not key:
            continue
        try:
            parsed = max(float(value or 0.0), 0.0)
        except (TypeError, ValueError):
            continue
        if parsed > 1e-12 or key == "CASH":
            out[key] = parsed
    return out


def _normalize_cash_first(weights: dict[str, float]) -> dict[str, float]:
    clean = dict(weights or {})
    equity = {
        ticker: max(float(weight or 0.0), 0.0)
        for ticker, weight in clean.items()
        if ticker != "CASH" and float(weight or 0.0) > 1e-12
    }
    equity_sum = sum(equity.values())
    cash = max(float(clean.get("CASH", 0.0) or 0.0), 0.0)
    total = equity_sum + cash
    if total <= 0:
        return {"CASH": 1.0}
    if total > 1.0 + 1e-9:
        return {
            ticker: round(weight / total, 6)
            for ticker, weight in sorted({**equity, "CASH": cash}.items())
            if weight > 1e-12 or ticker == "CASH"
        }
    out = {ticker: round(weight, 6) for ticker, weight in sorted(equity.items())}
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 6)
    return out


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed < 0:
        return None
    return parsed
