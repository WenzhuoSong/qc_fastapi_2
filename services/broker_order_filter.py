"""Broker-aware executable order filtering before SetWeights commands."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from services.execution_lifecycle import is_reduce_only_vs_actual


DEFAULT_BROKER_ORDER_FILTER_CONFIG: dict[str, Any] = {
    "broker_order_filter_enabled": True,
    "broker_allow_reduce_only_micro_sells": True,
    "broker_buy_round_up_enabled": True,
    "broker_buy_round_up_max_multiplier": 1.5,
    "broker_min_non_liquidation_share_delta": 2.0,
    "broker_min_order_notional_usd": 500.0,
    "broker_estimated_order_fee_usd": 1.0,
    "broker_max_fee_bps": 25.0,
    "broker_liquidation_weight_epsilon": 0.00001,
    "broker_noop_weight_delta_epsilon": 0.000001,
}


async def apply_broker_order_filter(
    *,
    target_weights: dict[str, Any],
    current_weights: dict[str, Any] | None,
    config: dict[str, Any] | None = None,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return target weights with broker-inefficient micro orders suppressed."""
    effective_snapshot = snapshot if snapshot is not None else await _load_latest_broker_snapshot()
    return apply_broker_order_filter_to_snapshot(
        target_weights=target_weights,
        current_weights=current_weights,
        snapshot=effective_snapshot,
        config=config,
    )


def apply_broker_order_filter_to_snapshot(
    *,
    target_weights: dict[str, Any],
    current_weights: dict[str, Any] | None,
    snapshot: dict[str, Any] | None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Pure broker-order filter for tests and execution path reuse."""
    cfg = default_broker_order_filter_config(config)
    target = _clean_weights(target_weights)
    current = _clean_weights(current_weights or {})
    target = _with_cash_residual(target)
    before_metrics = _delta_metrics(target, current)
    portfolio_reduce_only = is_reduce_only_vs_actual(
        target,
        current,
        tolerance=float(cfg["broker_noop_weight_delta_epsilon"]),
    )

    diagnostic: dict[str, Any] = {
        "schema_version": "broker_order_filter_v1",
        "enabled": bool(cfg["broker_order_filter_enabled"]),
        "adjusted": False,
        "reason": None,
        "config": cfg,
        "suppressed_orders": [],
        "rounded_orders": [],
        "allowed_orders": [],
        "missing_inputs": [],
        "portfolio_reduce_only": portfolio_reduce_only,
        "metrics_before": before_metrics,
        "metrics_after": before_metrics,
        "target_weights": target,
        "no_executable_delta": before_metrics["gross_turnover"] <= float(cfg["broker_noop_weight_delta_epsilon"]),
    }
    if not bool(cfg["broker_order_filter_enabled"]):
        diagnostic["reason"] = "disabled"
        return diagnostic

    total_value = _float_or_none((snapshot or {}).get("total_value"))
    prices = _clean_price_map((snapshot or {}).get("prices") or {})
    if total_value is None or total_value <= 0:
        diagnostic["missing_inputs"].append("total_value")
        diagnostic["reason"] = "missing_total_value"
        return diagnostic

    after = dict(target)
    for ticker in sorted((set(target) | set(current)) - {"CASH"}):
        cur_w = float(current.get(ticker, 0.0) or 0.0)
        tgt_w = float(target.get(ticker, 0.0) or 0.0)
        delta_w = tgt_w - cur_w
        if abs(delta_w) <= float(cfg["broker_noop_weight_delta_epsilon"]):
            continue
        price = prices.get(ticker)
        if price is None or price <= 0:
            diagnostic["missing_inputs"].append(f"price:{ticker}")
            continue

        notional = abs(delta_w) * float(total_value)
        raw_share_delta = notional / float(price)
        liquidation = cur_w > 0 and tgt_w <= float(cfg["broker_liquidation_weight_epsilon"])
        fee_bps = (
            float(cfg["broker_estimated_order_fee_usd"]) / notional * 10000.0
            if notional > 0
            else None
        )
        order = {
            "ticker": ticker,
            "side": "buy" if delta_w > 0 else "sell",
            "current_weight": round(cur_w, 6),
            "target_weight": round(tgt_w, 6),
            "delta_weight": round(delta_w, 6),
            "estimated_notional_usd": round(notional, 2),
            "estimated_share_delta": round(raw_share_delta, 4),
            "price": round(float(price), 4),
            "fee_bps": round(fee_bps, 4) if fee_bps is not None else None,
            "liquidation_to_zero": bool(liquidation),
        }
        suppress_reason = _suppression_reason(order, cfg)
        reduce_only_micro_sell = (
            portfolio_reduce_only
            and delta_w < 0
            and bool(cfg.get("broker_allow_reduce_only_micro_sells", True))
        )
        if liquidation or reduce_only_micro_sell or suppress_reason is None:
            if reduce_only_micro_sell and suppress_reason is not None:
                order["micro_order_override"] = "portfolio_reduce_only_sell"
                order["would_have_suppressed_reason"] = suppress_reason
            diagnostic["allowed_orders"].append(order)
            continue
        rounded_order = _try_round_up_buy_order(
            order=order,
            cfg=cfg,
            current_weight=cur_w,
            total_value=float(total_value),
        )
        if rounded_order is not None and rounded_order.get("allowed"):
            rounded_target_weight = float(rounded_order["rounded_target_weight"])
            after[ticker] = rounded_target_weight
            diagnostic["rounded_orders"].append(rounded_order)
            diagnostic["allowed_orders"].append(rounded_order)
            continue
        after[ticker] = cur_w
        order["sanitized_target_weight"] = round(cur_w, 6)
        order["reason"] = suppress_reason
        if rounded_order is not None:
            order["round_up_attempt"] = rounded_order
        diagnostic["suppressed_orders"].append(order)

    after = _with_cash_residual(after)
    after_metrics = _delta_metrics(after, current)
    diagnostic["target_weights"] = after
    diagnostic["metrics_after"] = after_metrics
    diagnostic["adjusted"] = bool(diagnostic["suppressed_orders"] or diagnostic["rounded_orders"])
    diagnostic["reason"] = _filter_reason(diagnostic)
    diagnostic["no_executable_delta"] = after_metrics["gross_turnover"] <= float(cfg["broker_noop_weight_delta_epsilon"])
    return diagnostic


def default_broker_order_filter_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    out = dict(DEFAULT_BROKER_ORDER_FILTER_CONFIG)
    for key, default in DEFAULT_BROKER_ORDER_FILTER_CONFIG.items():
        raw = (config or {}).get(key, default)
        if isinstance(default, bool):
            out[key] = _bool_value(raw, default)
            continue
        try:
            out[key] = float(raw)
        except (TypeError, ValueError):
            out[key] = default
    return out


def reconciliation_target_weights_from_command_payload(payload: dict[str, Any] | None) -> dict[str, float]:
    """Return the strategic reconciliation target for a broker-filtered command.

    `sent_weights` may include operational round-up hints that make a small buy
    executable. Reconciliation should not require the account to land exactly on
    that rounded hint when the strategic target was lower and the residual is
    within normal share-rounding tolerance.
    """
    if not isinstance(payload, dict):
        return {}
    target = _clean_weights(payload.get("sent_weights") or payload.get("proposed_weights") or {})
    if not target:
        return {}
    broker = payload.get("command_preflight")
    broker = broker.get("broker_order_filter") if isinstance(broker, dict) else None
    if not isinstance(broker, dict):
        return target
    for order in broker.get("rounded_orders") or []:
        if not isinstance(order, dict):
            continue
        ticker = str(order.get("ticker") or "").upper().strip()
        original_target = _float_or_none(order.get("original_target_weight"))
        if ticker and original_target is not None:
            target[ticker] = max(original_target, 0.0)
    return _with_cash_residual(target)


def reconciliation_target_diagnostics_from_command_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Return auditable reconciliation target semantics for broker-rounded commands."""
    diagnostic: dict[str, Any] = {
        "schema_version": "broker_reconciliation_target_v1",
        "target_source": "none",
        "rounded_target_overrides": [],
        "target_weight_count": 0,
    }
    if not isinstance(payload, dict):
        return diagnostic

    sent = _clean_weights(payload.get("sent_weights") or {})
    proposed = _clean_weights(payload.get("proposed_weights") or {})
    target = reconciliation_target_weights_from_command_payload(payload)
    if target:
        diagnostic["target_weight_count"] = len(target)
    if sent:
        diagnostic["target_source"] = "sent_weights"
    elif proposed:
        diagnostic["target_source"] = "proposed_weights"

    broker = payload.get("command_preflight")
    broker = broker.get("broker_order_filter") if isinstance(broker, dict) else None
    if not isinstance(broker, dict):
        return diagnostic

    overrides: list[dict[str, Any]] = []
    for order in broker.get("rounded_orders") or []:
        if not isinstance(order, dict):
            continue
        ticker = str(order.get("ticker") or "").upper().strip()
        original_target = _float_or_none(order.get("original_target_weight"))
        rounded_target = _float_or_none(order.get("rounded_target_weight") or order.get("target_weight"))
        if not ticker or original_target is None:
            continue
        overrides.append(
            {
                "ticker": ticker,
                "original_target_weight": round(float(original_target), 6),
                "rounded_target_weight": round(float(rounded_target), 6) if rounded_target is not None else None,
                "sent_target_weight": sent.get(ticker),
                "reconciliation_target_weight": round(float(target.get(ticker, original_target)), 6),
                "reason": "broker_round_up_execution_hint_not_strategic_target",
            }
        )
    if overrides:
        diagnostic["target_source"] = "sent_weights_with_broker_round_up_original_target_overrides"
        diagnostic["rounded_target_overrides"] = overrides
    return diagnostic


def _suppression_reason(order: dict[str, Any], cfg: dict[str, Any]) -> str | None:
    if float(order["estimated_share_delta"]) < float(cfg["broker_min_non_liquidation_share_delta"]):
        return "below_min_non_liquidation_share_delta"
    if float(order["estimated_notional_usd"]) < float(cfg["broker_min_order_notional_usd"]):
        return "below_min_order_notional"
    fee_bps = order.get("fee_bps")
    if fee_bps is not None and float(fee_bps) > float(cfg["broker_max_fee_bps"]):
        return "fee_bps_above_threshold"
    return None


def _try_round_up_buy_order(
    *,
    order: dict[str, Any],
    cfg: dict[str, Any],
    current_weight: float,
    total_value: float,
) -> dict[str, Any] | None:
    if not bool(cfg.get("broker_buy_round_up_enabled", True)):
        return None
    if order.get("side") != "buy":
        return None
    if total_value <= 0:
        return None
    original_delta = float(order.get("delta_weight") or 0.0)
    if original_delta <= 0:
        return None
    min_shares = float(cfg["broker_min_non_liquidation_share_delta"])
    original_shares = float(order.get("estimated_share_delta") or 0.0)
    if original_shares >= min_shares:
        return None
    price = float(order.get("price") or 0.0)
    if price <= 0:
        return None

    rounded_notional = min_shares * price
    rounded_delta = rounded_notional / total_value
    multiplier = rounded_delta / original_delta if original_delta > 0 else float("inf")
    attempt = dict(order)
    attempt.update(
        {
            "round_up_policy": "buy_min_executable_shares_v1",
            "original_target_weight": order.get("target_weight"),
            "original_delta_weight": order.get("delta_weight"),
            "original_estimated_share_delta": order.get("estimated_share_delta"),
            "rounded_share_delta": round(min_shares, 4),
            "rounded_notional_usd": round(rounded_notional, 2),
            "rounded_delta_weight": round(rounded_delta, 6),
            "rounded_target_weight": round(current_weight + rounded_delta, 6),
            "round_up_multiplier": round(multiplier, 6),
        }
    )
    max_multiplier = float(cfg["broker_buy_round_up_max_multiplier"])
    if multiplier > max_multiplier:
        attempt["allowed"] = False
        attempt["reason"] = "round_up_multiplier_exceeds_limit"
        attempt["max_round_up_multiplier"] = round(max_multiplier, 6)
        return attempt

    rounded_fee_bps = (
        float(cfg["broker_estimated_order_fee_usd"]) / rounded_notional * 10000.0
        if rounded_notional > 0
        else None
    )
    rounded_check = {
        **order,
        "estimated_share_delta": min_shares,
        "estimated_notional_usd": rounded_notional,
        "fee_bps": rounded_fee_bps,
    }
    residual_reason = _suppression_reason(rounded_check, cfg)
    if residual_reason is not None:
        attempt["allowed"] = False
        attempt["reason"] = f"round_up_still_{residual_reason}"
        return attempt

    attempt["allowed"] = True
    attempt["reason"] = "rounded_up_to_min_executable_buy"
    attempt["target_weight"] = attempt["rounded_target_weight"]
    attempt["delta_weight"] = attempt["rounded_delta_weight"]
    attempt["estimated_notional_usd"] = attempt["rounded_notional_usd"]
    attempt["estimated_share_delta"] = attempt["rounded_share_delta"]
    attempt["fee_bps"] = round(rounded_fee_bps, 4) if rounded_fee_bps is not None else None
    return attempt


def _filter_reason(diagnostic: dict[str, Any]) -> str:
    suppressed = bool(diagnostic.get("suppressed_orders"))
    rounded = bool(diagnostic.get("rounded_orders"))
    if suppressed and rounded:
        return "micro_orders_suppressed_and_rounded"
    if rounded:
        return "micro_buy_orders_rounded_up"
    if suppressed:
        return "micro_orders_suppressed"
    return "pass"


async def _load_latest_broker_snapshot() -> dict[str, Any]:
    from sqlalchemy import desc, select

    from db.models import AccountStateSnapshot, HoldingsFactor
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(
                select(AccountStateSnapshot)
                .order_by(desc(AccountStateSnapshot.recorded_at), desc(AccountStateSnapshot.id))
                .limit(1)
            )
        ).scalar_one_or_none()
        if row is None:
            return {}
        prices = _prices_from_account_snapshot_row(row)
        if row.qc_snapshot_id:
            holdings = (
                await db.execute(
                    select(HoldingsFactor).where(HoldingsFactor.snapshot_id == row.qc_snapshot_id)
                )
            ).scalars().all()
            for item in holdings:
                ticker = str(getattr(item, "ticker", "") or "").upper().strip()
                price = _float_or_none(getattr(item, "price", None))
                if ticker and price and price > 0:
                    prices.setdefault(ticker, price)
        return {
            "source": "account_state_snapshot",
            "snapshot_id": row.id,
            "qc_snapshot_id": row.qc_snapshot_id,
            "recorded_at": _iso_or_none(row.recorded_at),
            "total_value": _float_or_none(row.total_value),
            "prices": prices,
        }


def _prices_from_account_snapshot_row(row: Any) -> dict[str, float]:
    raw = getattr(row, "raw_snapshot", None) or {}
    prices: dict[str, float] = {}
    for item in raw.get("holdings_detail_rows") or []:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").upper().strip()
        price = _float_or_none(item.get("market_price") or item.get("price"))
        if ticker and price and price > 0:
            prices[ticker] = price
    return prices


def _delta_metrics(target_weights: dict[str, float], current_weights: dict[str, float]) -> dict[str, float]:
    buy_delta = 0.0
    sell_delta = 0.0
    for ticker in sorted((set(target_weights) | set(current_weights)) - {"CASH"}):
        delta = float(target_weights.get(ticker, 0.0) or 0.0) - float(current_weights.get(ticker, 0.0) or 0.0)
        if delta > 0:
            buy_delta += delta
        elif delta < 0:
            sell_delta += abs(delta)
    gross = buy_delta + sell_delta
    return {
        "buy_delta": round(buy_delta, 6),
        "sell_delta": round(sell_delta, 6),
        "gross_turnover": round(gross / 2.0, 6),
    }


def _with_cash_residual(weights: dict[str, float]) -> dict[str, float]:
    out = dict(weights or {})
    equity_sum = sum(max(float(value or 0.0), 0.0) for ticker, value in out.items() if ticker != "CASH")
    out["CASH"] = max(1.0 - equity_sum, 0.0)
    return {ticker: round(float(value or 0.0), 6) for ticker, value in out.items() if value is not None}


def _clean_weights(weights: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = str(raw_ticker or "").upper().strip()
        if not ticker:
            continue
        value = _float_or_none(raw_weight)
        if value is None:
            continue
        out[ticker] = max(value, 0.0)
    return out


def _clean_price_map(prices: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw_ticker, raw_price in (prices or {}).items():
        ticker = str(raw_ticker or "").upper().strip()
        price = _float_or_none(raw_price)
        if ticker and price and price > 0:
            out[ticker] = price
    return out


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        return None
    return parsed


def _bool_value(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
