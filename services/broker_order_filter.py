"""Broker-aware executable order filtering before SetWeights commands."""
from __future__ import annotations

from datetime import datetime
from typing import Any


DEFAULT_BROKER_ORDER_FILTER_CONFIG: dict[str, Any] = {
    "broker_order_filter_enabled": True,
    "broker_allow_reduce_only_micro_sells": True,
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
    portfolio_reduce_only = _is_portfolio_reduce_only(target, current)

    diagnostic: dict[str, Any] = {
        "schema_version": "broker_order_filter_v1",
        "enabled": bool(cfg["broker_order_filter_enabled"]),
        "adjusted": False,
        "reason": None,
        "config": cfg,
        "suppressed_orders": [],
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
        after[ticker] = cur_w
        order["sanitized_target_weight"] = round(cur_w, 6)
        order["reason"] = suppress_reason
        diagnostic["suppressed_orders"].append(order)

    after = _with_cash_residual(after)
    after_metrics = _delta_metrics(after, current)
    diagnostic["target_weights"] = after
    diagnostic["metrics_after"] = after_metrics
    diagnostic["adjusted"] = bool(diagnostic["suppressed_orders"])
    diagnostic["reason"] = "micro_orders_suppressed" if diagnostic["adjusted"] else "pass"
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


def _suppression_reason(order: dict[str, Any], cfg: dict[str, Any]) -> str | None:
    if float(order["estimated_share_delta"]) < float(cfg["broker_min_non_liquidation_share_delta"]):
        return "below_min_non_liquidation_share_delta"
    if float(order["estimated_notional_usd"]) < float(cfg["broker_min_order_notional_usd"]):
        return "below_min_order_notional"
    fee_bps = order.get("fee_bps")
    if fee_bps is not None and float(fee_bps) > float(cfg["broker_max_fee_bps"]):
        return "fee_bps_above_threshold"
    return None


def _is_portfolio_reduce_only(target: dict[str, float], current: dict[str, float]) -> bool:
    """Return true only when the whole target cannot increase any risk asset."""
    if not target:
        return False
    saw_reduction = False
    for ticker in sorted((set(target) | set(current)) - {"CASH"}):
        target_w = float(target.get(ticker, 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        if target_w > current_w + 1e-9:
            return False
        if target_w < current_w - 1e-9:
            saw_reduction = True
    return saw_reduction


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
