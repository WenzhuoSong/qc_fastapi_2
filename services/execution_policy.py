"""Shared execution policy for ticker roles and hard caps.

This module answers the execution question: how much may be sent for a ticker.
It is intentionally separate from group_contract, which answers exposure and
basket-review questions.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any


class TickerRole(str, Enum):
    CORE = "core"
    SECTOR = "sector"
    THEMATIC = "thematic"
    SATELLITE = "satellite"
    HEDGE = "hedge"
    WATCHLIST = "watchlist"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RolePolicy:
    role: TickerRole
    max_single_weight: float
    max_total_group_weight: float
    hedge_only: bool = False


POLICY_VERSION = "sprint8a"


ROLE_POLICIES: dict[TickerRole, RolePolicy] = {
    TickerRole.CORE: RolePolicy(TickerRole.CORE, 0.25, 0.75),
    TickerRole.SECTOR: RolePolicy(TickerRole.SECTOR, 0.15, 0.45),
    TickerRole.THEMATIC: RolePolicy(TickerRole.THEMATIC, 0.075, 0.25),
    TickerRole.SATELLITE: RolePolicy(TickerRole.SATELLITE, 0.05, 0.20),
    TickerRole.HEDGE: RolePolicy(TickerRole.HEDGE, 0.03, 0.08, hedge_only=True),
    TickerRole.WATCHLIST: RolePolicy(TickerRole.WATCHLIST, 0.0, 0.0),
    TickerRole.UNKNOWN: RolePolicy(TickerRole.UNKNOWN, 0.0, 0.0),
}


TICKER_ROLES: dict[str, TickerRole] = {
    # Core / broad market
    "SPY": TickerRole.CORE,
    "QQQ": TickerRole.CORE,
    "IWM": TickerRole.CORE,
    "RSP": TickerRole.CORE,
    # Sectors
    "XLK": TickerRole.SECTOR,
    "XLF": TickerRole.SECTOR,
    "XLE": TickerRole.SECTOR,
    "XLV": TickerRole.SECTOR,
    "XLI": TickerRole.SECTOR,
    "XLY": TickerRole.SECTOR,
    "XLP": TickerRole.SECTOR,
    "XLU": TickerRole.SECTOR,
    "XLRE": TickerRole.SECTOR,
    "XLB": TickerRole.SECTOR,
    "XLC": TickerRole.SECTOR,
    "ITA": TickerRole.SECTOR,
    "XAR": TickerRole.SECTOR,
    "IBB": TickerRole.SECTOR,
    "XBI": TickerRole.SECTOR,
    # Thematic / factor tilts
    "SOXX": TickerRole.THEMATIC,
    "PSI": TickerRole.THEMATIC,
    "FTXL": TickerRole.THEMATIC,
    "SMH": TickerRole.THEMATIC,
    "XSD": TickerRole.THEMATIC,
    "AIQ": TickerRole.THEMATIC,
    "BOTZ": TickerRole.THEMATIC,
    "CIBR": TickerRole.THEMATIC,
    "HACK": TickerRole.THEMATIC,
    "IGV": TickerRole.THEMATIC,
    "ICLN": TickerRole.THEMATIC,
    "TAN": TickerRole.THEMATIC,
    "URA": TickerRole.THEMATIC,
    "GRID": TickerRole.THEMATIC,
    "VUG": TickerRole.THEMATIC,
    "VTV": TickerRole.THEMATIC,
    "USMV": TickerRole.THEMATIC,
    # Satellites / defensive diversifiers
    "DRAM": TickerRole.SATELLITE,
    "VEA": TickerRole.SATELLITE,
    "VWO": TickerRole.SATELLITE,
    "TLT": TickerRole.SATELLITE,
    "IEF": TickerRole.SATELLITE,
    "BND": TickerRole.SATELLITE,
    "SGOV": TickerRole.SATELLITE,
    "GLD": TickerRole.SATELLITE,
    # Tightly capped hedge/tactical instruments.
    "TQQQ": TickerRole.HEDGE,
    "SQQQ": TickerRole.HEDGE,
    "SOXL": TickerRole.HEDGE,
    "SOXS": TickerRole.HEDGE,
    "SPXL": TickerRole.HEDGE,
    "SPXS": TickerRole.HEDGE,
    "UVXY": TickerRole.HEDGE,
    "VIXY": TickerRole.HEDGE,
}


def _clean_ticker(ticker: str) -> str:
    return str(ticker or "").upper().strip()


def get_role(ticker: str) -> TickerRole:
    if _clean_ticker(ticker) == "CASH":
        return TickerRole.CORE
    return TICKER_ROLES.get(_clean_ticker(ticker), TickerRole.UNKNOWN)


def get_policy(ticker: str) -> RolePolicy:
    return ROLE_POLICIES[get_role(ticker)]


def is_tradable(ticker: str) -> bool:
    role = get_role(ticker)
    return role not in {TickerRole.WATCHLIST, TickerRole.UNKNOWN}


def check_weight_allowed(ticker: str, proposed_weight: float) -> tuple[bool, str]:
    ticker = _clean_ticker(ticker)
    weight = float(proposed_weight or 0.0)
    if ticker == "CASH" or weight <= 0.0:
        return True, "zero/non-positive weight allowed for removal"

    role = get_role(ticker)
    if role == TickerRole.UNKNOWN:
        return False, f"{ticker} UNKNOWN - not registered in execution_policy.TICKER_ROLES"
    if role == TickerRole.WATCHLIST:
        return False, f"{ticker} WATCHLIST - observation only"

    policy = ROLE_POLICIES[role]
    if weight > policy.max_single_weight + 1e-12:
        return False, (
            f"{ticker} ({role.value}) {weight:.2%} "
            f"> hard cap {policy.max_single_weight:.2%}"
        )
    return True, "within policy"


def check_portfolio_exposure(weights: dict[str, Any]) -> list[dict[str, Any]]:
    totals: dict[TickerRole, float] = defaultdict(float)
    for ticker, raw_weight in (weights or {}).items():
        ticker = _clean_ticker(ticker)
        if ticker == "CASH":
            continue
        weight = float(raw_weight or 0.0)
        if weight <= 0.0:
            continue
        totals[get_role(ticker)] += weight

    results: list[dict[str, Any]] = []
    for role, policy in ROLE_POLICIES.items():
        if role in {TickerRole.WATCHLIST, TickerRole.UNKNOWN}:
            continue
        current = totals.get(role, 0.0)
        results.append(
            {
                "role": role.value,
                "current_total": current,
                "cap": policy.max_total_group_weight,
                "violated": current > policy.max_total_group_weight + 1e-12,
            }
        )
    return results


def evaluate_policy(
    *,
    weights: dict[str, Any],
    current_weights: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate a proposed target against the canonical execution policy.

    This function is intentionally read-only. It reports violations and
    suggested cap events but does not repair weights.
    """
    clean = _clean_weight_map(weights)
    current = _clean_weight_map(current_weights or {})
    ctx = context or {}
    violations: list[str] = []
    cap_violations: list[dict[str, Any]] = []
    cap_events: list[dict[str, Any]] = []

    unknown_positive: list[str] = []
    watchlist_positive: list[str] = []
    single_cap_violations: list[dict[str, Any]] = []
    hedge_policy_violations: list[str] = []

    hedge_allowed = bool(ctx.get("hedge_allowed", True))
    for ticker, weight in sorted(clean.items()):
        if ticker == "CASH" or weight <= 0.0:
            continue
        role = get_role(ticker)
        policy = ROLE_POLICIES[role]
        allowed, reason = check_weight_allowed(ticker, weight)
        if not allowed:
            row = {
                "ticker": ticker,
                "weight": round(weight, 6),
                "role": role.value,
                "cap": policy.max_single_weight,
                "reason": reason,
            }
            cap_violations.append(row)
            cap_events.append(row)
            violations.append(reason)
        if role == TickerRole.UNKNOWN:
            unknown_positive.append(ticker)
        if role == TickerRole.WATCHLIST:
            watchlist_positive.append(ticker)
        if role not in {TickerRole.UNKNOWN, TickerRole.WATCHLIST} and weight > policy.max_single_weight + 1e-12:
            single_cap_violations.append(
                {
                    "ticker": ticker,
                    "weight": round(weight, 6),
                    "role": role.value,
                    "cap": policy.max_single_weight,
                    "ratio_to_cap": round(weight / policy.max_single_weight, 6)
                    if policy.max_single_weight > 0
                    else None,
                }
            )
        if policy.hedge_only and not hedge_allowed and weight > current.get(ticker, 0.0) + 1e-12:
            reason = f"{ticker} hedge-only exposure cannot be increased in this context"
            hedge_policy_violations.append(ticker)
            violations.append(reason)

    exposure_rows = check_portfolio_exposure(clean)
    group_violations = [row for row in exposure_rows if row["violated"]]
    for row in group_violations:
        reason = (
            f"{row['role']} exposure {float(row['current_total']):.2%} "
            f"> cap {float(row['cap']):.2%}"
        )
        violations.append(reason)
        cap_events.append(
            {
                "group_role": row["role"],
                "current_total": round(float(row["current_total"]), 6),
                "cap": round(float(row["cap"]), 6),
                "reason": reason,
            }
        )

    min_cash = _optional_float(ctx.get("min_cash_weight", ctx.get("min_cash_pct")))
    cash = float(clean.get("CASH", 0.0) or 0.0)
    cash_ok = True
    if min_cash is not None and cash < min_cash - 1e-9:
        cash_ok = False
        violations.append(f"cash {cash:.2%} below floor {min_cash:.2%}")

    max_equity = _optional_float(ctx.get("max_equity_weight", ctx.get("max_equity_pct")))
    equity = sum(weight for ticker, weight in clean.items() if ticker != "CASH")
    equity_ok = True
    if max_equity is not None and equity > max_equity + 1e-9:
        equity_ok = False
        violations.append(f"equity {equity:.2%} exceeds cap {max_equity:.2%}")

    max_single_position = _optional_float(ctx.get("max_single_position"))
    max_single_position_violations: list[dict[str, Any]] = []
    if max_single_position is not None:
        for ticker, weight in sorted(clean.items()):
            if ticker == "CASH" or weight <= max_single_position + 1e-9:
                continue
            max_single_position_violations.append(
                {"ticker": ticker, "weight": round(weight, 6), "cap": max_single_position}
            )
            violations.append(f"{ticker} weight {weight:.2%} exceeds context single cap {max_single_position:.2%}")

    max_turnover = _optional_float(ctx.get("max_turnover_per_cycle"))
    turnover = _turnover(clean, current)
    turnover_ok = True
    if max_turnover is not None and turnover > max_turnover + 1e-9:
        turnover_ok = False
        violations.append(f"turnover {turnover:.2%} exceeds cap {max_turnover:.2%}")

    max_single_delta = _optional_float(ctx.get("max_single_delta", ctx.get("max_adjustment_from_base")))
    single_delta_violations: list[dict[str, Any]] = []
    if max_single_delta is not None:
        for ticker in sorted((set(clean) | set(current)) - {"CASH"}):
            delta = float(clean.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0)
            if abs(delta) <= max_single_delta + 1e-9:
                continue
            single_delta_violations.append(
                {
                    "ticker": ticker,
                    "delta": round(delta, 6),
                    "cap": max_single_delta,
                }
            )
            violations.append(f"{ticker} delta {delta:.2%} exceeds cap {max_single_delta:.2%}")

    role_exposure = {
        row["role"]: {
            "current_total": round(float(row["current_total"]), 6),
            "cap": round(float(row["cap"]), 6),
            "violated": bool(row["violated"]),
        }
        for row in exposure_rows
    }
    checks = {
        "unknown_ticker_ok": {
            "pass": not unknown_positive,
            "actual": unknown_positive,
            "threshold": "registered ticker required for positive weight",
        },
        "watchlist_ticker_ok": {
            "pass": not watchlist_positive,
            "actual": watchlist_positive,
            "threshold": "watchlist positive weight forbidden",
        },
        "single_cap_ok": {
            "pass": not single_cap_violations and not max_single_position_violations,
            "actual": single_cap_violations + max_single_position_violations,
            "threshold": "role/context single cap",
        },
        "role_group_cap_ok": {
            "pass": not group_violations,
            "actual": group_violations,
            "threshold": "role group cap",
        },
        "hedge_only_ok": {
            "pass": not hedge_policy_violations,
            "actual": hedge_policy_violations,
            "threshold": "hedge increase requires hedge_allowed context",
        },
        "cash_floor_ok": {
            "pass": cash_ok,
            "actual": round(cash, 6),
            "threshold": min_cash,
        },
        "max_equity_ok": {
            "pass": equity_ok,
            "actual": round(equity, 6),
            "threshold": max_equity,
        },
        "turnover_ok": {
            "pass": turnover_ok,
            "actual": round(turnover, 6),
            "threshold": max_turnover,
        },
        "single_delta_ok": {
            "pass": not single_delta_violations,
            "actual": single_delta_violations,
            "threshold": max_single_delta,
        },
    }
    return {
        "allowed": all(row["pass"] for row in checks.values()),
        "policy_version": POLICY_VERSION,
        "violations": violations,
        "cap_violations": cap_violations,
        "group_violations": group_violations,
        "cap_events": cap_events,
        "role_exposure": role_exposure,
        "checks": checks,
    }


def apply_policy_caps(raw_targets: dict[str, Any]) -> tuple[dict[str, float], list[dict[str, Any]], float]:
    capped: dict[str, float] = {}
    cap_events: list[dict[str, Any]] = []
    cash_raised = 0.0

    for raw_ticker, raw_weight in (raw_targets or {}).items():
        ticker = _clean_ticker(raw_ticker)
        weight = float(raw_weight or 0.0)
        if ticker == "CASH":
            capped[ticker] = capped.get(ticker, 0.0) + max(weight, 0.0)
            continue
        if weight <= 0.0:
            capped[ticker] = weight
            continue

        allowed, reason = check_weight_allowed(ticker, weight)
        if allowed:
            capped[ticker] = weight
            continue

        policy = get_policy(ticker)
        capped_weight = min(weight, policy.max_single_weight)
        released = max(weight - capped_weight, 0.0)
        cash_raised += released
        capped[ticker] = capped_weight
        cap_events.append(
            {
                "ticker": ticker,
                "role": get_role(ticker).value,
                "original": round(weight, 6),
                "capped_to": round(capped_weight, 6),
                "released_to_cash": round(released, 6),
                "reason": reason,
            }
        )

    for result in [row for row in check_portfolio_exposure(capped) if row["violated"]]:
        role = TickerRole(result["role"])
        before = {ticker: weight for ticker, weight in capped.items() if get_role(ticker) == role}
        capped = _scale_down_role(capped, role, float(result["cap"]))
        released = sum(before[ticker] - capped.get(ticker, 0.0) for ticker in before)
        cash_raised += max(released, 0.0)
        cap_events.append(
            {
                "group_role": role.value,
                "original_total": round(float(result["current_total"]), 6),
                "cap": round(float(result["cap"]), 6),
                "released_to_cash": round(max(released, 0.0), 6),
                "action": "proportional_scale_down",
            }
        )

    return capped, cap_events, round(cash_raised, 6)


def _scale_down_role(weights: dict[str, float], role: TickerRole, cap: float) -> dict[str, float]:
    tickers = [ticker for ticker in weights if ticker != "CASH" and get_role(ticker) == role]
    total = sum(max(float(weights.get(ticker, 0.0) or 0.0), 0.0) for ticker in tickers)
    if total <= cap or total <= 0.0:
        return dict(weights)
    scale = cap / total
    result = dict(weights)
    for ticker in tickers:
        result[ticker] = float(result[ticker]) * scale
    return result


def policy_snapshot() -> dict[str, Any]:
    return {
        "version": POLICY_VERSION,
        "roles": {ticker: role.value for ticker, role in sorted(TICKER_ROLES.items())},
        "caps": {
            role.value: {
                "max_single": policy.max_single_weight,
                "max_total_group": policy.max_total_group_weight,
                "hedge_only": policy.hedge_only,
            }
            for role, policy in ROLE_POLICIES.items()
        },
    }


def _clean_weight_map(weights: dict[str, Any] | None) -> dict[str, float]:
    cleaned: dict[str, float] = {}
    for raw_ticker, raw_weight in (weights or {}).items():
        ticker = _clean_ticker(raw_ticker)
        if not ticker:
            continue
        try:
            weight = float(raw_weight or 0.0)
        except (TypeError, ValueError):
            weight = 0.0
        cleaned[ticker] = max(weight, 0.0)
    return cleaned


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _turnover(target: dict[str, float], current: dict[str, float]) -> float:
    tickers = set(target) | set(current)
    return sum(
        abs(float(target.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0))
        for ticker in tickers
    ) / 2.0
