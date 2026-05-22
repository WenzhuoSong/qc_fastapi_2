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
