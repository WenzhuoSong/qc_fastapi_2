"""Audit strategy/ETF knowledge mapping coverage.

This module is diagnostic-only. It does not build EvidenceCards and does not
change execution behavior. Its job is to separate hard knowledge/configuration
errors from normal watch/abstain behavior so the alpha input layer can be
cleaned before attribution or evidence caps become more authoritative.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from services.execution_policy import TICKER_ROLES, get_role
from services.strategy_evidence import REQUIRED_SAFETY_FIELDS
from services.universe_policy import is_tradable_research_row
from strategies import STRATEGY_REGISTRY, get_strategy


HARD_MAPPING_REASONS = {
    "missing_strategy_profile",
    "missing_compatibility_mapping",
    "missing_asset_profile",
    "missing_required_safety_field",
}

NORMAL_WATCH_REASONS = {
    "action_not_allowed_by_asset_profile",
    "watch_only_mapping",
}

NON_VOTING_ACTIONS = {"watch", "avoid", "neutral"}


def build_strategy_mapping_audit(
    *,
    strategy_ids: list[str],
    tickers: list[str],
    strategy_profiles: dict[str, dict[str, Any]],
    asset_profiles: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Return a structured strategy/ETF mapping audit.

    Coverage denominator is the strategy's eligible ticker universe. Coverage
    numerator is mapped rows that can become either `voted` or normal `watch`.
    Abstain is intentionally excluded from this audit's coverage metric because
    abstain means "no voting right", not "covered but constrained".
    """
    clean_strategies = _clean_strategy_ids(strategy_ids)
    clean_tickers = _clean_tickers(tickers)
    profiles = {
        str(key or "").strip(): dict(value or {})
        for key, value in (strategy_profiles or {}).items()
        if str(key or "").strip()
    }
    assets = {
        str(key or "").upper().strip(): dict(value or {})
        for key, value in (asset_profiles or {}).items()
        if str(key or "").strip()
    }

    rows: list[dict[str, Any]] = []
    by_reason: Counter[str] = Counter()
    strategy_acc: dict[str, dict[str, Any]] = defaultdict(_strategy_accumulator)
    ticker_acc: dict[str, dict[str, Any]] = defaultdict(_ticker_accumulator)

    for strategy_id in clean_strategies:
        eligible_tickers = _eligible_tickers_for_strategy(strategy_id, clean_tickers)
        profile = profiles.get(strategy_id)
        strategy_row = strategy_acc[strategy_id]
        strategy_row["eligible_ticker_count"] = len(eligible_tickers)

        for ticker in eligible_tickers:
            asset = assets.get(ticker)
            row = _classify_mapping_row(
                strategy_id=strategy_id,
                ticker=ticker,
                strategy_profile=profile,
                asset_profile=asset,
            )
            rows.append(row)
            reason = str(row["reason"])
            by_reason[reason] += 1
            _update_strategy_accumulator(strategy_row, row)
            _update_ticker_accumulator(ticker_acc[ticker], row)

    hard_rows = [row for row in rows if row["reason"] in HARD_MAPPING_REASONS]
    watch_rows = [row for row in rows if row["reason"] in NORMAL_WATCH_REASONS]
    strategy_coverage = {
        strategy_id: _finalize_strategy_coverage(data)
        for strategy_id, data in sorted(strategy_acc.items())
    }
    ticker_coverage = {
        ticker: _finalize_ticker_coverage(data)
        for ticker, data in sorted(ticker_acc.items())
    }

    return {
        "contract_version": "strategy_mapping_audit_v1",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "total_rows": len(rows),
        "by_reason": dict(sorted(by_reason.items())),
        "hard_mapping_error_count": len(hard_rows),
        "normal_watch_count": len(watch_rows),
        "hard_mapping_errors": sorted(hard_rows, key=_row_sort_key),
        "normal_watch_rows": sorted(watch_rows, key=_row_sort_key),
        "strategy_coverage": strategy_coverage,
        "ticker_coverage": ticker_coverage,
        "top_hard_mapping_errors": _top_rows(hard_rows, limit=20),
        "missing_strategy_profiles": sorted({
            row["strategy_id"] for row in hard_rows
            if row["reason"] == "missing_strategy_profile"
        }),
        "missing_asset_profiles": sorted({
            row["ticker"] for row in hard_rows
            if row["reason"] == "missing_asset_profile"
        }),
    }


def build_current_strategy_mapping_audit() -> dict[str, Any]:
    """Build an audit for the current registered strategies and policy universe."""
    from services.knowledge_base import load_knowledge_base

    kb = load_knowledge_base()
    return build_strategy_mapping_audit(
        strategy_ids=sorted(STRATEGY_REGISTRY),
        tickers=sorted(TICKER_ROLES),
        strategy_profiles=kb.get("strategies") or {},
        asset_profiles=kb.get("assets") or {},
    )


def _classify_mapping_row(
    *,
    strategy_id: str,
    ticker: str,
    strategy_profile: dict[str, Any] | None,
    asset_profile: dict[str, Any] | None,
) -> dict[str, Any]:
    base = {
        "strategy_id": strategy_id,
        "ticker": ticker,
        "execution_role": get_role(ticker).value,
    }
    if not strategy_profile:
        return {
            **base,
            "reason": "missing_strategy_profile",
            "status": "mapping_error",
            "eligible_for_coverage": False,
        }
    if not asset_profile:
        return {
            **base,
            "reason": "missing_asset_profile",
            "status": "mapping_error",
            "eligible_for_coverage": False,
        }

    role = str(asset_profile.get("role") or asset_profile.get("asset_class") or "unknown").strip()
    missing_fields = _missing_safety_fields(asset_profile)
    row = {
        **base,
        "asset_role": role,
        "strategy_profile_source": strategy_profile.get("_source_file"),
        "asset_profile_source": asset_profile.get("_source_file"),
    }
    if missing_fields:
        return {
            **row,
            "reason": "missing_required_safety_field",
            "status": "mapping_error",
            "missing_fields": missing_fields,
            "eligible_for_coverage": False,
        }

    mapping = _mapping_for_role(strategy_profile, role)
    if not mapping:
        return {
            **row,
            "reason": "missing_compatibility_mapping",
            "status": "mapping_error",
            "eligible_for_coverage": False,
        }

    actions = _mapping_actions(mapping)
    allowed_actions = {str(value) for value in (asset_profile.get("allowed_actions") or [])}
    disallowed = sorted(action for action in actions if action and action not in allowed_actions)
    if disallowed and actions and all(action not in allowed_actions for action in actions):
        return {
            **row,
            "reason": "action_not_allowed_by_asset_profile",
            "status": "watch",
            "mapping_role": mapping.get("role"),
            "requested_actions": sorted(actions),
            "allowed_actions": sorted(allowed_actions),
            "eligible_for_coverage": True,
        }
    if actions and all(action in NON_VOTING_ACTIONS for action in actions):
        return {
            **row,
            "reason": "watch_only_mapping",
            "status": "watch",
            "mapping_role": mapping.get("role"),
            "requested_actions": sorted(actions),
            "eligible_for_coverage": True,
        }

    return {
        **row,
        "reason": "mapped",
        "status": "covered",
        "mapping_role": mapping.get("role"),
        "requested_actions": sorted(actions),
        "eligible_for_coverage": True,
    }


def _eligible_tickers_for_strategy(strategy_id: str, tickers: list[str]) -> list[str]:
    try:
        strategy = get_strategy(strategy_id)
    except ValueError:
        return []
    if strategy.universe_tickers:
        allowed = {str(ticker or "").upper().strip() for ticker in strategy.universe_tickers}
        return sorted(ticker for ticker in tickers if ticker in allowed)
    if strategy.allow_hedge_research_tickers:
        return sorted(ticker for ticker in tickers if ticker and ticker != "CASH")
    return sorted(
        ticker for ticker in tickers
        if is_tradable_research_row({
            "ticker": ticker,
            "universe_role": get_role(ticker).value,
        })
    )


def _strategy_accumulator() -> dict[str, Any]:
    return {
        "eligible_ticker_count": 0,
        "voted_or_watch_rows": 0,
        "mapping_error_rows": 0,
        "watch_rows": 0,
        "mapped_rows": 0,
        "reasons": Counter(),
    }


def _ticker_accumulator() -> dict[str, Any]:
    return {
        "eligible_strategy_count": 0,
        "mapping_error_count": 0,
        "watch_count": 0,
        "abstain_count": 0,
        "covered_count": 0,
        "reasons": Counter(),
    }


def _update_strategy_accumulator(acc: dict[str, Any], row: dict[str, Any]) -> None:
    reason = str(row.get("reason") or "unknown")
    status = str(row.get("status") or "unknown")
    acc["reasons"][reason] += 1
    if reason in HARD_MAPPING_REASONS:
        acc["mapping_error_rows"] += 1
    if status in {"covered", "watch"}:
        acc["voted_or_watch_rows"] += 1
    if status == "watch":
        acc["watch_rows"] += 1
    if status == "covered":
        acc["mapped_rows"] += 1


def _update_ticker_accumulator(acc: dict[str, Any], row: dict[str, Any]) -> None:
    reason = str(row.get("reason") or "unknown")
    status = str(row.get("status") or "unknown")
    acc["eligible_strategy_count"] += 1
    acc["reasons"][reason] += 1
    if reason in HARD_MAPPING_REASONS:
        acc["mapping_error_count"] += 1
    if status == "watch":
        acc["watch_count"] += 1
    if status == "covered":
        acc["covered_count"] += 1


def _finalize_strategy_coverage(data: dict[str, Any]) -> dict[str, Any]:
    denominator = int(data.get("eligible_ticker_count") or 0)
    numerator = int(data.get("voted_or_watch_rows") or 0)
    return {
        "voted_or_watch_rows": numerator,
        "mapping_error_rows": int(data.get("mapping_error_rows") or 0),
        "eligible_ticker_count": denominator,
        "coverage_pct": round(numerator / denominator, 6) if denominator else 0.0,
        "watch_rows": int(data.get("watch_rows") or 0),
        "mapped_rows": int(data.get("mapped_rows") or 0),
        "by_reason": dict(sorted((data.get("reasons") or Counter()).items())),
    }


def _finalize_ticker_coverage(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "eligible_strategy_count": int(data.get("eligible_strategy_count") or 0),
        "mapping_error_count": int(data.get("mapping_error_count") or 0),
        "watch_count": int(data.get("watch_count") or 0),
        "abstain_count": int(data.get("abstain_count") or 0),
        "covered_count": int(data.get("covered_count") or 0),
        "by_reason": dict(sorted((data.get("reasons") or Counter()).items())),
    }


def _mapping_for_role(strategy_profile: dict[str, Any], role: str) -> dict[str, Any] | None:
    for item in strategy_profile.get("compatibility_mappings") or []:
        if isinstance(item, dict) and str(item.get("role") or "") == role:
            return item
    return None


def _mapping_actions(mapping: dict[str, Any]) -> set[str]:
    actions: set[str] = set()
    for threshold in mapping.get("score_thresholds") or []:
        if not isinstance(threshold, dict):
            continue
        action = str(threshold.get("action") or "").strip()
        if action:
            actions.add(action)
    return actions


def _missing_safety_fields(asset: dict[str, Any]) -> list[str]:
    return [
        field for field in REQUIRED_SAFETY_FIELDS
        if _missing_value(asset.get(field))
    ]


def _missing_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return not value
    return False


def _clean_strategy_ids(strategy_ids: list[str]) -> list[str]:
    return sorted({
        str(strategy_id or "").strip()
        for strategy_id in (strategy_ids or [])
        if str(strategy_id or "").strip()
    })


def _clean_tickers(tickers: list[str]) -> list[str]:
    return sorted({
        str(ticker or "").upper().strip()
        for ticker in (tickers or [])
        if str(ticker or "").strip()
    })


def _top_rows(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str]] = Counter(
        (
            str(row.get("reason") or ""),
            str(row.get("strategy_id") or ""),
            str(row.get("asset_role") or row.get("execution_role") or ""),
        )
        for row in rows
    )
    out = []
    for (reason, strategy_id, role), count in counts.most_common(limit):
        out.append({
            "reason": reason,
            "strategy_id": strategy_id,
            "role": role,
            "count": count,
        })
    return out


def _row_sort_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("reason") or ""),
        str(row.get("strategy_id") or ""),
        str(row.get("ticker") or ""),
    )

