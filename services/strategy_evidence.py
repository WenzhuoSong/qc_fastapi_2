"""Normalize strategy raw scores into ETF-aware evidence cards."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Callable

from strategies.base import ScoredTicker, Strategy


EVIDENCE_CONTRACT_VERSION = "v1"
PREFERRED_CONVICTION_HORIZON_DAYS = 5

ALLOWED_ACTIONS = {
    "increase",
    "reduce",
    "hold",
    "watch",
    "avoid",
    "hedge",
    "de_risk",
    "neutral",
}

REQUIRED_SAFETY_FIELDS = (
    "allowed_actions",
    "max_reasonable_weight",
    "risk_budget_cost",
    "decay_risk",
)

CONVICTION_SOURCE_PRIORITY = {
    "combined": 0,
    "live_paper": 1,
    "historical_prior": 2,
}


@dataclass(frozen=True)
class _ConvictionOverlay:
    conviction: float | None
    status: str
    source_bucket: str | None
    n: int
    effective_confidence: float
    reason_code: str | None
    diagnostics: dict[str, Any]


@dataclass
class EvidenceCard:
    ticker: str
    strategy: str
    strategy_version: str
    role: str
    action: str
    signal_type: str
    horizon: str
    confidence: float
    conviction: float | None
    raw_score: float | None
    normalized_score: float
    max_reasonable_weight: float
    risk_budget_cost: float
    branch: str | None
    reason: str
    conviction_status: str = "missing_profile"
    conviction_source_bucket: str | None = None
    conviction_n: int = 0
    effective_confidence: float = 0.0
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_evidence_cards(
    *,
    strategy: Strategy,
    scored: list[ScoredTicker],
    knowledge_context: dict[str, Any],
    mode: str = "playground",
    strict: bool = False,
    conviction_profiles: list[Any] | None = None,
) -> list[EvidenceCard]:
    """Translate raw strategy scores into ETF-aware evidence cards.

    `confidence` measures current signal clarity and changes every run.
    `conviction` is historical/live reliability and is `None` until a matching
    calibration profile exists. When provided, conviction profiles only affect
    diagnostics/effective_confidence, never the production-facing
    max_reasonable_weight.
    """
    assets = _assets_by_ticker(knowledge_context)
    strategy_profile = _strategy_profile(knowledge_context, strategy.name)
    conviction_rows = _normalize_conviction_profiles(conviction_profiles or [])
    cards: list[EvidenceCard] = []
    for item in scored:
        ticker = str(item.ticker or "").upper().strip()
        if not ticker:
            continue
        cards.append(_build_one_card(
            strategy=strategy,
            scored=item,
            asset=assets.get(ticker),
            strategy_profile=strategy_profile,
            mode=mode,
            strict=strict,
            regime=((knowledge_context.get("selection") or {}).get("regime")),
            conviction_profiles=conviction_rows,
        ))
    return cards


def summarize_evidence_cards(cards: list[EvidenceCard | dict[str, Any]]) -> dict[str, Any]:
    rows = [card.to_dict() if isinstance(card, EvidenceCard) else dict(card) for card in cards]
    actions: dict[str, int] = {}
    fallback_count = 0
    missing_mapping_count = 0
    max_weight_by_action: dict[str, float] = {}
    conviction_statuses: dict[str, int] = {}
    for row in rows:
        action = str(row.get("action") or "unknown")
        actions[action] = actions.get(action, 0) + 1
        reason = str(row.get("reason") or "")
        if "fallback" in reason or "missing_" in reason or "not_allowed" in reason:
            fallback_count += 1
        if "missing_compatibility_mapping" in reason:
            missing_mapping_count += 1
        max_weight_by_action[action] = round(
            max(max_weight_by_action.get(action, 0.0), _to_float(row.get("max_reasonable_weight"), 0.0)),
            6,
        )
        status = str(row.get("conviction_status") or "missing_profile")
        conviction_statuses[status] = conviction_statuses.get(status, 0) + 1
    return {
        "cards_generated": len(rows),
        "missing_mapping_count": missing_mapping_count,
        "fallback_count": fallback_count,
        "actions": dict(sorted(actions.items())),
        "max_weight_by_action": dict(sorted(max_weight_by_action.items())),
        "conviction_statuses": dict(sorted(conviction_statuses.items())),
    }


def _build_one_card(
    *,
    strategy: Strategy,
    scored: ScoredTicker,
    asset: dict[str, Any] | None,
    strategy_profile: dict[str, Any] | None,
    mode: str,
    strict: bool,
    regime: Any,
    conviction_profiles: list[dict[str, Any]],
) -> EvidenceCard:
    ticker = str(scored.ticker or "").upper().strip()
    raw_score = _optional_float(scored.score)
    normalized_score = _clamp(raw_score if raw_score is not None else 0.0)
    diagnostics: dict[str, Any] = {
        "contract_version": EVIDENCE_CONTRACT_VERSION,
        "mode": mode,
    }

    if not asset:
        return _fallback_card(
            strategy=strategy,
            scored=scored,
            reason="missing_asset_profile",
            diagnostics=diagnostics,
            confidence=min(normalized_score, 0.25),
        )

    role = str(asset.get("role") or asset.get("asset_class") or "unknown").strip()
    diagnostics["asset_role"] = role
    missing_safety = _missing_safety_fields(asset)
    if missing_safety:
        message = f"{ticker} missing required safety field: {', '.join(missing_safety)}"
        if strict:
            raise ValueError(message)
        return _fallback_card(
            strategy=strategy,
            scored=scored,
            asset=asset,
            reason=f"missing_required_safety_field:{','.join(missing_safety)}",
            diagnostics={**diagnostics, "missing_safety_fields": missing_safety},
            confidence=min(normalized_score, 0.25),
        )

    if not strategy_profile:
        return _fallback_card(
            strategy=strategy,
            scored=scored,
            asset=asset,
            reason="missing_strategy_profile",
            diagnostics=diagnostics,
            confidence=min(normalized_score, 0.25),
        )

    mapping = _mapping_for_role(strategy_profile, role)
    if not mapping:
        return _fallback_card(
            strategy=strategy,
            scored=scored,
            asset=asset,
            reason="missing_compatibility_mapping",
            diagnostics=diagnostics,
            confidence=min(normalized_score, 0.25),
        )

    threshold = _match_threshold(mapping.get("score_thresholds") or [], normalized_score)
    if not threshold:
        return _fallback_card(
            strategy=strategy,
            scored=scored,
            asset=asset,
            reason="no_score_threshold_match",
            diagnostics={**diagnostics, "mapping_role": mapping.get("role")},
            confidence=min(normalized_score, 0.25),
        )

    action = str(threshold.get("action") or "watch")
    if action not in ALLOWED_ACTIONS:
        action = "watch"
        diagnostics["invalid_mapping_action"] = threshold.get("action")
    allowed_actions = {str(value) for value in (asset.get("allowed_actions") or [])}
    if action not in allowed_actions:
        return _fallback_card(
            strategy=strategy,
            scored=scored,
            asset=asset,
            reason="action_not_allowed_by_asset_profile",
            diagnostics={
                **diagnostics,
                "mapping_role": mapping.get("role"),
                "requested_action": action,
                "allowed_actions": sorted(allowed_actions),
            },
            confidence=min(normalized_score, 0.25),
        )

    formula_id = str(mapping.get("weight_formula") or "zero")
    formula = FORMULAS.get(formula_id)
    if formula is None:
        diagnostics["unknown_weight_formula"] = formula_id
        formula = _zero_weight

    confidence = normalized_score
    base_cap = _mode_cap(asset.get("max_reasonable_weight"), mode)
    multiplier = max(0.0, _to_float(mapping.get("max_weight_multiplier"), 1.0))
    max_reasonable_weight = _clamp_weight(base_cap * multiplier * formula(confidence=confidence))
    branch = _branch(scored, mapping, regime=regime, ticker=ticker, role=role, action=action, strategy=strategy.name)
    conviction = _conviction_overlay(
        strategy=strategy.name,
        ticker=ticker,
        branch=branch,
        action=action,
        confidence=confidence,
        profiles=conviction_profiles,
    )
    diagnostics.update({
        "mapping_role": mapping.get("role"),
        "threshold": threshold,
        "base_cap": round(base_cap, 6),
        "max_weight_multiplier": round(multiplier, 6),
        "weight_formula": formula_id,
        "conviction": conviction.diagnostics,
    })
    reason = "mapped_by_compatibility_threshold"
    if conviction.reason_code:
        reason = f"{reason};{conviction.reason_code}"
    return EvidenceCard(
        ticker=ticker,
        strategy=strategy.name,
        strategy_version=strategy.version,
        role=role,
        action=action,
        signal_type=str(threshold.get("signal_type") or "unspecified"),
        horizon=str(mapping.get("horizon") or strategy_profile.get("horizon") or "unspecified"),
        confidence=round(confidence, 6),
        conviction=conviction.conviction,
        raw_score=raw_score,
        normalized_score=round(normalized_score, 6),
        max_reasonable_weight=round(max_reasonable_weight, 6),
        risk_budget_cost=round(_to_float(asset.get("risk_budget_cost"), 1.0), 6),
        branch=branch,
        reason=reason,
        conviction_status=conviction.status,
        conviction_source_bucket=conviction.source_bucket,
        conviction_n=conviction.n,
        effective_confidence=conviction.effective_confidence,
        diagnostics=diagnostics,
    )


def _fallback_card(
    *,
    strategy: Strategy,
    scored: ScoredTicker,
    reason: str,
    diagnostics: dict[str, Any],
    confidence: float,
    asset: dict[str, Any] | None = None,
) -> EvidenceCard:
    raw_score = _optional_float(scored.score)
    normalized_score = _clamp(raw_score if raw_score is not None else 0.0)
    ticker = str(scored.ticker or "").upper().strip()
    role = str((asset or {}).get("role") or (asset or {}).get("asset_class") or "unknown")
    return EvidenceCard(
        ticker=ticker,
        strategy=strategy.name,
        strategy_version=strategy.version,
        role=role,
        action="watch",
        signal_type="fallback",
        horizon="unspecified",
        confidence=round(_clamp(confidence), 6),
        conviction=None,
        raw_score=raw_score,
        normalized_score=round(normalized_score, 6),
        max_reasonable_weight=0.0,
        risk_budget_cost=round(_to_float((asset or {}).get("risk_budget_cost"), 1.0), 6),
        branch=_branch(scored, {}, regime=None, ticker=ticker, role=role, action="watch", strategy=strategy.name),
        reason=reason,
        conviction_status="missing_profile",
        conviction_source_bucket=None,
        conviction_n=0,
        effective_confidence=0.0,
        diagnostics=diagnostics,
    )


def _normalize_conviction_profiles(profiles: list[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for profile in profiles:
        if isinstance(profile, dict):
            row = dict(profile)
        else:
            row = {
                field: getattr(profile, field, None)
                for field in (
                    "strategy_id",
                    "strategy",
                    "ticker",
                    "branch",
                    "action",
                    "horizon_days",
                    "horizon",
                    "source_bucket",
                    "conviction",
                    "status",
                    "n",
                    "conviction_n",
                    "data_lag_filtered",
                    "requires_live_confirmation",
                    "source_counts",
                    "hit_rate",
                    "avg_excess_vs_spy",
                    "ic",
                )
            }
        strategy_id = str(row.get("strategy_id") or row.get("strategy") or "").strip()
        ticker = str(row.get("ticker") or "").upper().strip()
        action = str(row.get("action") or "").strip()
        if not strategy_id or not ticker or not action:
            continue
        row["strategy_id"] = strategy_id
        row["ticker"] = ticker
        row["action"] = action
        row["branch"] = str(row.get("branch")) if row.get("branch") is not None else None
        row["source_bucket"] = str(row.get("source_bucket") or "unknown")
        row["status"] = str(row.get("status") or "unknown")
        row["horizon_days"] = _to_int(row.get("horizon_days") or row.get("horizon"), 0)
        row["n"] = _to_int(row.get("n") or row.get("conviction_n"), 0)
        row["conviction"] = _optional_float(row.get("conviction"))
        rows.append(row)
    return rows


def _conviction_overlay(
    *,
    strategy: str,
    ticker: str,
    branch: str | None,
    action: str,
    confidence: float,
    profiles: list[dict[str, Any]],
) -> _ConvictionOverlay:
    profile = _match_conviction_profile(
        strategy=strategy,
        ticker=ticker,
        branch=branch,
        action=action,
        profiles=profiles,
    )
    if profile is None:
        return _missing_conviction_overlay(confidence)

    status = str(profile.get("status") or "unknown")
    source_bucket = str(profile.get("source_bucket") or "unknown")
    conviction = _optional_float(profile.get("conviction"))
    n = _to_int(profile.get("n"), 0)
    if status == "insufficient_samples" or conviction is None:
        effective = 0.0
        reason_code = "insufficient_conviction_samples"
    elif status == "historical_prior_requires_live_confirmation":
        effective = _clamp(confidence * 0.5)
        reason_code = "historical_prior_requires_live_confirmation"
    else:
        effective = _clamp(confidence * _clamp(conviction))
        reason_code = None

    return _ConvictionOverlay(
        conviction=round(conviction, 6) if conviction is not None else None,
        status=status,
        source_bucket=source_bucket,
        n=n,
        effective_confidence=round(effective, 6),
        reason_code=reason_code,
        diagnostics={
            "status": status,
            "source_bucket": source_bucket,
            "n": n,
            "horizon_days": _to_int(profile.get("horizon_days"), 0),
            "data_lag_filtered": _to_int(profile.get("data_lag_filtered"), 0),
            "requires_live_confirmation": bool(profile.get("requires_live_confirmation")),
            "source_counts": dict(profile.get("source_counts") or {}),
            "hit_rate": _optional_float(profile.get("hit_rate")),
            "avg_excess_vs_spy": _optional_float(profile.get("avg_excess_vs_spy")),
            "ic": _optional_float(profile.get("ic")),
            "effective_confidence_rule": _effective_confidence_rule(status, conviction),
            "shadow_only": True,
        },
    )


def _missing_conviction_overlay(confidence: float) -> _ConvictionOverlay:
    return _ConvictionOverlay(
        conviction=None,
        status="missing_profile",
        source_bucket=None,
        n=0,
        effective_confidence=0.0,
        reason_code=None,
        diagnostics={
            "status": "missing_profile",
            "source_bucket": None,
            "n": 0,
            "effective_confidence_rule": "missing_profile->0",
            "shadow_only": True,
        },
    )


def _match_conviction_profile(
    *,
    strategy: str,
    ticker: str,
    branch: str | None,
    action: str,
    profiles: list[dict[str, Any]],
) -> dict[str, Any] | None:
    candidates = [
        profile
        for profile in profiles
        if profile.get("strategy_id") == strategy
        and profile.get("ticker") == ticker
        and profile.get("action") == action
        and profile.get("branch") == branch
    ]
    if not candidates:
        return None
    return sorted(candidates, key=_conviction_profile_sort_key)[0]


def _conviction_profile_sort_key(profile: dict[str, Any]) -> tuple[int, int, int]:
    source_rank = CONVICTION_SOURCE_PRIORITY.get(str(profile.get("source_bucket") or ""), 9)
    horizon = _to_int(profile.get("horizon_days"), 0)
    horizon_rank = 0 if horizon == PREFERRED_CONVICTION_HORIZON_DAYS else 1
    return (source_rank, horizon_rank, -_to_int(profile.get("n"), 0))


def _effective_confidence_rule(status: str, conviction: float | None) -> str:
    if status == "insufficient_samples" or conviction is None:
        return "insufficient_or_missing->0"
    if status == "historical_prior_requires_live_confirmation":
        return "confidence*0.5"
    return "confidence*conviction"


def _assets_by_ticker(context: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("id") or "").upper().strip(): item
        for item in context.get("assets") or []
        if isinstance(item, dict) and item.get("id")
    }


def _strategy_profile(context: dict[str, Any], strategy_name: str) -> dict[str, Any] | None:
    for item in context.get("strategies") or []:
        if isinstance(item, dict) and str(item.get("id") or "") == strategy_name:
            return item
    return None


def _mapping_for_role(strategy_profile: dict[str, Any], role: str) -> dict[str, Any] | None:
    for item in strategy_profile.get("compatibility_mappings") or []:
        if isinstance(item, dict) and str(item.get("role") or "") == role:
            return item
    return None


def _match_threshold(thresholds: list[dict[str, Any]], score: float) -> dict[str, Any] | None:
    for threshold in thresholds:
        if not isinstance(threshold, dict):
            continue
        if "gte" in threshold and score >= _to_float(threshold.get("gte"), 0.0):
            return threshold
        if "gt" in threshold and score > _to_float(threshold.get("gt"), 0.0):
            return threshold
        if "lte" in threshold and score <= _to_float(threshold.get("lte"), 0.0):
            return threshold
        if "lt" in threshold and score < _to_float(threshold.get("lt"), 0.0):
            return threshold
    return None


def _missing_safety_fields(asset: dict[str, Any]) -> list[str]:
    return [
        field
        for field in REQUIRED_SAFETY_FIELDS
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


def _mode_cap(value: Any, mode: str) -> float:
    if isinstance(value, dict):
        if mode in value:
            return _clamp_weight(_to_float(value.get(mode), 0.0))
        if "playground" in value:
            return _clamp_weight(_to_float(value.get("playground"), 0.0))
        return 0.0
    return _clamp_weight(_to_float(value, 0.0))


def _branch(
    scored: ScoredTicker,
    mapping: dict[str, Any],
    *,
    regime: Any,
    ticker: str,
    role: str,
    action: str,
    strategy: str,
) -> str | None:
    raw_branch = (scored.raw_factors or {}).get("branch")
    if raw_branch:
        return str(raw_branch)
    template = mapping.get("branch_label_template")
    if not template:
        return None
    try:
        return str(template).format(
            regime=regime or "unknown",
            ticker=ticker,
            role=role,
            action=action,
            strategy=strategy,
        )
    except Exception:
        return None


def _zero_weight(*, confidence: float) -> float:
    return 0.0


def _cap_only(*, confidence: float) -> float:
    return 1.0


def _confidence_cap_multiplier(*, confidence: float) -> float:
    return _clamp(confidence)


FORMULAS: dict[str, Callable[..., float]] = {
    "zero": _zero_weight,
    "cap_only": _cap_only,
    "confidence_cap_multiplier": _confidence_cap_multiplier,
}


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _clamp(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


def _clamp_weight(value: float) -> float:
    return max(0.0, min(float(value), 1.0))
