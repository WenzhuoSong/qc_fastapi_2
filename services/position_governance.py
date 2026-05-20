"""
Deterministic position governance.

This layer manages position lifecycle after risk approval and before quantity /
frequency controls. It does not forecast returns; it translates existing
evidence into auditable per-position permissions and conservative target-weight
adjustments.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from services.advisory_quality import build_advisory_quality_diagnostics
from services.group_contract import calc_primary_group_exposure, get_primary_group
from services.thesis_scheduler import build_thesis_review_queue


@dataclass
class GovernanceConfig:
    loss_review_pct: float = -0.04
    loss_trim_pct: float = -0.08
    core_loss_review_pct: float = -0.05
    core_loss_trim_pct: float = -0.10
    satellite_loss_review_pct: float = -0.04
    satellite_loss_trim_pct: float = -0.08
    winner_trim_pct: float = 0.08
    high_weight_pct: float = 0.12
    high_atr_pct: float = 0.05
    concentration_limit_pct: float = 0.25
    semiconductors_limit_pct: float = 0.25
    tech_growth_limit_pct: float = 0.35
    defensive_bonds_limit_pct: float = 0.35
    cyclicals_limit_pct: float = 0.30
    real_estate_limit_pct: float = 0.15
    high_risk_contribution_pct: float = 0.003
    crowding_multiplier_cap: float = 1.75
    review_trim_pct: float = 0.01
    loss_trim_step_pct: float = 0.03
    winner_trim_step_pct: float = 0.02
    replacement_enabled: float = 1.0
    replacement_max_total_pct: float = 0.05
    replacement_max_single_pct: float = 0.02
    llm_advisory_enabled: float = 1.0
    llm_advisory_max_trim_pct: float = 0.01
    llm_advisory_max_add_pct: float = 0.01
    advisory_basket_loss_review_pct: float = -0.06
    advisory_basket_loss_manual_review_enabled: float = 1.0
    advisory_basket_loss_auto_trim_enabled: float = 0.0
    advisory_basket_loss_auto_trim_pct: float = 0.01

    @classmethod
    def from_config(cls, raw: dict[str, Any] | None) -> "GovernanceConfig":
        defaults = asdict(cls())
        clean: dict[str, Any] = {}
        for key, default in defaults.items():
            try:
                clean[key] = float((raw or {}).get(key, default))
            except (TypeError, ValueError):
                clean[key] = default
        return cls(**clean)


@dataclass
class PositionGovernanceOutput:
    adjusted_weights: dict[str, float]
    position_decisions: list[dict[str, Any]]
    blocked_actions: list[str]
    forced_trims: list[str]
    replacements: list[dict[str, Any]]
    advisory_overrides: list[dict[str, Any]]
    manual_action_hints: list[dict[str, Any]]
    trade_summary: dict[str, Any]
    portfolio_summary: dict[str, Any]
    config: dict[str, Any]


def apply_position_governance(
    *,
    target_weights: dict[str, float],
    current_weights: dict[str, float],
    holdings_meta: list[dict[str, Any]] | None,
    strategy_evidence: dict[str, Any] | None,
    market_scorecard: dict[str, Any] | None,
    news_evidence: dict[str, Any] | None,
    llm_advisory_proposals: list[dict[str, Any]] | None = None,
    config: dict[str, Any] | None = None,
) -> PositionGovernanceOutput:
    cfg = GovernanceConfig.from_config(config)
    current = _clean_weights(current_weights)
    target = _clean_weights(target_weights)
    target.setdefault("CASH", 0.0)
    meta = _meta_by_ticker(holdings_meta or [])
    strategy_support = _strategy_support_by_ticker(strategy_evidence or {})
    hard_risk_tickers = _hard_risk_tickers(news_evidence or {})
    llm_thesis_by_ticker = _llm_thesis_by_ticker(llm_advisory_proposals or [])
    group_exposure = _group_exposure(current)
    group_limits = _group_limits(cfg)
    permission = str((market_scorecard or {}).get("investment_permission") or "normal_rebalance")
    require_human = bool((market_scorecard or {}).get("require_human_confirmation"))

    decisions: list[dict[str, Any]] = []
    blocked_actions: list[str] = []
    forced_trims: list[str] = []
    replacements: list[dict[str, Any]] = []
    replacement_candidates: list[dict[str, Any]] = []
    advisory_overrides: list[dict[str, Any]] = []
    manual_action_hints: list[dict[str, Any]] = []
    work = dict(target)

    tickers = sorted((set(current) | set(target) | set(meta)) - {"CASH"})
    for ticker in tickers:
        current_w = float(current.get(ticker, 0.0) or 0.0)
        target_w = float(work.get(ticker, 0.0) or 0.0)
        before = target_w
        row = meta.get(ticker) or {}
        support = strategy_support.get(ticker, {"level": "none", "strategies": []})
        support_level = support["level"]
        pnl = _to_float(row.get("unrealized_pnl_pct"))
        atr = _to_float(row.get("atr_pct"))
        role = _position_role(row, ticker)
        loss_review_pct, loss_trim_pct = _loss_thresholds(role, cfg)
        group = _ticker_group(ticker)
        group_limit = group_limits.get(group, cfg.concentration_limit_pct) if group else None
        group_headroom = (group_limit - group_exposure.get(group, 0.0)) if group and group_limit is not None else None
        group_crowded = bool(group and group_limit is not None and group_exposure.get(group, 0.0) > group_limit)
        raw_risk_contribution = current_w * (atr or 0.0)
        crowding_multiplier = _sector_crowding_multiplier(
            group_exposure.get(group, 0.0),
            group_limit,
            cfg.crowding_multiplier_cap,
        )
        risk_contribution = raw_risk_contribution * crowding_multiplier
        risk_budget_status = _risk_budget_status(risk_contribution, cfg)
        reasons: list[str] = []
        exits: list[str] = []
        allowed_actions = {"hold", "trim"}
        decision = "hold"

        if support_level in {"primary", "advisory"} and permission not in {"hold_or_trim", "reduce_risk_only", "cash_only"}:
            allowed_actions.add("add")
        if require_human:
            reasons.append("scorecard_human_required")
            allowed_actions.discard("add")
        if permission in {"hold_or_trim", "reduce_risk_only", "cash_only", "defensive_only"}:
            reasons.append(f"scorecard_{permission}")
            allowed_actions.discard("add")

        if ticker in hard_risk_tickers:
            reasons.append("hard_risk")
            exits.append("hard_risk_resolved_or_manual_exit_review")
            allowed_actions = {"trim", "exit"}
            target_w = min(target_w, max(current_w - cfg.loss_trim_step_pct, 0.0))
            decision = "trim"

        if pnl is not None and pnl <= loss_review_pct:
            reasons.append("unrealized_loss_review")
            reasons.append(f"{role}_loss_threshold")
            exits.append(f"loss_below_{loss_trim_pct:.0%}_and_strategy_support_weak")
            decision = "hold_review" if decision == "hold" else decision
            if support_level in {"none", "watch_only", "ignore"}:
                reasons.append("strategy_support_weak")
                allowed_actions.discard("add")
                if pnl <= loss_trim_pct:
                    target_w = min(target_w, max(current_w - cfg.loss_trim_step_pct, 0.0))
                    decision = "trim"
                else:
                    if target_w > current_w:
                        blocked_actions.append(f"buy_blocked:{ticker}:strategy_support_weak")
                    target_w = min(target_w, current_w)
        elif support_level in {"none", "watch_only", "ignore"} and current_w > 0.01:
            reasons.append("strategy_support_weak")
            allowed_actions.discard("add")
            decision = "hold_review" if decision == "hold" else decision

        if pnl is not None and pnl >= cfg.winner_trim_pct and current_w >= cfg.high_weight_pct:
            reasons.append("winner_risk_budget_review")
            target_w = min(target_w, max(current_w - cfg.winner_trim_step_pct, 0.0))
            decision = "trim"

        if atr is not None and atr >= cfg.high_atr_pct:
            reasons.append("high_atr")
            allowed_actions.discard("add")
            target_w = min(target_w, current_w)
            decision = "trim_review" if decision in {"hold", "hold_review"} else decision

        if group_crowded:
            reasons.append(f"{group}_concentration_high")
            exits.append(f"{group}_exposure_below_{group_limit:.0%}")
            if target_w > current_w:
                target_w = current_w
                blocked_actions.append(f"concentration_add_blocked:{ticker}")
            if support_level in {"none", "watch_only", "ignore"} and current_w > 0.01:
                target_w = min(target_w, max(current_w - cfg.review_trim_pct, 0.0))
                decision = "trim_review" if decision == "hold" else decision

        if "add" not in allowed_actions and target_w > current_w:
            blocked_actions.append(f"buy_blocked:{ticker}:{','.join(sorted(reasons)) or 'permission'}")
            target_w = current_w

        target_w = max(target_w, 0.0)
        if target_w < before - 1e-9:
            work["CASH"] = float(work.get("CASH", 0.0) or 0.0) + (before - target_w)
            work[ticker] = target_w
            forced_trims.append(f"{ticker} {before:.2%}->{target_w:.2%}")
        else:
            work[ticker] = target_w

        if target_w < current_w - 0.01 and decision in {"hold", "hold_review"}:
            decision = "trim_review"
        if current_w <= 0.01 and target_w > 0.01 and "add" in allowed_actions:
            decision = "add"

        decisions.append({
            "ticker": ticker,
            "decision": decision,
            "action_permission": _permission_from_actions(allowed_actions),
            "allowed_actions": sorted(allowed_actions),
            "strategy_support": support_level,
            "supporting_strategies": support["strategies"],
            "position_role": role,
            "loss_review_threshold": round(loss_review_pct, 6),
            "loss_trim_threshold": round(loss_trim_pct, 6),
            "group": group,
            "group_exposure": round(group_exposure.get(group, 0.0), 6) if group else None,
            "group_limit": round(group_limit, 6) if group_limit is not None else None,
            "group_headroom": round(group_headroom, 6) if group_headroom is not None else None,
            "raw_risk_contribution": round(raw_risk_contribution, 8),
            "sector_crowding_multiplier": round(crowding_multiplier, 4),
            "risk_contribution": round(risk_contribution, 8),
            "risk_budget_status": risk_budget_status,
            "unrealized_pnl_pct": pnl,
            "atr_pct": atr,
            "holding_days": row.get("holding_days"),
            "last_thesis_review_at": row.get("last_thesis_review_at"),
            "pnl_at_last_thesis_review": row.get("pnl_at_last_thesis_review"),
            "current_weight": round(current_w, 6),
            "target_before": round(before, 6),
            "target_after": round(target_w, 6),
            "reason_codes": list(dict.fromkeys(reasons)),
            "exit_triggers": list(dict.fromkeys(exits)),
        })

    _apply_basket_reviews(decisions)
    _apply_advisory_basket_loss_reviews(
        decisions,
        cfg,
        work=work,
        forced_trims=forced_trims,
    )
    for row in decisions:
        row["thesis_status"] = _validate_thesis_status(
            row=row,
            news_evidence=news_evidence or {},
            strategy_evidence=strategy_evidence or {},
            market_scorecard=market_scorecard or {},
            llm_thesis=llm_thesis_by_ticker.get(str(row.get("ticker") or "").upper()),
        )
    _assign_risk_ranks(decisions)
    if cfg.llm_advisory_enabled:
        advisory_overrides = _apply_llm_advisory_overrides(
            proposals=llm_advisory_proposals or [],
            work=work,
            current=current,
            decisions=decisions,
            blocked_actions=blocked_actions,
            forced_trims=forced_trims,
            cfg=cfg,
            permission=permission,
            require_human=require_human,
        )
    if cfg.replacement_enabled and _replacement_allowed(permission, require_human):
        replacement_candidates = _replacement_candidates(strategy_evidence or {}, decisions)
        replacements = _apply_replacements(
            work=work,
            current=current,
            decisions=decisions,
            candidates=replacement_candidates,
            cfg=cfg,
        )

    adjusted = _normalize_weights(work)
    manual_action_hints = _manual_action_hints(
        decisions=decisions,
        require_human=require_human,
        permission=permission,
    )
    portfolio_summary = _portfolio_summary(
        decisions=decisions,
        group_exposure=group_exposure,
        group_limits=group_limits,
        blocked_actions=blocked_actions,
        forced_trims=forced_trims,
        replacements=replacements,
        replacement_candidates=replacement_candidates,
        advisory_overrides=advisory_overrides,
        manual_action_hints=manual_action_hints,
    )
    return PositionGovernanceOutput(
        adjusted_weights=adjusted,
        position_decisions=decisions,
        blocked_actions=blocked_actions,
        forced_trims=forced_trims,
        replacements=replacements,
        advisory_overrides=advisory_overrides,
        manual_action_hints=manual_action_hints,
        trade_summary={
            "decisions": _decision_counts(decisions),
            "blocked_actions": len(blocked_actions),
            "forced_trims": len(forced_trims),
            "replacements": len(replacements),
            "manual_action_hints": len(manual_action_hints),
            "advisory_overrides": len([row for row in advisory_overrides if row.get("validator_result", "").startswith("accepted")]),
            "position_count": sum(1 for t, w in adjusted.items() if t != "CASH" and w > 0.01),
        },
        portfolio_summary=portfolio_summary,
        config=asdict(cfg),
    )


def _replacement_allowed(permission: str, require_human: bool) -> bool:
    if require_human:
        return False
    return permission not in {"hold_or_trim", "reduce_risk_only", "cash_only", "defensive_only"}


def _apply_llm_advisory_overrides(
    *,
    proposals: list[dict[str, Any]],
    work: dict[str, float],
    current: dict[str, float],
    decisions: list[dict[str, Any]],
    blocked_actions: list[str],
    forced_trims: list[str],
    cfg: GovernanceConfig,
    permission: str,
    require_human: bool,
) -> list[dict[str, Any]]:
    if not proposals:
        return []
    decision_by_ticker = {str(row.get("ticker") or "").upper(): row for row in decisions}
    results: list[dict[str, Any]] = []
    for raw in proposals[:12]:
        if not isinstance(raw, dict):
            continue
        ticker = str(raw.get("ticker") or "").upper().strip()
        action = _normalize_advisory_action(raw)
        reason = str(raw.get("reason") or raw.get("llm_reason") or "")[:240]
        row = decision_by_ticker.get(ticker)
        if not ticker or row is None:
            results.append(_advisory_result(ticker, action, raw, "rejected_unknown_ticker", reason))
            continue
        if action not in {"add", "trim", "hold", "hold_review", "trim_review", "exit"}:
            results.append(_advisory_result(ticker, action, raw, "rejected_unknown_action", reason, row))
            continue

        before = float(work.get(ticker, row.get("target_after") or 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        allowed = set(row.get("allowed_actions") or [])
        reason_codes = row.get("reason_codes") or []

        if require_human and action == "add":
            blocked_actions.append(f"llm_advisory_rejected:{ticker}:human_required_add")
            results.append(_advisory_result(ticker, action, raw, "rejected_human_required_add", reason, row))
            continue

        if action == "add":
            if "add" not in allowed:
                blocked_actions.append(f"llm_advisory_rejected:{ticker}:add_not_allowed")
                results.append(_advisory_result(ticker, action, raw, "rejected_add_not_allowed", reason, row))
                continue
            if row.get("strategy_support") not in {"primary", "advisory"}:
                blocked_actions.append(f"llm_advisory_rejected:{ticker}:weak_strategy_support")
                results.append(_advisory_result(ticker, action, raw, "rejected_weak_strategy_support", reason, row))
                continue
            cash = float(work.get("CASH", 0.0) or 0.0)
            if cash <= 1e-9:
                results.append(_advisory_result(ticker, action, raw, "rejected_no_cash_budget", reason, row))
                continue
            desired = _proposal_target(raw, default=current_w + cfg.llm_advisory_max_add_pct)
            clipped = min(desired, current_w + cfg.llm_advisory_max_add_pct, before + cfg.llm_advisory_max_add_pct, before + cash)
            if clipped <= before + 1e-9:
                results.append(_advisory_result(ticker, action, raw, "accepted_noop", reason, row))
                continue
            add = clipped - before
            work[ticker] = clipped
            work["CASH"] = cash - add
            row["target_after"] = round(clipped, 6)
            row["decision"] = "add"
            row.setdefault("reason_codes", []).append("llm_advisory_validated")
            row["reason_codes"] = list(dict.fromkeys(row["reason_codes"]))
            results.append(_advisory_result(ticker, action, raw, f"accepted_as_add_{add:.2%}", reason, row, before, clipped))
            continue

        if action in {"trim", "trim_review"}:
            if "trim" not in allowed:
                results.append(_advisory_result(ticker, action, raw, "rejected_trim_not_allowed", reason, row))
                continue
            desired = _proposal_target(raw, default=current_w - cfg.llm_advisory_max_trim_pct)
            floor = max(current_w - cfg.llm_advisory_max_trim_pct, 0.0)
            clipped = max(desired, floor)
            clipped = min(clipped, before)
            if clipped >= before - 1e-9:
                results.append(_advisory_result(ticker, action, raw, "accepted_noop", reason, row))
                continue
            trim = before - clipped
            work[ticker] = clipped
            work["CASH"] = float(work.get("CASH", 0.0) or 0.0) + trim
            forced_trims.append(f"{ticker} {before:.2%}->{clipped:.2%} llm_advisory")
            row["target_after"] = round(clipped, 6)
            row["decision"] = "trim" if action == "trim" else "trim_review"
            row.setdefault("reason_codes", []).append("llm_advisory_validated")
            row["reason_codes"] = list(dict.fromkeys(row["reason_codes"]))
            results.append(_advisory_result(ticker, action, raw, f"accepted_as_trim_{trim:.2%}", reason, row, before, clipped))
            continue

        if action == "exit":
            if "exit" in allowed or "hard_risk" in reason_codes or row.get("exit_triggers"):
                desired = 0.0
                floor = max(current_w - cfg.loss_trim_step_pct, 0.0)
                clipped = max(desired, floor)
                clipped = min(clipped, before)
                if clipped < before - 1e-9:
                    trim = before - clipped
                    work[ticker] = clipped
                    work["CASH"] = float(work.get("CASH", 0.0) or 0.0) + trim
                    forced_trims.append(f"{ticker} {before:.2%}->{clipped:.2%} llm_exit_clipped")
                    row["target_after"] = round(clipped, 6)
                    row["decision"] = "trim"
                    row.setdefault("reason_codes", []).append("llm_exit_clipped_to_trim")
                    row["reason_codes"] = list(dict.fromkeys(row["reason_codes"]))
                results.append(_advisory_result(ticker, action, raw, "accepted_exit_clipped_to_trim", reason, row, before, clipped))
            else:
                row["decision"] = "hold_review"
                row.setdefault("reason_codes", []).append("llm_exit_converted_to_review")
                row["reason_codes"] = list(dict.fromkeys(row["reason_codes"]))
                results.append(_advisory_result(ticker, action, raw, "converted_exit_to_hold_review", reason, row, before, before))
            continue

        if action in {"hold", "hold_review"}:
            if before < current_w - 1e-9 and ("hard_risk" in reason_codes or "unrealized_loss_review" in reason_codes):
                results.append(_advisory_result(ticker, action, raw, "rejected_cannot_loosen_required_trim", reason, row))
                continue
            row["decision"] = "hold_review" if action == "hold_review" else "hold"
            row.setdefault("reason_codes", []).append("llm_advisory_deescalated")
            row["reason_codes"] = list(dict.fromkeys(row["reason_codes"]))
            results.append(_advisory_result(ticker, action, raw, "accepted_deescalation", reason, row, before, before))

    return results


def _normalize_advisory_action(raw: dict[str, Any]) -> str:
    value = raw.get("advisory_action", raw.get("llm_advisory", raw.get("action", raw.get("proposal"))))
    return str(value or "").lower().strip()


def _proposal_target(raw: dict[str, Any], *, default: float) -> float:
    target = _to_float(raw.get("target_weight", raw.get("proposed_target_weight")))
    if target is not None:
        return max(target, 0.0)
    delta = _to_float(raw.get("delta_weight", raw.get("proposed_delta_weight")))
    current = _to_float(raw.get("current_weight"))
    if delta is not None and current is not None:
        return max(current + delta, 0.0)
    return max(default, 0.0)


def _advisory_result(
    ticker: str,
    action: str,
    raw: dict[str, Any],
    validator_result: str,
    reason: str,
    decision: dict[str, Any] | None = None,
    before: float | None = None,
    after: float | None = None,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "deterministic_decision": (decision or {}).get("decision"),
        "llm_advisory": action,
        "llm_reason": reason,
        "validator_result": validator_result,
        "target_before_override": round(before, 6) if before is not None else None,
        "target_after_override": round(after, 6) if after is not None else None,
        "final_decision": (decision or {}).get("decision"),
        "raw_confidence": raw.get("confidence"),
    }


def _group_limits(cfg: GovernanceConfig) -> dict[str, float]:
    return {
        "semiconductors": cfg.semiconductors_limit_pct,
        "tech_growth": cfg.tech_growth_limit_pct,
        "defensive_bonds": cfg.defensive_bonds_limit_pct,
        "cyclicals": cfg.cyclicals_limit_pct,
        "real_estate": cfg.real_estate_limit_pct,
    }


def _sector_crowding_multiplier(group_exposure: float | None, group_limit: float | None, cap: float) -> float:
    if group_exposure is None or group_limit is None or group_limit <= 0:
        return 1.0
    if group_exposure <= group_limit:
        return 1.0
    overage_ratio = (group_exposure - group_limit) / group_limit
    return min(max(cap, 1.0), 1.0 + overage_ratio)


def _risk_budget_status(risk_contribution: float, cfg: GovernanceConfig) -> str:
    if risk_contribution >= cfg.high_risk_contribution_pct:
        return "high"
    if risk_contribution >= cfg.high_risk_contribution_pct * 0.5:
        return "medium"
    return "normal"


def _assign_risk_ranks(decisions: list[dict[str, Any]]) -> None:
    ranked = sorted(
        decisions,
        key=lambda row: float(row.get("risk_contribution") or 0.0),
        reverse=True,
    )
    for idx, row in enumerate(ranked, start=1):
        row["risk_rank"] = idx


def _apply_basket_reviews(decisions: list[dict[str, Any]]) -> None:
    by_group: dict[str, list[dict[str, Any]]] = {}
    for row in decisions:
        group = row.get("group")
        if group:
            by_group.setdefault(str(group), []).append(row)

    for group, rows in by_group.items():
        problem_rows = [
            row for row in rows
            if float(row.get("current_weight") or 0.0) > 0.01
            and (
                "unrealized_loss_review" in (row.get("reason_codes") or [])
                or row.get("risk_budget_status") == "high"
            )
        ]
        if len(problem_rows) < 2:
            continue
        tickers = sorted(str(row.get("ticker") or "") for row in problem_rows if row.get("ticker"))
        for row in problem_rows:
            row.setdefault("reason_codes", []).append("basket_review")
            row["reason_codes"] = list(dict.fromkeys(row["reason_codes"]))
            row["basket_review"] = {
                "group": group,
                "tickers": tickers,
                "reason": "multiple correlated positions are in review",
            }


def _apply_advisory_basket_loss_reviews(
    decisions: list[dict[str, Any]],
    cfg: GovernanceConfig,
    *,
    work: dict[str, float],
    forced_trims: list[str],
) -> None:
    """Escalate weak-positive basket losers to manual trim review.

    This is intentionally diagnostic/manual by default. It changes decision
    state and reason codes, but does not reduce target weights unless the
    explicit auto-trim flag is enabled.
    """
    if not cfg.advisory_basket_loss_manual_review_enabled:
        return
    for row in decisions:
        reasons = row.get("reason_codes") or []
        if "basket_review" not in reasons or "unrealized_loss_review" not in reasons:
            continue
        role = str(row.get("position_role") or "")
        if role == "core":
            continue
        support = str(row.get("strategy_support") or "none")
        if support not in {"advisory", "none", "watch_only", "ignore"}:
            continue
        pnl = row.get("unrealized_pnl_pct")
        if not isinstance(pnl, (int, float)) or float(pnl) > cfg.advisory_basket_loss_review_pct:
            continue

        row.setdefault("reason_codes", []).append("advisory_basket_loss_review")
        row["reason_codes"] = list(dict.fromkeys(row["reason_codes"]))
        if row.get("decision") in {"hold", "hold_review"}:
            row["decision"] = "trim_review"

        loss_trim_threshold = row.get("loss_trim_threshold")
        auto_allowed = (
            bool(cfg.advisory_basket_loss_auto_trim_enabled)
            and isinstance(loss_trim_threshold, (int, float))
            and isinstance(pnl, (int, float))
            and float(pnl) <= float(loss_trim_threshold)
        )
        if auto_allowed:
            current_w = float(row.get("current_weight") or 0.0)
            target_w = float(row.get("target_after") or current_w)
            trim_step = max(float(cfg.advisory_basket_loss_auto_trim_pct or 0.0), 0.0)
            clipped = min(target_w, max(current_w - trim_step, 0.0))
            if clipped < target_w - 1e-9:
                ticker = str(row.get("ticker") or "")
                work[ticker] = clipped
                work["CASH"] = float(work.get("CASH", 0.0) or 0.0) + (target_w - clipped)
                forced_trims.append(f"{ticker} {target_w:.2%}->{clipped:.2%} advisory_basket_loss_auto")
                row["target_after"] = round(clipped, 6)
                row.setdefault("reason_codes", []).append("advisory_basket_loss_auto_trim")
                row["reason_codes"] = list(dict.fromkeys(row["reason_codes"]))


def _manual_action_hints(
    *,
    decisions: list[dict[str, Any]],
    require_human: bool,
    permission: str,
) -> list[dict[str, Any]]:
    constrained = require_human or permission in {"small_overweight_only", "hold_or_trim", "reduce_risk_only", "defensive_only"}
    if not constrained:
        return []
    hints: list[dict[str, Any]] = []
    for row in decisions:
        current_w = float(row.get("current_weight") or 0.0)
        target_w = float(row.get("target_after") or 0.0)
        reasons = row.get("reason_codes") or []
        manual_review_only = "advisory_basket_loss_review" in reasons
        if target_w >= current_w - 1e-9 and not manual_review_only:
            continue
        if not any(code in reasons for code in ("hard_risk", "unrealized_loss_review", "winner_risk_budget_review", "basket_review")):
            continue
        suggested_target = target_w
        if manual_review_only and target_w >= current_w - 1e-9:
            suggested_target = max(current_w - 0.01, 0.0)
        hints.append({
            "ticker": row.get("ticker"),
            "suggested_action": "manual_trim_review",
            "current_weight": round(current_w, 6),
            "suggested_target": round(suggested_target, 6),
            "delta": round(suggested_target - current_w, 6),
            "reason_codes": reasons[:5],
            "note": "risk-reducing trim requires human confirmation before execution",
        })
    return hints


def _llm_thesis_by_ticker(proposals: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for raw in proposals:
        if not isinstance(raw, dict):
            continue
        ticker = str(raw.get("ticker") or "").upper().strip()
        status = str(raw.get("thesis_status") or raw.get("llm_thesis_status") or "").lower().strip()
        if ticker and status in {"intact", "weakening", "broken", "unknown"}:
            out[ticker] = {
                "status": status,
                "reason": str(raw.get("thesis_reason") or raw.get("reason") or "")[:240],
                "confidence": raw.get("confidence"),
            }
    return out


def _validate_thesis_status(
    *,
    row: dict[str, Any],
    news_evidence: dict[str, Any],
    strategy_evidence: dict[str, Any],
    market_scorecard: dict[str, Any],
    llm_thesis: dict[str, Any] | None,
) -> dict[str, Any]:
    ticker = str(row.get("ticker") or "").upper().strip()
    reasons = set(str(item) for item in (row.get("reason_codes") or []))
    support = str(row.get("strategy_support") or "none")
    pnl = row.get("unrealized_pnl_pct")
    status = "unknown"
    evidence: list[str] = []
    warnings: list[str] = []

    if support in {"primary", "advisory"}:
        evidence.append(f"strategy_support:{support}")
    elif support in {"none", "watch_only", "ignore"}:
        evidence.append(f"strategy_support:{support}")

    if isinstance(pnl, (int, float)):
        evidence.append(f"pnl:{float(pnl):.2%}")
    if "hard_risk" in reasons:
        evidence.append("hard_risk_event")
    if "basket_review" in reasons:
        evidence.append("basket_review")
    if row.get("risk_budget_status") in {"medium", "high"}:
        evidence.append(f"risk_budget:{row.get('risk_budget_status')}")

    ticker_news = (news_evidence.get("ticker_news_scores") or {}).get(ticker) or {}
    if isinstance(ticker_news, dict):
        bias = ticker_news.get("bias") or ticker_news.get("action_bias")
        if bias:
            evidence.append(f"news:{bias}")

    evidence_summary = strategy_evidence.get("evidence_summary") or {}
    live_fit = str(evidence_summary.get("live_fit") or "")
    if live_fit in {"conflicted", "insufficient"}:
        evidence.append(f"live_fit:{live_fit}")
    if market_scorecard.get("dominant_constraint"):
        evidence.append(f"scorecard:{market_scorecard.get('dominant_constraint')}")

    if "hard_risk" in reasons:
        status = "broken"
    elif "unrealized_loss_review" in reasons and "strategy_support_weak" in reasons:
        status = "broken" if row.get("decision") == "trim" else "weakening"
    elif "basket_review" in reasons and "strategy_support_weak" in reasons:
        status = "weakening"
    elif support in {"primary", "advisory"} and "unrealized_loss_review" not in reasons and "hard_risk" not in reasons:
        status = "intact"
    elif support in {"none", "watch_only", "ignore"} and isinstance(pnl, (int, float)) and pnl < 0:
        status = "weakening"

    llm_status = (llm_thesis or {}).get("status")
    llm_result = "missing"
    if llm_status:
        if not evidence:
            llm_result = "rejected_no_supporting_evidence"
            warnings.append("LLM thesis status ignored because supporting evidence is missing")
        elif llm_status == status or status == "unknown":
            llm_result = "accepted"
            status = llm_status
        else:
            llm_result = f"overridden_by_validator:{status}"
            warnings.append(f"LLM thesis_status={llm_status} conflicted with deterministic evidence")

    return {
        "status": status,
        "llm_status": llm_status,
        "llm_validator_result": llm_result,
        "evidence": list(dict.fromkeys(evidence))[:8],
        "warnings": warnings,
        "execution_authority": "none",
    }


def _portfolio_summary(
    *,
    decisions: list[dict[str, Any]],
    group_exposure: dict[str, float],
    group_limits: dict[str, float],
    blocked_actions: list[str],
    forced_trims: list[str],
    replacements: list[dict[str, Any]],
    replacement_candidates: list[dict[str, Any]],
    advisory_overrides: list[dict[str, Any]],
    manual_action_hints: list[dict[str, Any]],
) -> dict[str, Any]:
    group_rows: dict[str, dict[str, Any]] = {}
    for group, exposure in group_exposure.items():
        limit = group_limits.get(group)
        headroom = (limit - exposure) if limit is not None else None
        if limit is None:
            status = "unknown"
        elif exposure > limit:
            status = "over_limit"
        elif exposure >= limit * 0.85:
            status = "near_limit"
        else:
            status = "ok"
        group_rows[group] = {
            "exposure": round(exposure, 6),
            "limit": round(limit, 6) if limit is not None else None,
            "headroom": round(headroom, 6) if headroom is not None else None,
            "status": status,
        }

    top_risk = [
        {
            "ticker": row.get("ticker"),
            "group": row.get("group"),
            "risk_rank": row.get("risk_rank"),
            "risk_contribution": row.get("risk_contribution"),
            "risk_budget_status": row.get("risk_budget_status"),
        }
        for row in sorted(
            decisions,
            key=lambda item: float(item.get("risk_contribution") or 0.0),
            reverse=True,
        )[:5]
        if float(row.get("risk_contribution") or 0.0) > 0
    ]

    return {
        "group_exposures": group_rows,
        "top_risk_contributors": top_risk,
        "governance_counts": {
            "decisions": _decision_counts(decisions),
            "blocked_actions": len(blocked_actions),
            "forced_trims": len(forced_trims),
            "replacements": len(replacements),
            "advisory_overrides": len([row for row in advisory_overrides if str(row.get("validator_result") or "").startswith("accepted")]),
        },
        "advisory_overrides": advisory_overrides[:8],
        "advisory_quality": build_advisory_quality_diagnostics(advisory_overrides),
        "basket_reviews": _basket_reviews(decisions),
        "manual_action_hints": manual_action_hints[:8],
        "thesis_status_summary": _thesis_status_summary(decisions),
        "thesis_review_queue": build_thesis_review_queue(decisions)[:12],
        "position_explanations": _position_explanations(decisions, blocked_actions),
        "replacement_candidates": [
            {
                "ticker": item.get("ticker"),
                "score": item.get("score"),
                "why": item.get("why") or [],
                "support": item.get("support"),
                "strategy_name": item.get("strategy_name"),
            }
            for item in replacement_candidates[:8]
        ],
    }


def _basket_reviews(decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for row in decisions:
        review = row.get("basket_review") or {}
        group = review.get("group")
        if not group:
            continue
        current = seen.setdefault(str(group), {
            "group": group,
            "tickers": set(),
            "reason": review.get("reason"),
        })
        current["tickers"].update(review.get("tickers") or [])
    out = []
    for item in seen.values():
        out.append({
            "group": item["group"],
            "tickers": sorted(item["tickers"]),
            "reason": item.get("reason") or "multiple correlated positions are in review",
        })
    return sorted(out, key=lambda item: item["group"])


def _thesis_status_summary(decisions: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    problem_tickers: list[dict[str, Any]] = []
    for row in decisions:
        thesis = row.get("thesis_status") or {}
        status = str(thesis.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        if status in {"weakening", "broken"}:
            problem_tickers.append({
                "ticker": row.get("ticker"),
                "status": status,
                "evidence": thesis.get("evidence") or [],
                "validator": thesis.get("llm_validator_result"),
            })
    return {
        "counts": counts,
        "problem_tickers": problem_tickers[:8],
        "execution_authority": "none",
    }


def _position_explanations(
    decisions: list[dict[str, Any]],
    blocked_actions: list[str],
) -> list[dict[str, Any]]:
    blocked_by_ticker: dict[str, list[str]] = {}
    for item in blocked_actions:
        text = str(item or "")
        parts = text.split(":")
        ticker = parts[1].upper() if len(parts) > 1 else ""
        if ticker:
            blocked_by_ticker.setdefault(ticker, []).append(text)

    explanations = [_explain_position(row, blocked_by_ticker.get(str(row.get("ticker") or "").upper(), [])) for row in decisions]
    return sorted(
        explanations,
        key=lambda row: (
            -float(row.get("current_weight") or 0.0),
            str(row.get("ticker") or ""),
        ),
    )


def _explain_position(row: dict[str, Any], blocked: list[str]) -> dict[str, Any]:
    ticker = str(row.get("ticker") or "").upper()
    reasons = set(str(item) for item in (row.get("reason_codes") or []))
    allowed = set(str(item) for item in (row.get("allowed_actions") or []))
    decision = str(row.get("decision") or "hold")
    state = _position_state(row, reasons)
    facts = _explanation_facts(row, reasons, allowed, blocked)
    why_hold = _why_hold(row, reasons, allowed, facts)
    why_not_add = _why_not_add(row, reasons, allowed, blocked, facts)
    why_not_exit = _why_not_exit(row, reasons, allowed, facts)
    next_trigger = _next_trigger(row, reasons, facts)
    priority = _explanation_priority(row, reasons, blocked)
    return {
        "ticker": ticker,
        "position_state": state,
        "decision": decision,
        "current_weight": row.get("current_weight"),
        "target_after": row.get("target_after"),
        "unrealized_pnl_pct": row.get("unrealized_pnl_pct"),
        "risk_contribution": row.get("risk_contribution"),
        "risk_budget_status": row.get("risk_budget_status"),
        "basket_review": row.get("basket_review"),
        "thesis_status": row.get("thesis_status"),
        "explanation_facts": facts,
        "strategy_support": row.get("strategy_support"),
        "action_permission": row.get("action_permission"),
        "why_hold": why_hold,
        "why_not_add": why_not_add,
        "why_not_exit": why_not_exit,
        "next_trigger": next_trigger,
        "blocked_actions": blocked[:3],
        "priority": priority,
    }


def _position_state(row: dict[str, Any], reasons: set[str]) -> str:
    decision = str(row.get("decision") or "hold")
    pnl = row.get("unrealized_pnl_pct")
    support = row.get("strategy_support")
    if "hard_risk" in reasons:
        return "hard_risk_review"
    if "replacement_candidate" in reasons:
        return "replacement_candidate"
    if "unrealized_loss_review" in reasons:
        return "loss_trim_candidate" if decision == "trim" else "loss_review"
    if "winner_risk_budget_review" in reasons:
        return "supported_winner" if support in {"primary", "advisory"} else "unsupported_winner"
    if "strategy_support_weak" in reasons:
        return "unsupported_winner" if isinstance(pnl, (int, float)) and pnl > 0 else "normal_hold"
    if isinstance(pnl, (int, float)) and pnl >= 0.08:
        return "supported_winner" if support in {"primary", "advisory"} else "unsupported_winner"
    return "normal_hold"


def _explanation_facts(
    row: dict[str, Any],
    reasons: set[str],
    allowed: set[str],
    blocked: list[str],
) -> dict[str, Any]:
    basket = row.get("basket_review") or {}
    thesis = row.get("thesis_status") or {"status": "unknown", "evidence": []}
    if "hard_risk" in reasons:
        severity = "hard_risk"
        primary_reason = "hard_risk_event_active"
        risk_action = "manual_trim_review" if "exit" in allowed else "trim_review"
    elif "basket_review" in reasons:
        severity = "basket_review"
        primary_reason = "correlated_basket_review"
        risk_action = "manual_trim_review" if "trim" in allowed else "hold_review"
    elif "unrealized_loss_review" in reasons:
        severity = "loss_review"
        primary_reason = "unrealized_loss_review"
        risk_action = "trim" if row.get("decision") == "trim" else "hold_review"
    elif "winner_risk_budget_review" in reasons:
        severity = "winner_review"
        primary_reason = "winner_risk_budget_review"
        risk_action = "trim_review" if "trim" in allowed else "hold"
    else:
        severity = "normal"
        primary_reason = "no_active_review_reason"
        risk_action = str(row.get("decision") or "hold")

    execution_blocker = None
    if "scorecard_human_required" in reasons:
        execution_blocker = "human_required"
    elif any("risk_rejected" in str(item) for item in blocked):
        execution_blocker = "risk_rejected"

    basket_context = None
    if basket:
        basket_context = {
            "group": basket.get("group"),
            "tickers": basket.get("tickers") or [],
            "trigger": "multiple_loss_review_positions",
            "reason": basket.get("reason"),
        }

    return {
        "severity": severity,
        "primary_reason": primary_reason,
        "execution_blocker": execution_blocker,
        "risk_action": risk_action,
        "basket_context": basket_context,
        "thesis_status": {
            "status": thesis.get("status", "unknown"),
            "evidence": thesis.get("evidence") or [],
        },
    }


def _why_hold(
    row: dict[str, Any],
    reasons: set[str],
    allowed: set[str],
    facts: dict[str, Any],
) -> list[str]:
    out: list[str] = []
    pnl = row.get("unrealized_pnl_pct")
    support = row.get("strategy_support")
    if "hard_risk" in reasons:
        out.append("hard-risk event is active; position requires manual trim/exit review")
        if facts.get("execution_blocker") == "human_required":
            out.append("automatic execution is blocked by human confirmation")
        return list(dict.fromkeys(out))[:4]
    if "basket_review" in reasons:
        basket = facts.get("basket_context") or {}
        group = basket.get("group") or row.get("group") or "correlated basket"
        tickers = ",".join((basket.get("tickers") or [])[:4])
        suffix = f" ({tickers})" if tickers else ""
        out.append(f"{group} basket has multiple correlated positions in review{suffix}")
    if "unrealized_loss_review" in reasons and isinstance(pnl, (int, float)):
        if pnl > -0.08:
            out.append("loss is above hard trim threshold")
        else:
            out.append("loss reached trim zone; target was reduced, not forced to full exit")
    if "hard_risk" not in reasons:
        out.append("no hard-risk event requires immediate exit")
    if support == "primary":
        out.append("primary strategy support remains")
    elif support == "advisory":
        out.append("only advisory strategy support remains")
    if "trim" in allowed and "exit" not in allowed:
        out.append("governance allows trim but not unrestricted exit")
    if not out:
        out.append("no deterministic rule requires reduction")
    return list(dict.fromkeys(out))[:4]


def _why_not_add(
    row: dict[str, Any],
    reasons: set[str],
    allowed: set[str],
    blocked: list[str],
    facts: dict[str, Any],
) -> list[str]:
    out: list[str] = []
    if "add" in allowed and not blocked and not ({"unrealized_loss_review", "basket_review", "high_atr"} & reasons):
        return ["add is allowed within risk limits"]
    if "scorecard_human_required" in reasons:
        out.append("market scorecard requires human confirmation")
    if any(reason.startswith("scorecard_") and reason != "scorecard_human_required" for reason in reasons):
        out.append("scorecard permission restricts new risk")
    if "unrealized_loss_review" in reasons:
        out.append("position is in unrealized loss review")
    if "basket_review" in reasons:
        basket = facts.get("basket_context") or {}
        group = basket.get("group") or row.get("group") or "correlated basket"
        out.append(f"{group} basket is in correlated review")
    if "strategy_support_weak" in reasons:
        out.append("strategy support is weak or absent")
    elif row.get("strategy_support") == "advisory" and "unrealized_loss_review" in reasons:
        out.append("advisory support is not strong enough to justify adding")
    if "high_atr" in reasons:
        out.append("ATR is elevated")
    if any(reason.endswith("_concentration_high") for reason in reasons):
        out.append("group exposure is above limit")
    if blocked:
        out.append("buy was blocked by governance")
    if not out:
        out.append("add is not in allowed action set")
    return list(dict.fromkeys(out))[:4]


def _why_not_exit(
    row: dict[str, Any],
    reasons: set[str],
    allowed: set[str],
    facts: dict[str, Any],
) -> list[str]:
    out: list[str] = []
    if "exit" in allowed:
        return ["exit is permitted for manual/hard-risk review"]
    if "hard_risk" not in reasons:
        out.append("no hard-risk event is active")
    if "unrealized_loss_review" in reasons and "strategy_support_weak" not in reasons:
        out.append("loss review exists but strategy support is not weak")
    elif "unrealized_loss_review" in reasons:
        out.append("governance uses staged trim before full exit")
    else:
        out.append("exit requires hard risk or deep weak-support loss")
    return list(dict.fromkeys(out))[:4]


def _next_trigger(row: dict[str, Any], reasons: set[str], facts: dict[str, Any]) -> str:
    if "hard_risk" in reasons:
        return "review manual exit when hard-risk event remains unresolved"
    if "basket_review" in reasons:
        basket = facts.get("basket_context") or {}
        group = basket.get("group") or row.get("group") or "basket"
        return f"manual trim review if {group} basket weakness persists"
    if "unrealized_loss_review" in reasons:
        threshold = row.get("loss_trim_threshold")
        threshold_text = f"{float(threshold):.0%}" if isinstance(threshold, (int, float)) else "-8%"
        return f"trim if loss <= {threshold_text} and strategy support remains weak"
    if "high_atr" in reasons:
        return "allow add only after ATR falls below high-volatility threshold"
    if any(reason.endswith("_concentration_high") for reason in reasons):
        group = row.get("group") or "group"
        limit = row.get("group_limit")
        limit_text = f"{float(limit):.0%}" if isinstance(limit, (int, float)) else "limit"
        return f"allow add after {group} exposure falls below {limit_text}"
    if "winner_risk_budget_review" in reasons:
        return "trim again if winner remains oversized and risk budget stays high"
    if "strategy_support_weak" in reasons:
        return "hold until strategy support improves or risk trigger worsens"
    return "continue monitoring for loss, volatility, concentration, or hard-risk triggers"


def _explanation_priority(row: dict[str, Any], reasons: set[str], blocked: list[str]) -> int:
    score = 0
    if "hard_risk" in reasons:
        score += 50
    if row.get("decision") in {"trim", "trim_review"}:
        score += 30
    if "unrealized_loss_review" in reasons:
        score += 25
    if blocked:
        score += 20
    if "high_atr" in reasons or row.get("risk_budget_status") == "high":
        score += 15
    if any(reason.endswith("_concentration_high") for reason in reasons):
        score += 12
    if "strategy_support_weak" in reasons:
        score += 10
    return score


def _apply_replacements(
    *,
    work: dict[str, float],
    current: dict[str, float],
    decisions: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    cfg: GovernanceConfig,
) -> list[dict[str, Any]]:
    cash = float(work.get("CASH", 0.0) or 0.0)
    current_cash = float(current.get("CASH", 0.0) or 0.0)
    freed_cash = max(cash - current_cash, 0.0)
    budget = min(freed_cash, cfg.replacement_max_total_pct)
    if budget <= 1e-9:
        return []

    replacements: list[dict[str, Any]] = []
    for candidate in candidates:
        if budget <= 1e-9:
            break
        ticker = candidate["ticker"]
        add = min(cfg.replacement_max_single_pct, budget)
        if add <= 1e-9:
            continue
        before = float(work.get(ticker, 0.0) or 0.0)
        work[ticker] = before + add
        work["CASH"] = float(work.get("CASH", 0.0) or 0.0) - add
        budget -= add
        replacements.append({
            "ticker": ticker,
            "added_weight": round(add, 6),
            "reason": "replace_trimmed_cash_with_supported_candidate",
            "support": candidate["support"],
            "strategy_name": candidate["strategy_name"],
            "score": candidate.get("score"),
            "why": candidate.get("why") or [],
        })
        for row in decisions:
            if row.get("ticker") == ticker:
                row["target_after"] = round(float(work.get(ticker, 0.0) or 0.0), 6)
                row.setdefault("reason_codes", []).append("replacement_candidate")
                row["reason_codes"] = list(dict.fromkeys(row["reason_codes"]))
                if row.get("decision") == "hold":
                    row["decision"] = "add"
                break
    return replacements


def _replacement_candidates(
    strategy_evidence: dict[str, Any],
    decisions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    decision_by_ticker = {str(row.get("ticker") or "").upper(): row for row in decisions}
    rank = {"primary": 0, "advisory": 1}
    candidates: dict[str, dict[str, Any]] = {}
    for row in strategy_evidence.get("strategy_results") or []:
        if not isinstance(row, dict):
            continue
        use = str(row.get("suggested_use") or "")
        if use not in rank:
            continue
        strategy_name = str(row.get("strategy_name") or "")
        confidence = _confidence_score(row.get("confidence_score", row.get("confidence")))
        for order, ticker in enumerate(row.get("selected_tickers") or []):
            key = str(ticker or "").upper().strip()
            if not key or key == "CASH":
                continue
            decision = decision_by_ticker.get(key, {})
            if decision.get("action_permission") not in {"hold_or_add_or_trim"}:
                continue
            reason_codes = decision.get("reason_codes") or []
            if any(code in reason_codes for code in (
                "hard_risk",
                "high_atr",
                "strategy_support_weak",
                "unrealized_loss_review",
                "winner_risk_budget_review",
            )):
                continue
            if any(str(code).endswith("_concentration_high") for code in reason_codes):
                continue
            score, why = _replacement_score(
                use=use,
                confidence=confidence,
                selected_order=order,
                decision=decision,
            )
            current = candidates.get(key)
            if current is None or score > current["score"]:
                candidates[key] = {
                    "ticker": key,
                    "support": use,
                    "strategy_name": strategy_name,
                    "score": score,
                    "why": why,
                }
    return sorted(candidates.values(), key=lambda item: (-float(item["score"]), item["ticker"]))


def _replacement_score(
    *,
    use: str,
    confidence: float,
    selected_order: int,
    decision: dict[str, Any],
) -> tuple[float, list[str]]:
    score = 0.0
    why: list[str] = []

    if use == "primary":
        score += 0.45
        why.append("primary_strategy")
    elif use == "advisory":
        score += 0.35
        why.append("advisory_strategy")

    score += confidence * 0.25
    if confidence >= 0.65:
        why.append("high_strategy_confidence")
    elif confidence >= 0.50:
        why.append("medium_strategy_confidence")

    order_bonus = max(0.0, 0.15 - selected_order * 0.03)
    score += order_bonus
    if selected_order <= 2:
        why.append("high_strategy_rank")

    current_weight = float(decision.get("current_weight") or 0.0)
    if current_weight > 0.01:
        score += 0.05
        why.append("existing_position")

    atr = decision.get("atr_pct")
    if isinstance(atr, (int, float)):
        atr_penalty = min(0.20, max(0.0, float(atr)) * 2.0)
        score -= atr_penalty
        if atr_penalty <= 0.04:
            why.append("low_atr")
        elif atr_penalty >= 0.10:
            why.append("atr_penalty")

    risk_contribution = float(decision.get("risk_contribution") or 0.0)
    if risk_contribution > 0:
        score -= min(0.15, risk_contribution * 20.0)
        if decision.get("risk_budget_status") == "normal":
            why.append("normal_risk_budget")
        elif decision.get("risk_budget_status") in {"medium", "high"}:
            why.append(f"{decision.get('risk_budget_status')}_risk_budget")

    headroom = decision.get("group_headroom")
    limit = decision.get("group_limit")
    if isinstance(headroom, (int, float)) and isinstance(limit, (int, float)) and limit > 0:
        if headroom > limit * 0.15:
            score += 0.05
            why.append("group_headroom")
        elif headroom < limit * 0.05:
            score -= 0.08
            why.append("limited_group_headroom")

    return round(max(0.0, min(1.0, score)), 4), list(dict.fromkeys(why))


def _confidence_score(value: Any) -> float:
    raw = _to_float(value)
    if raw is None:
        return 0.50
    if raw > 1.0:
        raw = raw / 100.0
    return max(0.0, min(1.0, raw))


def _strategy_support_by_ticker(strategy_evidence: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rank = {"primary": 4, "advisory": 3, "watch_only": 2, "ignore": 1, "none": 0}
    out: dict[str, dict[str, Any]] = {}
    for row in strategy_evidence.get("strategy_results") or []:
        if not isinstance(row, dict):
            continue
        use = str(row.get("suggested_use") or "watch_only")
        name = str(row.get("strategy_name") or "")
        for ticker in row.get("selected_tickers") or []:
            key = str(ticker or "").upper().strip()
            if not key or key == "CASH":
                continue
            current = out.setdefault(key, {"level": "none", "strategies": []})
            if rank.get(use, 0) > rank.get(current["level"], 0):
                current["level"] = use
            if name:
                current["strategies"].append(name)
    return out


def _hard_risk_tickers(news_evidence: dict[str, Any]) -> set[str]:
    hard = news_evidence.get("hard_risk_events") or {}
    return {str(ticker or "").upper().strip() for ticker in hard if str(ticker or "").strip()}


def _position_role(row: dict[str, Any], ticker: str) -> str:
    raw = str(row.get("universe_role") or row.get("position_role") or "").lower().strip()
    if raw in {"core", "satellite"}:
        return raw
    group = _ticker_group(ticker)
    if group in {"semiconductors", "tech_growth", "real_estate"}:
        return "satellite"
    return "core"


def _loss_thresholds(role: str, cfg: GovernanceConfig) -> tuple[float, float]:
    if role == "satellite":
        return cfg.satellite_loss_review_pct, cfg.satellite_loss_trim_pct
    if role == "core":
        return cfg.core_loss_review_pct, cfg.core_loss_trim_pct
    return cfg.loss_review_pct, cfg.loss_trim_pct


def _group_exposure(current_weights: dict[str, float]) -> dict[str, float]:
    return calc_primary_group_exposure(current_weights)


def _ticker_group(ticker: str) -> str | None:
    return get_primary_group(ticker)


def _permission_from_actions(actions: set[str]) -> str:
    if "exit" in actions:
        return "trim_or_exit"
    if "add" in actions:
        return "hold_or_add_or_trim"
    if "trim" in actions:
        return "hold_or_trim"
    return "hold_only"


def _decision_counts(decisions: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in decisions:
        key = str(row.get("decision") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _meta_by_ticker(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = str((row or {}).get("ticker") or "").upper().strip()
        if ticker:
            out[ticker] = dict(row)
    return out


def _clean_weights(weights: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (weights or {}).items():
        key = str(ticker or "").upper().strip()
        if not key:
            continue
        try:
            out[key] = max(float(value or 0.0), 0.0)
        except (TypeError, ValueError):
            out[key] = 0.0
    return out


def _normalize_weights(weights: dict[str, Any] | None) -> dict[str, float]:
    clean = _clean_weights(weights)
    total = sum(clean.values())
    if total <= 0:
        return {"CASH": 1.0}
    out = {ticker: round(weight / total, 6) for ticker, weight in clean.items() if weight > 1e-9}
    diff = round(1.0 - sum(out.values()), 6)
    if abs(diff) > 1e-9:
        target = "CASH" if "CASH" in out else max(out, key=out.get)
        out[target] = round(out.get(target, 0.0) + diff, 6)
    return out


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
