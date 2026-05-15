# agents/risk_manager.py
"""
Stage 4: RISK MGR — chief risk officer (pure Python)

Per update.txt: "second review + defensive overlays". Not a hard gate — an auditor:
    1. Take researcher.adjusted_weights (draft_proposal) as input
    2. Apply three overlays in order, turning the LLM proposal into final target_weights:
         Overlay 1  transmission_tilt   — macro-driven sector tilt
         Overlay 2  defensive_adjust    — regime defense matrix
         Overlay 3  hard_risk_filter    — event risk: no new positions into flagged names; existing held
    3. Compute rebalance_actions + estimated cost from target_weights
    4. Six quantitative checks:
         vol_ok / drawdown_ok / position_ok / broad_market_ok / cash_ok / cost_ok
    5. If pass, issue one-time approval_token
    6. If reject, concrete rejection_reasons

vs legacy:
    · Old strategy_engine mixed overlay + scoring + optimization; overlays moved here
      merged with the "floor" role — Stage 4 intent from update.txt.
    · Old risk_manager only asserted; new version may modify then assert (deterministic Python, not LLM).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from services.transmission import (
    apply_transmission,
    generate_transmission_vector,
    match_event_to_pattern,
)
from strategies import (
    compute_rebalance_actions,
    defensive_adjust,
    estimate_cost_pct,
)
from tools.db_tools import tool_write_approval_token
from services.market_scorecard import is_evidence_stale

logger = logging.getLogger("qc_fastapi_2.risk_mgr")

BROAD_MARKET_TICKERS = {"SPY", "QQQ", "IWM"}

# Regimes that trigger defensive_adjust
DEFENSIVE_REGIMES = {"bear_weak", "bear_trend", "high_vol"}


# ─────────────────────────────── Main entry ───────────────────────────────


async def run_risk_manager_async(
    pipeline_context: dict,
    brief:            dict,
    quant_baseline:   dict,
    researcher_out:   dict,
) -> dict:
    """
    Stage 4 entry.
    Inputs:
        pipeline_context — risk_params / override_mode / active_strategy
        brief            — portfolio / holdings / hard_risks_map / current_weights
        quant_baseline   — audit reference (not used in overlay math)
        researcher_out   — adjusted_weights + regime + key_events
    Output: final target_weights + six checks + token (if approved).
    """
    risk_params = pipeline_context.get("risk_params") or {}
    override_mode = pipeline_context.get("override_mode")

    adjusted_weights = researcher_out.get("adjusted_weights") or {}
    if not adjusted_weights:
        logger.warning("RiskMgr: researcher.adjusted_weights empty, falling back to base_weights")
        adjusted_weights = quant_baseline.get("base_weights") or {"CASH": 1.0}

    market_judgment = researcher_out.get("market_judgment") or {}
    regime = str(market_judgment.get("regime", "neutral"))
    uncertainty = bool(market_judgment.get("uncertainty_flag", False))
    key_events = researcher_out.get("key_events") or []
    # 确保 key_events 是一个列表
    if not isinstance(key_events, list):
        key_events = []
    # 确保 key_events 是一个列表
    if not isinstance(key_events, list):
        key_events = []

    current_weights = brief.get("current_weights") or {}
    hard_risks_map = brief.get("hard_risks_map") or {}
    portfolio = brief.get("portfolio") or {}
    holdings = brief.get("holdings") or []
    critical_alerts = brief.get("critical_alerts") or []
    market_scorecard = (
        pipeline_context.get("market_scorecard")
        or brief.get("market_scorecard")
        or {}
    )
    decision_style = (
        pipeline_context.get("decision_style")
        or brief.get("decision_style")
        or (brief.get("evidence_bundle") or {}).get("decision_style")
        or {}
    )
    evidence_bundle = brief.get("evidence_bundle") or {}

    # ═══ 3-layer overlay chain ═══
    overlays_applied: list[str] = []
    working = dict(adjusted_weights)

    # Overlay 1: transmission_tilt (macro-driven)
    working = _apply_transmission_tilt(
        working,
        key_events=key_events,
        risk_params=risk_params,
        overlays_applied=overlays_applied,
    )

    # Overlay 2: defensive_adjust (regime defense)
    if override_mode == "DEFENSIVE" or regime in DEFENSIVE_REGIMES:
        working = defensive_adjust(
            working,
            {"regime": regime, "uncertainty_flag": uncertainty},
        )
        tag = f"defensive:{regime if regime in DEFENSIVE_REGIMES else 'override'}"
        overlays_applied.append(tag)

    # Overlay 3: hard_risk_filter (event risk)
    working = _apply_hard_risk_filter(
        working,
        current_weights=current_weights,
        hard_risks_map=hard_risks_map,
        overlays_applied=overlays_applied,
    )

    # Overlay 4: critical_alerts — QC webhook emergency/critical alerts (P1-1)
    if critical_alerts:
        working = _apply_critical_alerts_overlay(
            working,
            current_weights=current_weights,
            critical_alerts=critical_alerts,
            overlays_applied=overlays_applied,
        )

    target_weights = _normalize_weights(working)
    scorecard_enforcement = apply_scorecard_constraints(
        target_weights=target_weights,
        base_weights=quant_baseline.get("base_weights") or {},
        market_scorecard=market_scorecard,
    )
    target_weights = scorecard_enforcement["target_weights_post_scorecard_clip"]
    if scorecard_enforcement["violations"]:
        overlays_applied.append("scorecard_constraints")
        logger.warning(
            "[RiskMgr] scorecard constraints adjusted target weights | "
            f"violations={scorecard_enforcement['violations']}"
        )
    style_enforcement = apply_style_constraints(
        target_weights=target_weights,
        base_weights=quant_baseline.get("base_weights") or {},
        current_weights=current_weights,
        decision_style=decision_style,
        market_scorecard=market_scorecard,
    )
    target_weights = style_enforcement["target_weights_post_style_clip"]
    if style_enforcement["violations"]:
        overlays_applied.append("style_constraints")
        logger.warning(
            "[RiskMgr] style constraints adjusted target weights | "
            f"violations={style_enforcement['violations']}"
        )

    # ═══ rebalance_actions + cost ═══
    rebalance_threshold = float(risk_params.get("rebalance_threshold", 0.02))
    rebalance_actions = compute_rebalance_actions(
        target_weights, current_weights, rebalance_threshold
    )
    estimated_cost = estimate_cost_pct(rebalance_actions)

    # ═══ Six quantitative checks ═══
    checks, reasons = _run_checks(
        target_weights=target_weights,
        estimated_cost=estimated_cost,
        holdings=holdings,
        portfolio=portfolio,
        risk_params=risk_params,
    )
    scorecard_check = scorecard_enforcement["post_clip_compliance"]
    checks["scorecard_ok"] = {
        "pass": bool(scorecard_check.get("compliant", True)),
        "actual": scorecard_check,
        "threshold": "market_scorecard",
    }
    if not scorecard_check.get("compliant", True):
        reasons.extend(scorecard_check.get("violations") or [])
    style_check = style_enforcement["post_clip_compliance"]
    checks["style_ok"] = {
        "pass": bool(style_check.get("compliant", True)),
        "actual": style_check,
        "threshold": "decision_style",
    }
    if not style_check.get("compliant", True):
        reasons.extend(style_check.get("violations") or [])

    evidence_stale = bool(evidence_bundle and is_evidence_stale(evidence_bundle))
    if evidence_stale:
        checks["evidence_fresh_ok"] = {
            "pass": False,
            "actual": "stale",
            "threshold": evidence_bundle.get("max_age_seconds"),
        }
        reasons.append("Evidence bundle is stale; execution requires fresh market evidence")

    human_required = bool(market_scorecard.get("require_human_confirmation"))
    full_auto_blocked = (
        pipeline_context.get("auth_mode") == "FULL_AUTO"
        and human_required
    )
    if full_auto_blocked:
        checks["human_confirmation_ok"] = {
            "pass": False,
            "actual": "required",
            "threshold": "FULL_AUTO must not execute scorecard-human-required proposal",
        }
        reasons.append("Market scorecard requires human confirmation; FULL_AUTO execution blocked")

    approved = all(c["pass"] for c in checks.values())
    failed_checks = {name: c for name, c in checks.items() if not c["pass"]}

    result: dict[str, Any] = {
        "approved":            approved,
        "target_weights":      target_weights,
        "rebalance_actions":   rebalance_actions,
        "estimated_cost_pct":  estimated_cost,
        "overlays_applied":    overlays_applied,
        "quantitative_checks": checks,
        "rejection_reasons":   reasons,
        "failed_checks":       failed_checks,
        "n_holdings":          _count_non_cash(target_weights),
        "scorecard_enforcement": scorecard_enforcement,
        "style_enforcement": style_enforcement,
        "reviewed_at":         datetime.utcnow().isoformat(),
    }

    if approved:
        token_result = await tool_write_approval_token({})
        result["approval_token"]   = token_result["approval_token"]
        result["token_expires_at"] = token_result["expires_at"]
        logger.info(
            f"RiskMgr approved | regime={regime} | n_actions={len(rebalance_actions)} "
            f"| cost={estimated_cost:.4%} | overlays={overlays_applied}"
        )
    else:
        logger.warning(
            f"RiskMgr rejected | {len(reasons)} checks failed: {reasons} "
            f"| overlays={overlays_applied}"
        )

    return result


# ═══════════════════════════════════════════════════════════════
# Overlay 1: Transmission
# ═══════════════════════════════════════════════════════════════


def _apply_transmission_tilt(
    weights:          dict[str, float],
    *,
    key_events:       list[str],
    risk_params:      dict,
    overlays_applied: list[str],
) -> dict[str, float]:
    """RESEARCHER.key_events → match pattern → tilt. Pass-through if no match."""
    if not key_events:
        return weights
    
    # 确保 key_events 是字符串列表
    if not isinstance(key_events, list):
        return weights
    
    # 过滤掉非字符串的元素
    event_strings = [str(e) for e in key_events if e]
    if not event_strings:
        return weights

    pattern = match_event_to_pattern(event_strings)
    if not pattern:
        return weights

    vector = generate_transmission_vector(pattern)
    if not vector:
        return weights

    max_pos = float(risk_params.get("max_single_position", 0.20))
    tilted = apply_transmission(weights, vector, max_pos)
    overlays_applied.append(f"transmission:{pattern}")
    return tilted


# ═══════════════════════════════════════════════════════════════
# Overlay 3: Hard Risk Filter
# ═══════════════════════════════════════════════════════════════


def _apply_hard_risk_filter(
    weights:          dict[str, float],
    *,
    current_weights:  dict[str, float],
    hard_risks_map:   dict[str, dict],
    overlays_applied: list[str],
) -> dict[str, float]:
    """
    Hard-risk tickers:
      - Not held → zero target (no new position), weight to CASH
      - Held → unchanged (Phase 1 ETF universe rarely hits this)
    """
    if not hard_risks_map:
        return weights

    filtered = dict(weights)
    freed_weight = 0.0
    flagged: list[str] = []

    for ticker, risks in hard_risks_map.items():
        if not risks:
            continue
        if ticker not in current_weights or current_weights.get(ticker, 0) == 0:
            if filtered.get(ticker, 0) > 0:
                freed_weight += filtered[ticker]
                filtered[ticker] = 0.0
                flagged.append(ticker)

    if flagged:
        filtered["CASH"] = round(filtered.get("CASH", 0) + freed_weight, 4)
        overlays_applied.append(f"hard_risk:{'/'.join(flagged)}")

    return filtered


def _apply_critical_alerts_overlay(
    weights:          dict[str, float],
    *,
    current_weights:  dict[str, float],
    critical_alerts:  list[dict],
    overlays_applied: list[str],
) -> dict[str, float]:
    """
    P1-1: Overlay for QC webhook critical alerts.
    Based on alert type, reduce exposure to affected tickers or overall equity.
    """
    if not critical_alerts:
        return weights

    filtered = dict(weights)
    freed_weight = 0.0
    affected: list[str] = []

    for alert in critical_alerts:
        ticker = (alert.get("ticker") or "").upper().strip()
        alert_type = (alert.get("type") or "").lower()
        message = alert.get("message") or ""

        # Determine action based on alert type
        if ticker and ticker in filtered and filtered.get(ticker, 0) > 0:
            if any(kw in alert_type or kw in message.lower()
                   for kw in ("drawdown", "loss", "risk", "emergency", "critical")):
                freed_weight += filtered[ticker]
                filtered[ticker] = 0.0
                affected.append(ticker)
        elif not ticker or ticker == "":
            # Portfolio-level alert (no specific ticker) — reduce overall equity by 10%
            if any(kw in alert_type or kw in message.lower()
                   for kw in ("drawdown", "loss", "risk", "emergency", "critical")):
                for t in list(filtered.keys()):
                    if t == "CASH":
                        continue
                    freed_weight += filtered[t] * 0.10
                    filtered[t] = round(filtered[t] * 0.90, 4)
                affected.append("PORTFOLIO")

    if affected:
        filtered["CASH"] = round(filtered.get("CASH", 0) + freed_weight, 4)
        overlays_applied.append(f"critical_alerts:{'/'.join(affected)}")
        logger.warning(f"[RiskMgr] critical_alerts overlay applied: {affected}")

    return filtered


# ═══════════════════════════════════════════════════════════════
# Scorecard hard constraints
# ═══════════════════════════════════════════════════════════════


def apply_scorecard_constraints(
    *,
    target_weights: dict[str, float],
    base_weights: dict[str, float],
    market_scorecard: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Enforce Market Scorecard limits using cash-first redistribution.

    Freed or clipped equity goes to CASH. We do not proportionally re-expand
    equity after clipping because that can violate reduce-risk constraints.
    """
    scorecard = market_scorecard or {}
    pre = _normalize_weights(target_weights)
    if not scorecard:
        return {
            "applied": False,
            "target_weights_pre_scorecard_clip": pre,
            "target_weights_post_scorecard_clip": pre,
            "violations": [],
            "clip_log": [],
            "post_clip_compliance": _check_scorecard_compliance(pre, base_weights, scorecard),
        }

    base = _clean_weight_map(base_weights)
    work = _clean_weight_map(pre)
    work.setdefault("CASH", 0.0)
    clip_log: list[str] = []

    permission = str(scorecard.get("investment_permission") or "")
    if permission == "cash_only":
        for ticker in list(work):
            if ticker != "CASH" and work.get(ticker, 0.0) > 0:
                clip_log.append(f"cash_only:{ticker} {work[ticker]:.2%}->0.00%")
                work[ticker] = 0.0
        work["CASH"] = 1.0
        post = _cash_first_normalize(work)
        return _scorecard_enforcement_result(pre, post, base, scorecard, clip_log)

    max_delta = _optional_float(scorecard.get("max_adjustment_from_base"))
    max_single = _optional_float(scorecard.get("max_single_position"))
    allow_new = bool(scorecard.get("allow_new_positions", True))

    # Per-position caps. Any freed weight is retained as CASH.
    for ticker in list(work.keys()):
        if ticker == "CASH":
            continue
        current = float(work.get(ticker, 0.0) or 0.0)
        base_w = float(base.get(ticker, 0.0) or 0.0)

        if not allow_new and base_w <= 0.01 and current > 0.01:
            work[ticker] = base_w
            work["CASH"] += max(current - base_w, 0.0)
            clip_log.append(f"new_position_blocked:{ticker} {current:.2%}->{base_w:.2%}")
            current = work[ticker]

        if max_delta is not None:
            upper = base_w + max_delta
            lower = max(base_w - max_delta, 0.0)
            if current > upper:
                work[ticker] = upper
                work["CASH"] += current - upper
                clip_log.append(f"max_delta:{ticker} {current:.2%}->{upper:.2%}")
                current = upper
            elif current < lower:
                needed = lower - current
                available_cash = max(float(work.get("CASH", 0.0) or 0.0), 0.0)
                add_back = min(needed, available_cash)
                if add_back > 0:
                    work[ticker] = current + add_back
                    work["CASH"] = available_cash - add_back
                    clip_log.append(
                        f"max_sell_delta:{ticker} {current:.2%}->{work[ticker]:.2%}"
                    )
                    current = work[ticker]

        if max_single is not None and current > max_single:
            work[ticker] = max_single
            work["CASH"] += current - max_single
            clip_log.append(f"max_single:{ticker} {current:.2%}->{max_single:.2%}")

    # Portfolio-level equity cap sends excess to cash.
    max_equity = _optional_float(scorecard.get("max_equity_weight"))
    if max_equity is not None:
        equity = _equity_sum(work)
        if equity > max_equity + 1e-9:
            scale = max_equity / equity if equity > 0 else 0.0
            freed = 0.0
            for ticker in list(work.keys()):
                if ticker == "CASH":
                    continue
                old = work[ticker]
                work[ticker] = old * scale
                freed += old - work[ticker]
            work["CASH"] = float(work.get("CASH", 0.0) or 0.0) + freed
            clip_log.append(f"max_equity:{equity:.2%}->{max_equity:.2%}")

    # Cash floor also reduces equity and moves proceeds to cash.
    min_cash = _optional_float(scorecard.get("min_cash_weight"))
    if min_cash is not None:
        cash = float(work.get("CASH", 0.0) or 0.0)
        if cash < min_cash - 1e-9:
            shortfall = min_cash - cash
            equity = _equity_sum(work)
            if equity > 0:
                target_equity = max(equity - shortfall, 0.0)
                scale = target_equity / equity if equity > 0 else 0.0
                for ticker in list(work.keys()):
                    if ticker != "CASH":
                        work[ticker] *= scale
                work["CASH"] = min_cash
                clip_log.append(f"min_cash:{cash:.2%}->{min_cash:.2%}")
            else:
                work["CASH"] = 1.0

    post = _cash_first_normalize(work)
    return _scorecard_enforcement_result(pre, post, base, scorecard, clip_log)


def _scorecard_enforcement_result(
    pre: dict[str, float],
    post: dict[str, float],
    base: dict[str, float],
    scorecard: dict[str, Any],
    clip_log: list[str],
) -> dict[str, Any]:
    compliance = _check_scorecard_compliance(post, base, scorecard)
    return {
        "applied": True,
        "target_weights_pre_scorecard_clip": pre,
        "target_weights_post_scorecard_clip": post,
        "violations": clip_log,
        "clip_log": clip_log,
        "post_clip_compliance": compliance,
    }


def _check_scorecard_compliance(
    weights: dict[str, float],
    base_weights: dict[str, float],
    scorecard: dict[str, Any] | None,
) -> dict[str, Any]:
    if not scorecard:
        return {"compliant": True, "violations": [], "checked": False}

    clean = _clean_weight_map(weights)
    base = _clean_weight_map(base_weights)
    violations: list[str] = []

    max_delta = _optional_float(scorecard.get("max_adjustment_from_base"))
    max_equity = _optional_float(scorecard.get("max_equity_weight"))
    min_cash = _optional_float(scorecard.get("min_cash_weight"))
    max_single = _optional_float(scorecard.get("max_single_position"))
    allow_new = bool(scorecard.get("allow_new_positions", True))
    permission = str(scorecard.get("investment_permission") or "")

    for ticker, weight in clean.items():
        if ticker == "CASH":
            continue
        base_w = float(base.get(ticker, 0.0) or 0.0)
        if max_delta is not None and abs(weight - base_w) > max_delta + 1e-6:
            violations.append(
                f"{ticker} delta {(weight - base_w):.2%} exceeds scorecard max {max_delta:.2%}"
            )
        if max_single is not None and weight > max_single + 1e-6:
            violations.append(
                f"{ticker} weight {weight:.2%} exceeds scorecard single cap {max_single:.2%}"
            )
        if not allow_new and base_w <= 0.01 and weight > 0.01:
            violations.append(f"{ticker} new position not allowed by scorecard")

    equity = _equity_sum(clean)
    cash = float(clean.get("CASH", 0.0) or 0.0)
    if max_equity is not None and equity > max_equity + 1e-6:
        violations.append(f"equity {equity:.2%} exceeds scorecard max {max_equity:.2%}")
    if min_cash is not None and cash < min_cash - 1e-6:
        violations.append(f"cash {cash:.2%} below scorecard floor {min_cash:.2%}")
    if permission == "cash_only" and equity > 1e-6:
        violations.append("cash_only permission forbids non-cash exposure")

    return {
        "compliant": not violations,
        "violations": violations,
        "checked": True,
    }


# ═══════════════════════════════════════════════════════════════
# Decision style hard constraints
# ═══════════════════════════════════════════════════════════════


def apply_style_constraints(
    *,
    target_weights: dict[str, float],
    base_weights: dict[str, float],
    current_weights: dict[str, float],
    decision_style: dict[str, Any] | None,
    market_scorecard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Enforce Decision Style limits after scorecard clipping.

    Style limits only tighten. Freed buy weight goes to CASH; sells are not
    re-expanded into equity.
    """
    style = decision_style or {}
    pre = _normalize_weights(target_weights)
    scorecard_turnover_limit = _effective_turnover_limit({}, market_scorecard)
    if not style and scorecard_turnover_limit is None:
        return {
            "applied": False,
            "target_weights_pre_style_clip": pre,
            "target_weights_post_style_clip": pre,
            "violations": [],
            "clip_log": [],
            "post_clip_compliance": _check_style_compliance(
                pre, base_weights, current_weights, style, market_scorecard
            ),
            "one_way_tightening_ok": True,
        }

    base = _clean_weight_map(base_weights)
    current = _clean_weight_map(current_weights)
    work = _clean_weight_map(pre)
    work.setdefault("CASH", 0.0)
    limits = style.get("style_limits") or {}
    clip_log: list[str] = []

    trade_style = str(style.get("trade_style") or "")
    if trade_style == "cash_only":
        for ticker in list(work):
            if ticker != "CASH" and work.get(ticker, 0.0) > 0:
                clip_log.append(f"style_cash_only:{ticker} {work[ticker]:.2%}->0.00%")
                work[ticker] = 0.0
        work["CASH"] = 1.0
        post = _cash_first_normalize(work)
        return _style_enforcement_result(pre, post, base, current, style, market_scorecard, clip_log)

    allow_new = bool(limits.get("allow_new_positions", True))
    max_new_buys = _optional_int(limits.get("max_new_buys_per_cycle"))
    max_single_trade = _optional_float(limits.get("max_single_trade_pct"))
    max_buy_trade = _optional_float(limits.get("max_buy_trade_pct"))
    max_turnover = _effective_turnover_limit(limits, market_scorecard)
    min_cash_add = _optional_float(limits.get("min_cash_floor_addition")) or 0.0
    max_multiplier = _optional_float(limits.get("max_adjustment_multiplier"))
    scorecard_delta = _optional_float((market_scorecard or {}).get("max_adjustment_from_base"))
    style_delta = scorecard_delta * max_multiplier if scorecard_delta is not None and max_multiplier is not None else None
    buy_cap = min(
        [v for v in (max_single_trade, max_buy_trade) if v is not None],
        default=None,
    )

    for ticker in list(work.keys()):
        if ticker == "CASH":
            continue
        target = float(work.get(ticker, 0.0) or 0.0)
        base_w = float(base.get(ticker, 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        delta_from_current = target - current_w

        if not allow_new and current_w <= 0.01 and base_w <= 0.01 and target > 0.01:
            replacement = max(current_w, base_w)
            work[ticker] = replacement
            work["CASH"] += max(target - replacement, 0.0)
            clip_log.append(f"style_new_position_blocked:{ticker} {target:.2%}->{replacement:.2%}")
            target = work[ticker]
            delta_from_current = target - current_w

        if buy_cap is not None and delta_from_current > buy_cap + 1e-9:
            clipped = current_w + buy_cap
            work[ticker] = clipped
            work["CASH"] += max(target - clipped, 0.0)
            clip_log.append(f"style_buy_cap:{ticker} {delta_from_current:.2%}->{buy_cap:.2%}")
            target = clipped

        if style_delta is not None:
            upper = base_w + style_delta
            lower = max(base_w - style_delta, 0.0)
            if target > upper:
                work[ticker] = upper
                work["CASH"] += target - upper
                clip_log.append(f"style_max_delta:{ticker} {target:.2%}->{upper:.2%}")
            elif target < lower:
                needed = lower - target
                available_cash = max(float(work.get("CASH", 0.0) or 0.0), 0.0)
                add_back = min(needed, available_cash)
                if add_back > 0:
                    work[ticker] = target + add_back
                    work["CASH"] = available_cash - add_back
                    clip_log.append(f"style_sell_delta:{ticker} {target:.2%}->{work[ticker]:.2%}")

    work = _cap_style_new_buys(work, current, max_new_buys, clip_log)
    work = _cap_style_turnover(work, current, max_turnover, clip_log)

    if min_cash_add > 0:
        scorecard_cash = _optional_float((market_scorecard or {}).get("min_cash_weight")) or 0.0
        cash_floor = min(1.0, scorecard_cash + min_cash_add)
        cash = float(work.get("CASH", 0.0) or 0.0)
        if cash < cash_floor - 1e-9:
            shortfall = cash_floor - cash
            equity = _equity_sum(work)
            if equity > 0:
                target_equity = max(equity - shortfall, 0.0)
                scale = target_equity / equity if equity > 0 else 0.0
                for ticker in list(work.keys()):
                    if ticker != "CASH":
                        work[ticker] *= scale
                work["CASH"] = cash_floor
                clip_log.append(f"style_min_cash:{cash:.2%}->{cash_floor:.2%}")

    post = _cash_first_normalize(work)
    return _style_enforcement_result(pre, post, base, current, style, market_scorecard, clip_log)


def _style_enforcement_result(
    pre: dict[str, float],
    post: dict[str, float],
    base: dict[str, float],
    current: dict[str, float],
    style: dict[str, Any],
    scorecard: dict[str, Any] | None,
    clip_log: list[str],
) -> dict[str, Any]:
    compliance = _check_style_compliance(post, base, current, style, scorecard)
    return {
        "applied": True,
        "target_weights_pre_style_clip": pre,
        "target_weights_post_style_clip": post,
        "violations": clip_log,
        "clip_log": clip_log,
        "post_clip_compliance": compliance,
        "one_way_tightening_ok": _one_way_tightening_ok(pre, post, scorecard),
    }


def _check_style_compliance(
    weights: dict[str, float],
    base_weights: dict[str, float],
    current_weights: dict[str, float],
    decision_style: dict[str, Any] | None,
    market_scorecard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scorecard_turnover_limit = _effective_turnover_limit({}, market_scorecard)
    if not decision_style and scorecard_turnover_limit is None:
        return {"compliant": True, "violations": [], "checked": False}

    clean = _clean_weight_map(weights)
    base = _clean_weight_map(base_weights)
    current = _clean_weight_map(current_weights)
    decision_style = decision_style or {}
    limits = decision_style.get("style_limits") or {}
    violations: list[str] = []

    trade_style = str(decision_style.get("trade_style") or "")
    if trade_style == "cash_only" and _equity_sum(clean) > 1e-6:
        violations.append("cash_only trade style forbids non-cash exposure")

    allow_new = bool(limits.get("allow_new_positions", True))
    max_new_buys = _optional_int(limits.get("max_new_buys_per_cycle"))
    max_single_trade = _optional_float(limits.get("max_single_trade_pct"))
    max_buy_trade = _optional_float(limits.get("max_buy_trade_pct"))
    max_turnover = _effective_turnover_limit(limits, market_scorecard)
    min_cash_add = _optional_float(limits.get("min_cash_floor_addition")) or 0.0
    max_multiplier = _optional_float(limits.get("max_adjustment_multiplier"))
    scorecard_delta = _optional_float((market_scorecard or {}).get("max_adjustment_from_base"))
    style_delta = scorecard_delta * max_multiplier if scorecard_delta is not None and max_multiplier is not None else None
    buy_cap = min(
        [v for v in (max_single_trade, max_buy_trade) if v is not None],
        default=None,
    )

    new_buys = 0
    for ticker, weight in clean.items():
        if ticker == "CASH":
            continue
        base_w = float(base.get(ticker, 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        delta_current = weight - current_w
        if current_w <= 0.01 and base_w <= 0.01 and weight > 0.01:
            new_buys += 1
            if not allow_new:
                violations.append(f"{ticker} new position blocked by style")
        if buy_cap is not None and delta_current > buy_cap + 1e-6:
            violations.append(f"{ticker} buy delta {delta_current:.2%} exceeds style cap {buy_cap:.2%}")
        if style_delta is not None and abs(weight - base_w) > style_delta + 1e-6:
            violations.append(f"{ticker} delta {(weight - base_w):.2%} exceeds style max {style_delta:.2%}")

    if max_new_buys is not None and new_buys > max_new_buys:
        violations.append(f"new buys {new_buys} exceeds style max {max_new_buys}")
    if max_turnover is not None:
        turnover = _turnover(clean, current)
        if turnover > max_turnover + 1e-6:
            violations.append(f"turnover {turnover:.2%} exceeds style max {max_turnover:.2%}")
    if min_cash_add > 0:
        cash_floor = min(1.0, (_optional_float((market_scorecard or {}).get("min_cash_weight")) or 0.0) + min_cash_add)
        cash = float(clean.get("CASH", 0.0) or 0.0)
        if cash < cash_floor - 1e-6:
            violations.append(f"cash {cash:.2%} below style floor {cash_floor:.2%}")

    return {
        "compliant": not violations,
        "violations": violations,
        "checked": True,
    }


def _effective_turnover_limit(
    limits: dict[str, Any] | None,
    market_scorecard: dict[str, Any] | None,
) -> float | None:
    style_turnover = _optional_float((limits or {}).get("max_turnover_per_cycle"))
    scorecard_turnover = _optional_float((market_scorecard or {}).get("max_turnover_per_cycle"))
    candidates = [v for v in (style_turnover, scorecard_turnover) if v is not None]
    return min(candidates) if candidates else None


def _cap_style_new_buys(
    weights: dict[str, float],
    current: dict[str, float],
    max_new_buys: int | None,
    clip_log: list[str],
) -> dict[str, float]:
    if max_new_buys is None:
        return weights
    deltas = {ticker: weights.get(ticker, 0.0) - current.get(ticker, 0.0) for ticker in set(weights) | set(current)}
    new_buys = [
        ticker for ticker, delta in deltas.items()
        if ticker != "CASH" and delta > 0.01 and current.get(ticker, 0.0) <= 0.01
    ]
    if len(new_buys) <= max_new_buys:
        return weights
    keep = set(sorted(new_buys, key=lambda ticker: deltas[ticker], reverse=True)[:max_new_buys])
    out = dict(weights)
    freed = 0.0
    blocked = []
    for ticker in new_buys:
        if ticker in keep:
            continue
        old = out.get(ticker, 0.0)
        replacement = current.get(ticker, 0.0)
        freed += max(old - replacement, 0.0)
        out[ticker] = replacement
        blocked.append(ticker)
    out["CASH"] = out.get("CASH", 0.0) + freed
    clip_log.append(f"style_new_buys_capped:{blocked}")
    return out


def _cap_style_turnover(
    weights: dict[str, float],
    current: dict[str, float],
    max_turnover: float | None,
    clip_log: list[str],
) -> dict[str, float]:
    if max_turnover is None:
        return weights
    turnover = _turnover(weights, current)
    if turnover <= max_turnover + 1e-9:
        return weights
    scale = max_turnover / turnover if turnover > 0 else 1.0
    out: dict[str, float] = {}
    for ticker in set(weights) | set(current):
        out[ticker] = current.get(ticker, 0.0) + (weights.get(ticker, 0.0) - current.get(ticker, 0.0)) * scale
    clip_log.append(f"style_turnover_scaled:{turnover:.2%}->{max_turnover:.2%}")
    return out


def _one_way_tightening_ok(
    pre: dict[str, float],
    post: dict[str, float],
    scorecard: dict[str, Any] | None,
) -> bool:
    scorecard = scorecard or {}
    max_equity = _optional_float(scorecard.get("max_equity_weight"))
    min_cash = _optional_float(scorecard.get("min_cash_weight"))
    if max_equity is not None and _equity_sum(post) > max_equity + 1e-6:
        return False
    if min_cash is not None and float(post.get("CASH", 0.0) or 0.0) < min_cash - 1e-6:
        return False
    return _equity_sum(post) <= _equity_sum(pre) + 1e-6 or not scorecard


# ═══════════════════════════════════════════════════════════════
# Six quantitative checks
# ═══════════════════════════════════════════════════════════════


def _run_checks(
    *,
    target_weights: dict[str, float],
    estimated_cost: float,
    holdings:       list[dict],
    portfolio:      dict,
    risk_params:    dict,
) -> tuple[dict[str, dict], list[str]]:
    max_hist_vol       = float(risk_params.get("max_hist_vol",       0.35))
    max_drawdown       = float(risk_params.get("max_drawdown",       0.15))
    max_single_pos     = float(risk_params.get("max_single_position", 0.20))
    max_broad_market   = float(risk_params.get("max_broad_market",    0.40))
    min_cash_pct       = float(risk_params.get("min_cash_pct",        0.05))
    max_trade_cost_pct = float(risk_params.get("max_trade_cost_pct",  0.005))

    drawdown_pct = abs(float(portfolio.get("current_drawdown_pct") or 0))

    vol_by_ticker: dict[str, float] = {}
    for h in holdings:
        t = (h.get("ticker") or "").upper().strip()
        if not t:
            continue
        v = h.get("hist_vol_20d")
        if v is not None:
            try:
                vol_by_ticker[t] = float(v)
            except (TypeError, ValueError):
                pass

    checks: dict[str, dict] = {}
    reasons: list[str] = []

    # 1. vol_ok — position-weighted hist_vol_20d
    weighted_vol = 0.0
    covered = 0.0
    for ticker, w in target_weights.items():
        if ticker == "CASH":
            continue
        if ticker in vol_by_ticker:
            weighted_vol += float(w) * vol_by_ticker[ticker]
            covered += float(w)
    vol_value = (weighted_vol / covered) if covered > 0 else 0.0
    vol_ok = vol_value < max_hist_vol
    checks["vol_ok"] = {
        "pass":      vol_ok,
        "actual":    round(vol_value, 4),
        "threshold": max_hist_vol,
    }
    if not vol_ok:
        reasons.append(
            f"Position-weighted hist vol {vol_value:.2%} exceeds cap {max_hist_vol:.2%}"
        )

    # 2. drawdown_ok
    drawdown_ok = drawdown_pct < max_drawdown
    checks["drawdown_ok"] = {
        "pass":      drawdown_ok,
        "actual":    round(drawdown_pct, 4),
        "threshold": max_drawdown,
    }
    if not drawdown_ok:
        reasons.append(
            f"Current drawdown {drawdown_pct:.2%} at or above cap {max_drawdown:.2%}"
        )

    # 3. position_ok — max non-CASH single name
    max_weight = 0.0
    max_weight_ticker = None
    for ticker, w in target_weights.items():
        if ticker == "CASH":
            continue
        wf = float(w)
        if wf > max_weight:
            max_weight = wf
            max_weight_ticker = ticker
    position_ok = max_weight <= max_single_pos + 1e-6
    checks["position_ok"] = {
        "pass":      position_ok,
        "actual":    round(max_weight, 4),
        "threshold": max_single_pos,
        "ticker":    max_weight_ticker,
    }
    if not position_ok:
        reasons.append(
            f"{max_weight_ticker} position {max_weight:.2%} exceeds cap {max_single_pos:.2%}"
        )

    # 4. broad_market_ok
    broad_sum = sum(
        float(target_weights.get(t, 0) or 0) for t in BROAD_MARKET_TICKERS
    )
    broad_market_ok = broad_sum <= max_broad_market + 1e-6
    checks["broad_market_ok"] = {
        "pass":      broad_market_ok,
        "actual":    round(broad_sum, 4),
        "threshold": max_broad_market,
    }
    if not broad_market_ok:
        reasons.append(
            f"Broad ETFs (SPY+QQQ+IWM) {broad_sum:.2%} exceeds cap {max_broad_market:.2%}"
        )

    # 5. cash_ok
    cash_weight = float(target_weights.get("CASH", 0) or 0)
    cash_ok = cash_weight >= min_cash_pct - 1e-6
    checks["cash_ok"] = {
        "pass":      cash_ok,
        "actual":    round(cash_weight, 4),
        "threshold": min_cash_pct,
    }
    if not cash_ok:
        reasons.append(
            f"Cash {cash_weight:.2%} below floor {min_cash_pct:.2%}"
        )

    # 6. cost_ok
    cost_ok = estimated_cost <= max_trade_cost_pct + 1e-9
    checks["cost_ok"] = {
        "pass":      cost_ok,
        "actual":    round(estimated_cost, 6),
        "threshold": max_trade_cost_pct,
    }
    if not cost_ok:
        reasons.append(
            f"Estimated cost {estimated_cost:.4%} exceeds cap {max_trade_cost_pct:.4%}"
        )

    return checks, reasons


# ─────────────────────────────── Helpers ───────────────────────────────


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    """Renormalize after overlays so sum = 1.0 and weights are non-negative."""
    cleaned = _clean_weight_map(weights)

    total = sum(cleaned.values())
    if total <= 0:
        return {"CASH": 1.0}

    scaled = {t: w / total for t, w in cleaned.items()}

    out = {t: round(w, 4) for t, w in scaled.items() if t != "CASH"}
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
    return out


def _cash_first_normalize(weights: dict[str, float]) -> dict[str, float]:
    """
    Normalize without re-expanding equity. Non-cash weights keep their clipped
    absolute levels; CASH absorbs the remainder.
    """
    cleaned = _clean_weight_map(weights)
    equity = _equity_sum(cleaned)
    if equity >= 1.0:
        scale = 1.0 / equity if equity > 0 else 0.0
        out = {
            ticker: round(weight * scale, 4)
            for ticker, weight in cleaned.items()
            if ticker != "CASH" and weight > 0
        }
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
        return out

    out = {
        ticker: round(weight, 4)
        for ticker, weight in cleaned.items()
        if ticker != "CASH" and weight > 0
    }
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
    return out


def _clean_weight_map(weights: dict[str, Any] | None) -> dict[str, float]:
    cleaned: dict[str, float] = {}
    for t, w in (weights or {}).items():
        ticker = str(t or "").upper().strip()
        if not ticker:
            continue
        try:
            wf = float(w)
        except (TypeError, ValueError):
            wf = 0.0
        cleaned[ticker] = max(wf, 0.0)
    return cleaned


def _equity_sum(weights: dict[str, float]) -> float:
    return sum(float(w or 0.0) for t, w in weights.items() if t != "CASH")


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _turnover(target: dict[str, float], current: dict[str, float]) -> float:
    keys = set(target) | set(current)
    return sum(
        abs(float(target.get(t, 0.0) or 0.0) - float(current.get(t, 0.0) or 0.0))
        for t in keys
    ) / 2.0


def _count_non_cash(weights: dict[str, float]) -> int:
    return sum(1 for t, w in weights.items() if t != "CASH" and w > 0)
