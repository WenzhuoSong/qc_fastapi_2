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

    target_weights = _normalize_weights(working)

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
    cleaned: dict[str, float] = {}
    for t, w in weights.items():
        try:
            wf = float(w)
        except (TypeError, ValueError):
            wf = 0.0
        if wf < 0:
            wf = 0.0
        cleaned[t] = wf

    total = sum(cleaned.values())
    if total <= 0:
        return {"CASH": 1.0}

    scaled = {t: w / total for t, w in cleaned.items()}

    out = {t: round(w, 4) for t, w in scaled.items() if t != "CASH"}
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
    return out


def _count_non_cash(weights: dict[str, float]) -> int:
    return sum(1 for t, w in weights.items() if t != "CASH" and w > 0)
