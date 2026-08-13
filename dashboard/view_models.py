"""Pure view-model builders for the read-only operator dashboard."""
from __future__ import annotations

from typing import Any, Iterable


def number(value: Any) -> float:
    return float(value or 0)


def daily_account_series(snapshots: Iterable[Any]) -> list[dict[str, Any]]:
    """Keep the final reconciled account observation for each UTC trading date."""
    by_day: dict[str, Any] = {}
    for row in sorted(snapshots, key=lambda item: item.event_time):
        if not row.broker_reconciled:
            continue
        key = row.event_time.date().isoformat()
        by_day[key] = row
    result = []
    for day, row in sorted(by_day.items()):
        equity = number(row.equity)
        cash = number(row.cash)
        result.append({
            "date": day,
            "as_of": row.event_time.isoformat(),
            "equity": equity,
            "cash": cash,
            "cash_pct": cash / equity if equity else None,
        })
    return result


def momentum_view(
    latest_rankings: Iterable[Any],
    prior_rankings: Iterable[Any],
    holding_tickers: Iterable[str],
    *,
    boundary_gap: Any | None,
) -> dict[str, Any]:
    latest = {str(row.ticker): row for row in latest_rankings if row.ticker}
    prior = {str(row.ticker): row for row in prior_rankings if row.ticker}
    holdings = []
    improving = 0
    weakening = 0
    for ticker in holding_tickers:
        current = latest.get(ticker)
        previous = prior.get(ticker)
        if current is None:
            holdings.append({"ticker": ticker, "rank": None, "score": None, "rank_change": None, "score_change": None})
            continue
        score = number(current.score)
        previous_score = number(previous.score) if previous is not None else None
        score_change = score - previous_score if previous_score is not None else None
        rank_change = int(previous.rank) - int(current.rank) if previous is not None else None
        if score_change is not None and score_change > 0:
            improving += 1
        elif score_change is not None and score_change < 0:
            weakening += 1
        holdings.append({
            "ticker": ticker,
            "rank": int(current.rank),
            "score": score,
            "rank_change": rank_change,
            "score_change": score_change,
        })
    holdings.sort(key=lambda item: item["rank"] if item["rank"] is not None else 10**9)
    scores = [number(row.score) for row in latest.values() if row.score is not None]
    positive = sum(1 for score in scores if score > 0)
    ranked_holdings = [item for item in holdings if item["rank"] is not None]
    top_eight = sum(1 for item in ranked_holdings if item["rank"] <= 8)
    return {
        "as_of": None,
        "holdings": holdings,
        "universe_count": len(scores),
        "positive_score_count": positive,
        "positive_score_pct": positive / len(scores) if scores else None,
        "held_ranked_count": len(ranked_holdings),
        "held_top8_count": top_eight,
        "held_improving_count": improving,
        "held_weakening_count": weakening,
        "rank_boundary_gap_3_4": number(boundary_gap) if boundary_gap is not None else None,
        "interpretation": (
            f"当前 {len(ranked_holdings)} 只可排名持仓中，{top_eight} 只位于策略前 8；"
            f"较上周分数上升 {improving} 只、下降 {weakening} 只。"
        ),
    }


def weekly_rebalance_logs(
    decisions: list[Any],
    rankings_by_manifest: dict[int, list[Any]],
    audits_by_manifest: dict[int, Any],
) -> list[dict[str, Any]]:
    logs = []
    ordered = sorted(decisions, key=lambda row: row.event_time)
    prior_targets: dict[str, Any] = {}
    for decision in ordered:
        targets = dict(decision.final_target_weights or {})
        raw = dict(decision.raw_target_weights or {})
        risk = dict(decision.risk_adjusted_weights or {})
        suppressed = dict(decision.suppressed_deltas or {})
        rankings = sorted(rankings_by_manifest.get(decision.id, []), key=lambda row: row.rank)
        audit = audits_by_manifest.get(decision.id)
        opened = sorted(ticker for ticker, weight in targets.items() if number(weight) and not number(prior_targets.get(ticker)))
        removed = sorted(ticker for ticker, weight in prior_targets.items() if number(weight) and not number(targets.get(ticker)))
        changed = sorted(
            ticker for ticker in set(targets) | set(prior_targets)
            if abs(number(targets.get(ticker)) - number(prior_targets.get(ticker))) >= 0.000001
        )
        leaders = [
            {"ticker": row.ticker, "rank": int(row.rank), "score": number(row.score)}
            for row in rankings[:4]
        ]
        reasons = [
            "排名层：" + "、".join(f"{item['ticker']} #{item['rank']} ({item['score']:.3f})" for item in leaders),
            f"风险层：raw {len(raw)} 只，经 3ATR 风险预算形成 {len(risk)} 只，最终目标敞口 {sum(number(value) for value in targets.values()):.2%}。",
        ]
        if suppressed:
            reasons.append(f"交易策略：1% no-trade band 保留 {len(suppressed)} 项小幅权重变化，避免无效换手。")
        else:
            reasons.append("交易策略：本周没有权重变化被 1% no-trade band 抑制。")
        if audit is not None:
            ratio = number(audit.aggregate_fill_ratio)
            reasons.append(
                f"执行结果：{audit.terminal_intent_count}/{audit.intent_count} 项订单终态，"
                f"成交率 {ratio:.1%}，费用 ${number(audit.total_fees):,.2f}。"
            )
        logs.append({
            "decision_id": decision.decision_id,
            "as_of": decision.event_time.isoformat(),
            "session": (decision.payload_extra or {}).get("exchange_session"),
            "target_exposure": sum(number(value) for value in targets.values()),
            "leaders": leaders,
            "opened": opened,
            "removed": removed,
            "changed": changed,
            "suppressed": suppressed,
            "reasons": reasons,
            "audit_status": audit.status if audit is not None else "pending",
        })
        prior_targets = targets
    return list(reversed(logs))
