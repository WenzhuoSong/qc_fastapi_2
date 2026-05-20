"""
Walk-forward validation for deterministic strategy evidence.

This module audits already-computed forward returns across chronological folds.
It does not train strategies, select weights, or grant execution authority.
"""
from __future__ import annotations

import math
from statistics import median
from typing import Any


MIN_WALK_FORWARD_FOLDS = 3
MIN_WALK_FORWARD_SAMPLES_PER_FOLD = 8


def validate_walk_forward(
    strategy_returns_by_fold: dict[str, list[list[float]]],
    *,
    min_folds: int = MIN_WALK_FORWARD_FOLDS,
    min_samples_per_fold: int = MIN_WALK_FORWARD_SAMPLES_PER_FOLD,
) -> dict[str, Any]:
    items: dict[str, dict[str, Any]] = {}
    for name, folds in sorted((strategy_returns_by_fold or {}).items()):
        rows = [
            _fold_metrics(index=idx, returns=returns, min_samples_per_fold=min_samples_per_fold)
            for idx, returns in enumerate(folds)
        ]
        valid_rows = [row for row in rows if row["sample_count"] >= min_samples_per_fold]
        passed_rows = [row for row in valid_rows if row["passed"]]
        sharpes = [float(row["sharpe"]) for row in valid_rows if row.get("sharpe") is not None]
        pass_rate = len(passed_rows) / len(valid_rows) if valid_rows else 0.0
        positive_sharpe_rate = (
            sum(1 for value in sharpes if value > 0) / len(sharpes)
            if sharpes else 0.0
        )
        worst_sharpe = min(sharpes) if sharpes else None
        median_sharpe = median(sharpes) if sharpes else None
        stability_score = _stability_score(
            valid_fold_count=len(valid_rows),
            min_folds=min_folds,
            pass_rate=pass_rate,
            positive_sharpe_rate=positive_sharpe_rate,
            worst_sharpe=worst_sharpe,
        )
        level = _validation_level(
            valid_fold_count=len(valid_rows),
            min_folds=min_folds,
            pass_rate=pass_rate,
            positive_sharpe_rate=positive_sharpe_rate,
            worst_sharpe=worst_sharpe,
        )
        items[name] = {
            "strategy_name": name,
            "level": level,
            "fold_count": len(rows),
            "valid_fold_count": len(valid_rows),
            "min_folds": int(min_folds),
            "min_samples_per_fold": int(min_samples_per_fold),
            "pass_rate": round(pass_rate, 4),
            "positive_sharpe_rate": round(positive_sharpe_rate, 4),
            "median_sharpe": round(median_sharpe, 4) if median_sharpe is not None else None,
            "worst_sharpe": round(worst_sharpe, 4) if worst_sharpe is not None else None,
            "stability_score": round(stability_score, 4),
            "folds": rows,
            "reason_codes": _reason_codes(
                level=level,
                valid_fold_count=len(valid_rows),
                min_folds=min_folds,
                pass_rate=pass_rate,
                worst_sharpe=worst_sharpe,
            ),
            "execution_authority": "none",
        }

    summary = _summary(items)
    return {
        "items": items,
        "summary": summary,
        "policy": {
            "min_folds": int(min_folds),
            "min_samples_per_fold": int(min_samples_per_fold),
            "pass_rule": "fold passes when sharpe > 0 and hit_rate >= 0.50",
            "execution_authority": "none",
        },
        "execution_authority": "none",
    }


def _fold_metrics(
    *,
    index: int,
    returns: list[float],
    min_samples_per_fold: int,
) -> dict[str, Any]:
    clean = [float(value) for value in returns if value is not None]
    sample_count = len(clean)
    sharpe = _annualized_sharpe(clean) if sample_count >= min_samples_per_fold else None
    hit_rate = (
        sum(1 for value in clean if value > 0) / sample_count
        if sample_count else None
    )
    passed = bool(
        sample_count >= min_samples_per_fold
        and sharpe is not None
        and sharpe > 0
        and hit_rate is not None
        and hit_rate >= 0.50
    )
    return {
        "fold_index": int(index),
        "sample_count": sample_count,
        "mean_return": round(sum(clean) / sample_count, 6) if sample_count else None,
        "total_return": round(sum(clean), 6) if sample_count else None,
        "sharpe": round(sharpe, 4) if sharpe is not None else None,
        "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
        "passed": passed,
    }


def _annualized_sharpe(returns: list[float]) -> float | None:
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    stdev = math.sqrt(variance)
    if stdev <= 0:
        return None
    return (mean / stdev) * math.sqrt(252)


def _stability_score(
    *,
    valid_fold_count: int,
    min_folds: int,
    pass_rate: float,
    positive_sharpe_rate: float,
    worst_sharpe: float | None,
) -> float:
    fold_score = min(1.0, valid_fold_count / max(1, min_folds))
    drawdown_score = 0.0 if worst_sharpe is None else max(0.0, min(1.0, (worst_sharpe + 1.0) / 2.0))
    return max(0.0, min(1.0, 0.25 * fold_score + 0.45 * pass_rate + 0.20 * positive_sharpe_rate + 0.10 * drawdown_score))


def _validation_level(
    *,
    valid_fold_count: int,
    min_folds: int,
    pass_rate: float,
    positive_sharpe_rate: float,
    worst_sharpe: float | None,
) -> str:
    if valid_fold_count < min_folds:
        return "insufficient"
    if pass_rate >= 0.75 and positive_sharpe_rate >= 0.75 and (worst_sharpe is None or worst_sharpe > -0.50):
        return "high"
    if pass_rate >= 0.50 and positive_sharpe_rate >= 0.50:
        return "medium"
    return "weak"


def _reason_codes(
    *,
    level: str,
    valid_fold_count: int,
    min_folds: int,
    pass_rate: float,
    worst_sharpe: float | None,
) -> list[str]:
    codes = [f"walk_forward_{level}"]
    if valid_fold_count < min_folds:
        codes.append("walk_forward_folds_insufficient")
    if pass_rate < 0.50:
        codes.append("walk_forward_pass_rate_low")
    if worst_sharpe is not None and worst_sharpe < 0:
        codes.append("walk_forward_has_negative_fold")
    return codes


def _summary(items: dict[str, dict[str, Any]]) -> dict[str, Any]:
    counts = {"high": 0, "medium": 0, "weak": 0, "insufficient": 0}
    for row in items.values():
        level = str(row.get("level") or "insufficient")
        counts[level] = counts.get(level, 0) + 1
    stable = [
        name for name, row in items.items()
        if row.get("level") in {"high", "medium"}
    ]
    return {
        "counts": counts,
        "stable_strategy_count": len(stable),
        "stable_strategies": stable,
    }
