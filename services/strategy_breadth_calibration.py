"""Strategy breadth calibration report.

This module standardizes existing strategy-independence diagnostics into a
basket-calibration friendly contract. It is diagnostics-only and must not be
used as trade authority.
"""
from __future__ import annotations

from typing import Any


REPORT_VERSION = "strategy_breadth_calibration_v1"
DEFAULT_DUPLICATE_CORRELATION_THRESHOLD = 0.70
DEFAULT_DIVERSIFYING_CORRELATION_THRESHOLD = 0.20


def build_strategy_breadth_calibration_report(
    independence_summary: dict[str, Any] | None,
    *,
    duplicate_correlation_threshold: float = DEFAULT_DUPLICATE_CORRELATION_THRESHOLD,
    diversifying_correlation_threshold: float = DEFAULT_DIVERSIFYING_CORRELATION_THRESHOLD,
) -> dict[str, Any]:
    """Convert strategy-independence diagnostics into a breadth report."""
    raw = independence_summary if isinstance(independence_summary, dict) else {}
    strategy_rows = [
        row for row in (raw.get("strategy_rows") or [])
        if isinstance(row, dict)
    ]
    pair_rows = [
        row for row in (raw.get("pair_rows") or [])
        if isinstance(row, dict)
    ]
    minimum_overlap = int(raw.get("min_overlap") or raw.get("minimum_overlap") or 0)
    eligible_alpha_names = sorted({
        str(row.get("strategy_name") or "").strip()
        for row in strategy_rows
        if row.get("alpha_source")
        and str(row.get("strategy_name") or "").strip()
        and _to_int(row.get("sample_count"), 0) >= max(minimum_overlap, 2)
    })
    valid_pairs = [
        row for row in pair_rows
        if _to_float(row.get("correlation")) is not None
    ]
    insufficient_overlap_pairs = len([
        row for row in pair_rows
        if row.get("status") == "insufficient_overlap" or _to_float(row.get("correlation")) is None
    ])
    duplicate_pairs = [
        _pair_out(row)
        for row in valid_pairs
        if _to_float(row.get("correlation"), 0.0) >= duplicate_correlation_threshold
    ]
    diversifying_pairs = [
        _pair_out(row)
        for row in valid_pairs
        if _to_float(row.get("correlation"), 1.0) <= diversifying_correlation_threshold
    ]
    duplicate_pairs.sort(key=lambda row: (-abs(float(row["corr"])), row["a"], row["b"]))
    diversifying_pairs.sort(key=lambda row: (float(row["corr"]), row["a"], row["b"]))
    estimated_clusters = _estimated_duplicate_clusters(
        alpha_names=eligible_alpha_names,
        duplicate_pairs=duplicate_pairs,
    )
    total_alpha = len(eligible_alpha_names)
    duplication_ratio = (
        round(1.0 - estimated_clusters / total_alpha, 4)
        if total_alpha > 0
        else None
    )
    warnings = []
    if insufficient_overlap_pairs:
        warnings.append(f"insufficient_overlap_pairs:{insufficient_overlap_pairs}:minimum_overlap={minimum_overlap}")
    if duplicate_pairs:
        warnings.append(f"duplicate_alpha_pairs:{len(duplicate_pairs)}")
    if total_alpha > 0 and estimated_clusters < total_alpha:
        warnings.append(f"estimated_breadth_below_alpha_count:{estimated_clusters}/{total_alpha}")

    return {
        "report_version": REPORT_VERSION,
        "source_contract_version": raw.get("contract_version"),
        "status": "available" if valid_pairs else "insufficient_overlap",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "trade_authority": "none",
        "total_strategies": int(raw.get("strategy_count") or len(strategy_rows)),
        "alpha_strategy_count": int(raw.get("alpha_strategy_count") or total_alpha),
        "eligible_alpha_strategy_count": total_alpha,
        "estimated_independent_clusters": estimated_clusters,
        "estimated_breadth_is_approximation": True,
        "cluster_method": "connected_components_of_alpha_pairs_corr_gte_duplicate_threshold",
        "duplication_ratio": duplication_ratio,
        "high_correlation_pairs": duplicate_pairs,
        "diversifying_pairs": diversifying_pairs,
        "minimum_overlap": minimum_overlap,
        "insufficient_overlap_pairs": insufficient_overlap_pairs,
        "duplicate_correlation_threshold": duplicate_correlation_threshold,
        "diversifying_correlation_threshold": diversifying_correlation_threshold,
        "warnings": warnings,
        "active_basket_input": {
            "estimated_independent_clusters": estimated_clusters,
            "eligible_alpha_strategy_count": total_alpha,
            "use_as_execution_authority": False,
        },
    }


async def load_strategy_breadth_calibration_report(
    db: Any,
    *,
    lookback_days: int = 420,
    source: str = "yfinance",
    strategy_names: list[str] | None = None,
    min_overlap: int | None = None,
) -> dict[str, Any]:
    """Load existing independence diagnostics and standardize them."""
    from services.strategy_independence import DEFAULT_MIN_OVERLAP, load_strategy_independence_diagnostics

    diagnostics = await load_strategy_independence_diagnostics(
        db,
        lookback_days=lookback_days,
        source=source,
        strategy_names=strategy_names,
        min_overlap=DEFAULT_MIN_OVERLAP if min_overlap is None else int(min_overlap),
    )
    return build_strategy_breadth_calibration_report(diagnostics)


def _estimated_duplicate_clusters(*, alpha_names: list[str], duplicate_pairs: list[dict[str, Any]]) -> int:
    if not alpha_names:
        return 0
    parent = {name: name for name in alpha_names}

    def find(name: str) -> str:
        while parent[name] != name:
            parent[name] = parent[parent[name]]
            name = parent[name]
        return name

    def union(left: str, right: str) -> None:
        if left not in parent or right not in parent:
            return
        root_left = find(left)
        root_right = find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    for row in duplicate_pairs:
        union(str(row.get("a") or ""), str(row.get("b") or ""))
    return len({find(name) for name in alpha_names})


def _pair_out(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "a": row.get("left"),
        "b": row.get("right"),
        "corr": _to_float(row.get("correlation")),
        "abs_corr": _to_float(row.get("abs_correlation")),
        "overlap": _to_int(row.get("overlap"), 0),
        "a_family": row.get("left_family"),
        "b_family": row.get("right_family"),
        "same_family": row.get("same_family"),
    }


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if number != number:
        return default
    return number
