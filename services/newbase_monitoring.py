"""Observer-only monitoring support for QC/newBase.

FastAPI/Railway observes, records, audits, and reports.
QC/newBase decides and trades.
Monitoring has eyes, not hands.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, date, datetime
from typing import Any, Iterable


NEWBASE_STRATEGY_ID = "newbase"
NEWBASE_LIVE_SNAPSHOT_SCHEMA_VERSION = "newbase_live_snapshot_v1"
NEWBASE_OPERATOR_SNAPSHOT_SCHEMA_VERSION = "newbase_operator_snapshot_v1"
EXECUTION_AUTHORITY = "none"
TARGET_WEIGHT_MUTATION = "none"
PRIMARY_BENCHMARK = "QQQ"
SECONDARY_BENCHMARK = "SPY"

ARCHITECTURE_INVARIANTS = {
    "fastapi_role": "observes_records_audits_reports",
    "qc_role": "decides_and_trades",
    "monitoring_has_hands": False,
    "execution_authority": EXECUTION_AUTHORITY,
    "target_weight_mutation": TARGET_WEIGHT_MUTATION,
}


def build_newbase_registry_record() -> dict[str, Any]:
    """Return the minimal registry row for newBase.

    The registry is descriptive. It does not authorize trading or target
    mutation. QQQ is the primary mirror because newBase is a growth/technology
    momentum strategy; SPY is retained only as secondary market context.
    """
    return {
        "strategy_id": NEWBASE_STRATEGY_ID,
        "source": "QuantConnect",
        "display_name": "newBase",
        "benchmark_primary": PRIMARY_BENCHMARK,
        "benchmark_secondary": SECONDARY_BENCHMARK,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "review_only": True,
        "notes": (
            "Registry is descriptive only. FastAPI/Railway observes, records, "
            "audits, and reports; QC/newBase decides and trades."
        ),
        "expected_profile": {
            "schema_version": "newbase_expected_profile_v1",
            "comparison_basis": "QQQ-relative primary; SPY secondary reference",
            "profile_source": "QC backtest overviews supplied by operator",
            "review_only": True,
            "execution_authority": EXECUTION_AUTHORITY,
            "target_weight_mutation": TARGET_WEIGHT_MUTATION,
            "absolute_backtest_profile": {
                "full_2010_2026": {
                    "cagr": 0.21781,
                    "drawdown": 0.307,
                    "sharpe": 0.824,
                    "sortino": 0.915,
                    "alpha": 0.088,
                    "beta": 0.663,
                    "information_ratio": 0.391,
                    "turnover": 0.0383,
                    "orders": 1980,
                    "fees": 13440.86,
                },
                "recent_2023_2026": {
                    "cagr": 0.21701,
                    "drawdown": 0.266,
                    "sharpe": 0.547,
                    "sortino": 0.617,
                    "alpha": 0.041,
                    "beta": 0.916,
                    "information_ratio": 0.182,
                    "turnover": 0.0402,
                    "orders": 483,
                    "fees": 533.17,
                },
            },
            "benchmark_relative_profile": {
                "primary_benchmark": PRIMARY_BENCHMARK,
                "recent_2023_2026_note": (
                    "Operator-side audit indicates recent newBase CAGR lagged QQQ; "
                    "health reports must lead with live excess vs QQQ."
                ),
                "relative_metrics_to_fill_from_live": [
                    "cumulative_excess_vs_qqq",
                    "rolling_excess_vs_qqq",
                    "rolling_beta_vs_qqq",
                    "drawdown_vs_qqq_context",
                ],
            },
            "monitoring_thresholds": {
                "rolling_beta_review_drift": 0.25,
                "drawdown_review_threshold": 0.30,
                "turnover_review_multiple": 2.0,
            },
            "operator_contract": {
                "red_flags_are_review_only": True,
                "automatic_trade_response": "forbidden",
            },
        },
    }


def build_strategy_live_snapshot_record(
    payload: dict[str, Any],
    *,
    qc_snapshot_id: int | None = None,
    received_at: datetime | None = None,
) -> dict[str, Any]:
    """Normalize a QC newBase live snapshot into DB row kwargs."""
    payload = payload or {}
    strategy = _dict(payload.get("strategy"))
    portfolio = _dict(payload.get("portfolio"))
    metrics = _dict(payload.get("metrics"))
    benchmarks = _dict(payload.get("benchmarks"))

    strategy_id = str(
        strategy.get("strategy_id")
        or payload.get("strategy_id")
        or NEWBASE_STRATEGY_ID
    ).strip().lower()
    recorded_at = _parse_datetime(
        payload.get("recorded_at")
        or payload.get("timestamp_utc")
        or payload.get("as_of")
        or (received_at.isoformat() if received_at else None)
    ) or _utcnow_naive()
    trading_date = _parse_date(payload.get("trading_date")) or recorded_at.date()
    primary = str(payload.get("benchmark_primary") or PRIMARY_BENCHMARK).upper()
    secondary = str(payload.get("benchmark_secondary") or SECONDARY_BENCHMARK).upper()
    primary_payload = _dict(benchmarks.get(primary) or benchmarks.get(primary.lower()))
    secondary_payload = _dict(benchmarks.get(secondary) or benchmarks.get(secondary.lower()))

    normalized_payload = _json_safe(payload)
    content_hash = _content_hash(normalized_payload)
    snapshot_uid = str(
        payload.get("snapshot_uid")
        or payload.get("export_id")
        or payload.get("id")
        or f"{strategy_id}:{content_hash}"
    ).strip()

    return {
        "snapshot_uid": snapshot_uid,
        "strategy_id": strategy_id,
        "qc_snapshot_id": qc_snapshot_id,
        "recorded_at": recorded_at,
        "trading_date": trading_date,
        "source": str(payload.get("source") or "quantconnect"),
        "mode": _clean_str(strategy.get("mode") or payload.get("mode")),
        "algorithm_version": _clean_str(strategy.get("algorithm_version") or strategy.get("version")),
        "total_value": _num(portfolio.get("total_value") or portfolio.get("equity")),
        "cash": _num(portfolio.get("cash")),
        "cash_pct": _num(portfolio.get("cash_pct") or portfolio.get("cash_weight")),
        "daily_return": _return_num(portfolio.get("daily_return") or portfolio.get("daily_return_pct")),
        "cumulative_return": _return_num(
            portfolio.get("cumulative_return") or portfolio.get("total_return")
        ),
        "current_drawdown": _return_num(
            portfolio.get("current_drawdown") or portfolio.get("drawdown")
        ),
        "turnover": _return_num(portfolio.get("turnover") or metrics.get("turnover")),
        "fees": _num(portfolio.get("fees") or metrics.get("fees")),
        "benchmark_primary": primary,
        "benchmark_primary_return": _return_num(
            primary_payload.get("daily_return") or primary_payload.get("return_1d")
        ),
        "benchmark_primary_cumulative_return": _return_num(
            primary_payload.get("cumulative_return") or primary_payload.get("total_return")
        ),
        "benchmark_secondary": secondary,
        "benchmark_secondary_return": _return_num(
            secondary_payload.get("daily_return") or secondary_payload.get("return_1d")
        ),
        "benchmark_secondary_cumulative_return": _return_num(
            secondary_payload.get("cumulative_return") or secondary_payload.get("total_return")
        ),
        "rolling_beta_primary": _num(
            metrics.get("rolling_beta_vs_qqq")
            or metrics.get("rolling_beta_primary")
            or metrics.get("beta_vs_qqq")
        ),
        "rolling_excess_primary": _return_num(
            metrics.get("rolling_excess_vs_qqq")
            or metrics.get("rolling_excess_primary")
        ),
        "holdings": _rows(payload.get("holdings") or portfolio.get("holdings")),
        "orders": _rows(payload.get("orders")),
        "fills": _rows(payload.get("fills")),
        "diagnostics": {
            "schema_version": payload.get("schema_version"),
            "packet_type": payload.get("packet_type"),
            "architecture_invariants": ARCHITECTURE_INVARIANTS,
            "raw_metrics": _json_safe(metrics),
        },
        "raw_payload": normalized_payload,
        "content_hash": content_hash,
    }


def build_newbase_operator_snapshot(
    rows: Iterable[Any],
    *,
    registry: dict[str, Any] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Build a review-only operator snapshot from live snapshot rows."""
    clean_rows = [_snapshot_row_to_dict(row) for row in rows]
    clean_rows = [row for row in clean_rows if row.get("strategy_id") == NEWBASE_STRATEGY_ID]
    clean_rows.sort(key=lambda row: (row.get("recorded_at") or datetime.min, row.get("snapshot_uid") or ""))
    expected = _dict((registry or {}).get("expected_profile")) or build_newbase_registry_record()["expected_profile"]
    base = {
        "schema_version": NEWBASE_OPERATOR_SNAPSHOT_SCHEMA_VERSION,
        "strategy_id": NEWBASE_STRATEGY_ID,
        "generated_at": (as_of or _utcnow_naive()).isoformat(),
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "review_only": True,
        "architecture_invariants": ARCHITECTURE_INVARIANTS,
        "benchmark_primary": PRIMARY_BENCHMARK,
        "benchmark_secondary": SECONDARY_BENCHMARK,
        "sample_count": len(clean_rows),
    }
    if not clean_rows:
        return {
            **base,
            "status": "insufficient_data",
            "headline": {
                "live_newbase_vs_qqq_cumulative_excess": None,
                "reason": "no_newbase_live_snapshots",
            },
            "review_flags": [],
            "operator_action": "collect_more_live_snapshots",
        }

    latest = clean_rows[-1]
    returns = [_num(row.get("daily_return")) for row in clean_rows]
    qqq_returns = [_num(row.get("benchmark_primary_return")) for row in clean_rows]
    spy_returns = [_num(row.get("benchmark_secondary_return")) for row in clean_rows]
    joined_primary = [
        (portfolio, primary)
        for portfolio, primary in zip(returns, qqq_returns)
        if portfolio is not None and primary is not None
    ]
    joined_secondary = [
        (portfolio, secondary)
        for portfolio, secondary in zip(returns, spy_returns)
        if portfolio is not None and secondary is not None
    ]

    strategy_cum = _latest_or_compound(latest.get("cumulative_return"), [p for p, _ in joined_primary] or returns)
    qqq_cum = _latest_or_compound(latest.get("benchmark_primary_cumulative_return"), [b for _, b in joined_primary])
    spy_cum = _latest_or_compound(latest.get("benchmark_secondary_cumulative_return"), [b for _, b in joined_secondary])
    excess_vs_qqq = _sub_or_none(strategy_cum, qqq_cum)
    excess_vs_spy = _sub_or_none(strategy_cum, spy_cum)
    beta_vs_qqq = _num(latest.get("rolling_beta_primary"))
    if beta_vs_qqq is None and len(joined_primary) >= 3:
        beta_vs_qqq = _beta([b for _, b in joined_primary], [p for p, _ in joined_primary])
    rolling_excess = _num(latest.get("rolling_excess_primary"))
    if rolling_excess is None and joined_primary:
        rolling_excess = sum(p - b for p, b in joined_primary) / len(joined_primary)

    review_flags = _review_flags(
        latest=latest,
        beta_vs_qqq=beta_vs_qqq,
        expected=expected,
    )
    return {
        **base,
        "status": "ok",
        "as_of_snapshot_uid": latest.get("snapshot_uid"),
        "as_of_recorded_at": _iso_or_none(latest.get("recorded_at")),
        "headline": {
            "live_newbase_vs_qqq_cumulative_excess": _round(excess_vs_qqq, 6),
            "live_newbase_cumulative_return": _round(strategy_cum, 6),
            "qqq_cumulative_return": _round(qqq_cum, 6),
            "primary_interpretation": "positive_is_newbase_outperforming_qqq",
        },
        "benchmarks": {
            "primary": {
                "ticker": PRIMARY_BENCHMARK,
                "cumulative_return": _round(qqq_cum, 6),
                "excess": _round(excess_vs_qqq, 6),
                "joined_sample_count": len(joined_primary),
            },
            "secondary": {
                "ticker": SECONDARY_BENCHMARK,
                "cumulative_return": _round(spy_cum, 6),
                "excess": _round(excess_vs_spy, 6),
                "joined_sample_count": len(joined_secondary),
            },
        },
        "profile_monitor": {
            "rolling_beta_vs_qqq": _round(beta_vs_qqq, 6),
            "rolling_excess_vs_qqq": _round(rolling_excess, 6),
            "current_drawdown": _round(_num(latest.get("current_drawdown")), 6),
            "turnover": _round(_num(latest.get("turnover")), 6),
            "fees": _round(_num(latest.get("fees")), 2),
            "holding_count": len(_rows(latest.get("holdings"))),
            "order_count": len(_rows(latest.get("orders"))),
            "fill_count": len(_rows(latest.get("fills"))),
        },
        "review_flags": review_flags,
        "operator_action": "review_only",
        "red_light_contract": {
            "red_flags_enter_operator_pack_only": True,
            "automatic_trade_response": "forbidden",
        },
    }


def format_newbase_operator_snapshot_text(snapshot: dict[str, Any]) -> str:
    headline = snapshot.get("headline") or {}
    monitor = snapshot.get("profile_monitor") or {}
    flags = snapshot.get("review_flags") or []
    return (
        "newBase operator snapshot (review-only)\n"
        f"newBase vs QQQ cumulative excess: {_fmt_pct(headline.get('live_newbase_vs_qqq_cumulative_excess'))}\n"
        f"newBase cumulative: {_fmt_pct(headline.get('live_newbase_cumulative_return'))} | "
        f"QQQ cumulative: {_fmt_pct(headline.get('qqq_cumulative_return'))}\n"
        f"Beta vs QQQ: {_fmt_num(monitor.get('rolling_beta_vs_qqq'))} | "
        f"Drawdown: {_fmt_pct(monitor.get('current_drawdown'))} | "
        f"Turnover: {_fmt_pct(monitor.get('turnover'))} | Fees: {_fmt_money(monitor.get('fees'))}\n"
        f"Holdings: {monitor.get('holding_count', 0)} | Orders: {monitor.get('order_count', 0)} | "
        f"Fills: {monitor.get('fill_count', 0)}\n"
        f"Review flags: {len(flags)} | operator_action=review_only | execution_authority=none"
    )


async def persist_newbase_registry(db: Any) -> dict[str, Any]:
    from sqlalchemy.dialects.postgresql import insert

    from db.models import StrategyRegistryEntry

    record = build_newbase_registry_record()
    stmt = insert(StrategyRegistryEntry).values(record)
    update_cols = {
        key: getattr(stmt.excluded, key)
        for key in record
        if key not in {"strategy_id", "created_at"}
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=["strategy_id"],
        set_=update_cols,
    )
    await db.execute(stmt)
    return record


async def persist_strategy_live_snapshot(
    db: Any,
    payload: dict[str, Any],
    *,
    qc_snapshot_id: int | None = None,
    received_at: datetime | None = None,
) -> dict[str, Any]:
    """Persist a QC/newBase live snapshot and ensure the registry exists."""
    from sqlalchemy.dialects.postgresql import insert

    from db.models import StrategyLiveSnapshot

    await persist_newbase_registry(db)
    record = build_strategy_live_snapshot_record(
        payload,
        qc_snapshot_id=qc_snapshot_id,
        received_at=received_at,
    )
    stmt = insert(StrategyLiveSnapshot).values(record)
    update_cols = {
        key: getattr(stmt.excluded, key)
        for key in record
        if key not in {"id", "snapshot_uid", "created_at"}
    }
    stmt = stmt.on_conflict_do_update(
        constraint="uq_strategy_live_snapshot_uid",
        set_=update_cols,
    )
    await db.execute(stmt)
    await db.commit()
    return {
        "ingested": True,
        "strategy_id": record["strategy_id"],
        "snapshot_uid": record["snapshot_uid"],
        "trading_date": record["trading_date"].isoformat(),
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
    }


async def load_latest_newbase_operator_snapshot(*, limit: int = 90) -> dict[str, Any] | None:
    from sqlalchemy import desc, select

    from db.models import StrategyLiveSnapshot, StrategyRegistryEntry
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        registry = await db.get(StrategyRegistryEntry, NEWBASE_STRATEGY_ID)
        result = await db.execute(
            select(StrategyLiveSnapshot)
            .where(StrategyLiveSnapshot.strategy_id == NEWBASE_STRATEGY_ID)
            .order_by(desc(StrategyLiveSnapshot.recorded_at), desc(StrategyLiveSnapshot.id))
            .limit(max(int(limit), 1))
        )
        rows = list(result.scalars().all())
    if not rows:
        return None
    return build_newbase_operator_snapshot(
        list(reversed(rows)),
        registry=_registry_to_dict(registry),
    )


def _review_flags(*, latest: dict[str, Any], beta_vs_qqq: float | None, expected: dict[str, Any]) -> list[dict[str, Any]]:
    thresholds = _dict(expected.get("monitoring_thresholds"))
    flags: list[dict[str, Any]] = []
    drawdown = _num(latest.get("current_drawdown"))
    drawdown_threshold = _num(thresholds.get("drawdown_review_threshold")) or 0.30
    if drawdown is not None and abs(drawdown) >= drawdown_threshold:
        flags.append({
            "flag": "drawdown_profile_review",
            "value": round(drawdown, 6),
            "threshold": drawdown_threshold,
            "operator_action": "review_only",
            "automatic_trade_response": "forbidden",
        })
    recent_beta = _num(
        _dict(_dict(expected.get("absolute_backtest_profile")).get("recent_2023_2026")).get("beta")
    )
    beta_drift_threshold = _num(thresholds.get("rolling_beta_review_drift")) or 0.25
    if beta_vs_qqq is not None and recent_beta is not None and abs(beta_vs_qqq - recent_beta) >= beta_drift_threshold:
        flags.append({
            "flag": "beta_profile_drift_review",
            "value": round(beta_vs_qqq, 6),
            "expected_reference": recent_beta,
            "threshold": beta_drift_threshold,
            "operator_action": "review_only",
            "automatic_trade_response": "forbidden",
        })
    return flags


def _snapshot_row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        out = dict(row)
    else:
        keys = (
            "snapshot_uid",
            "strategy_id",
            "recorded_at",
            "trading_date",
            "daily_return",
            "cumulative_return",
            "current_drawdown",
            "turnover",
            "fees",
            "benchmark_primary_return",
            "benchmark_primary_cumulative_return",
            "benchmark_secondary_return",
            "benchmark_secondary_cumulative_return",
            "rolling_beta_primary",
            "rolling_excess_primary",
            "holdings",
            "orders",
            "fills",
        )
        out = {key: getattr(row, key, None) for key in keys}
    out["strategy_id"] = str(out.get("strategy_id") or "").lower()
    if isinstance(out.get("recorded_at"), str):
        out["recorded_at"] = _parse_datetime(out["recorded_at"])
    return out


def _registry_to_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return build_newbase_registry_record()
    return {
        "strategy_id": getattr(row, "strategy_id", None),
        "expected_profile": getattr(row, "expected_profile", None) or {},
    }


def _latest_or_compound(latest_value: Any, returns: Iterable[Any]) -> float | None:
    value = _return_num(latest_value)
    if value is not None:
        return value
    clean = [_return_num(item) for item in returns]
    clean = [item for item in clean if item is not None]
    if not clean:
        return None
    compounded = 1.0
    for item in clean:
        compounded *= 1.0 + item
    return compounded - 1.0


def _beta(x: list[float], y: list[float]) -> float | None:
    if len(x) != len(y) or len(x) < 3:
        return None
    mean_x = sum(x) / len(x)
    mean_y = sum(y) / len(y)
    denom = sum((item - mean_x) ** 2 for item in x)
    if denom <= 1e-12:
        return None
    return sum((xv - mean_x) * (yv - mean_y) for xv, yv in zip(x, y)) / denom


def _content_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _rows(value: Any) -> list[dict[str, Any]]:
    return [_json_safe(row) for row in value or [] if isinstance(row, dict)]


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _return_num(value: Any) -> float | None:
    number = _num(value)
    if number is None:
        return None
    if abs(number) > 2.0:
        return number / 100.0
    return number


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.astimezone(UTC).replace(tzinfo=None) if parsed.tzinfo else parsed


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _utcnow_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _clean_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _sub_or_none(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _round(value: Any, digits: int) -> float | None:
    number = _num(value)
    return round(number, digits) if number is not None else None


def _iso_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value is not None else None


def _fmt_pct(value: Any) -> str:
    number = _num(value)
    return "n/a" if number is None else f"{number:.2%}"


def _fmt_num(value: Any) -> str:
    number = _num(value)
    return "n/a" if number is None else f"{number:.3f}"


def _fmt_money(value: Any) -> str:
    number = _num(value)
    return "n/a" if number is None else f"${number:,.2f}"
