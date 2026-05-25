"""Strategy return-correlation and independence diagnostics.

This module is research-only. It estimates replay returns from historical
feature rows and reports whether strategy variants are statistically distinct.
It has no execution authority and cannot mutate target weights.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from constants import DEFAULT_ETF_UNIVERSE
from services.macro_regime_builder import build_deterministic_macro_regime
from services.quant_baseline import classify_market_regime
from services.strategy_diversity import canonical_strategy_family, is_strategy_alpha_source
from services.strategy_feature_contract import build_strategy_feature_contract
from strategies import STRATEGY_REGISTRY, get_strategy


CONTRACT_VERSION = "strategy_independence_diagnostics_v1"
DEFAULT_MIN_OVERLAP = 30
HIGH_CORRELATION_THRESHOLD = 0.70
INVERSE_CORRELATION_THRESHOLD = -0.50


def empty_strategy_independence_summary(reason: str = "no_strategy_return_series") -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "status": "insufficient_data",
        "reason": reason,
        "strategy_count": 0,
        "alpha_strategy_count": 0,
        "effective_independent_alpha_count": 0.0,
        "avg_positive_correlation": None,
        "avg_abs_correlation": None,
        "pair_rows": [],
        "high_correlation_pairs": [],
        "inverse_correlation_pairs": [],
        "family_correlation_rows": [],
        "strategy_rows": [],
        "correlation_matrix": {},
        "warnings": [reason],
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


async def load_strategy_independence_diagnostics(
    db: Any,
    *,
    lookback_days: int = 420,
    source: str = "yfinance",
    strategy_names: list[str] | None = None,
    min_overlap: int = DEFAULT_MIN_OVERLAP,
) -> dict[str, Any]:
    """Load historical feature rows and build strategy-independence diagnostics."""
    from sqlalchemy import select

    from db.models import MarketDailyFeature

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=max(int(lookback_days), 1))
    stmt = (
        select(MarketDailyFeature)
        .where(MarketDailyFeature.source == source)
        .where(MarketDailyFeature.trading_date >= cutoff)
        .order_by(MarketDailyFeature.trading_date, MarketDailyFeature.ticker)
    )
    result = await db.execute(stmt)
    rows = list(result.scalars().all())
    return build_strategy_independence_diagnostics(
        feature_rows=rows,
        strategy_names=strategy_names,
        min_overlap=min_overlap,
        source=source,
    )


def build_strategy_independence_diagnostics(
    *,
    feature_rows: Iterable[Any],
    strategy_names: list[str] | None = None,
    min_overlap: int = DEFAULT_MIN_OVERLAP,
    source: str = "yfinance",
) -> dict[str, Any]:
    snapshots = _feature_rows_to_snapshots(feature_rows, source=source)
    return build_strategy_independence_diagnostics_from_snapshots(
        snapshots=snapshots,
        strategy_names=strategy_names,
        min_overlap=min_overlap,
    )


def build_strategy_independence_diagnostics_from_snapshots(
    *,
    snapshots: list[dict[str, Any]],
    strategy_names: list[str] | None = None,
    min_overlap: int = DEFAULT_MIN_OVERLAP,
) -> dict[str, Any]:
    names = _strategy_names(strategy_names)
    if len(snapshots) < 2 or not names:
        return empty_strategy_independence_summary("insufficient_snapshots")
    series, replay_summary = build_strategy_return_series_from_snapshots(
        snapshots=snapshots,
        strategy_names=names,
    )
    metadata = _strategy_metadata(names)
    summary = build_strategy_independence_summary(
        return_series=series,
        strategy_metadata=metadata,
        min_overlap=min_overlap,
    )
    return {
        **summary,
        "snapshot_count": len(snapshots),
        "replay_summary": replay_summary,
    }


def build_strategy_return_series_from_snapshots(
    *,
    snapshots: list[dict[str, Any]],
    strategy_names: list[str],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Replay each strategy on T and score its next-session return on T+1."""
    clean_snapshots = [
        snapshot for snapshot in sorted(snapshots, key=lambda item: str(item.get("trading_date") or ""))
        if _parse_date(snapshot.get("trading_date")) is not None
    ]
    series: dict[str, list[dict[str, Any]]] = {name: [] for name in strategy_names}
    skipped: dict[str, int] = {}
    ready_counts: dict[str, int] = {name: 0 for name in strategy_names}

    for idx, snapshot in enumerate(clean_snapshots[:-1]):
        signal_date = _parse_date(snapshot.get("trading_date"))
        next_date = _parse_date(clean_snapshots[idx + 1].get("trading_date"))
        if signal_date is None or next_date is None:
            skipped["missing_trading_date"] = skipped.get("missing_trading_date", 0) + 1
            continue
        holdings = _snapshot_rows(snapshot)
        if not holdings:
            skipped["missing_holdings"] = skipped.get("missing_holdings", 0) + 1
            continue
        max_feature_date = _max_feature_date(holdings)
        if max_feature_date is not None and max_feature_date > signal_date:
            raise AssertionError(
                f"Feature leak detected: max date {max_feature_date} > signal_date {signal_date}"
            )
        next_returns = _daily_returns(_snapshot_rows(clean_snapshots[idx + 1]))
        if not next_returns:
            skipped["missing_forward_returns"] = skipped.get("missing_forward_returns", 0) + 1
            continue
        context = _context_for_holdings(holdings)

        for name in strategy_names:
            strategy = get_strategy(name)
            strategy_rows = strategy.eligible_rows(holdings)
            if not strategy_rows:
                skipped[f"{name}:no_eligible_rows"] = skipped.get(f"{name}:no_eligible_rows", 0) + 1
                continue
            readiness = strategy.data_readiness(strategy_rows)
            feature_contract = build_strategy_feature_contract(
                strategy,
                strategy_rows,
                as_of=signal_date,
            )
            if not readiness.get("ready") or not feature_contract.get("can_influence_allocation"):
                skipped[f"{name}:not_ready"] = skipped.get(f"{name}:not_ready", 0) + 1
                continue
            try:
                scored = strategy.score(strategy_rows, context)
                weights = strategy.optimize(scored, context)
            except Exception as exc:
                skipped[f"{name}:score_error:{type(exc).__name__}"] = (
                    skipped.get(f"{name}:score_error:{type(exc).__name__}", 0) + 1
                )
                continue

            forward_return = _weighted_forward_return(weights, next_returns)
            if forward_return is None:
                skipped[f"{name}:missing_weighted_forward_return"] = (
                    skipped.get(f"{name}:missing_weighted_forward_return", 0) + 1
                )
                continue
            ready_counts[name] += 1
            selected = [
                ticker for ticker, weight in sorted(weights.items())
                if ticker != "CASH" and _to_float(weight, 0.0) > 0.01
            ]
            series[name].append({
                "date": signal_date.isoformat(),
                "next_date": next_date.isoformat(),
                "return": round(float(forward_return), 8),
                "regime": context.get("regime"),
                "selected_tickers": selected,
                "gross_non_cash_weight": round(
                    sum(
                        abs(_to_float(weight, 0.0))
                        for ticker, weight in weights.items()
                        if ticker != "CASH"
                    ),
                    6,
                ),
            })

    return series, {
        "contract_version": "strategy_return_replay_v1",
        "snapshot_count": len(clean_snapshots),
        "strategy_count": len(strategy_names),
        "ready_counts": dict(sorted(ready_counts.items())),
        "skipped": dict(sorted(skipped.items())),
        "no_lookahead_rule": "score_on_signal_date_use_next_snapshot_return_as_outcome",
    }


def build_strategy_independence_summary(
    *,
    return_series: dict[str, list[dict[str, Any]]],
    strategy_metadata: dict[str, dict[str, Any]] | None = None,
    min_overlap: int = DEFAULT_MIN_OVERLAP,
    high_correlation_threshold: float = HIGH_CORRELATION_THRESHOLD,
    inverse_correlation_threshold: float = INVERSE_CORRELATION_THRESHOLD,
) -> dict[str, Any]:
    metadata = strategy_metadata or {}
    normalized = {
        name: _normalize_series(rows)
        for name, rows in return_series.items()
    }
    strategy_rows = [
        _strategy_row(name, rows, metadata.get(name, {}))
        for name, rows in sorted(normalized.items())
    ]
    pair_rows = _pair_rows(
        normalized,
        metadata=metadata,
        min_overlap=max(int(min_overlap), 2),
    )
    valid_pairs = [row for row in pair_rows if row.get("correlation") is not None]
    high_pairs = [
        row for row in valid_pairs
        if float(row["correlation"]) >= high_correlation_threshold
    ]
    inverse_pairs = [
        row for row in valid_pairs
        if float(row["correlation"]) <= inverse_correlation_threshold
    ]
    positive_corrs = [max(float(row["correlation"]), 0.0) for row in valid_pairs]
    abs_corrs = [abs(float(row["correlation"])) for row in valid_pairs]
    alpha_names = [
        row["strategy_name"]
        for row in strategy_rows
        if row["alpha_source"] and row["sample_count"] >= max(int(min_overlap), 2)
    ]
    alpha_pairs = [
        row for row in valid_pairs
        if row["left"] in alpha_names and row["right"] in alpha_names
    ]
    alpha_positive = [max(float(row["correlation"]), 0.0) for row in alpha_pairs]
    avg_positive = _avg(positive_corrs) if positive_corrs else None
    avg_abs = _avg(abs_corrs) if abs_corrs else None
    avg_alpha_positive = _avg(alpha_positive) if alpha_positive else None
    effective_alpha = _effective_independent_count(
        strategy_count=len(alpha_names),
        avg_positive_correlation=avg_alpha_positive,
    )
    warnings = _warnings(
        high_pairs=high_pairs,
        pair_rows=pair_rows,
        min_overlap=max(int(min_overlap), 2),
        alpha_names=alpha_names,
        effective_alpha=effective_alpha,
    )

    if not any(row["sample_count"] for row in strategy_rows):
        return empty_strategy_independence_summary("no_strategy_return_series")

    return {
        "contract_version": CONTRACT_VERSION,
        "status": "available" if valid_pairs else "insufficient_overlap",
        "min_overlap": max(int(min_overlap), 2),
        "strategy_count": len(strategy_rows),
        "alpha_strategy_count": len(alpha_names),
        "effective_independent_alpha_count": effective_alpha,
        "avg_positive_correlation": round(avg_positive, 4) if avg_positive is not None else None,
        "avg_abs_correlation": round(avg_abs, 4) if avg_abs is not None else None,
        "avg_alpha_positive_correlation": (
            round(avg_alpha_positive, 4) if avg_alpha_positive is not None else None
        ),
        "pair_rows": pair_rows,
        "high_correlation_pairs": high_pairs,
        "inverse_correlation_pairs": inverse_pairs,
        "family_correlation_rows": _family_correlation_rows(valid_pairs),
        "strategy_rows": strategy_rows,
        "correlation_matrix": _correlation_matrix(strategy_rows, valid_pairs),
        "warnings": warnings,
        "method": {
            "return_series": "strategy_weights_on_T_times_ticker_return_on_T_plus_1",
            "effective_count": "n_alpha/(1+(n_alpha-1)*avg_positive_alpha_correlation)",
            "positive_correlation_penalty_only": True,
        },
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def _feature_rows_to_snapshots(rows: Iterable[Any], *, source: str) -> list[dict[str, Any]]:
    by_date: dict[date, list[dict[str, Any]]] = {}
    for row in rows:
        trading_date = _parse_date(_record_get(row, "trading_date"))
        ticker = str(_record_get(row, "ticker") or "").upper().strip()
        if trading_date is None or not ticker:
            continue
        holding = _feature_row_to_holding(row, source=source)
        by_date.setdefault(trading_date, []).append(holding)
    return [
        {
            "packet_type": f"{source}_historical",
            "trading_date": trading_date.isoformat(),
            "features": holdings,
            "holdings": holdings,
            "portfolio": {},
        }
        for trading_date, holdings in sorted(by_date.items())
        if holdings
    ]


def _feature_row_to_holding(row: Any, *, source: str) -> dict[str, Any]:
    ticker = str(_record_get(row, "ticker") or "").upper().strip()
    trading_date = _parse_date(_record_get(row, "trading_date"))
    mapped = {
        "ticker": ticker,
        "universe_role": "research",
        "price": _first_number(_record_get(row, "close_price"), _record_get(row, "adj_close_price")),
        "close_price": _first_number(_record_get(row, "close_price"), _record_get(row, "adj_close_price")),
        "open_price": _to_float(_record_get(row, "open_price")),
        "high_price": _to_float(_record_get(row, "high_price")),
        "low_price": _to_float(_record_get(row, "low_price")),
        "volume": _to_int(_record_get(row, "volume")),
        "dollar_volume": _to_float(_record_get(row, "dollar_volume")),
        "daily_return_pct": _to_float(_record_get(row, "return_1d")),
        "return_1d": _to_float(_record_get(row, "return_1d")),
        "return_5d": _to_float(_record_get(row, "return_5d")),
        "return_20d": _to_float(_record_get(row, "return_20d")),
        "return_60d": _to_float(_record_get(row, "return_60d")),
        "return_252d": _to_float(_record_get(row, "return_252d")),
        "mom_20d": _to_float(_record_get(row, "return_20d")),
        "mom_60d": _to_float(_record_get(row, "return_60d")),
        "mom_252d": _to_float(_record_get(row, "return_252d")),
        "sma_20": _to_float(_record_get(row, "sma_20")),
        "sma_50": _to_float(_record_get(row, "sma_50")),
        "sma_200": _to_float(_record_get(row, "sma_200")),
        "hist_vol_20d": _to_float(_record_get(row, "hist_vol_20d")),
        "rsi_10": _to_float(_record_get(row, "rsi_10")),
        "rsi_14": _to_float(_record_get(row, "rsi_14")),
        "atr_pct": _to_float(_record_get(row, "atr_pct")),
        "bb_position": _to_float(_record_get(row, "bb_position")),
        "beta_vs_spy": _to_float(_record_get(row, "beta_vs_spy")),
    }
    filled_fields = sorted(
        field for field, value in mapped.items()
        if field not in {"ticker", "universe_role", "feature_sources"} and value is not None
    )
    mapped["feature_sources"] = [{
        "source": f"{source}_historical",
        "filled_fields": filled_fields,
        "authority_by_field": {},
        "trading_date": trading_date.isoformat() if trading_date else None,
    }]
    return mapped


def _snapshot_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = snapshot.get("holdings") or snapshot.get("features") or []
    return [_normalize_holding(row, fallback_date=_parse_date(snapshot.get("trading_date"))) for row in rows]


def _normalize_holding(row: dict[str, Any], *, fallback_date: date | None) -> dict[str, Any]:
    out = dict(row)
    ticker = str(out.get("ticker") or "").upper().strip()
    out["ticker"] = ticker
    out.setdefault("universe_role", "research" if ticker in DEFAULT_ETF_UNIVERSE else "strategy_research")
    if out.get("daily_return_pct") is None and out.get("return_1d") is not None:
        out["daily_return_pct"] = out.get("return_1d")
    if out.get("return_1d") is None and out.get("daily_return_pct") is not None:
        out["return_1d"] = out.get("daily_return_pct")
    for legacy, canonical in (
        ("mom_20d", "return_20d"),
        ("mom_60d", "return_60d"),
        ("mom_252d", "return_252d"),
    ):
        if out.get(legacy) is None and out.get(canonical) is not None:
            out[legacy] = out.get(canonical)
        if out.get(canonical) is None and out.get(legacy) is not None:
            out[canonical] = out.get(legacy)
    if not out.get("feature_sources") and fallback_date is not None:
        out["feature_sources"] = [{
            "source": "yfinance_historical",
            "filled_fields": [
                key for key, value in out.items()
                if key not in {"ticker", "universe_role", "feature_sources"} and value is not None
            ],
            "authority_by_field": {},
            "trading_date": fallback_date.isoformat(),
        }]
    return out


def _context_for_holdings(holdings: list[dict[str, Any]]) -> dict[str, Any]:
    spy = next((row for row in holdings if row.get("ticker") == "SPY"), {})
    regime = classify_market_regime({}, spy, holdings=holdings)
    macro = build_deterministic_macro_regime(holdings)
    return {
        "regime": regime.regime.value,
        "confidence": _confidence_to_float(regime.confidence),
        "uncertainty_flag": regime.confidence == "low",
        "stance": _stance_for_regime(regime.regime.value),
        "direction_bias": _direction_bias_for_regime(regime.regime.value),
        "risk_params": {},
        "current_weights": {},
        "sector_rotation": {},
        "macro_context": macro,
        "rate_regime_label": macro.get("rate_regime_label"),
        "inflation_regime_label": macro.get("inflation_regime_label"),
        "growth_regime_label": macro.get("growth_regime_label"),
    }


def _strategy_metadata(strategy_names: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for name in strategy_names:
        try:
            strategy = get_strategy(name)
            card = strategy.strategy_card()
            family = canonical_strategy_family(card.get("canonical_family") or card.get("family"))
            out[name] = {
                "strategy_name": name,
                "raw_family": card.get("family"),
                "canonical_family": family,
                "alpha_source": is_strategy_alpha_source(name, family, card.get("alpha_source")),
            }
        except Exception:
            out[name] = {
                "strategy_name": name,
                "raw_family": "unknown",
                "canonical_family": "unknown",
                "alpha_source": False,
            }
    return out


def _strategy_names(names: list[str] | None) -> list[str]:
    if names is None:
        return sorted(STRATEGY_REGISTRY)
    return [name for name in names if name in STRATEGY_REGISTRY]


def _normalize_series(rows: list[dict[str, Any]]) -> dict[date, float]:
    out: dict[date, float] = {}
    for row in rows or []:
        row_date = _parse_date(row.get("date"))
        value = _to_float(row.get("return"))
        if row_date is not None and value is not None and math.isfinite(value):
            out[row_date] = float(value)
    return out


def _strategy_row(
    name: str,
    series: dict[date, float],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    returns = list(series.values())
    family = canonical_strategy_family(metadata.get("canonical_family") or metadata.get("raw_family"))
    return {
        "strategy_name": name,
        "canonical_family": family,
        "raw_family": metadata.get("raw_family") or family,
        "alpha_source": is_strategy_alpha_source(name, family, metadata.get("alpha_source")),
        "sample_count": len(returns),
        "avg_daily_return": round(_avg(returns), 6) if returns else None,
        "daily_vol": round(_sample_std(returns), 6) if len(returns) >= 2 else None,
        "annualized_sharpe": _annualized_sharpe(returns),
        "hit_rate": round(sum(1 for value in returns if value > 0) / len(returns), 4) if returns else None,
        "first_date": min(series).isoformat() if series else None,
        "last_date": max(series).isoformat() if series else None,
    }


def _pair_rows(
    series: dict[str, dict[date, float]],
    *,
    metadata: dict[str, dict[str, Any]],
    min_overlap: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    names = sorted(series)
    for idx, left in enumerate(names):
        for right in names[idx + 1:]:
            common = sorted(set(series[left]) & set(series[right]))
            left_family = canonical_strategy_family(
                metadata.get(left, {}).get("canonical_family")
                or metadata.get(left, {}).get("raw_family")
            )
            right_family = canonical_strategy_family(
                metadata.get(right, {}).get("canonical_family")
                or metadata.get(right, {}).get("raw_family")
            )
            correlation = None
            if len(common) >= min_overlap:
                correlation = _correlation(
                    [series[left][item] for item in common],
                    [series[right][item] for item in common],
                )
            rows.append({
                "left": left,
                "right": right,
                "left_family": left_family,
                "right_family": right_family,
                "same_family": left_family == right_family,
                "overlap": len(common),
                "correlation": round(correlation, 4) if correlation is not None else None,
                "abs_correlation": round(abs(correlation), 4) if correlation is not None else None,
                "status": "available" if correlation is not None else "insufficient_overlap",
            })
    rows.sort(
        key=lambda row: (
            row.get("status") != "available",
            -float(row.get("abs_correlation") or 0.0),
            row["left"],
            row["right"],
        )
    )
    return rows


def _family_correlation_rows(pair_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in pair_rows:
        left = str(row.get("left_family") or "unknown")
        right = str(row.get("right_family") or "unknown")
        key = tuple(sorted((left, right)))
        groups.setdefault(key, []).append(row)
    out = []
    for (left, right), rows in sorted(groups.items()):
        corrs = [float(row["correlation"]) for row in rows if row.get("correlation") is not None]
        out.append({
            "left_family": left,
            "right_family": right,
            "pair_count": len(rows),
            "available_pair_count": len(corrs),
            "avg_correlation": round(_avg(corrs), 4) if corrs else None,
            "avg_positive_correlation": round(_avg([max(value, 0.0) for value in corrs]), 4) if corrs else None,
            "max_abs_correlation": round(max(abs(value) for value in corrs), 4) if corrs else None,
        })
    return out


def _correlation_matrix(
    strategy_rows: list[dict[str, Any]],
    pair_rows: list[dict[str, Any]],
) -> dict[str, dict[str, float | None]]:
    names = [row["strategy_name"] for row in strategy_rows]
    matrix: dict[str, dict[str, float | None]] = {
        name: {other: (1.0 if other == name else None) for other in names}
        for name in names
    }
    for row in pair_rows:
        left = str(row["left"])
        right = str(row["right"])
        corr = row.get("correlation")
        matrix.setdefault(left, {})[right] = corr
        matrix.setdefault(right, {})[left] = corr
    return matrix


def _warnings(
    *,
    high_pairs: list[dict[str, Any]],
    pair_rows: list[dict[str, Any]],
    min_overlap: int,
    alpha_names: list[str],
    effective_alpha: float,
) -> list[str]:
    warnings = [
        f"high_strategy_correlation:{row['left']}:{row['right']}:{row['correlation']}"
        for row in high_pairs
    ]
    insufficient = sum(1 for row in pair_rows if row.get("status") == "insufficient_overlap")
    if insufficient:
        warnings.append(f"insufficient_overlap_pairs:{insufficient}:min_overlap={min_overlap}")
    if len(alpha_names) >= 2 and effective_alpha < max(1.0, len(alpha_names) * 0.6):
        warnings.append(
            f"effective_independent_alpha_count_low:{effective_alpha:.2f}/{len(alpha_names)}"
        )
    return warnings


def _effective_independent_count(
    *,
    strategy_count: int,
    avg_positive_correlation: float | None,
) -> float:
    if strategy_count <= 0:
        return 0.0
    if strategy_count == 1:
        return 1.0
    rho = max(min(float(avg_positive_correlation or 0.0), 0.99), 0.0)
    return round(strategy_count / (1.0 + (strategy_count - 1) * rho), 2)


def _daily_returns(rows: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").upper().strip()
        value = _first_number(row.get("return_1d"), row.get("daily_return_pct"))
        if ticker and value is not None:
            out[ticker] = float(value)
    return out


def _weighted_forward_return(
    weights: dict[str, Any],
    returns: dict[str, float],
) -> float | None:
    known_weight = 0.0
    total = 0.0
    non_cash_weight = 0.0
    for ticker, raw_weight in (weights or {}).items():
        clean = str(ticker or "").upper().strip()
        if clean == "CASH":
            continue
        weight = _to_float(raw_weight, 0.0) or 0.0
        non_cash_weight += abs(weight)
        if clean in returns:
            known_weight += abs(weight)
            total += weight * float(returns[clean])
    if non_cash_weight <= 1e-12:
        return 0.0
    if known_weight <= 1e-12:
        return None
    return float(total)


def _max_feature_date(rows: list[dict[str, Any]]) -> date | None:
    dates: list[date] = []
    for row in rows:
        for source in row.get("feature_sources") or []:
            parsed = _parse_date(source.get("trading_date"))
            if parsed is not None:
                dates.append(parsed)
    return max(dates) if dates else None


def _confidence_to_float(confidence: str) -> float:
    return {"high": 0.8, "medium": 0.6, "low": 0.4}.get(str(confidence), 0.5)


def _direction_bias_for_regime(regime: str) -> str:
    if regime == "trending_bull":
        return "bullish"
    if regime in {"trending_bear", "defensive"}:
        return "bearish"
    return "neutral"


def _stance_for_regime(regime: str) -> str:
    if regime == "trending_bull":
        return "increase"
    if regime in {"trending_bear", "defensive", "high_vol"}:
        return "defensive"
    return "maintain"


def _annualized_sharpe(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    std = _sample_std(values)
    if std <= 1e-12:
        return None
    return round((_avg(values) / std) * math.sqrt(252), 4)


def _correlation(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    mean_left = _avg(left)
    mean_right = _avg(right)
    num = sum((l - mean_left) * (r - mean_right) for l, r in zip(left, right))
    den_left = math.sqrt(sum((l - mean_left) ** 2 for l in left))
    den_right = math.sqrt(sum((r - mean_right) ** 2 for r in right))
    denom = den_left * den_right
    if denom <= 1e-12:
        return None
    return num / denom


def _sample_std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = _avg(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / (len(values) - 1))


def _avg(values: list[float]) -> float:
    clean = []
    for value in values:
        parsed = _to_float(value)
        if parsed is not None and math.isfinite(parsed):
            clean.append(parsed)
    return sum(clean) / len(clean) if clean else 0.0


def _first_number(*values: Any) -> float | None:
    for value in values:
        parsed = _to_float(value)
        if parsed is not None:
            return parsed
    return None


def _record_get(row: Any, field: str) -> Any:
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _to_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
