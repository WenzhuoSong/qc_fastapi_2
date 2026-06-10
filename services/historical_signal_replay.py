"""Historical EvidenceCard replay for signal conviction priors.

This module is intentionally storage-agnostic for PR5A. Callers pass normalized
daily feature rows, and the replay returns immutable signal/outcome objects.
Persistence can be added later without changing the date semantics.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timezone
from typing import Any, Iterable

from services.construction_epoch import (
    build_historical_replay_construction_epoch,
    unknown_construction_epoch,
)
from services.knowledge_base import build_knowledge_context
from services.strategy_evidence import EVIDENCE_CONTRACT_VERSION, build_evidence_cards
from strategies import get_strategy


DEFAULT_HORIZONS = (1, 5, 20)
SIGNAL_SOURCE_YFINANCE_REPLAY = "yfinance_replay"
OUTCOME_SOURCE_YFINANCE = "yfinance"
EXCESS_CALCULATION_RAW = "raw"


@dataclass(frozen=True)
class FrozenSignal:
    signal_id: str
    signal_source: str
    signal_date: date
    generated_at: datetime
    tradable_from_date: date
    strategy_id: str
    strategy_version: str
    ticker: str
    role: str
    branch: str | None
    action: str
    signal_type: str
    confidence: float
    raw_score: float | None
    normalized_score: float
    max_reasonable_weight: float
    risk_budget_cost: float
    feature_data_date: date | None
    data_lag_days: int | None
    feature_source: str
    feature_authority: str
    regime_at_signal: str
    vix_at_signal: float | None
    evidence_contract_version: str
    diagnostics: dict[str, Any]
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SignalOutcome:
    outcome_id: str
    signal_id: str
    signal_source: str
    signal_date: date
    label_date: date
    strategy_id: str
    ticker: str
    branch: str | None
    action: str
    horizon_days: int
    forward_return: float
    spy_forward_return: float
    excess_vs_spy: float
    drawdown_during_horizon: float
    spy_drawdown_during_horizon: float
    target_pool_drawdown: float | None
    hit: bool | None
    hit_definition: str
    excess_calculation_method: str
    outcome_source: str
    data_quality: str
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HistoricalSignalReplayResult:
    signals: list[FrozenSignal]
    outcomes: list[SignalOutcome]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "signals": [item.to_dict() for item in self.signals],
            "outcomes": [item.to_dict() for item in self.outcomes],
            "summary": dict(self.summary),
        }


def replay_historical_signals(
    feature_rows: Iterable[Any],
    *,
    strategy_names: list[str],
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    start_date: date | None = None,
    end_date: date | None = None,
    max_dates: int | None = None,
    mode: str = "playground",
    signal_source: str = SIGNAL_SOURCE_YFINANCE_REPLAY,
    generated_at: datetime | None = None,
) -> HistoricalSignalReplayResult:
    """Replay strategy EvidenceCards from yfinance-style daily feature rows.

    Signals for date T are generated only from rows with `trading_date <= T`.
    Outcomes start from the next trading day after T, never from T's same-day
    return.
    """
    rows = [_normalize_row(row) for row in feature_rows]
    rows = [row for row in rows if row.get("ticker") and row.get("trading_date")]
    rows.sort(key=lambda row: (row["trading_date"], row["ticker"]))
    all_trading_dates = sorted({row["trading_date"] for row in rows})
    trading_dates = _selected_dates(rows, start_date=start_date, end_date=end_date)
    if max_dates is not None:
        trading_dates = trading_dates[:max(0, int(max_dates))]

    price_by_ticker = _price_index(rows)
    generated = generated_at or datetime.now(timezone.utc)
    construction_epoch = build_historical_replay_construction_epoch()
    signals: list[FrozenSignal] = []
    outcomes: list[SignalOutcome] = []
    skipped: dict[str, int] = {}

    for signal_date in trading_dates:
        available_rows = [row for row in rows if row["trading_date"] <= signal_date]
        assert_no_future_features(available_rows, signal_date)
        snapshot = [row for row in available_rows if row["trading_date"] == signal_date]
        if not snapshot:
            skipped["empty_snapshot"] = skipped.get("empty_snapshot", 0) + 1
            continue
        tradable_from = next_trading_date(all_trading_dates, signal_date)
        if tradable_from is None:
            skipped["no_tradable_from_date"] = skipped.get("no_tradable_from_date", 0) + 1
            continue
        regime = _regime_from_snapshot(snapshot)
        context = {
            "regime": regime,
            "confidence": 0.5,
            "uncertainty_flag": False,
            "stance": "maintain",
            "direction_bias": "neutral",
            "risk_params": {},
            "current_weights": {},
        }
        for strategy_name in strategy_names:
            strategy = get_strategy(strategy_name)
            readiness = strategy.data_readiness(snapshot)
            if not readiness.get("ready"):
                skipped[f"{strategy_name}:not_ready"] = skipped.get(f"{strategy_name}:not_ready", 0) + 1
                continue
            scored = strategy.score(snapshot, context)
            if not scored:
                skipped[f"{strategy_name}:no_scores"] = skipped.get(f"{strategy_name}:no_scores", 0) + 1
                continue
            tickers = [item.ticker for item in scored if item.ticker]
            knowledge = build_knowledge_context(
                tickers=tickers,
                strategy_names=[strategy.name],
                regime=regime,
                max_assets=max(12, len(tickers)),
            )
            cards = build_evidence_cards(
                strategy=strategy,
                scored=scored,
                knowledge_context=knowledge,
                mode=mode,
            )
            for card in cards:
                signal = freeze_evidence_card(
                    card.to_dict(),
                    signal_date=signal_date,
                    tradable_from_date=tradable_from,
                    generated_at=generated,
                    signal_source=signal_source,
                    feature_data_date=signal_date,
                    feature_source=_feature_source(snapshot),
                    feature_authority="daily_research",
                    regime_at_signal=regime,
                    vix_at_signal=None,
                    construction_epoch=construction_epoch,
                )
                signals.append(signal)
                outcomes.extend(label_signal_outcomes(
                    signal,
                    price_by_ticker=price_by_ticker,
                    trading_dates=all_trading_dates,
                    horizons=horizons,
                    created_at=generated,
                ))

    return HistoricalSignalReplayResult(
        signals=signals,
        outcomes=outcomes,
        summary={
            "signal_source": signal_source,
            "outcome_source": OUTCOME_SOURCE_YFINANCE,
            "reliability": "historical_prior",
            "strategy_names": list(strategy_names),
            "horizons": list(horizons),
            "trading_dates_seen": len(trading_dates),
            "signals_generated": len(signals),
            "outcomes_generated": len(outcomes),
            "skipped": dict(sorted(skipped.items())),
        },
    )


def assert_no_future_features(feature_rows: Iterable[Any], signal_date: date) -> None:
    dates = [
        parsed
        for parsed in (_parse_date(_row_get(row, "trading_date")) for row in feature_rows)
        if parsed is not None
    ]
    if not dates:
        return
    max_feature_date = max(dates)
    assert max_feature_date <= signal_date, (
        f"Feature leak detected: max date {max_feature_date} > signal_date {signal_date}"
    )


def freeze_evidence_card(
    card: dict[str, Any],
    *,
    signal_date: date,
    tradable_from_date: date,
    generated_at: datetime,
    signal_source: str,
    feature_data_date: date | None,
    feature_source: str,
    feature_authority: str,
    regime_at_signal: str,
    vix_at_signal: float | None,
    construction_epoch: dict[str, Any] | None = None,
) -> FrozenSignal:
    ticker = str(card.get("ticker") or "").upper().strip()
    strategy_id = str(card.get("strategy") or "")
    branch = card.get("branch")
    action = str(card.get("action") or "watch")
    diagnostics = dict(card.get("diagnostics") or {})
    diagnostics.setdefault("source_bucket", "historical_prior")
    diagnostics.setdefault(
        "construction_epoch",
        construction_epoch or unknown_construction_epoch(),
    )
    data_lag_days = (signal_date - feature_data_date).days if feature_data_date is not None else None
    signal_id = _stable_id(
        "signal",
        signal_source,
        signal_date.isoformat(),
        strategy_id,
        ticker,
        branch,
        action,
        card.get("evidence_contract_version") or EVIDENCE_CONTRACT_VERSION,
    )
    return FrozenSignal(
        signal_id=signal_id,
        signal_source=signal_source,
        signal_date=signal_date,
        generated_at=generated_at,
        tradable_from_date=tradable_from_date,
        strategy_id=strategy_id,
        strategy_version=str(card.get("strategy_version") or ""),
        ticker=ticker,
        role=str(card.get("role") or "unknown"),
        branch=str(branch) if branch is not None else None,
        action=action,
        signal_type=str(card.get("signal_type") or "unspecified"),
        confidence=round(_to_float(card.get("confidence"), 0.0), 6),
        raw_score=_optional_float(card.get("raw_score")),
        normalized_score=round(_to_float(card.get("normalized_score"), 0.0), 6),
        max_reasonable_weight=round(_to_float(card.get("max_reasonable_weight"), 0.0), 6),
        risk_budget_cost=round(_to_float(card.get("risk_budget_cost"), 1.0), 6),
        feature_data_date=feature_data_date,
        data_lag_days=data_lag_days,
        feature_source=feature_source,
        feature_authority=feature_authority,
        regime_at_signal=regime_at_signal,
        vix_at_signal=vix_at_signal,
        evidence_contract_version=EVIDENCE_CONTRACT_VERSION,
        diagnostics=diagnostics,
        created_at=generated_at,
    )


def label_signal_outcomes(
    signal: FrozenSignal,
    *,
    price_by_ticker: dict[str, dict[date, float]],
    trading_dates: list[date],
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    created_at: datetime | None = None,
) -> list[SignalOutcome]:
    created = created_at or datetime.now(timezone.utc)
    effective_tradable_from = outcome_tradable_from_date(
        signal,
        trading_dates=trading_dates,
    )
    if effective_tradable_from is None:
        return []
    effective_signal = (
        signal
        if effective_tradable_from == signal.tradable_from_date
        else replace(signal, tradable_from_date=effective_tradable_from)
    )
    outcomes: list[SignalOutcome] = []
    for horizon in horizons:
        label_date = label_date_for_horizon(
            trading_dates=trading_dates,
            tradable_from_date=effective_signal.tradable_from_date,
            horizon_days=horizon,
        )
        if label_date is None or label_date <= effective_signal.signal_date:
            continue
        outcome = _label_one_outcome(
            effective_signal,
            label_date=label_date,
            horizon_days=horizon,
            price_by_ticker=price_by_ticker,
            created_at=created,
        )
        if outcome is not None:
            outcomes.append(outcome)
    return outcomes


def outcome_tradable_from_date(
    signal: FrozenSignal,
    *,
    trading_dates: list[date],
) -> date | None:
    """Return the first valid outcome start date after the signal date."""
    if signal.tradable_from_date > signal.signal_date:
        return signal.tradable_from_date
    return next_trading_date(trading_dates, signal.signal_date)


def label_date_for_horizon(
    *,
    trading_dates: list[date],
    tradable_from_date: date,
    horizon_days: int,
) -> date | None:
    try:
        start_idx = trading_dates.index(tradable_from_date)
    except ValueError:
        return None
    label_idx = start_idx + max(1, int(horizon_days)) - 1
    if label_idx >= len(trading_dates):
        return None
    return trading_dates[label_idx]


def next_trading_date(trading_dates: list[date], signal_date: date) -> date | None:
    for item in trading_dates:
        if item > signal_date:
            return item
    return None


def _label_one_outcome(
    signal: FrozenSignal,
    *,
    label_date: date,
    horizon_days: int,
    price_by_ticker: dict[str, dict[date, float]],
    created_at: datetime,
) -> SignalOutcome | None:
    ticker_prices = price_by_ticker.get(signal.ticker) or {}
    spy_prices = price_by_ticker.get("SPY") or {}
    start_price = ticker_prices.get(signal.signal_date)
    end_price = ticker_prices.get(label_date)
    spy_start = spy_prices.get(signal.signal_date)
    spy_end = spy_prices.get(label_date)
    if not all(value not in (None, 0) for value in (start_price, end_price, spy_start, spy_end)):
        return None

    forward_return = (end_price / start_price) - 1.0
    spy_forward_return = (spy_end / spy_start) - 1.0
    excess_vs_spy = forward_return - spy_forward_return
    drawdown = _drawdown_from_base(
        base=start_price,
        prices=_path_prices(ticker_prices, signal.tradable_from_date, label_date),
    )
    spy_drawdown = _drawdown_from_base(
        base=spy_start,
        prices=_path_prices(spy_prices, signal.tradable_from_date, label_date),
    )
    hit, hit_definition = _hit_for_action(
        action=signal.action,
        forward_return=forward_return,
        excess_vs_spy=excess_vs_spy,
        spy_forward_return=spy_forward_return,
        spy_drawdown=spy_drawdown,
        target_pool_drawdown=None,
    )
    outcome_id = _stable_id(
        "outcome",
        signal.signal_id,
        str(horizon_days),
        OUTCOME_SOURCE_YFINANCE,
        EXCESS_CALCULATION_RAW,
    )
    return SignalOutcome(
        outcome_id=outcome_id,
        signal_id=signal.signal_id,
        signal_source=signal.signal_source,
        signal_date=signal.signal_date,
        label_date=label_date,
        strategy_id=signal.strategy_id,
        ticker=signal.ticker,
        branch=signal.branch,
        action=signal.action,
        horizon_days=int(horizon_days),
        forward_return=round(forward_return, 8),
        spy_forward_return=round(spy_forward_return, 8),
        excess_vs_spy=round(excess_vs_spy, 8),
        drawdown_during_horizon=round(drawdown, 8),
        spy_drawdown_during_horizon=round(spy_drawdown, 8),
        target_pool_drawdown=None,
        hit=hit,
        hit_definition=hit_definition,
        excess_calculation_method=EXCESS_CALCULATION_RAW,
        outcome_source=OUTCOME_SOURCE_YFINANCE,
        data_quality="ok",
        created_at=created_at,
    )


def _hit_for_action(
    *,
    action: str,
    forward_return: float,
    excess_vs_spy: float,
    spy_forward_return: float,
    spy_drawdown: float,
    target_pool_drawdown: float | None,
) -> tuple[bool | None, str]:
    action = str(action or "").strip()
    if action == "increase":
        return (
            forward_return > 0.0 and excess_vs_spy > -0.005,
            "increase:forward_return>0_and_excess_vs_spy>-0.005",
        )
    if action == "hedge":
        return (
            spy_forward_return < -0.02 or spy_drawdown < -0.02,
            "hedge:spy_forward_return<-0.02_or_spy_drawdown<-0.02",
        )
    if action == "de_risk":
        pool_drawdown = target_pool_drawdown if target_pool_drawdown is not None else 0.0
        return (
            spy_drawdown < -0.015 or pool_drawdown < -0.015,
            "de_risk:spy_drawdown<-0.015_or_target_pool_drawdown<-0.015",
        )
    if action == "avoid":
        return (forward_return < -0.01, "avoid:forward_return<-0.01")
    if action == "reduce":
        return (
            forward_return < 0.0 or excess_vs_spy < -0.005,
            "reduce:forward_return<0_or_excess_vs_spy<-0.005",
        )
    return (None, f"{action or 'unknown'}:no_hit_label")


def _selected_dates(
    rows: list[dict[str, Any]],
    *,
    start_date: date | None,
    end_date: date | None,
) -> list[date]:
    dates = sorted({row["trading_date"] for row in rows})
    if start_date:
        dates = [item for item in dates if item >= start_date]
    if end_date:
        dates = [item for item in dates if item <= end_date]
    return dates


def _price_index(rows: list[dict[str, Any]]) -> dict[str, dict[date, float]]:
    out: dict[str, dict[date, float]] = {}
    for row in rows:
        price = _close_price(row)
        if price is None:
            continue
        out.setdefault(row["ticker"], {})[row["trading_date"]] = price
    return out


def _path_prices(prices: dict[date, float], start: date, end: date) -> list[float]:
    return [
        price
        for dt, price in sorted(prices.items())
        if start <= dt <= end
    ]


def _drawdown_from_base(*, base: float, prices: list[float]) -> float:
    if not prices or base == 0:
        return 0.0
    return min((price / base) - 1.0 for price in prices)


def _regime_from_snapshot(snapshot: list[dict[str, Any]]) -> str:
    spy = next((row for row in snapshot if row.get("ticker") == "SPY"), None)
    if not spy:
        return "unknown"
    close = _close_price(spy)
    sma200 = _optional_float(spy.get("sma_200"))
    if close is not None and sma200 is not None:
        return "trending_bull" if close > sma200 else "defensive"
    return "unknown"


def _feature_source(snapshot: list[dict[str, Any]]) -> str:
    sources = sorted({str(row.get("source") or "yfinance") for row in snapshot})
    return sources[0] if len(sources) == 1 else "mixed"


def _normalize_row(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        raw = dict(row)
    else:
        raw = {
            key: getattr(row, key, None)
            for key in (
                "ticker",
                "trading_date",
                "source",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "adj_close_price",
                "return_1d",
                "return_5d",
                "return_20d",
                "return_60d",
                "return_252d",
                "sma_20",
                "sma_50",
                "sma_200",
                "hist_vol_20d",
                "rsi_10",
                "rsi_14",
                "atr_pct",
                "bb_position",
                "dollar_volume",
            )
        }
    raw["ticker"] = str(raw.get("ticker") or "").upper().strip()
    raw["trading_date"] = _parse_date(raw.get("trading_date"))
    raw.setdefault("source", "yfinance")
    raw.setdefault("price", raw.get("close_price") or raw.get("adj_close_price"))
    return raw


def _close_price(row: dict[str, Any]) -> float | None:
    return _optional_float(row.get("adj_close_price") or row.get("close_price") or row.get("price"))


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _row_get(row: Any, field: str) -> Any:
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)


def _stable_id(*parts: Any) -> str:
    payload = json.dumps([str(part) for part in parts], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(value: Any, default: float = 0.0) -> float:
    parsed = _optional_float(value)
    return parsed if parsed is not None else default
