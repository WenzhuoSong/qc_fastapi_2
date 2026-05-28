"""ETF-level vote aggregation for strategy EvidenceCards.

This module is observe-only. It summarizes which strategies can vote for each
ETF and which strategies abstained due to input exclusions. It must not mutate
scores, target weights, Portfolio Construction inputs, or execution state.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from services.strategy_evidence import EvidenceCard


ACTIONABLE_STATUSES = {"voted"}
DENOMINATOR_EXCLUDED_ABSTAIN_REASONS = {"strategy_universe_mismatch"}

ABSTAIN_REASON_MAP = {
    "insufficient_history": "insufficient_history",
    "field_not_applicable": "field_not_applicable",
    "data_stale": "data_stale",
    "stale_data": "data_stale",
    "strategy_universe_mismatch": "strategy_universe_mismatch",
    "missing_required_field": "missing_required_field",
    "missing_field": "missing_required_field",
    "non_authoritative_field": "missing_required_field",
}


@dataclass
class EtfVoteSummary:
    ticker: str
    voted_count: int = 0
    watch_count: int = 0
    abstain_count: int = 0
    mapping_error_count: int = 0
    eligible_strategy_count: int = 0
    coverage_ratio: float = 0.0
    actionable_score: float | None = None
    supporting_actions: dict[str, int] = field(default_factory=dict)
    voted_strategies: list[str] = field(default_factory=list)
    watch_strategies: list[str] = field(default_factory=list)
    mapping_error_strategies: list[str] = field(default_factory=list)
    abstain_reasons: list[dict[str, Any]] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def aggregate_etf_evidence(
    *,
    evidence_cards: list[EvidenceCard | dict[str, Any]],
    input_builder_exclusions: dict[str, dict[str, list[dict[str, Any]]]],
) -> dict[str, dict[str, Any]]:
    """Aggregate strategy votes by ETF.

    `abstain` rows are represented explicitly and never become score=0. Only
    `vote_status=voted` EvidenceCards contribute to `actionable_score`.
    """
    summaries: dict[str, _MutableSummary] = {}

    for card in evidence_cards or []:
        row = _card_dict(card)
        ticker = _ticker(row)
        strategy = _strategy(row)
        if not ticker or not strategy:
            continue
        summary = summaries.setdefault(ticker, _MutableSummary(ticker=ticker))
        summary.observe_strategy(strategy, counts_in_denominator=True)
        status = str(row.get("vote_status") or "voted").strip() or "voted"
        action = str(row.get("action") or "unknown").strip() or "unknown"
        summary.supporting_actions[action] = summary.supporting_actions.get(action, 0) + 1

        if status == "voted":
            summary.voted_strategies.add(strategy)
            summary.score_rows.append({
                "score": _to_float(row.get("normalized_score")),
                "weight": _score_weight(row),
            })
        elif status == "watch":
            summary.watch_strategies.add(strategy)
        elif status == "mapping_error":
            summary.mapping_error_strategies.add(strategy)
        elif status == "abstain":
            summary.add_abstain(
                strategy=strategy,
                reason=str(row.get("abstain_reason") or "missing_required_field"),
                fields=_missing_fields(row.get("vote_diagnostics") or {}),
            )
        else:
            summary.watch_strategies.add(strategy)
            summary.diagnostics.setdefault("unknown_vote_statuses", []).append({
                "strategy": strategy,
                "status": status,
            })

    for strategy, by_ticker in (input_builder_exclusions or {}).items():
        clean_strategy = str(strategy or "").strip()
        if not clean_strategy or not isinstance(by_ticker, dict):
            continue
        for ticker, reasons in by_ticker.items():
            clean_ticker = str(ticker or "").upper().strip()
            if not clean_ticker:
                continue
            summary = summaries.setdefault(clean_ticker, _MutableSummary(ticker=clean_ticker))
            normalized_reasons = _normalize_exclusion_reasons(reasons)
            counts_in_denominator = not all(
                item["reason"] in DENOMINATOR_EXCLUDED_ABSTAIN_REASONS
                for item in normalized_reasons
            )
            summary.observe_strategy(clean_strategy, counts_in_denominator=counts_in_denominator)
            for item in normalized_reasons:
                summary.add_abstain(
                    strategy=clean_strategy,
                    reason=item["reason"],
                    fields=item["fields"],
                    counts_in_denominator=counts_in_denominator,
                )

    return {
        ticker: summaries[ticker].freeze().to_dict()
        for ticker in sorted(summaries)
    }


def input_builder_exclusions_from_strategy_results(results: list[Any]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Normalize Playground StrategyResult exclusions into the PR2 interface."""
    out: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for result in results or []:
        if isinstance(result, dict):
            strategy = str(result.get("strategy_name") or "").strip()
            excluded = result.get("excluded_tickers") or {}
        else:
            strategy = str(getattr(result, "strategy_name", "") or "").strip()
            excluded = getattr(result, "excluded_tickers", {}) or {}
        if strategy and isinstance(excluded, dict):
            out[strategy] = excluded
    return out


def evidence_cards_from_strategy_results(results: list[Any]) -> list[dict[str, Any]]:
    """Flatten Playground StrategyResult EvidenceCards."""
    cards: list[dict[str, Any]] = []
    for result in results or []:
        raw_cards = result.get("evidence_cards") if isinstance(result, dict) else getattr(result, "evidence_cards", [])
        for card in raw_cards or []:
            cards.append(_card_dict(card))
    return cards


@dataclass
class _MutableSummary:
    ticker: str
    denominator_strategies: set[str] = field(default_factory=set)
    observed_strategies: set[str] = field(default_factory=set)
    voted_strategies: set[str] = field(default_factory=set)
    watch_strategies: set[str] = field(default_factory=set)
    mapping_error_strategies: set[str] = field(default_factory=set)
    abstain_strategies: set[str] = field(default_factory=set)
    abstain_rows: list[dict[str, Any]] = field(default_factory=list)
    supporting_actions: dict[str, int] = field(default_factory=dict)
    score_rows: list[dict[str, float]] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def observe_strategy(self, strategy: str, *, counts_in_denominator: bool) -> None:
        self.observed_strategies.add(strategy)
        if counts_in_denominator:
            self.denominator_strategies.add(strategy)

    def add_abstain(
        self,
        *,
        strategy: str,
        reason: str,
        fields: list[str],
        counts_in_denominator: bool = True,
    ) -> None:
        if counts_in_denominator:
            self.denominator_strategies.add(strategy)
        self.abstain_strategies.add(strategy)
        self.abstain_rows.append({
            "strategy": strategy,
            "reason": reason,
            "fields": sorted(dict.fromkeys(fields)),
            "counts_in_denominator": bool(counts_in_denominator),
        })

    def freeze(self) -> EtfVoteSummary:
        eligible = len(self.denominator_strategies)
        voted = len(self.voted_strategies)
        score = _weighted_score(self.score_rows) if self.score_rows else None
        return EtfVoteSummary(
            ticker=self.ticker,
            voted_count=voted,
            watch_count=len(self.watch_strategies),
            abstain_count=len(self.abstain_strategies),
            mapping_error_count=len(self.mapping_error_strategies),
            eligible_strategy_count=eligible,
            coverage_ratio=round((voted / eligible) if eligible else 0.0, 6),
            actionable_score=score,
            supporting_actions=dict(sorted(self.supporting_actions.items())),
            voted_strategies=sorted(self.voted_strategies),
            watch_strategies=sorted(self.watch_strategies),
            mapping_error_strategies=sorted(self.mapping_error_strategies),
            abstain_reasons=_merge_abstain_rows(self.abstain_rows),
            diagnostics={
                **self.diagnostics,
                "observed_strategy_count": len(self.observed_strategies),
                "denominator_excluded_reasons": sorted(DENOMINATOR_EXCLUDED_ABSTAIN_REASONS),
                "execution_effect": "diagnostic_only",
            },
        )


def _merge_abstain_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, bool], set[str]] = {}
    for row in rows:
        key = (
            str(row.get("strategy") or ""),
            str(row.get("reason") or ""),
            bool(row.get("counts_in_denominator", True)),
        )
        merged.setdefault(key, set()).update(str(field) for field in row.get("fields") or [] if str(field))
    return [
        {
            "strategy": strategy,
            "reason": reason,
            "fields": sorted(fields),
            "counts_in_denominator": counts,
        }
        for (strategy, reason, counts), fields in sorted(merged.items())
        if strategy and reason
    ]


def _normalize_exclusion_reasons(reasons: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for reason in reasons or []:
        if not isinstance(reason, dict):
            reason_type = str(reason or "").strip() or "missing_required_field"
            field = None
        else:
            reason_type = str(reason.get("type") or reason.get("reason") or "").strip() or "missing_required_field"
            field = reason.get("field")
        normalized = ABSTAIN_REASON_MAP.get(reason_type, "missing_required_field")
        fields = [str(field)] if field else []
        out.append({"reason": normalized, "fields": fields})
    return out or [{"reason": "missing_required_field", "fields": []}]


def _card_dict(card: EvidenceCard | dict[str, Any]) -> dict[str, Any]:
    if isinstance(card, EvidenceCard):
        return card.to_dict()
    if isinstance(card, dict):
        return dict(card)
    return {}


def _ticker(row: dict[str, Any]) -> str:
    return str(row.get("ticker") or "").upper().strip()


def _strategy(row: dict[str, Any]) -> str:
    return str(row.get("strategy") or row.get("strategy_name") or "").strip()


def _missing_fields(diagnostics: dict[str, Any]) -> list[str]:
    return [str(item) for item in diagnostics.get("missing_fields") or [] if str(item)]


def _score_weight(row: dict[str, Any]) -> float:
    confidence = _to_float(row.get("confidence"), 0.0)
    effective = _to_float(row.get("effective_confidence"), 0.0)
    return max(confidence, effective, 1e-9)


def _weighted_score(rows: list[dict[str, float]]) -> float | None:
    denom = sum(max(float(row.get("weight") or 0.0), 0.0) for row in rows)
    if denom <= 0:
        return None
    num = sum(
        float(row.get("score") or 0.0) * max(float(row.get("weight") or 0.0), 0.0)
        for row in rows
    )
    return round(num / denom, 6)


def _to_float(value: Any, default: float | None = None) -> float:
    try:
        return float(value) if value is not None else float(default or 0.0)
    except (TypeError, ValueError):
        return float(default or 0.0)
