"""Deterministic thesis review scheduler.

The scheduler decides when a position needs thesis review. It does not judge
the thesis and has no execution authority.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any


REVIEW_STATES = {"loss_review", "loss_trim_candidate", "basket_loss_review", "hard_risk_review"}


@dataclass(frozen=True)
class ThesisReviewConfig:
    scheduled_review_days: int = 5
    pnl_change_threshold: float = 0.03


@dataclass(frozen=True)
class ThesisReviewDecision:
    ticker: str
    required: bool
    reason: str
    position_state: str
    thesis_status: str
    last_review_at: str | None
    days_since_review: int | None
    pnl_change_since_review: float | None
    execution_authority: str = "none"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def get_review_required(
    *,
    ticker: str,
    position_state: str,
    thesis_status: str | None = None,
    last_thesis_review_at: str | datetime | None = None,
    current_pnl_pct: float | None = None,
    pnl_at_last_review: float | None = None,
    basket_review_active: bool = False,
    now: datetime | None = None,
    config: ThesisReviewConfig | None = None,
) -> ThesisReviewDecision:
    cfg = config or ThesisReviewConfig()
    current_time = now or datetime.now(UTC)
    last_review = _parse_datetime(last_thesis_review_at)
    days_since = (current_time - last_review).days if last_review else None
    pnl_change = _pnl_change(current_pnl_pct, pnl_at_last_review)
    clean_state = str(position_state or "unknown")
    clean_status = str(thesis_status or "unknown")
    clean_ticker = str(ticker or "").upper().strip()

    if clean_state in REVIEW_STATES:
        return ThesisReviewDecision(
            ticker=clean_ticker,
            required=True,
            reason=f"position_state_{clean_state}_requires_daily_review",
            position_state=clean_state,
            thesis_status=clean_status,
            last_review_at=last_review.isoformat() if last_review else None,
            days_since_review=days_since,
            pnl_change_since_review=pnl_change,
        )
    if basket_review_active:
        return ThesisReviewDecision(
            ticker=clean_ticker,
            required=True,
            reason="basket_review_requires_daily_review",
            position_state=clean_state,
            thesis_status=clean_status,
            last_review_at=last_review.isoformat() if last_review else None,
            days_since_review=days_since,
            pnl_change_since_review=pnl_change,
        )
    if last_review is None:
        return ThesisReviewDecision(
            ticker=clean_ticker,
            required=True,
            reason="never_reviewed",
            position_state=clean_state,
            thesis_status=clean_status,
            last_review_at=None,
            days_since_review=None,
            pnl_change_since_review=pnl_change,
        )
    if days_since is not None and days_since >= cfg.scheduled_review_days:
        return ThesisReviewDecision(
            ticker=clean_ticker,
            required=True,
            reason=f"scheduled_review_{days_since}d_elapsed",
            position_state=clean_state,
            thesis_status=clean_status,
            last_review_at=last_review.isoformat(),
            days_since_review=days_since,
            pnl_change_since_review=pnl_change,
        )
    if pnl_change is not None and abs(pnl_change) >= cfg.pnl_change_threshold:
        return ThesisReviewDecision(
            ticker=clean_ticker,
            required=True,
            reason=f"pnl_change_{pnl_change:.1%}_triggers_review",
            position_state=clean_state,
            thesis_status=clean_status,
            last_review_at=last_review.isoformat(),
            days_since_review=days_since,
            pnl_change_since_review=pnl_change,
        )
    return ThesisReviewDecision(
        ticker=clean_ticker,
        required=False,
        reason="no_review_needed",
        position_state=clean_state,
        thesis_status=clean_status,
        last_review_at=last_review.isoformat(),
        days_since_review=days_since,
        pnl_change_since_review=pnl_change,
    )


def build_thesis_review_queue(
    decisions: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    config: ThesisReviewConfig | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in decisions:
        if not isinstance(row, dict):
            continue
        thesis = row.get("thesis_status") or {}
        review = get_review_required(
            ticker=str(row.get("ticker") or ""),
            position_state=str(row.get("position_state") or _position_state_from_decision(row)),
            thesis_status=str(thesis.get("status") or "unknown"),
            last_thesis_review_at=(
                row.get("last_thesis_review_at")
                or thesis.get("last_review_at")
                or thesis.get("reviewed_at")
            ),
            current_pnl_pct=_optional_float(row.get("unrealized_pnl_pct")),
            pnl_at_last_review=_optional_float(row.get("pnl_at_last_thesis_review")),
            basket_review_active=bool(row.get("basket_review")),
            now=now,
            config=config,
        ).to_dict()
        if review["required"]:
            review["review_input"] = _review_input(row, thesis, review)
            rows.append(review)
    return sorted(rows, key=lambda item: _review_priority(item))


def _review_input(row: dict[str, Any], thesis: dict[str, Any], review: dict[str, Any]) -> dict[str, Any]:
    return {
        "review_purpose": "thesis_review",
        "ticker": review.get("ticker"),
        "current_state": {
            "position_state": review.get("position_state"),
            "unrealized_pnl_pct": row.get("unrealized_pnl_pct"),
            "holding_days": row.get("holding_days"),
            "last_thesis_status": thesis.get("status") or "unknown",
            "last_review_at": review.get("last_review_at"),
            "review_reason": review.get("reason"),
        },
        "evidence": {
            "basket_review": row.get("basket_review"),
            "strategy_support": row.get("strategy_support"),
            "risk_budget_status": row.get("risk_budget_status"),
            "reason_codes": row.get("reason_codes") or [],
            "thesis_evidence": thesis.get("evidence") or [],
        },
        "execution_authority": "none",
    }


def _position_state_from_decision(row: dict[str, Any]) -> str:
    reasons = set(str(item) for item in row.get("reason_codes") or [])
    if "hard_risk" in reasons:
        return "hard_risk_review"
    if "unrealized_loss_review" in reasons:
        return "loss_trim_candidate" if row.get("decision") == "trim" else "loss_review"
    if "basket_review" in reasons:
        return "basket_loss_review"
    return "normal_hold"


def _review_priority(item: dict[str, Any]) -> tuple[int, str]:
    reason = str(item.get("reason") or "")
    if "hard_risk" in reason:
        score = 0
    elif "position_state" in reason or "basket_review" in reason:
        score = 1
    elif "pnl_change" in reason:
        score = 2
    elif "scheduled_review" in reason:
        score = 3
    else:
        score = 4
    return (score, str(item.get("ticker") or ""))


def _parse_datetime(value: str | datetime | None) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _pnl_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return round(float(current) - float(previous), 6)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
