"""Run a one-shot historical-prior signal replay backfill.

Default mode is dry-run. Pass ``--write`` to persist yfinance_replay signals,
outcomes, and conviction profiles.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.session import AsyncSessionLocal
from services.historical_signal_backfill import (
    DEFAULT_HISTORICAL_BACKFILL_STRATEGIES,
    run_historical_signal_backfill,
)
from services.historical_signal_replay import DEFAULT_HORIZONS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill historical-prior strategy signal labels")
    parser.add_argument("--write", action="store_true", help="Persist rows to DB; omitted means dry-run")
    parser.add_argument("--start-date", type=_parse_date, default=None)
    parser.add_argument("--end-date", type=_parse_date, default=None)
    parser.add_argument("--max-dates", type=int, default=120, help="Use the latest N trading dates in range")
    parser.add_argument("--source", default="yfinance", help="market_daily_features source")
    parser.add_argument(
        "--horizons",
        default=",".join(str(item) for item in DEFAULT_HORIZONS),
        help="Comma-separated horizons, e.g. 1,5,20",
    )
    parser.add_argument(
        "--strategy",
        action="append",
        default=None,
        help="Strategy name; repeat for multiple. Defaults to playground strategy set.",
    )
    parser.add_argument("--profile-signal-limit", type=int, default=20000)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    strategies = args.strategy or DEFAULT_HISTORICAL_BACKFILL_STRATEGIES
    horizons = _parse_horizons(args.horizons)
    async with AsyncSessionLocal() as db:
        result = await run_historical_signal_backfill(
            db,
            strategy_names=strategies,
            horizons=horizons,
            start_date=args.start_date,
            end_date=args.end_date,
            max_dates=args.max_dates,
            source=args.source,
            write=bool(args.write),
            profile_signal_limit=args.profile_signal_limit,
        )
    print(json.dumps(_json_safe(result.to_dict()), indent=2, sort_keys=True))


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value[:10])


def _parse_horizons(value: Any) -> tuple[int, ...]:
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    else:
        items = list(value or [])
    parsed = sorted({int(item) for item in items if str(item).strip()})
    return tuple(parsed) or DEFAULT_HORIZONS


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date,)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


if __name__ == "__main__":
    asyncio.run(main())
