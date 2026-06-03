"""Run the strategy/ETF mapping audit and optionally save a baseline artifact."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.strategy_mapping_audit import build_current_strategy_mapping_audit  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON file path for the audit output.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print only summary metrics to stdout.",
    )
    args = parser.parse_args()

    audit = build_current_strategy_mapping_audit()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(_to_json(audit), encoding="utf-8")

    if args.summary_only:
        print(_to_json(_summary(audit)))
    else:
        print(_to_json(audit))
    return 0


def _summary(audit: dict[str, Any]) -> dict[str, Any]:
    return {
        "contract_version": audit.get("contract_version"),
        "generated_at": audit.get("generated_at"),
        "total_rows": audit.get("total_rows"),
        "hard_mapping_error_count": audit.get("hard_mapping_error_count"),
        "normal_watch_count": audit.get("normal_watch_count"),
        "by_reason": audit.get("by_reason") or {},
        "missing_strategy_profiles": audit.get("missing_strategy_profiles") or [],
        "missing_asset_profiles_count": len(audit.get("missing_asset_profiles") or []),
        "top_hard_mapping_errors": audit.get("top_hard_mapping_errors") or [],
    }


def _to_json(value: dict[str, Any]) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
