#!/usr/bin/env python3
"""Generate the diagnostic alpha readiness report for the current universe."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.alpha_readiness_report import build_current_alpha_readiness_report  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate alpha readiness report.")
    parser.add_argument("--output", help="Optional JSON output path.")
    parser.add_argument("--summary-only", action="store_true", help="Print compact summary.")
    args = parser.parse_args()

    report = build_current_alpha_readiness_report()
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    payload = report
    if args.summary_only:
        payload = {
            "contract_version": report.get("contract_version"),
            "status": report.get("status"),
            "strategy_count": report.get("strategy_count"),
            "authority_counts": report.get("authority_counts"),
            "hard_mapping_error_count": report.get("hard_mapping_error_count"),
            "candidate_count": report.get("candidate_count"),
            "warnings": report.get("warnings") or [],
        }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
