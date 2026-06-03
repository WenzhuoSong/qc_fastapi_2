#!/usr/bin/env python3
"""Run the post-execution stabilization Global DoD check."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.post_execution_stabilization_check import build_post_execution_stabilization_check  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--qc-fallback",
        default="../quantconnect_files/test1.py",
        help="Path to the QuantConnect algorithm file to compare.",
    )
    parser.add_argument("--output", type=Path, help="Optional JSON output path.")
    parser.add_argument("--summary-only", action="store_true", help="Print compact summary.")
    args = parser.parse_args()

    report = build_post_execution_stabilization_check(
        qc_fallback_path=args.qc_fallback,
        repo_root=REPO_ROOT,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(_json(report), encoding="utf-8")

    payload = _summary(report) if args.summary_only else report
    print(_json(payload))
    return 0 if report.get("status") == "passed" else 1


def _summary(report: dict) -> dict:
    return {
        "contract_version": report.get("contract_version"),
        "status": report.get("status"),
        "passed_count": report.get("passed_count"),
        "failed_count": report.get("failed_count"),
        "failed_checks": [
            row.get("check") for row in report.get("checks") or []
            if not row.get("passed")
        ],
        "mapping_summary": report.get("mapping_summary") or {},
        "alpha_readiness_summary": report.get("alpha_readiness_summary") or {},
        "qc_fallback_policy": report.get("qc_fallback_policy") or {},
    }


def _json(value: dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
