#!/usr/bin/env python3
"""Print the latest review-only newBase operator snapshot.

This tool reads stored QC/newBase telemetry and never contacts QC, submits
orders, or mutates target weights.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.newbase_monitoring import (  # noqa: E402
    format_newbase_operator_snapshot_text,
    load_latest_newbase_operator_snapshot,
)


async def _amain() -> int:
    parser = argparse.ArgumentParser(description="Generate newBase operator snapshot.")
    parser.add_argument("--limit", type=int, default=90, help="Number of recent live snapshots to read.")
    parser.add_argument("--text", action="store_true", help="Print compact text instead of JSON.")
    parser.add_argument("--output", help="Optional path to write JSON snapshot.")
    args = parser.parse_args()

    snapshot = await load_latest_newbase_operator_snapshot(limit=args.limit)
    if snapshot is None:
        print("No newBase live snapshots found", file=sys.stderr)
        return 1

    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.text:
        print(format_newbase_operator_snapshot_text(snapshot))
    else:
        print(json.dumps(snapshot, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_amain()))
