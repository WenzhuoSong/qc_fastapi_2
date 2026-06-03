"""Generate and check the QuantConnect fallback execution policy snippet.

This tool is intentionally read-only for QC files. It derives the expected
fallback constants from services.execution_policy and can compare them against
the QuantConnect algorithm file before deployment.
"""
from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.execution_policy import POLICY_VERSION, ROLE_POLICIES, TICKER_ROLES  # noqa: E402


POLICY_KEYS = ("POLICY_VERSION", "TICKER_ROLES", "ROLE_CAPS")


def expected_qc_fallback_policy() -> dict[str, Any]:
    """Return the FastAPI policy in the compact shape used by QC fallback."""
    return {
        "POLICY_VERSION": POLICY_VERSION,
        "TICKER_ROLES": {
            ticker: role.value
            for ticker, role in sorted(TICKER_ROLES.items())
        },
        "ROLE_CAPS": {
            role.value: {
                "max_single": policy.max_single_weight,
                "max_total_group": policy.max_total_group_weight,
            }
            for role, policy in sorted(ROLE_POLICIES.items(), key=lambda item: item[0].value)
        },
    }


def generate_policy_snippet() -> str:
    """Return deterministic Python class constants for the QC fallback policy."""
    policy = expected_qc_fallback_policy()
    lines = [
        f'POLICY_VERSION = "{policy["POLICY_VERSION"]}"',
        f'TICKER_ROLES = {_compact_json(policy["TICKER_ROLES"])}',
        f'ROLE_CAPS = {_compact_json(policy["ROLE_CAPS"])}',
    ]
    return "\n".join(lines) + "\n"


def load_qc_fallback_policy(path: Path | str) -> dict[str, Any]:
    """Load QC fallback policy constants from a QuantConnect algorithm file."""
    file_path = Path(path)
    tree = ast.parse(file_path.read_text(encoding="utf-8"))
    class_node = _find_qc_algorithm_class(tree)
    env: dict[str, Any] = {}

    for node in class_node.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if target.id in {"POLICY_VERSION", "TICKER_ROLES", "ROLE_CAPS"}:
            env[target.id] = ast.literal_eval(node.value)

    if "POLICY_VERSION" not in env:
        version = _find_compiled_policy_version(class_node)
        if version:
            env["POLICY_VERSION"] = version

    missing = [key for key in POLICY_KEYS if key not in env]
    if missing:
        raise ValueError(f"{file_path} missing QC fallback policy keys: {missing}")

    return {
        "POLICY_VERSION": str(env["POLICY_VERSION"]),
        "TICKER_ROLES": {
            str(ticker): str(role)
            for ticker, role in dict(env["TICKER_ROLES"]).items()
        },
        "ROLE_CAPS": _normalize_role_caps(dict(env["ROLE_CAPS"])),
    }


def compare_qc_fallback_policy(path: Path | str) -> dict[str, Any]:
    """Compare a QC file's fallback policy with FastAPI's canonical policy."""
    expected = expected_qc_fallback_policy()
    actual = load_qc_fallback_policy(path)
    differences: list[dict[str, Any]] = []

    if actual["POLICY_VERSION"] != expected["POLICY_VERSION"]:
        differences.append({
            "field": "POLICY_VERSION",
            "expected": expected["POLICY_VERSION"],
            "actual": actual["POLICY_VERSION"],
        })

    role_diff = _dict_diff(expected["TICKER_ROLES"], actual["TICKER_ROLES"])
    if role_diff:
        differences.append({"field": "TICKER_ROLES", **role_diff})

    caps_diff = _dict_diff(expected["ROLE_CAPS"], actual["ROLE_CAPS"])
    if caps_diff:
        differences.append({"field": "ROLE_CAPS", **caps_diff})

    return {
        "ok": not differences,
        "differences": differences,
        "expected_counts": {
            "tickers": len(expected["TICKER_ROLES"]),
            "roles": len(expected["ROLE_CAPS"]),
        },
        "actual_counts": {
            "tickers": len(actual["TICKER_ROLES"]),
            "roles": len(actual["ROLE_CAPS"]),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print", action="store_true", dest="print_snippet")
    parser.add_argument("--check", type=Path, help="Path to QuantConnect algorithm file.")
    parser.add_argument("--json", action="store_true", help="Emit check result as JSON.")
    args = parser.parse_args()

    if args.print_snippet or not args.check:
        print(generate_policy_snippet(), end="")

    if args.check:
        result = compare_qc_fallback_policy(args.check)
        if args.json:
            print(json.dumps(result, indent=2, sort_keys=True))
        elif result["ok"]:
            print(f"QC fallback policy is in sync: {args.check}")
        else:
            print(f"QC fallback policy drift detected: {args.check}", file=sys.stderr)
            for diff in result["differences"]:
                print(json.dumps(diff, sort_keys=True), file=sys.stderr)
        return 0 if result["ok"] else 1

    return 0


def _compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _find_qc_algorithm_class(tree: ast.Module) -> ast.ClassDef:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "QCAgenticV1":
            return node
    raise ValueError("QCAgenticV1 class not found")


def _find_compiled_policy_version(class_node: ast.ClassDef) -> str | None:
    for node in ast.walk(class_node):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if (
                isinstance(target, ast.Attribute)
                and target.attr == "_execution_policy"
                and isinstance(node.value, ast.Dict)
            ):
                version = _dict_constant(node.value, "version")
                if version is not None:
                    return str(version)
    return None


def _dict_constant(node: ast.Dict, key_name: str) -> Any:
    for key, value in zip(node.keys, node.values):
        if isinstance(key, ast.Constant) and key.value == key_name:
            if isinstance(value, ast.Constant):
                return value.value
    return None


def _normalize_role_caps(raw_caps: dict[str, Any]) -> dict[str, dict[str, float]]:
    normalized: dict[str, dict[str, float]] = {}
    for role, policy in raw_caps.items():
        policy_dict = dict(policy or {})
        normalized[str(role)] = {
            "max_single": float(policy_dict.get("max_single", 0.0) or 0.0),
            "max_total_group": float(policy_dict.get("max_total_group", 0.0) or 0.0),
        }
    return normalized


def _dict_diff(expected: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    changed = {
        key: {"expected": expected[key], "actual": actual[key]}
        for key in sorted(set(expected) & set(actual))
        if expected[key] != actual[key]
    }
    out: dict[str, Any] = {}
    if missing:
        out["missing"] = missing
    if extra:
        out["extra"] = extra
    if changed:
        out["changed"] = changed
    return out


if __name__ == "__main__":
    raise SystemExit(main())
