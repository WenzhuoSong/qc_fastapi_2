"""
Structured trading knowledge base loader.

The knowledge base is intentionally small and deterministic. It gives agents
stable context about strategies, assets, regimes, and risk principles without
allowing prose knowledge to bypass the rule engines.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

try:  # PyYAML is in production requirements; fallback keeps local tests portable.
    import yaml
except ModuleNotFoundError:  # pragma: no cover - exercised only in minimal envs.
    yaml = None


KNOWLEDGE_ROOT = Path(__file__).resolve().parents[1] / "knowledge"

COLLECTIONS = ("strategies", "assets", "regimes", "risk_principles")

REASON_TO_PRINCIPLE = {
    "high_atr": "high_atr_no_add",
    "atr_elevated": "high_atr_no_add",
    "high_risk_contribution": "high_atr_no_add",
    "loss_review": "loss_review",
    "unrealized_loss_review": "loss_review",
    "deep_loss_weak_support": "loss_review",
    "sector_concentration": "sector_concentration",
    "concentration_high": "sector_concentration",
    "scorecard_human_required": "human_required_boundary",
    "human_required": "human_required_boundary",
    "execution_blocked": "human_required_boundary",
    "replacement_candidate": "replacement_budget",
    "freed_cash_only": "replacement_budget",
    "llm_advisory": "llm_advisory_boundary",
    "llm_advisory_validated": "llm_advisory_boundary",
    "advisory_override": "llm_advisory_boundary",
    "staged_trim": "staged_trim",
}

LEVERAGED_TICKERS = {"SOXL", "SOXS", "TQQQ", "SQQQ", "UPRO", "SPXU"}


def load_knowledge_base(root: Path | str | None = None) -> dict[str, Any]:
    """Load and minimally validate all YAML knowledge files."""
    root_path = Path(root) if root is not None else KNOWLEDGE_ROOT
    return _load_knowledge_base_cached(str(root_path.resolve()))


@lru_cache(maxsize=4)
def _load_knowledge_base_cached(root_str: str) -> dict[str, Any]:
    root = Path(root_str)
    collections: dict[str, dict[str, dict[str, Any]]] = {name: {} for name in COLLECTIONS}
    warnings: list[str] = []

    for collection in COLLECTIONS:
        folder = root / collection
        if not folder.exists():
            warnings.append(f"knowledge collection missing: {collection}")
            continue
        for file_path in sorted(folder.glob("*.yaml")):
            item = _read_yaml(file_path, warnings)
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id") or file_path.stem)
            item_type = str(item.get("type") or "")
            if not item_type:
                warnings.append(f"{file_path.name}: missing type")
            if item_type and item_type != collection.rstrip("s"):
                if collection == "strategies" and item_type == "strategy":
                    pass
                elif collection == "risk_principles" and item_type == "risk_principle":
                    pass
                else:
                    warnings.append(f"{file_path.name}: type {item_type} mismatches {collection}")
            item["_source_file"] = str(file_path.relative_to(root))
            collections[collection][item_id] = item

    source_registry = _read_yaml(root / "sources" / "registry.yaml", warnings)
    sources = {}
    if isinstance(source_registry, dict):
        sources = source_registry.get("sources") or {}
    if not isinstance(sources, dict):
        warnings.append("source registry is missing a sources mapping")
        sources = {}

    return {
        **collections,
        "sources": sources,
        "object_counts": {
            "strategies": len(collections["strategies"]),
            "assets": len(collections["assets"]),
            "regimes": len(collections["regimes"]),
            "risk_principles": len(collections["risk_principles"]),
            "sources": len(sources),
        },
        "warnings": _unique(warnings),
    }


def build_knowledge_context(
    *,
    tickers: list[str] | tuple[str, ...] | set[str] | None = None,
    strategy_names: list[str] | tuple[str, ...] | set[str] | None = None,
    regime: str | None = None,
    reason_codes: list[str] | tuple[str, ...] | set[str] | None = None,
    root: Path | str | None = None,
    max_assets: int = 12,
    max_risk_principles: int = 6,
) -> dict[str, Any]:
    """Return compact, prompt-safe knowledge relevant to the current run."""
    kb = load_knowledge_base(root)
    selected_tickers = _normalize_symbols(tickers)
    selected_strategies = _normalize_ids(strategy_names)
    selected_reasons = _normalize_ids(reason_codes)
    regime_id = str(regime or "").strip()

    strategies = [
        _compact_strategy(item)
        for item_id, item in kb["strategies"].items()
        if item_id in selected_strategies
    ]

    assets = [
        _compact_asset(kb["assets"][ticker])
        for ticker in selected_tickers[:max_assets]
        if ticker in kb["assets"]
    ]

    regimes = []
    if regime_id and regime_id in kb["regimes"]:
        regimes.append(_compact_regime(kb["regimes"][regime_id]))
    if regime_id == "trending_bull" and "risk_on" in kb["regimes"]:
        regimes.append(_compact_regime(kb["regimes"]["risk_on"]))

    principle_ids = _principle_ids(
        tickers=selected_tickers,
        reason_codes=selected_reasons,
    )
    risk_principles = [
        _compact_risk_principle(kb["risk_principles"][principle_id])
        for principle_id in principle_ids[:max_risk_principles]
        if principle_id in kb["risk_principles"]
    ]

    missing_assets = [ticker for ticker in selected_tickers if ticker not in kb["assets"]]
    missing_strategies = [
        name for name in selected_strategies if name not in kb["strategies"]
    ]
    warnings = list(kb.get("warnings") or [])
    if missing_assets:
        warnings.append(f"knowledge missing assets: {', '.join(missing_assets[:8])}")
    if missing_strategies:
        warnings.append(f"knowledge missing strategies: {', '.join(missing_strategies[:8])}")

    return {
        "available": True,
        "object_counts": kb["object_counts"],
        "selection": {
            "tickers": selected_tickers[:max_assets],
            "strategies": selected_strategies,
            "regime": regime_id or None,
            "reason_codes": selected_reasons,
        },
        "strategies": strategies,
        "assets": assets,
        "regimes": regimes,
        "risk_principles": risk_principles,
        "warnings": _unique(warnings),
    }


def _read_yaml(file_path: Path, warnings: list[str]) -> Any:
    try:
        with file_path.open("r", encoding="utf-8") as handle:
            text = handle.read()
            if yaml is not None:
                return yaml.safe_load(text) or {}
            return _parse_yaml_subset(text)
    except FileNotFoundError:
        warnings.append(f"knowledge file missing: {file_path.name}")
    except Exception as exc:
        if yaml is not None and isinstance(exc, yaml.YAMLError):
            warnings.append(f"{file_path.name}: invalid yaml: {exc}")
        else:
            warnings.append(f"{file_path.name}: failed to load knowledge yaml: {exc}")
    return {}


def _parse_yaml_subset(text: str) -> Any:
    """Parse the controlled YAML subset used by this repository's knowledge files."""
    lines = []
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        lines.append((indent, raw.strip()))
    value, _ = _parse_block(lines, 0, lines[0][0] if lines else 0)
    return value


def _parse_block(lines: list[tuple[int, str]], index: int, indent: int) -> tuple[Any, int]:
    if index >= len(lines):
        return {}, index
    is_list = lines[index][1].startswith("- ")
    if is_list:
        out: list[Any] = []
        while index < len(lines):
            current_indent, content = lines[index]
            if current_indent < indent or not content.startswith("- "):
                break
            if current_indent > indent:
                break
            item_text = content[2:].strip()
            if item_text:
                out.append(_parse_scalar(item_text))
                index += 1
            else:
                item, index = _parse_block(lines, index + 1, indent + 2)
                out.append(item)
        return out, index

    out: dict[str, Any] = {}
    while index < len(lines):
        current_indent, content = lines[index]
        if current_indent < indent:
            break
        if current_indent > indent:
            break
        if ":" not in content:
            index += 1
            continue
        key, value_text = content.split(":", 1)
        key = key.strip()
        value_text = value_text.strip()
        if value_text:
            out[key] = _parse_scalar(value_text)
            index += 1
        else:
            value, index = _parse_block(lines, index + 1, indent + 2)
            out[key] = value
    return out, index


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if value in {"null", "None", "~"}:
        return None
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part.strip()) for part in inner.split(",")]
    if value.startswith("{") and value.endswith("}"):
        inner = value[1:-1].strip()
        if not inner:
            return {}
        out: dict[str, Any] = {}
        for part in inner.split(","):
            if ":" not in part:
                continue
            key, raw = part.split(":", 1)
            out[key.strip()] = _parse_scalar(raw.strip())
        return out
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value.strip("\"'")


def _principle_ids(*, tickers: list[str], reason_codes: list[str]) -> list[str]:
    ids: list[str] = []
    for code in reason_codes:
        lowered = code.lower()
        if lowered in REASON_TO_PRINCIPLE:
            ids.append(REASON_TO_PRINCIPLE[lowered])
        if "atr" in lowered or "volatility" in lowered:
            ids.append("high_atr_no_add")
        if "concentration" in lowered or "crowded" in lowered:
            ids.append("sector_concentration")
        if "human_required" in lowered or "blocked" in lowered:
            ids.append("human_required_boundary")
        if "turnover" in lowered or "trim" in lowered:
            ids.append("staged_trim")
    if any(ticker in LEVERAGED_TICKERS for ticker in tickers):
        ids.append("leveraged_etf")
    return _unique(ids)


def _compact_strategy(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "category": item.get("category"),
        "summary": item.get("summary"),
        "best_regimes": item.get("best_regimes") or [],
        "weak_regimes": item.get("weak_regimes") or [],
        "required_features": item.get("required_features") or [],
        "failure_modes": item.get("failure_modes") or [],
        "governance_implications": item.get("governance_implications") or [],
        "sources": item.get("sources") or [],
    }


def _compact_asset(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "asset_class": item.get("asset_class"),
        "sector_group": item.get("sector_group"),
        "summary": item.get("summary"),
        "risk_drivers": item.get("risk_drivers") or [],
        "positive_regimes": item.get("positive_regimes") or [],
        "weak_regimes": item.get("weak_regimes") or [],
        "holding_policy": item.get("holding_policy"),
        "governance_notes": item.get("governance_notes") or [],
        "sources": item.get("sources") or [],
    }


def _compact_regime(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "summary": item.get("summary"),
        "supports_strategies": item.get("supports_strategies") or [],
        "weak_strategies": item.get("weak_strategies") or [],
        "risk_notes": item.get("risk_notes") or [],
        "sources": item.get("sources") or [],
    }


def _compact_risk_principle(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "summary": item.get("summary"),
        "trigger_conditions": item.get("trigger_conditions") or [],
        "governance_action": item.get("governance_action"),
        "applies_to": item.get("applies_to") or [],
        "cannot_override": item.get("cannot_override") or [],
        "sources": item.get("sources") or [],
    }


def _normalize_symbols(values) -> list[str]:
    return _unique(
        str(value).strip().upper()
        for value in (values or [])
        if str(value or "").strip()
    )


def _normalize_ids(values) -> list[str]:
    return _unique(
        str(value).strip()
        for value in (values or [])
        if str(value or "").strip()
    )


def _unique(values) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out
