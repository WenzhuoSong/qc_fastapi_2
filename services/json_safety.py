"""Helpers for values that must be persisted as JSON/JSONB."""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any


def json_safe(value: Any) -> Any:
    """Recursively convert Python-only containers into JSON-safe values."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [json_safe(item) for item in sorted(value, key=lambda item: str(item))]
    if is_dataclass(value):
        return json_safe(asdict(value))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)
