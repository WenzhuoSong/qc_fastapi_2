"""Scout knowledge base: the feature dictionary and the analysis lenses.

`features.yaml` is the single definition of every feature (meaning, unit,
interpretation bands). `lenses/*.md` are the methodology checklists the AI
interpretation follows. Both are plain files meant to be edited by hand.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

ROOT = Path(__file__).parent


@lru_cache()
def features() -> dict[str, dict]:
    return yaml.safe_load((ROOT / "features.yaml").read_text(encoding="utf-8"))


def zh(key: str) -> str:
    return (features().get(key) or {}).get("zh", key)


def reading(key: str, value) -> str | None:
    """First interpretation band that contains `value`."""
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        return None
    for band in (features().get(key) or {}).get("readings") or []:
        lo, hi = band.get("min"), band.get("max")
        if (lo is None or value >= lo) and (hi is None or value <= hi):
            return band["text"]
    return None


def fmt(key: str, value) -> str:
    """Human formatting by unit — the exact strings the LLM sees and must quote."""
    if value is None:
        return "—"
    if isinstance(value, str):
        return value
    unit = (features().get(key) or {}).get("unit")
    if unit == "pct":
        return f"{value * 100:+.1f}%"
    if unit == "pct_mag":
        return f"{value * 100:.1f}%"
    if unit == "score01":
        return f"{value * 100:.0f}%"
    if unit == "ratio":
        return f"{value:.1f} 倍"
    if unit == "score100":
        return f"{value:.1f}"
    if unit == "signed":
        return f"{value:+.2f}"
    if unit == "sigma":
        return f"{value:+.1f}σ"
    if unit == "rsi":
        return f"{value:.0f}"
    if unit in ("beta", "corr"):
        return f"{value:.2f}"
    if unit == "usd":
        sign, mag = ("-" if value < 0 else ""), abs(value)
        return f"{sign}${mag / 1e9:.1f}B" if mag >= 1e9 else f"{sign}${mag / 1e6:.1f}M"
    if unit == "count":
        return f"{value:.0f}"
    if unit == "days":
        return f"{value:.1f} 天"
    if unit == "num":
        return f"{value:.2f}"
    if unit == "pct2":
        return f"{value * 100:+.2f}%"
    if unit == "text":
        return str(value)
    if unit == "pp":
        return f"{value * 100:+.1f} 个百分点"
    return f"{value:.2f}"


@dataclass
class Evidence:
    key: str
    name: str
    value: str
    reading: str | None
    definition: str

    def as_dict(self) -> dict:
        out = {"字段": self.key, "名称": self.name, "数值": self.value, "含义": self.definition}
        if self.reading:
            out["读数"] = self.reading
        return out


def evidence(key: str, value) -> Evidence:
    spec = features().get(key) or {}
    return Evidence(key, spec.get("zh", key), fmt(key, value), reading(key, value), spec.get("definition", ""))


# ── lenses ──────────────────────────────────────────────────────────────

@dataclass
class Lens:
    id: str
    title: str
    fields: list[str]
    body: str              # methodology checklist (markdown)
    sources: list[str]


_FRONT = re.compile(r"^---\n(.*?)\n---\n(.*)$", re.S)


@lru_cache()
def lenses() -> list[Lens]:
    out = []
    for path in sorted((ROOT / "lenses").glob("*.md")):
        m = _FRONT.match(path.read_text(encoding="utf-8"))
        if not m:
            raise ValueError(f"{path.name}: missing front matter")
        meta = yaml.safe_load(m.group(1))
        out.append(Lens(meta["id"], meta["title"], meta.get("fields", []), m.group(2).strip(), meta.get("sources", [])))
    return out


def lens(lens_id: str) -> Lens:
    for item in lenses():
        if item.id == lens_id:
            return item
    raise KeyError(lens_id)


# ── Field names never reach the user ──
# The LLM sees feature keys in the evidence pack and sometimes echoes them
# ("(swing_trend)", "avg_edge_vs_hold 为 -1.4%"). Citations live in their own
# fields; prose gets the Chinese names.
STAT_ZH = {
    "avg_edge_vs_hold": "扣费后相对持有", "beat_hold_rate": "跑赢持有比例", "win_rate_net": "扣费胜率",
    "win_rate": "胜率", "avg_excess": "相对 SPY 超额", "avg_net_excess": "扣费后相对 SPY", "avg_return": "平均收益",
    "median_return": "收益中位数", "target_rate": "达标率", "stop_rate": "止损率", "avg_hold_return": "持有收益",
}


def _field_names() -> dict[str, str]:
    names = {k: (v or {}).get("zh", k) for k, v in features().items()}
    return {**names, **STAT_ZH}


_KEY = r"[a-z][a-z0-9]*(?:_[a-z0-9]+)+|[a-z]+\d+"      # snake_case or rsi14-style
# Evidence citations (news_4, wrap_1, earnings.next.date, XLE.rel_20, q.ES=F) belong in the evidence list.
_CITE = re.compile(r"(?:news|macro|wrap|econw?|focus|event|filing|web|insiders|earnings|kb)(?:_\d+)?|[A-Za-z][\w^=-]*\.[\w.]+")


def humanize(text: str) -> str:
    if not text:
        return text
    names = _field_names()

    def drop_group(m: re.Match) -> str:
        keys = [k.strip() for k in re.split(r"[,，、/]", m.group(1))]
        return "" if keys and all(k in names or _CITE.fullmatch(k) for k in keys) else m.group(0)

    tok = r"[A-Za-z][\w.=^-]*"
    text = re.sub(rf"\s*[（(]\s*({tok}(?:\s*[,，、/]\s*{tok})*)\s*[)）]", drop_group, text)
    return re.sub(rf"(?<![A-Za-z0-9_])({_KEY})(?![A-Za-z0-9_])", lambda m: names.get(m.group(1), m.group(1)), text)


def humanize_obj(value):
    """humanize every string in a dict / list (an LLM output dump)."""
    if isinstance(value, str):
        return humanize(value)
    if isinstance(value, list):
        return [humanize_obj(v) for v in value]
    if isinstance(value, dict):
        return {k: (v if k in ("evidence", "cites", "keys") else humanize_obj(v)) for k, v in value.items()}
    return value
