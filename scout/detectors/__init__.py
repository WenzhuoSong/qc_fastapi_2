"""Opportunity detectors.

A detector is a deterministic rule over the FeaturePanel, written as declarative
`Condition`s. `mask(panel)` ANDs them into a wide boolean frame (date × ticker),
`explain(row)` evaluates the same conditions for one stock ("why it was picked"),
and `levels(row)` turns the feature row at a hit into entry / stop / target prices. Live scanning reads the last date of
the mask; base-rate replay reads every date. Same code, same inputs.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd

from scout.features import FeaturePanel


@dataclass(frozen=True)
class Levels:
    entry_lo: float
    entry_hi: float
    stop: float
    target1: float
    target2: float

    @property
    def entry_mid(self) -> float:
        return (self.entry_lo + self.entry_hi) / 2

    @property
    def risk_reward(self) -> float:
        risk = self.entry_mid - self.stop
        return (self.target1 - self.entry_mid) / risk if risk > 0 else 0.0

    def valid(self) -> bool:
        return 0 < self.stop < self.entry_lo <= self.entry_hi < self.target1 <= self.target2


@dataclass(frozen=True)
class ExitPolicy:
    """How a position is closed. `fixed`: protective stop, all out at target 1,
    expiry at the rule's horizon. `trail_low`: protective stop, then once the
    trade is `lookback` sessions old the stop ratchets up to the lowest low of
    the last `lookback` sessions (never down); no fixed target, `max_hold` cap."""
    kind: str = "fixed"
    lookback: int = 10
    max_hold: int = 60

    @property
    def trailing(self) -> bool:
        return self.kind == "trail_low"

    def holding_limit(self, horizon: int) -> int:
        return self.max_hold if self.trailing else horizon

    def describe(self, horizon: int) -> str:
        if self.trailing:
            return (f"跟进止损：先用保护性止损；持有满 {self.lookback} 个交易日后，止损上移到近 {self.lookback} 日最低价（只升不降）；"
                    f"不设固定止盈，最长持有 {self.max_hold} 个交易日")
        return f"固定：跌破保护性止损离场，到目标 1 全部止盈，{horizon} 个交易日后到期离场"


FIXED_EXIT = ExitPolicy()
TRAIL_LOW_10 = ExitPolicy("trail_low", lookback=10, max_hold=60)


@dataclass(frozen=True)
class Ref:
    """Right-hand side of a condition: max(floor, mult × field), or a constant."""
    field: str | None = None
    mult: float = 1.0
    floor: float | None = None

    def series(self, p: FeaturePanel):
        v = p[self.field] * self.mult
        return np.maximum(self.floor, v) if self.floor is not None else v

    def value(self, row: dict) -> float | None:
        x = row.get(self.field)
        if x is None:
            return None
        v = x * self.mult
        return max(self.floor, v) if self.floor is not None else v


_OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}


@dataclass(frozen=True)
class Condition:
    """One declarative rule condition. The same object filters the panel
    (vectorised) and explains a hit ("why it was picked"), so the shown
    reasons can never drift from the logic that selected the stock."""
    label: str
    left: str
    op: str
    right: float | str | Ref

    def series(self, p: FeaturePanel) -> pd.DataFrame:
        rhs = self.right
        if isinstance(rhs, str):
            rhs = p[rhs]
        elif isinstance(rhs, Ref):
            rhs = rhs.series(p)
        return _OPS[self.op](p[self.left], rhs)

    def check(self, row: dict) -> dict:
        from scout import knowledge

        left = row.get(self.left)
        if isinstance(self.right, str):
            right = row.get(self.right)
        elif isinstance(self.right, Ref):
            right = self.right.value(row)
        else:
            right = self.right
        ok = left is not None and right is not None and bool(_OPS[self.op](left, right))
        return {
            "label": self.label,
            "ok": ok,
            "actual": knowledge.fmt(self.left, left),
            "op": self.op.replace(">=", "≥").replace("<=", "≤"),
            "threshold": knowledge.fmt(self.left, right),
        }


@dataclass(frozen=True)
class Detector:
    name: str
    label: str
    kind: str                 # setup: actionable long idea | watch: anomaly worth a look
    horizon_days: int
    description: str
    conditions: tuple[Condition, ...]
    levels: Callable[[dict], Levels]
    # False = research candidate: replayed into base rates and the scoreboard,
    # but never scanned or pushed until its replay shows an independent edge.
    live: bool = True
    # Sessions allowed for price to trade into the entry zone after the signal.
    trigger_window: int = 3
    exit: ExitPolicy = FIXED_EXIT

    def mask(self, p: FeaturePanel) -> pd.DataFrame:
        m = base_mask(p)
        for cond in self.conditions:
            m = m & cond.series(p)
        return m

    def explain(self, row: dict) -> list[dict]:
        return [c.check(row) for c in self.conditions]


def r_targets(entry_lo: float, entry_hi: float, stop: float, r1: float = 2.0, r2: float = 3.0) -> Levels:
    mid = (entry_lo + entry_hi) / 2
    risk = max(mid - stop, 1e-9)
    return Levels(
        entry_lo=round(entry_lo, 2),
        entry_hi=round(entry_hi, 2),
        stop=round(stop, 2),
        target1=round(mid + r1 * risk, 2),
        target2=round(mid + r2 * risk, 2),
    )


def base_mask(p: FeaturePanel) -> pd.DataFrame:
    """Liquid stocks with a complete feature history."""
    m = p["liquid"].fillna(False).astype(bool) & p["sma200"].notna() & p["atr14"].notna()
    non_stock = [t for t in m.columns if t not in set(p.stocks)]
    if non_stock:
        m[non_stock] = False
    return m


def registry(include_candidates: bool = True) -> list[Detector]:
    from scout.detectors import rules

    return [d for d in rules.DETECTORS if include_candidates or d.live]


def get(name: str) -> Detector:
    for d in registry():
        if d.name == name:
            return d
    raise KeyError(name)
