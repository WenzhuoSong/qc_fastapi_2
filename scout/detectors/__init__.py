"""Opportunity detectors.

A detector is a deterministic rule over the FeaturePanel. `mask(panel)` returns
a wide boolean frame (date × ticker) and `levels(row)` turns the feature row at
a hit into entry / stop / target prices. Live scanning reads the last date of
the mask; base-rate replay reads every date. Same code, same inputs.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

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
class Detector:
    name: str
    label: str
    kind: str                 # setup: actionable long idea | watch: anomaly worth a look
    horizon_days: int
    description: str
    mask: Callable[[FeaturePanel], pd.DataFrame]
    levels: Callable[[dict], Levels]


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


def registry() -> list[Detector]:
    from scout.detectors import rules

    return list(rules.DETECTORS)


def get(name: str) -> Detector:
    for d in registry():
        if d.name == name:
            return d
    raise KeyError(name)
