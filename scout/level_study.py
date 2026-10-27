"""Replay of the price-map buy points: do the pullback / breakout points beat buying now?

For sampled days the price map is rebuilt from what was known that day (features and
structure up to that close), each buy point becomes a pending order for a few sessions,
and a fill is managed exactly as a holding is (structural stop, a third at target 1,
the rest on the chandelier stop; `scout.exit_study`). The control buys the same stock
at the next open with a plan made the same way at that fill (`levels.locked_plan`), so
the comparison isolates waiting for the buy point.

Fill rules follow what the page tells the user:
- pullback: a bar that trades into the zone fills at its open clipped into the zone;
- breakout (page rule): a close inside the zone with volume ≥ 1.5× the 50-day average (`vol_ratio`),
  filled at that close (beyond the zone it is extended and not chased);
- breakout (touch): a bar that trades into the zone, as a buy-stop would (for contrast).
"""
from __future__ import annotations

import numpy as np

PENDING = 5              # sessions a buy point stays valid after the map's day
BREAKOUT_VOL = 1.5


def fill_limit(lo: float, hi: float, o: np.ndarray, h: np.ndarray, l: np.ndarray, start: int, end: int) -> tuple[int, float] | None:
    """First bar in [start, end) that trades into [lo, hi]: (index, open clipped into the zone)."""
    for j in range(start, min(end, len(o))):
        if np.isfinite(o[j]) and l[j] <= hi and h[j] >= lo:
            return j, float(min(max(o[j], lo), hi))
    return None


def fill_close_confirmed(lo: float, hi: float, c: np.ndarray, vol_ratio: np.ndarray, start: int, end: int) -> tuple[int, float] | None:
    """First close inside [lo, hi] on volume ≥ BREAKOUT_VOL × average: (index, that close)."""
    for j in range(start, min(end, len(c))):
        if np.isfinite(c[j]) and lo <= c[j] <= hi and np.isfinite(vol_ratio[j]) and vol_ratio[j] >= BREAKOUT_VOL:
            return j, float(c[j])
    return None
