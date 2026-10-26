"""Bracket simulation shared by base-rate replay and live opportunity tracking.

Rules (daily bars, conservative):
- Trigger: within `trigger_window` sessions after the signal, the first bar whose
  range overlaps the entry zone. Fill = open clipped into the zone.
- After the fill, each bar checks the stop before the target (worst case when
  both are touched the same day). Gaps through a level fill at the open.
- The trade expires at the close of the `horizon`-th session after the fill.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Sequence

from scout.detectors import Levels

TRIGGER_WINDOW = 3
# Round-trip trading cost (commission + spread + slippage) for liquid US stocks,
# charged once per filled trade in both replay and live outcomes.
ROUND_TRIP_COST = 0.0010


@dataclass
class Bar:
    date: date
    open: float
    high: float
    low: float
    close: float


@dataclass
class SimResult:
    state: str                         # pending | open | untriggered | hit_target | stopped | expired
    entry_date: date | None = None
    entry_price: float | None = None
    exit_date: date | None = None
    exit_price: float | None = None
    days_held: int = 0
    mfe: float | None = None           # max favourable excursion vs entry
    mae: float | None = None           # max adverse excursion vs entry
    last_close: float | None = None
    events: list[tuple[str, date, float]] = field(default_factory=list)

    @property
    def closed(self) -> bool:
        return self.state in {"untriggered", "hit_target", "stopped", "expired"}

    @property
    def return_pct(self) -> float | None:
        if self.entry_price is None:
            return None
        ref = self.exit_price if self.exit_price is not None else self.last_close
        return None if ref is None else ref / self.entry_price - 1


def simulate(
    bars: Sequence[Bar],
    levels: Levels,
    horizon: int,
    trigger_window: int = TRIGGER_WINDOW,
) -> SimResult:
    """`bars` are the sessions strictly after the signal date, ascending."""
    res = SimResult(state="pending")
    entry_idx = None
    for i, b in enumerate(bars[:trigger_window]):
        if b.low <= levels.entry_hi and b.high >= levels.entry_lo:
            entry_idx = i
            res.entry_date = b.date
            res.entry_price = min(max(b.open, levels.entry_lo), levels.entry_hi)
            res.events.append(("triggered", b.date, res.entry_price))
            break
    if entry_idx is None:
        if len(bars) >= trigger_window:
            res.state = "untriggered"
            res.exit_date = bars[trigger_window - 1].date
        return res

    res.state = "open"
    entry = res.entry_price
    hi_water, lo_water = entry, entry
    window = bars[entry_idx : entry_idx + horizon]
    for k, b in enumerate(window):
        first = k == 0
        # On the fill bar only the part of the range after the fill is knowable;
        # treat the whole bar as eligible (conservative: stop is checked first).
        hi_water = max(hi_water, b.high)
        lo_water = min(lo_water, b.low)
        res.days_held = k + 1
        res.last_close = b.close
        if b.low <= levels.stop:
            res.state = "stopped"
            res.exit_price = levels.stop if (first or b.open > levels.stop) else b.open
            res.exit_date = b.date
            break
        if b.high >= levels.target1:
            res.state = "hit_target"
            res.exit_price = levels.target1 if (first or b.open < levels.target1) else b.open
            res.exit_date = b.date
            break
    else:
        if len(window) >= horizon:
            res.state = "expired"
            res.exit_price = window[-1].close
            res.exit_date = window[-1].date
    res.mfe = hi_water / entry - 1
    res.mae = lo_water / entry - 1
    if res.closed and res.state != "untriggered":
        res.events.append((res.state, res.exit_date, res.exit_price))
    return res
