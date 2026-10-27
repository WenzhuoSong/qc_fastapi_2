"""One-off bootstrap: universe, full history, base rates and the latest
feature snapshot (no Telegram push).

`--extend` lengthens the stored history (after raising SCOUT_HISTORY_YEARS) by
upserting every date instead of deleting first, so pages never see a gap."""
import sys

from scout.jobs._run import run
from scout.pipeline import ensure_base_rates, load_panel, prepare, store_features


async def _backfill() -> dict:
    out = await prepare(full_backfill=True, replace="--extend" not in sys.argv)
    rates = await ensure_base_rates(force=True)
    out["base_rates"] = {d: b.get("all", {}) for d, b in rates.items()}
    # The Q&A agent and dashboard read features_daily; seed it so they work
    # before the first EOD scan.
    panel = await load_panel()
    out["features"] = await store_features(panel, panel.dates[-1])
    return out


if __name__ == "__main__":
    run("backfill", _backfill, alert=False)
