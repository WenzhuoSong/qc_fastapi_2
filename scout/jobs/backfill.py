"""One-off bootstrap: universe, full history and base rates (no Telegram push)."""
from scout.jobs._run import run
from scout.pipeline import ensure_base_rates, prepare


async def _backfill() -> dict:
    out = await prepare(full_backfill=True)
    rates = await ensure_base_rates(force=True)
    out["base_rates"] = {d: b.get("all", {}) for d, b in rates.items()}
    return out


if __name__ == "__main__":
    run("backfill", _backfill, alert=False)
