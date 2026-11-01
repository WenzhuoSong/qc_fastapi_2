"""Company profiles from annual reports: `python -m scout.jobs.company_profiles [TICKER ...] [--force]`.

Without tickers: every scanned stock whose latest 10-K / 20-F has no profile yet (the weekly job runs this).
`--force` rebuilds the profile of the latest filing.
"""
import sys

from scout import company_profile
from scout.jobs._run import run


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    run("company_profiles", lambda: company_profile.refresh([a.upper() for a in args] or None, force="--force" in sys.argv),
        alert=False)


if __name__ == "__main__":
    main()
