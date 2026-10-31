"""Trading-cost table for the small-cap studies (`scout/knowledge/external_specs/small_cap_costs_v1.yaml`):
`python -m scout.jobs.small_cap_costs`. Local only: writes git-ignored `exports/costs/small_cap_costs_v1.parquet`
(per ticker and month) and `summary.json`; rebuilding gives the same table for the same data."""
import json

from scout import cost_model as cm


def main() -> None:
    res = cm.run()
    (cm.OUT / "summary.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(cm.report_text(json.loads((cm.OUT / "summary.json").read_text())), flush=True)


if __name__ == "__main__":
    main()
