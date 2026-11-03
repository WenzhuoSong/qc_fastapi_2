"""Conditional base rates (pre-registered in `scout/knowledge/external_specs/conditional_rates_v1.yaml`):
`python -m scout.jobs.conditional_rates`. Runs once; writes scout.conditional_rates."""
from scout import conditional_rates
from scout.jobs._run import run

if __name__ == "__main__":
    run("conditional_rates", conditional_rates.run, alert=False)
