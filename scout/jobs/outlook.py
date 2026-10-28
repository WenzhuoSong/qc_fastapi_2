"""Industry outlook on demand: `python -m scout.jobs.outlook` (the weekly job runs it too)."""
from scout import outlook
from scout.jobs._run import run

if __name__ == "__main__":
    run("outlook", outlook.generate, alert=False)
