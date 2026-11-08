import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent / "scout" / "knowledge"
# Run before this test existed and never edited after their runs (the text is the record), though not valid YAML.
HISTORICAL = {"ml_v2b.yaml", "small_cap_costs_v1.yaml"}


class SpecFileTests(unittest.TestCase):
    def test_every_pre_registration_parses_and_names_itself(self):
        files = sorted(p for d in ("specs", "external_specs", "exit_specs") for p in (ROOT / d).glob("*.yaml"))
        self.assertTrue(files)
        for p in [p for p in files if p.name not in HISTORICAL]:
            with self.subTest(spec=p.name):
                spec = yaml.safe_load(p.read_text(encoding="utf-8"))
                self.assertIsInstance(spec, dict)
                if not p.stem.endswith(("_run_note", "_clarification")):
                    self.assertTrue(spec.get("key"), "a spec needs its key")


if __name__ == "__main__":
    unittest.main()
