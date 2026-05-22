import unittest
from pathlib import Path


class CommunicatorSemanticTest(unittest.TestCase):
    def test_no_order_executed_copy_before_qc_ack(self):
        for folder in ("agents", "tools"):
            for path in Path(folder).rglob("*.py"):
                text = path.read_text()
                self.assertNotIn("Order executed", text, str(path))

    def test_executor_uses_submission_not_execution_language(self):
        text = Path("agents/executor.py").read_text()

        self.assertIn("Command submitted", text)
        self.assertIn("QC command submission failed", text)
        self.assertIn("Awaiting QC algorithm confirmation", text)
        self.assertIn("_command_label(command_id)", text)
        self.assertNotIn("command_id[:8]", text)


if __name__ == "__main__":
    unittest.main()
