import hashlib
import hmac
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from services.qc_webhook_auth import verify_qc_signature


class QCAckAuthTests(unittest.TestCase):
    def test_valid_signature_passes(self):
        body = b'{"cmd_id":"analysis_1","status":"accepted"}'
        secret = "secret"
        signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        settings = types.SimpleNamespace(qc_webhook_secret=secret, webhook_secret="fallback")

        with patch("services.qc_webhook_auth._get_settings", return_value=settings):
            self.assertTrue(verify_qc_signature(body, signature))

    def test_invalid_signature_fails(self):
        settings = types.SimpleNamespace(qc_webhook_secret="secret", webhook_secret="fallback")

        with patch("services.qc_webhook_auth._get_settings", return_value=settings):
            self.assertFalse(verify_qc_signature(b"{}", "bad-signature"))

    def test_falls_back_to_webhook_secret(self):
        body = b"{}"
        signature = hmac.new(b"fallback", body, hashlib.sha256).hexdigest()
        settings = types.SimpleNamespace(qc_webhook_secret="", webhook_secret="fallback")

        with patch("services.qc_webhook_auth._get_settings", return_value=settings):
            self.assertTrue(verify_qc_signature(body, signature))

    def test_async_lifecycle_statuses_are_allowed_by_contract(self):
        text = Path("api/execution.py").read_text()

        self.assertIn("VALID_QC_EXECUTION_STATUSES", text)
        self.assertIn('"orders_submitted"', text)
        self.assertIn('"partial"', text)
        self.assertIn('"filled"', text)
        self.assertIn('"reconciled"', text)
        self.assertIn('"reconciliation_drift"', text)
        self.assertIn('"failed_no_fill"', text)
        self.assertIn('"superseded"', text)

    def test_ack_model_preserves_async_lifecycle_fields(self):
        text = Path("api/execution.py").read_text()

        self.assertIn("execution_state: str | None", text)
        self.assertIn("active_command_id: str | None", text)
        self.assertIn("superseded_command_id: str | None", text)
        self.assertIn("canceled_order_count: int | None", text)


if __name__ == "__main__":
    unittest.main()
