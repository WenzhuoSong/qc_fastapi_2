import hashlib
import hmac
import types
import unittest
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


if __name__ == "__main__":
    unittest.main()
