"""HMAC verification for callbacks sent by the QC algorithm."""
from __future__ import annotations

import hashlib
import hmac


def _get_settings():
    from config import get_settings

    return get_settings()


def verify_qc_signature(request_body: bytes, x_qc_signature: str | None) -> bool:
    settings = _get_settings()
    secret = settings.qc_webhook_secret or settings.webhook_secret
    if not secret:
        return False
    expected = hmac.new(secret.encode(), request_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, x_qc_signature or "")
