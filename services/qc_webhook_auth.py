"""HMAC verification for callbacks sent by the QC algorithm."""
from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


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


class ObserverRequestAuthError(ValueError):
    def __init__(self, code: str, message: str, *, status_code: int = 401):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True)
class ObserverRequestAuth:
    auth_version: str
    account_id: str | None
    deployment_id: str | None
    nonce: str | None
    request_timestamp: datetime | None
    body_hash: str


def authenticate_observer_request(
    request_body: bytes,
    *,
    signature: str | None,
    timestamp: str | None,
    nonce: str | None,
    account_id: str | None,
    deployment_id: str | None,
    now: datetime | None = None,
    settings: Any | None = None,
) -> ObserverRequestAuth:
    """Authenticate legacy transition requests or the frozen v2 envelope."""
    cfg = settings or _get_settings()
    secret = cfg.qc_webhook_secret or cfg.webhook_secret
    if not secret:
        raise ObserverRequestAuthError("observer_secret_missing", "observer secret is not configured")

    supplied = [timestamp, nonce, account_id, deployment_id]
    has_v2 = all(value is not None and str(value).strip() for value in supplied)
    if not has_v2:
        if any(value is not None and str(value).strip() for value in supplied):
            raise ObserverRequestAuthError(
                "incomplete_observer_auth_headers",
                "all observer v2 authentication headers are required",
            )
        if bool(getattr(cfg, "qc_observer_require_request_v2", False)):
            raise ObserverRequestAuthError(
                "observer_request_v2_required",
                "observer request v2 authentication is required",
            )
        expected = hmac.new(secret.encode(), request_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature or ""):
            raise ObserverRequestAuthError("invalid_observer_signature", "invalid observer signature")
        return ObserverRequestAuth(
            auth_version="v1_legacy_transition",
            account_id=None,
            deployment_id=None,
            nonce=None,
            request_timestamp=None,
            body_hash=hashlib.sha256(request_body).hexdigest(),
        )

    timestamp_text = str(timestamp).strip()
    try:
        request_timestamp = datetime.fromtimestamp(float(timestamp_text), tz=timezone.utc)
    except (TypeError, ValueError, OSError) as exc:
        raise ObserverRequestAuthError(
            "invalid_observer_timestamp", "observer timestamp must be Unix epoch seconds"
        ) from exc
    observed_at = now or datetime.now(timezone.utc)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    max_skew = max(int(getattr(cfg, "qc_observer_max_clock_skew_seconds", 600)), 1)
    skew_seconds = abs((observed_at - request_timestamp).total_seconds())
    if skew_seconds > max_skew:
        raise ObserverRequestAuthError(
            "observer_request_expired",
            f"observer timestamp skew {skew_seconds:.1f}s exceeds {max_skew}s",
        )

    nonce_text = str(nonce).strip()
    if len(nonce_text) < 16 or len(nonce_text) > 128:
        raise ObserverRequestAuthError(
            "invalid_observer_nonce", "observer nonce length must be between 16 and 128"
        )
    account_text = str(account_id).strip()
    deployment_text = str(deployment_id).strip()
    _require_allowlisted(
        account_text,
        getattr(cfg, "qc_observer_allowed_accounts", ""),
        "observer_account_not_allowed",
    )
    _require_allowlisted(
        deployment_text,
        getattr(cfg, "qc_observer_allowed_deployments", ""),
        "observer_deployment_not_allowed",
    )

    signed = (
        f"{timestamp_text}\n{nonce_text}\n{account_text}\n{deployment_text}\n".encode("utf-8")
        + request_body
    )
    expected = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature or ""):
        raise ObserverRequestAuthError("invalid_observer_signature", "invalid observer signature")
    return ObserverRequestAuth(
        auth_version="v2",
        account_id=account_text,
        deployment_id=deployment_text,
        nonce=nonce_text,
        request_timestamp=request_timestamp,
        body_hash=hashlib.sha256(request_body).hexdigest(),
    )


def _require_allowlisted(value: str, raw_allowlist: Any, code: str) -> None:
    allowed = {
        item.strip()
        for item in str(raw_allowlist or "").split(",")
        if item.strip()
    }
    if allowed and value not in allowed:
        raise ObserverRequestAuthError(code, f"{value} is not in the observer allowlist", status_code=403)
