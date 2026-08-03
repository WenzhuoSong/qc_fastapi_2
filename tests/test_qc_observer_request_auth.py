import hashlib
import hmac
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from services.qc_webhook_auth import (
    ObserverRequestAuthError,
    authenticate_observer_request,
)


NOW = datetime(2026, 7, 23, 2, 0, tzinfo=timezone.utc)
BODY = b'{"events":[]}'


def _settings(**overrides):
    values = {
        "qc_webhook_secret": "secret",
        "webhook_secret": "",
        "qc_observer_require_request_v2": False,
        "qc_observer_allowed_accounts": "paper-account",
        "qc_observer_allowed_deployments": "deploy-1",
        "qc_observer_max_clock_skew_seconds": 600,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _v2_signature(timestamp, nonce, account="paper-account", deployment="deploy-1"):
    signed = f"{timestamp}\n{nonce}\n{account}\n{deployment}\n".encode() + BODY
    return hmac.new(b"secret", signed, hashlib.sha256).hexdigest()


class QCObserverRequestAuthTests(unittest.TestCase):
    def test_legacy_transition_can_be_enabled_or_rejected(self):
        signature = hmac.new(b"secret", BODY, hashlib.sha256).hexdigest()
        auth = authenticate_observer_request(
            BODY,
            signature=signature,
            timestamp=None,
            nonce=None,
            account_id=None,
            deployment_id=None,
            now=NOW,
            settings=_settings(),
        )
        self.assertEqual(auth.auth_version, "v1_legacy_transition")

        with self.assertRaises(ObserverRequestAuthError) as error:
            authenticate_observer_request(
                BODY,
                signature=signature,
                timestamp=None,
                nonce=None,
                account_id=None,
                deployment_id=None,
                now=NOW,
                settings=_settings(qc_observer_require_request_v2=True),
            )
        self.assertEqual(error.exception.code, "observer_request_v2_required")

    def test_valid_v2_request_binds_identity_and_body(self):
        timestamp = str(int(NOW.timestamp()))
        nonce = "0123456789abcdef"
        auth = authenticate_observer_request(
            BODY,
            signature=_v2_signature(timestamp, nonce),
            timestamp=timestamp,
            nonce=nonce,
            account_id="paper-account",
            deployment_id="deploy-1",
            now=NOW,
            settings=_settings(),
        )
        self.assertEqual(auth.auth_version, "v2")
        self.assertEqual(auth.account_id, "paper-account")
        self.assertEqual(auth.deployment_id, "deploy-1")
        self.assertEqual(auth.body_hash, hashlib.sha256(BODY).hexdigest())

    def test_expired_partial_and_non_allowlisted_requests_fail_closed(self):
        timestamp = str(int((NOW - timedelta(minutes=11)).timestamp()))
        with self.assertRaises(ObserverRequestAuthError) as expired:
            authenticate_observer_request(
                BODY,
                signature=_v2_signature(timestamp, "0123456789abcdef"),
                timestamp=timestamp,
                nonce="0123456789abcdef",
                account_id="paper-account",
                deployment_id="deploy-1",
                now=NOW,
                settings=_settings(),
            )
        self.assertEqual(expired.exception.code, "observer_request_expired")

        with self.assertRaises(ObserverRequestAuthError) as partial:
            authenticate_observer_request(
                BODY,
                signature="bad",
                timestamp=str(int(NOW.timestamp())),
                nonce=None,
                account_id=None,
                deployment_id=None,
                now=NOW,
                settings=_settings(),
            )
        self.assertEqual(partial.exception.code, "incomplete_observer_auth_headers")

        timestamp = str(int(NOW.timestamp()))
        with self.assertRaises(ObserverRequestAuthError) as denied:
            authenticate_observer_request(
                BODY,
                signature=_v2_signature(timestamp, "0123456789abcdef", account="other"),
                timestamp=timestamp,
                nonce="0123456789abcdef",
                account_id="other",
                deployment_id="deploy-1",
                now=NOW,
                settings=_settings(),
            )
        self.assertEqual(denied.exception.code, "observer_account_not_allowed")


if __name__ == "__main__":
    unittest.main()
