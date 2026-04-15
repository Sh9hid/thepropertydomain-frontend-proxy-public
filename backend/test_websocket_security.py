import pytest


def test_websocket_token_round_trip():
    from core.websocket_security import issue_websocket_token, verify_websocket_token

    secret = "test-secret"
    token = issue_websocket_token(secret, ttl_seconds=60, now=1_700_000_000)

    assert verify_websocket_token(token, secret, now=1_700_000_030) is True


def test_websocket_token_rejects_expired_and_tampered_tokens():
    from core.websocket_security import issue_websocket_token, verify_websocket_token

    secret = "test-secret"
    token = issue_websocket_token(secret, ttl_seconds=60, now=1_700_000_000)

    assert verify_websocket_token(token, secret, now=1_700_000_061) is False
    assert verify_websocket_token(f"{token}tampered", secret, now=1_700_000_030) is False


def test_non_local_deployments_require_a_websocket_secret():
    from core.websocket_security import assert_secure_websocket_settings

    with pytest.raises(RuntimeError):
        assert_secure_websocket_settings(base_url="https://api.example.com", shared_secret="")


def test_local_deployments_can_run_without_a_websocket_secret():
    from core.websocket_security import assert_secure_websocket_settings

    assert_secure_websocket_settings(base_url="http://localhost:8001", shared_secret="")
