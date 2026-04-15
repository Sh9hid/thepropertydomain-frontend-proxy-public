from __future__ import annotations

import base64
import hashlib
import hmac
import json
from time import time


def _is_local_base_url(value: str) -> bool:
    lowered = (value or "").lower()
    return any(host in lowered for host in ("localhost", "127.0.0.1", "0.0.0.0"))


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}")


def issue_websocket_token(secret: str, ttl_seconds: int = 300, now: int | None = None) -> str:
    issued_at = int(now if now is not None else time())
    payload = {"exp": issued_at + max(30, int(ttl_seconds)), "scope": "ws"}
    payload_part = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = hmac.new(secret.encode("utf-8"), payload_part.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_part}.{_b64url_encode(signature)}"


def verify_websocket_token(token: str | None, secret: str, now: int | None = None) -> bool:
    if not token or not secret:
        return False
    try:
        payload_part, signature_part = token.split(".", 1)
        expected_signature = hmac.new(secret.encode("utf-8"), payload_part.encode("utf-8"), hashlib.sha256).digest()
        provided_signature = _b64url_decode(signature_part)
        if not hmac.compare_digest(expected_signature, provided_signature):
            return False
        payload = json.loads(_b64url_decode(payload_part))
        exp = int(payload.get("exp", 0))
        scope = payload.get("scope")
    except Exception:
        return False
    current_time = int(now if now is not None else time())
    return scope == "ws" and exp > current_time


def assert_secure_websocket_settings(base_url: str, shared_secret: str) -> None:
    if _is_local_base_url(base_url):
        return
    if not shared_secret:
        raise RuntimeError("WS_SHARED_SECRET must be set when WS_REQUIRE_AUTH=true for non-local deployments.")
