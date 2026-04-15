from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Any, Literal, Optional


PBKDF2_ITERATIONS = 600_000
DEFAULT_COOKIE_NAME = "propella_admin_session"
DEFAULT_SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 14
UserRole = Literal["admin", "operator", "lab"]


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}")


def _local_hosts() -> tuple[str, ...]:
    return ("localhost", "127.0.0.1", "0.0.0.0")


def _base_url() -> str:
    return (
        os.getenv("BASE_URL")
        or os.getenv("RENDER_EXTERNAL_URL")
        or "http://localhost:8001"
    ).strip()


def _is_secure_cookie() -> bool:
    lowered = _base_url().lower()
    return lowered.startswith("https://") and not any(host in lowered for host in _local_hosts())


def _cookie_samesite() -> str:
    return "none" if _is_secure_cookie() else "lax"


def _session_cookie_name() -> str:
    return (os.getenv("SESSION_COOKIE_NAME") or DEFAULT_COOKIE_NAME).strip() or DEFAULT_COOKIE_NAME


def _session_secret() -> str:
    return (os.getenv("SESSION_SECRET") or "").strip()


def _session_max_age_seconds() -> int:
    raw = (os.getenv("SESSION_MAX_AGE_SECONDS") or "").strip()
    if not raw:
        return DEFAULT_SESSION_MAX_AGE_SECONDS
    try:
        return max(300, int(raw))
    except ValueError:
        return DEFAULT_SESSION_MAX_AGE_SECONDS


def _normalized_identifiers(*values: str) -> list[str]:
    return [candidate for candidate in dict.fromkeys((value or "").strip().lower() for value in values) if candidate]


def admin_identifiers() -> list[str]:
    return _normalized_identifiers(os.getenv("ADMIN_EMAIL", ""), os.getenv("ADMIN_USERNAME", ""))


def admin_password_hash() -> str:
    return (os.getenv("ADMIN_PASSWORD_HASH") or "").strip()


def operator_identifiers() -> list[str]:
    return _normalized_identifiers(os.getenv("OPERATOR_EMAIL", ""), os.getenv("OPERATOR_USERNAME", ""))


def operator_password_hash() -> str:
    return (os.getenv("OPERATOR_PASSWORD_HASH") or "").strip()


def lab_identifiers() -> list[str]:
    return _normalized_identifiers(os.getenv("LAB_EMAIL", ""), os.getenv("LAB_USERNAME", ""))


def lab_password_hash() -> str:
    return (os.getenv("LAB_PASSWORD_HASH") or "").strip()


def configured_users() -> dict[str, dict[str, str]]:
    users: dict[str, dict[str, str]] = {}
    for role, identifiers, password_hash in (
        ("admin", admin_identifiers(), admin_password_hash()),
        ("operator", operator_identifiers(), operator_password_hash()),
        ("lab", lab_identifiers(), lab_password_hash()),
    ):
        if not identifiers or not password_hash:
            continue
        for identifier in identifiers:
            users[identifier] = {
                "identifier": identifier,
                "role": role,
                "password_hash": password_hash,
            }
    return users


def admin_login_ready() -> bool:
    return bool(configured_users() and _session_secret())


def hash_password(password: str, *, salt: bytes | None = None, iterations: int = PBKDF2_ITERATIONS) -> str:
    salt_bytes = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_bytes, iterations)
    return f"pbkdf2_sha256${iterations}${_b64url_encode(salt_bytes)}${_b64url_encode(digest)}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, iterations_raw, salt_raw, digest_raw = stored_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_raw)
        salt = _b64url_decode(salt_raw)
        expected = _b64url_decode(digest_raw)
    except (TypeError, ValueError, base64.binascii.Error):
        return False

    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def authenticate_user(identifier: str, password: str) -> Optional[dict[str, str]]:
    candidate = (identifier or "").strip().lower()
    if not candidate:
        return None
    user = configured_users().get(candidate)
    if not user:
        return None
    if not verify_password(password or "", user["password_hash"]):
        return None
    return {
        "identifier": user["identifier"],
        "role": user["role"],
    }


def authenticate_admin(identifier: str, password: str) -> Optional[str]:
    user = authenticate_user(identifier, password)
    if not user or user["role"] != "admin":
        return None
    return user["identifier"]


def _sign_message(message: str) -> str:
    secret = _session_secret()
    if not secret:
        raise RuntimeError("SESSION_SECRET must be configured for authenticated sessions")
    digest = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
    return _b64url_encode(digest)


def build_session_token(identifier: str, role: UserRole) -> str:
    payload = {
        "sub": identifier,
        "role": role,
        "exp": int(time.time()) + _session_max_age_seconds(),
    }
    body = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    return f"{body}.{_sign_message(body)}"


def parse_session_token(token: str) -> Optional[dict[str, Any]]:
    if not token or "." not in token:
        return None

    body, signature = token.split(".", 1)
    expected_signature = _sign_message(body)
    if not hmac.compare_digest(signature, expected_signature):
        return None

    try:
        payload = json.loads(_b64url_decode(body))
    except (ValueError, json.JSONDecodeError, base64.binascii.Error):
        return None

    if not isinstance(payload, dict):
        return None
    if int(payload.get("exp") or 0) <= int(time.time()):
        return None

    subject = str(payload.get("sub") or "").strip().lower()
    role = str(payload.get("role") or "").strip().lower()
    if role not in {"admin", "operator", "lab"}:
        return None
    user = configured_users().get(subject)
    if not user or user["role"] != role:
        return None
    return {"identifier": subject, "role": role}


def session_from_cookie(cookie_value: str | None) -> Optional[dict[str, Any]]:
    if not admin_login_ready():
        return None
    return parse_session_token(cookie_value or "")


def session_cookie_settings() -> dict[str, Any]:
    return {
        "key": _session_cookie_name(),
        "httponly": True,
        "secure": _is_secure_cookie(),
        "samesite": _cookie_samesite(),
        "max_age": _session_max_age_seconds(),
        "path": "/",
    }
