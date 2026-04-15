import base64
import hashlib
import os

import httpx
import pytest

from main import app


def _pbkdf2_hash(password: str, *, salt: bytes) -> str:
    iterations = 600_000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    salt_b64 = base64.urlsafe_b64encode(salt).decode("ascii").rstrip("=")
    digest_b64 = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"pbkdf2_sha256${iterations}${salt_b64}${digest_b64}"


@pytest.mark.asyncio
async def test_health_endpoint_is_public():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_admin_login_sets_cookie_and_allows_authenticated_access(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD_HASH", _pbkdf2_hash("S3curePass!", salt=b"unit-test-salt"))
    monkeypatch.setenv("SESSION_SECRET", "test-session-secret")
    monkeypatch.setenv("BASE_URL", "https://backend.example.com")

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://test") as ac:
        initial = await ac.get("/api/project/brand-profile")
        login = await ac.post(
            "/api/auth/login",
            json={"identifier": "admin@example.com", "password": "S3curePass!"},
        )
        session = await ac.get("/api/auth/session")
        protected = await ac.get("/api/project/brand-profile")
        logout = await ac.post("/api/auth/logout")
        after_logout = await ac.get("/api/project/brand-profile")

    assert initial.status_code == 403
    assert login.status_code == 200
    set_cookie = login.headers.get("set-cookie", "")
    assert "HttpOnly" in set_cookie
    assert "Secure" in set_cookie
    assert "samesite=none" in set_cookie.lower()
    assert session.status_code == 200
    assert session.json()["authenticated"] is True
    assert protected.status_code == 200
    assert logout.status_code == 200
    assert after_logout.status_code == 403
