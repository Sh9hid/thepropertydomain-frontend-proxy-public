import base64
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import httpx
import pytest
from fastapi import Response
from fastapi.testclient import TestClient


BACKEND_ROOT = Path(__file__).resolve().parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from runtime.oci_core_api import create_oci_core_app  # noqa: E402
from runtime.oci_hybrid_app import create_hybrid_app  # noqa: E402
import runtime.oci_hybrid_app as hybrid_runtime  # noqa: E402


def _pbkdf2_hash(password: str, *, salt: bytes) -> str:
    iterations = 600_000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    salt_b64 = base64.urlsafe_b64encode(salt).decode("ascii").rstrip("=")
    digest_b64 = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"pbkdf2_sha256${iterations}${salt_b64}${digest_b64}"


def _seed_sqlite_db(db_path: Path) -> None:
    connection = sqlite3.connect(str(db_path))
    connection.executescript(
        """
        CREATE TABLE leads (
            id TEXT PRIMARY KEY,
            address TEXT,
            suburb TEXT,
            postcode TEXT,
            owner_name TEXT,
            canonical_address TEXT,
            trigger_type TEXT,
            signal_status TEXT,
            status TEXT,
            heat_score INTEGER,
            est_value INTEGER,
            contact_phones TEXT,
            contact_emails TEXT,
            property_images TEXT,
            main_image TEXT,
            lat REAL,
            lng REAL,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        INSERT INTO leads (
            id, address, suburb, postcode, owner_name, canonical_address, trigger_type, signal_status,
            status, heat_score, est_value, contact_phones, contact_emails, property_images, main_image,
            lat, lng, notes, created_at, updated_at
        ) VALUES
        (
            'lead-1', '32 Example Street', 'Woonona', '2517', 'Alice Example', '32 Example Street Woonona',
            'mortgage_cliff', 'DELTA', 'captured', 86, 1200000, '["0499 123 456"]', '["alice@example.com"]',
            '[]', '', -34.35, 150.91, 'Priority seller lead', '2026-04-12T09:00:00+10:00', '2026-04-13T09:30:00+10:00'
        ),
        (
            'lead-2', '8 Harbour View Road', 'Bulli', '2516', 'Bob Harbour', '8 Harbour View Road Bulli',
            'suburb_report', 'WITHDRAWN', 'qualified', 54, 980000, '[]', '["bob@example.com"]',
            '["https://images.example.com/lead-2.jpg"]', '', -34.33, 150.92, 'Secondary lead', '2026-04-11T09:00:00+10:00', '2026-04-12T09:30:00+10:00'
        );
        """
    )
    connection.commit()
    connection.close()


def test_admin_auth_uses_postponed_annotations_for_python39_compatibility() -> None:
    source = (BACKEND_ROOT / "core" / "admin_auth.py").read_text(encoding="utf-8")
    first_lines = "\n".join(source.splitlines()[:3])
    assert "from __future__ import annotations" in first_lines


def test_oci_runtime_avoids_python310_union_syntax_for_python39() -> None:
    runtime_source = (BACKEND_ROOT / "runtime" / "oci_core_api.py").read_text(encoding="utf-8")
    assert " | " not in runtime_source


@pytest.fixture
def sqlite_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "oci-hybrid-leads.db"
    _seed_sqlite_db(db_path)
    return db_path


@pytest.mark.asyncio
async def test_oci_core_login_and_search_work_with_session(monkeypatch: pytest.MonkeyPatch, sqlite_db: Path) -> None:
    monkeypatch.setenv("TPD_SQLITE_PATH", str(sqlite_db))
    monkeypatch.setenv("LAB_USERNAME", "lab")
    monkeypatch.setenv("LAB_PASSWORD_HASH", _pbkdf2_hash("lab123", salt=b"oci-hybrid-salt"))
    monkeypatch.setenv("SESSION_SECRET", "oci-hybrid-session-secret")
    monkeypatch.setenv("BASE_URL", "https://thepropertydomain.com.au")
    monkeypatch.setenv("API_KEY", "unit-api-key")

    app = create_oci_core_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://test") as ac:
        initial = await ac.get("/api/auth/session")
        login = await ac.post("/api/auth/login", json={"identifier": "lab", "password": "lab123"})
        session = await ac.get("/api/auth/session")
        leads = await ac.get("/api/leads?limit=50&search=0499")

    assert initial.status_code == 200
    assert initial.json()["authenticated"] is False
    assert login.status_code == 200
    assert session.status_code == 200
    assert session.json()["authenticated"] is True
    assert leads.status_code == 200
    assert leads.json()["total"] == 1
    assert leads.json()["leads"][0]["id"] == "lead-1"


@pytest.mark.asyncio
async def test_oci_core_detail_metrics_and_events_work_with_api_key(monkeypatch: pytest.MonkeyPatch, sqlite_db: Path) -> None:
    monkeypatch.setenv("TPD_SQLITE_PATH", str(sqlite_db))
    monkeypatch.setenv("API_KEY", "unit-api-key")

    app = create_oci_core_app()
    headers = {"X-API-KEY": "unit-api-key"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://test") as ac:
        detail = await ac.get("/api/leads/lead-2", headers=headers)
        analytics = await ac.get("/api/analytics", headers=headers)
        pulse = await ac.get("/api/system/pulse", headers=headers)
        events = await ac.get("/api/analytics/events?hours=24&limit=10", headers=headers)

    assert detail.status_code == 200
    assert detail.json()["owner_name"] == "Bob Harbour"
    assert detail.json()["visual_url"] == "https://images.example.com/lead-2.jpg"

    assert analytics.status_code == 200
    analytics_payload = analytics.json()
    assert analytics_payload["active_leads"] == 2
    assert analytics_payload["withdrawn_count"] == 1
    assert analytics_payload["delta_count"] == 1
    assert analytics_payload["mortgage_cliff_count"] == 1

    assert pulse.status_code == 200
    pulse_payload = pulse.json()
    assert pulse_payload["lead_count"] == 2
    assert pulse_payload["with_phone"] == 1
    assert pulse_payload["with_email"] == 2

    assert events.status_code == 200
    assert events.json()["events"] == []


def test_oci_core_websocket_accepts_authorized_connection(monkeypatch: pytest.MonkeyPatch, sqlite_db: Path) -> None:
    monkeypatch.setenv("TPD_SQLITE_PATH", str(sqlite_db))
    monkeypatch.setenv("API_KEY", "unit-api-key")

    app = create_oci_core_app()
    with TestClient(app) as client:
        with client.websocket_connect("/ws/events?api_key=unit-api-key") as websocket:
            message = websocket.receive_json()

    assert message["type"] == "SYSTEM_HEALTH"
    assert message["data"]["status"] == "connected"


@pytest.mark.asyncio
async def test_hybrid_app_serves_frontend_and_proxies_unknown_api(monkeypatch: pytest.MonkeyPatch, sqlite_db: Path, tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist"
    assets_dir = dist_dir / "assets"
    assets_dir.mkdir(parents=True)
    (dist_dir / "index.html").write_text("<!doctype html><html><body>Hybrid Frontend</body></html>", encoding="utf-8")
    (assets_dir / "app.js").write_text("console.log('hybrid');", encoding="utf-8")

    monkeypatch.setenv("TPD_SQLITE_PATH", str(sqlite_db))
    monkeypatch.setenv("LAB_USERNAME", "lab")
    monkeypatch.setenv("LAB_PASSWORD_HASH", _pbkdf2_hash("lab123", salt=b"oci-hybrid-salt"))
    monkeypatch.setenv("SESSION_SECRET", "oci-hybrid-session-secret")
    monkeypatch.setenv("BASE_URL", "https://thepropertydomain.com.au")
    monkeypatch.setenv("API_KEY", "unit-api-key")
    monkeypatch.setenv("HYBRID_FRONTEND_DIST", str(dist_dir))
    monkeypatch.setenv("HYBRID_UPSTREAM_URL", "https://upstream.example.com")

    async def fake_proxy(request, upstream_path: str):
        return Response(
            content=json.dumps({"proxied": True, "path": upstream_path}),
            status_code=200,
            media_type="application/json",
        )

    monkeypatch.setattr(hybrid_runtime, "proxy_to_upstream", fake_proxy)

    app = create_hybrid_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://test") as ac:
        home = await ac.get("/")
        asset = await ac.get("/assets/app.js")
        proxied = await ac.get("/api/templates", headers={"X-API-KEY": "unit-api-key"})
        local = await ac.get("/api/leads?search=Alice", headers={"X-API-KEY": "unit-api-key"})

    assert home.status_code == 200
    assert "Hybrid Frontend" in home.text
    assert asset.status_code == 200
    assert "hybrid" in asset.text
    assert proxied.status_code == 200
    assert proxied.json() == {"proxied": True, "path": "/api/templates"}
    assert local.status_code == 200
    assert local.json()["total"] == 1


@pytest.mark.asyncio
async def test_hybrid_app_proxy_only_mode_forwards_api_routes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir(parents=True)
    (dist_dir / "index.html").write_text("<!doctype html><html><body>Proxy Only</body></html>", encoding="utf-8")

    monkeypatch.setenv("HYBRID_FRONTEND_DIST", str(dist_dir))
    monkeypatch.setenv("HYBRID_UPSTREAM_URL", "https://api.thepropertydomain.com.au")
    monkeypatch.setenv("HYBRID_CORE_MODE", "proxy_only")

    async def fake_proxy(request, upstream_path: str):
        return Response(
            content=json.dumps({"proxied": True, "path": upstream_path, "query": str(request.url.query)}),
            status_code=200,
            media_type="application/json",
        )

    monkeypatch.setattr(hybrid_runtime, "proxy_to_upstream", fake_proxy)

    app = create_hybrid_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://test") as ac:
        proxied = await ac.get("/api/leads?limit=50&search=Alice")

    assert proxied.status_code == 200
    assert proxied.json() == {
        "proxied": True,
        "path": "/api/leads",
        "query": "limit=50&search=Alice",
    }
