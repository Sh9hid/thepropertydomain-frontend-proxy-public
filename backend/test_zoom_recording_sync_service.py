import asyncio
import hashlib
import hmac
import json
import sqlite3
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import core.config
import core.database as db_module
from api.routes import communications, recordings
from core.database import get_session
from core.utils import now_iso


TEST_ROOT = Path("D:/woonona-lead-machine/backend/test_dbs")


@pytest.fixture
def isolated_db(monkeypatch):
    TEST_ROOT.mkdir(parents=True, exist_ok=True)
    test_db = TEST_ROOT / f"zoom_recording_sync_service_{uuid.uuid4().hex}.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db}")

    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(
        db_module,
        "_async_session_factory",
        sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False),
    )
    db_module.init_db()
    yield test_db
    asyncio.run(test_engine.dispose())


def _seed_zoom_account(db_path: str, *, product: str = "phone", webhook_secret: str = "secret") -> None:
    conn = sqlite3.connect(db_path)
    call_enabled = 1 if product == "phone" else 0
    text_enabled = 1 if product == "phone" else 0
    conn.execute(
        """
        INSERT INTO communication_accounts (
            id, label, provider, client_id, client_secret, account_id, token_url, api_base,
            use_account_credentials, webhook_secret, call_enabled, text_enabled, send_enabled, verify_ssl,
            created_at, updated_at
        ) VALUES (?, ?, 'zoom', ?, ?, ?, 'https://zoom.us/oauth/token', 'https://api.zoom.us/v2', 1, ?, ?, ?, 0, 1, '2026-03-29T00:00:00Z', '2026-03-29T00:00:00Z')
        """,
        (f"zoom-{product}", f"Zoom {product}", "cid", "sec", "aid", webhook_secret, call_enabled, text_enabled),
    )
    conn.commit()
    conn.close()


def _seed_lead(db_path: str, *, lead_id: str = "lead-1", phone: str = "+61412345678") -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO leads (
            id, address, suburb, postcode, owner_name, trigger_type, record_type, heat_score,
            confidence_score, contact_emails, contact_phones, lat, lng, est_value, created_at, updated_at,
            activity_log, stage_note_history, status, conversion_score, compliance_score, readiness_score,
            call_today_score, evidence_score, queue_bucket, lead_archetype, contactability_status,
            owner_verified, contact_role, cadence_name, cadence_step, next_action_type, next_action_channel,
            next_action_title, next_action_reason, next_message_template, last_outcome, objection_reason,
            preferred_channel, strike_zone, touches_14d, touches_30d, route_queue, days_on_market,
            preferred_contact_method, followup_frequency, followup_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            "1 Test Street",
            "Woonona",
            "2517",
            "Owner",
            "Manual",
            "manual_entry",
            55,
            70,
            '["owner@example.com"]',
            json.dumps([phone]),
            -34.3430,
            150.9130,
            950000,
            "2026-03-22T09:00:00+11:00",
            "2026-03-22T09:00:00+11:00",
            "[]",
            "[]",
            "captured",
            0,
            0,
            0,
            0,
            0,
            "",
            "",
            "",
            0,
            "",
            "",
            0,
            "",
            "",
            0,
            "",
            "",
            "",
            "",
            "",
            "",
            0,
            0,
            "",
            0,
            "",
            "none",
            "active",
        ),
    )
    conn.commit()
    conn.close()


def test_zoom_capabilities_default_to_phone_recordings_only():
    from services.zoom_recording_sync_service import get_zoom_capabilities

    capabilities = get_zoom_capabilities({"provider": "zoom", "call_enabled": 1, "text_enabled": 1})

    assert capabilities["product"] == "phone"
    assert capabilities["recordings_supported"] is True
    assert capabilities["ai_summary_supported"] is False


def test_validate_zoom_runtime_config_requires_complete_credentials(monkeypatch):
    from services.zoom_recording_sync_service import validate_zoom_runtime_config

    monkeypatch.setenv("ZOOM_CLIENT_ID", "cid")
    monkeypatch.delenv("ZOOM_CLIENT_SECRET", raising=False)
    monkeypatch.setenv("ZOOM_ACCOUNT_ID", "aid")

    with pytest.raises(RuntimeError):
        validate_zoom_runtime_config()


def test_parse_meeting_recordings_payload_creates_multiple_artifacts():
    from services.zoom_recording_sync_service import extract_meeting_recording_artifacts

    meeting = {
        "uuid": "meeting-uuid",
        "id": 123456789,
        "host_id": "host-1",
        "recording_files": [
            {"id": "file-1", "status": "completed", "file_type": "MP4", "download_url": "https://example.test/1"},
            {"id": "file-2", "status": "processing", "file_type": "M4A", "download_url": "https://example.test/2"},
        ],
    }

    artifacts = extract_meeting_recording_artifacts(meeting)

    assert len(artifacts) == 2
    assert artifacts[0]["external_id"] == "file-1"
    assert artifacts[1]["status"] == "discovered"


@pytest.mark.asyncio
async def test_manual_backfill_phone_persists_artifact_and_is_idempotent(isolated_db, monkeypatch):
    from services.zoom_recording_sync_service import ensure_zoom_recording_schema, sync_zoom_recordings

    _seed_zoom_account(core.config.DB_PATH, product="phone")
    _seed_lead(core.config.DB_PATH)

    async with db_module._async_session_factory() as session:
        await ensure_zoom_recording_schema(session)

    async def _fake_download(url, artifact_id, account=None):
        return f"D:/woonona-lead-machine/backend/recordings/{artifact_id}.mp3"

    def _fake_zoom_request(account, method, path, payload=None):
        if path.startswith("/phone/call_logs?"):
            return {
                "ok": True,
                "data": {
                    "call_logs": [
                        {
                            "id": "call-1",
                            "date_time": "2026-03-29T10:00:00Z",
                            "direction": "outbound",
                            "caller_number": "+61299990000",
                            "callee_number": "+61412345678",
                            "result": "Answered",
                            "duration": 60,
                        }
                    ]
                },
            }
        if path == "/phone/call_logs/call-1/recordings":
            return {
                "ok": True,
                "data": {
                    "recordings": [
                        {
                            "id": "phone-recording-1",
                            "file_url": "https://example.test/recording.mp3",
                            "download_url": "https://example.test/recording.mp3",
                            "file_type": "mp3",
                            "duration": 60,
                        }
                    ]
                },
            }
        raise AssertionError(path)

    monkeypatch.setattr("services.zoom_recording_sync_service._zoom_request", _fake_zoom_request)
    monkeypatch.setattr("services.zoom_recording_sync_service.download_recording", _fake_download)

    async with db_module._async_session_factory() as session:
        first = await sync_zoom_recordings(session, {"from": "2026-03-29", "to": "2026-03-29", "verbose": True})
    async with db_module._async_session_factory() as session:
        second = await sync_zoom_recordings(session, {"from": "2026-03-29", "to": "2026-03-29", "verbose": True})

    assert first["files_discovered"] == 1
    assert second["duplicates_skipped"] == 1

    conn = sqlite3.connect(core.config.DB_PATH)
    row = conn.execute("SELECT external_id, status, lead_id FROM zoom_recording_artifacts").fetchone()
    conn.close()
    assert row == ("phone-recording-1", "stored", "lead-1")


@pytest.mark.asyncio
async def test_manual_backfill_marks_processing_recordings_without_failing(isolated_db, monkeypatch):
    from services.zoom_recording_sync_service import ensure_zoom_recording_schema, sync_zoom_recordings

    _seed_zoom_account(core.config.DB_PATH, product="meetings")

    async with db_module._async_session_factory() as session:
        await ensure_zoom_recording_schema(session)

    def _fake_zoom_request(account, method, path, payload=None):
        if path.startswith("/users/me/recordings?"):
            return {
                "ok": True,
                "data": {
                    "meetings": [
                        {
                            "uuid": "meeting-uuid",
                            "id": 987,
                            "host_id": "host-1",
                            "recording_files": [
                                {"id": "file-processing", "status": "processing", "file_type": "MP4", "download_url": "https://example.test/file.mp4"}
                            ],
                        }
                    ]
                },
            }
        raise AssertionError(path)

    monkeypatch.setattr("services.zoom_recording_sync_service._zoom_request", _fake_zoom_request)

    async with db_module._async_session_factory() as session:
        result = await sync_zoom_recordings(session, {"from": "2026-03-29", "to": "2026-03-29"})

    assert result["files_discovered"] == 1
    assert result["files_stored"] == 0
    assert result["pending"] == 1


def test_zoom_webhook_signature_verification_and_validation(isolated_db):
    from services.zoom_recording_sync_service import ensure_zoom_recording_schema

    _seed_zoom_account(core.config.DB_PATH, webhook_secret="whsec")

    async def _prepare():
        async with db_module._async_session_factory() as session:
            await ensure_zoom_recording_schema(session)
    asyncio.run(_prepare())

    app = FastAPI()

    async def override_get_session():
        async with db_module._async_session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    app.include_router(communications.router)

    client = TestClient(app)
    body = {"event": "endpoint.url_validation", "payload": {"plainToken": "plain-token"}}
    body_bytes = json.dumps(body).encode("utf-8")
    timestamp = "1710000000"
    message = f"v0:{timestamp}:{body_bytes.decode('utf-8')}"
    signature = "v0=" + hmac.new(b"whsec", message.encode("utf-8"), hashlib.sha256).hexdigest()

    response = client.post(
        "/api/zoom/webhook",
        data=body_bytes,
        headers={"content-type": "application/json", "x-zm-request-timestamp": timestamp, "x-zm-signature": signature},
    )
    assert response.status_code == 200
    assert response.json()["plainToken"] == "plain-token"


@pytest.mark.asyncio
async def test_recordings_endpoint_exposes_ai_summary_unsupported_state(isolated_db, monkeypatch):
    from services.zoom_recording_sync_service import ensure_zoom_recording_schema, sync_zoom_recordings

    _seed_zoom_account(core.config.DB_PATH, product="phone")
    _seed_lead(core.config.DB_PATH)

    async with db_module._async_session_factory() as session:
        await ensure_zoom_recording_schema(session)

    async def _fake_download(url, artifact_id, account=None):
        return f"D:/woonona-lead-machine/backend/recordings/{artifact_id}.mp3"

    def _fake_zoom_request(account, method, path, payload=None):
        if path.startswith("/phone/call_logs?"):
            return {
                "ok": True,
                "data": {
                    "call_logs": [
                        {
                            "id": "call-1",
                            "date_time": "2026-03-29T10:00:00Z",
                            "direction": "outbound",
                            "caller_number": "+61299990000",
                            "callee_number": "+61412345678",
                            "result": "Answered",
                            "duration": 60,
                        }
                    ]
                },
            }
        if path == "/phone/call_logs/call-1/recordings":
            return {
                "ok": True,
                "data": {
                    "recordings": [
                        {
                            "id": "phone-recording-1",
                            "file_url": "https://example.test/recording.mp3",
                            "download_url": "https://example.test/recording.mp3",
                            "file_type": "mp3",
                            "duration": 60,
                        }
                    ]
                },
            }
        raise AssertionError(path)

    monkeypatch.setattr("services.zoom_recording_sync_service._zoom_request", _fake_zoom_request)
    monkeypatch.setattr("services.zoom_recording_sync_service.download_recording", _fake_download)

    async with db_module._async_session_factory() as session:
        await sync_zoom_recordings(session, {"from": "2026-03-29", "to": "2026-03-29"})

    app = FastAPI()

    async def override_get_session():
        async with db_module._async_session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    app.include_router(recordings.router)
    client = TestClient(app)

    response = client.get("/api/leads/lead-1/recordings", headers={"X-API-KEY": core.config.API_KEY})
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["artifact_status"] == "stored"
    assert payload[0]["ai_summary_status"] == "unsupported"
    assert payload[0]["recording_available"] is True
    assert payload[0]["source_endpoint"] == "/phone/call_logs"
    assert payload[0]["ai_summary_body_status"] == "unavailable"
    assert payload[0]["transcript_status"] == "unavailable"


@pytest.mark.asyncio
async def test_phone_dry_run_persists_remote_recording_metadata(isolated_db, monkeypatch):
    from services.zoom_recording_sync_service import ensure_zoom_recording_schema, sync_zoom_recordings

    _seed_zoom_account(core.config.DB_PATH, product="phone")
    _seed_lead(core.config.DB_PATH)

    async with db_module._async_session_factory() as session:
        await ensure_zoom_recording_schema(session)

    def _fake_zoom_request(account, method, path, payload=None):
        if path.startswith("/phone/call_logs?"):
            return {
                "ok": True,
                "data": {
                    "call_logs": [
                        {
                            "id": "call-log-1",
                            "call_id": "phone-call-1",
                            "date_time": "2026-03-29T10:00:00Z",
                            "direction": "outbound",
                            "caller_number": "+61299990000",
                            "callee_number": "+61412345678",
                            "result": "Auto Recorded",
                            "duration": 60,
                            "ai_call_summary_id": "ai-summary-1",
                            "owner": {"name": "Mansi Saxena", "extension_number": 800},
                        }
                    ]
                },
            }
        if path == "/phone/call_logs/call-log-1/recordings":
            return {
                "ok": True,
                "data": {
                    "recordings": [
                        {
                            "id": "phone-recording-1",
                            "file_url": "https://example.test/recording.mp3",
                            "download_url": "https://example.test/recording.mp3",
                            "file_type": "mp3",
                            "duration": 60,
                        }
                    ]
                },
            }
        raise AssertionError(path)

    monkeypatch.setattr("services.zoom_recording_sync_service._zoom_request", _fake_zoom_request)

    async with db_module._async_session_factory() as session:
        result = await sync_zoom_recordings(session, {"from": "2026-03-29", "to": "2026-03-29", "call_id": "call-log-1", "dry_run": True})

    assert result["files_discovered"] == 1

    conn = sqlite3.connect(core.config.DB_PATH)
    row = conn.execute(
        """
        SELECT provider_call_id, recording_url, recording_status, raw_payload
        FROM call_log
        WHERE provider = 'zoom' AND provider_call_id = 'phone-call-1'
        """
    ).fetchone()
    conn.close()

    assert row[:3] == (
        "phone-call-1",
        "https://example.test/recording.mp3",
        "available",
    )
    payload = json.loads(row[3])
    assert payload["ai_call_summary_id"] == "ai-summary-1"
    assert payload["zoom_call_log_id"] == "call-log-1"


def test_lead_recordings_returns_artifact_only_rows(isolated_db):
    from services.zoom_recording_sync_service import ensure_zoom_recording_schema

    _seed_lead(core.config.DB_PATH, lead_id="lead-artifact", phone="0488007722")

    async def _seed_artifact():
        async with db_module._async_session_factory() as session:
            await ensure_zoom_recording_schema(session)
            await session.execute(
                text(
                    """
                    INSERT INTO zoom_recording_artifacts (
                        id, integration_id, product, artifact_type, external_id, external_parent_id,
                        meeting_uuid, meeting_id, call_id, recording_file_id, lead_id, linked_entity_type,
                        linked_entity_id, download_url, file_url, file_type, status, ai_summary_status,
                        unmatched_reason, error_message, storage_uri, raw_payload, discovered_at, processed_at,
                        created_at, updated_at
                    ) VALUES (
                        :id, '', 'phone', 'recording', :external_id, :external_parent_id,
                        '', '', :call_id, :recording_file_id, :lead_id, 'lead',
                        :lead_id, '', 'https://example.test/recording.mp3', 'mp3', 'discovered', 'unsupported',
                        '', '', '', '{}', :created_at, NULL, :created_at, :created_at
                    )
                    """
                ),
                {
                    "id": "artifact-only-row",
                    "external_id": "artifact-only-external",
                    "external_parent_id": "artifact-call-1",
                    "call_id": "artifact-call-1",
                    "recording_file_id": "artifact-only-external",
                    "lead_id": "lead-artifact",
                    "created_at": now_iso(),
                },
            )
            await session.commit()

    asyncio.run(_seed_artifact())

    app = FastAPI()

    async def override_get_session():
        async with db_module._async_session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    app.include_router(recordings.router)
    client = TestClient(app)

    response = client.get("/api/leads/lead-artifact/recordings", headers={"X-API-KEY": core.config.API_KEY})
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["call_id"] == "artifact-call-1"
    assert payload[0]["artifact_status"] == "discovered"
    assert payload[0]["ai_summary_status"] == "unsupported"


@pytest.mark.asyncio
async def test_unmatched_zoom_calls_endpoint_returns_review_items(isolated_db):
    from services.zoom_recording_sync_service import ensure_zoom_recording_schema
    from services.zoom_call_sync_service import ensure_call_log_schema

    async with db_module._async_session_factory() as session:
        await ensure_zoom_recording_schema(session)
        await ensure_call_log_schema(session)
        await session.execute(
            text(
                """
                INSERT INTO call_log (
                    id, lead_id, lead_address, user_id, outcome, connected, timestamp, call_duration_seconds,
                    duration_seconds, note, operator, logged_at, logged_date, next_action_due, provider,
                    provider_call_id, direction, from_number, to_number, raw_payload, recording_url,
                    recording_status, recording_duration_seconds
                ) VALUES (
                    'zoom-unmatched-1', '', '', 'Zoom', 'spoke', 1, :ts, 45,
                    45, 'Answered', 'Zoom', :ts, :logged_date, NULL, 'zoom',
                    'zoom-call-1', 'outbound', '800', '+61412345678',
                    :raw_payload, 'https://example.test/recording.mp3', 'available', 45
                )
                """
            ),
            {
                "ts": datetime.now().isoformat(),
                "logged_date": date.today().isoformat(),
                "raw_payload": json.dumps(
                    {
                        "zoom_source_endpoint": "/phone/call_logs",
                        "zoom_owner_name": "Mansi Saxena",
                        "download_url": "https://example.test/recording.mp3",
                        "ai_call_summary_id": "ai-summary-ref-1",
                        "zoom_unmatched_reason": "phone_match_not_found",
                    }
                )
            },
        )
        await session.commit()

    app = FastAPI()

    async def override_get_session():
        async with db_module._async_session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    app.include_router(recordings.router)
    client = TestClient(app)

    response = client.get("/api/zoom/unmatched-calls?days=7&limit=10", headers={"X-API-KEY": core.config.API_KEY})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["items"][0]["remote_number"] == "+61412345678"
    assert payload["items"][0]["source_endpoint"] == "/phone/call_logs"
    assert payload["items"][0]["recording_available"] is True
    assert payload["items"][0]["ai_summary_body_status"] == "unavailable"
