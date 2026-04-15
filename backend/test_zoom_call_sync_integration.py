import asyncio
import sqlite3
import uuid
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import core.config
import core.database as db_module
from services.zoom_call_sync_service import sync_zoom_calls_for_date

TEST_ROOT = Path(__file__).resolve().parent / "test_dbs"


@pytest.fixture
def isolated_db(monkeypatch):
    TEST_ROOT.mkdir(parents=True, exist_ok=True)
    test_db = TEST_ROOT / f"zoom_call_sync_integration_{uuid.uuid4().hex}.db"
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


def _seed_lead(db_path: str) -> None:
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
            "lead-1",
            "lead-1 Example Street, Woonona NSW 2517",
            "Woonona",
            "2517",
            "Owner lead-1",
            "Manual",
            "manual_entry",
            55,
            70,
            '["owner@example.com"]',
            '["+61400000000"]',
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


@pytest.mark.asyncio
async def test_sync_zoom_calls_persists_recording_linkage_and_marks_shadow_call_stored(isolated_db, monkeypatch):
    _seed_lead(core.config.DB_PATH)

    async def _fake_download_recording(recording_url: str, call_id: str, account=None):
        assert recording_url == "https://example.test/recordings/call-1.mp3"
        return f"D:/woonona-lead-machine/backend/recordings/{call_id}.mp3"

    async def _fake_resolve_zoom_account(session):
        return {}

    monkeypatch.setattr("services.zoom_call_sync_service._resolve_zoom_account", _fake_resolve_zoom_account)
    monkeypatch.setattr(
        "services.zoom_call_sync_service._fetch_zoom_calls_for_date",
        lambda account, target_date: {
            "ok": True,
            "endpoint": "/phone/call_logs",
            "calls": [
                {
                    "id": "zoom-call-1",
                    "date_time": "2026-03-22T10:00:00+11:00",
                    "direction": "outbound",
                    "to": "+61400000000",
                    "from": "+61299990000",
                    "result": "Answered",
                    "duration": 84,
                    "download_url": "https://example.test/recordings/call-1.mp3",
                }
            ],
        },
    )
    monkeypatch.setattr("services.recording_service.download_recording", _fake_download_recording)

    async with db_module._async_session_factory() as session:
        result = await sync_zoom_calls_for_date(session, "2026-03-22", force=True)

    assert result["ok"] is True
    assert result["imported"] == 1

    conn = sqlite3.connect(core.config.DB_PATH)
    call_log_row = conn.execute(
        "SELECT provider_call_id, recording_url FROM call_log WHERE provider = 'zoom' AND provider_call_id = 'zoom-call-1'"
    ).fetchone()
    speech_call_row = conn.execute(
        "SELECT id, external_call_id, audio_uri, audio_storage_status, metadata_json FROM calls WHERE id = 'zoom-call-1'"
    ).fetchone()
    conn.close()

    assert call_log_row == ("zoom-call-1", "https://example.test/recordings/call-1.mp3")
    assert speech_call_row is not None
    assert speech_call_row[1] == "zoom-call-1"
    assert speech_call_row[2] == "recordings/zoom-call-1.mp3"
    assert speech_call_row[3] == "stored"
    assert "recording_url" in (speech_call_row[4] or "")


@pytest.mark.asyncio
async def test_sync_zoom_calls_fetches_recording_metadata_when_call_log_payload_has_no_url(isolated_db, monkeypatch):
    _seed_lead(core.config.DB_PATH)

    async def _fake_download_recording(recording_url: str, call_id: str, account=None):
        assert recording_url == "https://example.test/recordings/call-2.mp3"
        assert call_id == "zoom-call-2"
        return f"D:/woonona-lead-machine/backend/recordings/{call_id}.mp3"

    async def _fake_resolve_zoom_account(session):
        return {"client_id": "x", "client_secret": "y", "account_id": "z", "use_account_credentials": 1}

    monkeypatch.setattr("services.zoom_call_sync_service._resolve_zoom_account", _fake_resolve_zoom_account)
    monkeypatch.setattr(
        "services.zoom_call_sync_service._fetch_zoom_calls_for_date",
        lambda account, target_date: {
            "ok": True,
            "endpoint": "/phone/call_logs",
            "calls": [
                {
                    "id": "zoom-call-2",
                    "date_time": "2026-03-22T11:00:00+11:00",
                    "direction": "outbound",
                    "to": "+61400000000",
                    "from": "+61299990000",
                    "result": "Answered",
                    "duration": 64,
                }
            ],
        },
    )
    monkeypatch.setattr(
        "services.zoom_call_sync_service._fetch_zoom_recording_metadata",
        lambda account, provider_call_id: {"file_url": "https://example.test/recordings/call-2.mp3"},
    )
    monkeypatch.setattr("services.recording_service.download_recording", _fake_download_recording)

    async with db_module._async_session_factory() as session:
        result = await sync_zoom_calls_for_date(session, "2026-03-22", force=True)

    assert result["ok"] is True

    conn = sqlite3.connect(core.config.DB_PATH)
    call_log_row = conn.execute(
        "SELECT provider_call_id, recording_url FROM call_log WHERE provider = 'zoom' AND provider_call_id = 'zoom-call-2'"
    ).fetchone()
    speech_call_row = conn.execute(
        "SELECT id, audio_uri, audio_storage_status FROM calls WHERE id = 'zoom-call-2'"
    ).fetchone()
    conn.close()

    assert call_log_row == ("zoom-call-2", "https://example.test/recordings/call-2.mp3")
    assert speech_call_row == ("zoom-call-2", "recordings/zoom-call-2.mp3", "stored")
