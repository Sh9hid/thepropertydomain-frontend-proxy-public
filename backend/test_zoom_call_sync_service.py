import asyncio
import sqlite3
import uuid
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import core.config
import core.database as db_module
from services.zoom_call_sync_service import (
    _normalize_zoom_call_entry,
    ensure_call_log_schema,
    sync_zoom_calls_for_date,
)


TEST_ROOT = Path("D:/woonona-lead-machine/backend/test_dbs")


@pytest.fixture
def isolated_db(monkeypatch):
    TEST_ROOT.mkdir(parents=True, exist_ok=True)
    test_db = TEST_ROOT / f"zoom_call_sync_service_{uuid.uuid4().hex}.db"
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
    test_db.unlink(missing_ok=True)


def test_normalize_zoom_call_entry_maps_voicemail():
    row = _normalize_zoom_call_entry(
        {
            "id": "call-1",
            "date_time": "2026-03-22T00:15:00Z",
            "direction": "outbound",
            "to": "+61412345678",
            "from": "+61299990000",
            "result": "Left Voicemail",
            "duration": 0,
        },
        "2026-03-22",
    )
    assert row is not None
    assert row["outcome"] == "left_voicemail"
    assert row["connected"] is False
    assert row["to_number"] == "+61412345678"
    assert row["provider_call_id"] == "call-1"


def test_normalize_zoom_call_entry_maps_answered_call_to_spoke():
    row = _normalize_zoom_call_entry(
        {
            "call_id": "call-2",
            "start_time": "2026-03-22T02:30:00Z",
            "direction": "outbound",
            "callee_number": "+61430000000",
            "caller_number": "+61299990000",
            "status": "Answered",
            "duration_seconds": 84,
        },
        "2026-03-22",
    )
    assert row is not None
    assert row["outcome"] == "spoke"
    assert row["connected"] is True
    assert row["duration_seconds"] == 84


def test_normalize_zoom_call_entry_keeps_connected_inbound_calls():
    row = _normalize_zoom_call_entry(
        {
            "id": "call-3",
            "date_time": "2026-03-22T04:00:00Z",
            "direction": "inbound",
            "to": "+61299990000",
            "from": "+61499990000",
            "status": "Answered",
            "duration": 22,
        },
        "2026-03-22",
    )
    assert row is not None
    assert row["connected"] is True
    assert row["direction"] == "inbound"


@pytest.mark.asyncio
async def test_sync_zoom_calls_persists_recording_fields(isolated_db, monkeypatch):
    async with db_module._async_session_factory() as session:
        await ensure_call_log_schema(session)
        await session.commit()

    download_root = Path("D:/woonona-lead-machine/.tmp")
    download_root.mkdir(parents=True, exist_ok=True)
    downloaded = download_root / f"call-call-zoom-1-{uuid.uuid4().hex}.mp3"
    downloaded.write_bytes(b"fake-audio")

    async def _fake_resolve_zoom_account(session):
        return {"client_id": "x", "client_secret": "y", "account_id": "z", "use_account_credentials": 1}

    def _fake_fetch_zoom_calls_for_date(account, target_date):
        return {
            "ok": True,
            "endpoint": "/phone/call_logs",
            "calls": [
                {
                    "id": "call-zoom-1",
                    "date_time": "2026-03-24T09:00:00+11:00",
                    "direction": "outbound",
                    "to": "+61412345678",
                    "from": "+61299990000",
                    "result": "Answered",
                    "duration": 96,
                }
            ],
        }

    async def _fake_find_matching_lead(session, phone_number):
        return {"id": "lead-1", "address": "1 Test Street"}

    def _fake_fetch_zoom_recording_metadata(account, provider_call_id):
        assert provider_call_id == "call-zoom-1"
        return {
            "id": "recording-1",
            "download_url": "https://example.test/download/call-zoom-1",
            "file_url": "https://example.test/file/call-zoom-1.mp3",
            "file_type": "mp3",
            "duration": 91,
            "recording_start": "2026-03-24T09:00:00+11:00",
            "recording_end": "2026-03-24T09:01:31+11:00",
        }

    async def _fake_download_recording(recording_url, call_id, account=None):
        assert recording_url == "https://example.test/file/call-zoom-1.mp3"
        assert call_id == "call-zoom-1"
        assert account is not None
        return str(downloaded)

    monkeypatch.setattr("services.zoom_call_sync_service._resolve_zoom_account", _fake_resolve_zoom_account)
    monkeypatch.setattr("services.zoom_call_sync_service._fetch_zoom_calls_for_date", _fake_fetch_zoom_calls_for_date)
    monkeypatch.setattr("services.zoom_call_sync_service._find_matching_lead", _fake_find_matching_lead)
    monkeypatch.setattr("services.zoom_call_sync_service._fetch_zoom_recording_metadata", _fake_fetch_zoom_recording_metadata)
    monkeypatch.setattr("services.recording_service.download_recording", _fake_download_recording)

    async with db_module._async_session_factory() as session:
        result = await sync_zoom_calls_for_date(session, "2026-03-24", force=True)

    assert result["ok"] is True
    assert result["imported"] == 1

    conn = sqlite3.connect(core.config.DB_PATH)
    row = conn.execute(
        """
        SELECT recording_url, recording_status, recording_duration_seconds
        FROM call_log
        WHERE provider = 'zoom' AND provider_call_id = 'call-zoom-1'
        """
    ).fetchone()
    conn.close()

    assert row == (
        "https://example.test/file/call-zoom-1.mp3",
        "available",
        91,
    )
    downloaded.unlink(missing_ok=True)
