import asyncio
import sqlite3
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from api.routes import recordings
import core.config
import core.database as db_module
from services.zoom_call_sync_service import ensure_call_log_schema


app = FastAPI()
app.include_router(recordings.router)

TEST_ROOT = Path("D:/woonona-lead-machine/backend/test_dbs")


@pytest.fixture
def isolated_db(monkeypatch):
    TEST_ROOT.mkdir(parents=True, exist_ok=True)
    test_db = TEST_ROOT / f"recording_stream_fallback_{uuid.uuid4().hex}.db"
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


@pytest.mark.asyncio
async def test_stream_recording_downloads_on_demand_when_not_local(isolated_db, monkeypatch):
    async with db_module._async_session_factory() as session:
        await ensure_call_log_schema(session)
        await session.commit()

    conn = sqlite3.connect(core.config.DB_PATH)
    conn.execute(
        """
        INSERT INTO call_log (
            id, lead_id, lead_address, outcome, connected, duration_seconds, note, operator, logged_at, logged_date,
            provider, provider_call_id, direction, from_number, to_number, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "row-1",
            "lead-1",
            "1 Test St",
            "spoke",
            1,
            60,
            "",
            "Zoom",
            "2026-03-24T09:00:00+11:00",
            "2026-03-24",
            "zoom",
            "zoom-call-1",
            "outbound",
            "+61299990000",
            "+61400000000",
            "{}",
        ),
    )
    conn.commit()
    conn.close()

    download_root = Path("D:/woonona-lead-machine/.tmp")
    download_root.mkdir(parents=True, exist_ok=True)
    downloaded = download_root / f"zoom-call-1-{uuid.uuid4().hex}.mp3"
    downloaded.write_bytes(b"fake-audio")

    async def _fake_resolve_zoom_account(session):
        return {"client_id": "x", "client_secret": "y", "account_id": "z", "use_account_credentials": 1}

    def _fake_zoom_request(account, method, path, payload=None):
        assert path == "/phone/call_logs/zoom-call-1/recordings"
        return {"ok": True, "data": {"file_url": "https://example.test/recordings/zoom-call-1.mp3"}}

    async def _fake_download_recording(recording_url, call_id, account=None):
        assert recording_url == "https://example.test/recordings/zoom-call-1.mp3"
        assert call_id == "zoom-call-1"
        assert account is not None
        return str(downloaded)

    monkeypatch.setattr("api.routes.recordings._resolve_zoom_account", _fake_resolve_zoom_account)
    monkeypatch.setattr("api.routes.recordings._zoom_request", _fake_zoom_request)
    monkeypatch.setattr("api.routes.recordings.download_recording_asset", _fake_download_recording)

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/recordings/zoom-call-1/stream", headers=headers)

    assert response.status_code == 200
    assert response.content == b"fake-audio"

    conn = sqlite3.connect(core.config.DB_PATH)
    updated_row = conn.execute("SELECT recording_url FROM call_log WHERE id = 'row-1'").fetchone()
    conn.close()
    assert updated_row == ("https://example.test/recordings/zoom-call-1.mp3",)
    downloaded.unlink(missing_ok=True)
