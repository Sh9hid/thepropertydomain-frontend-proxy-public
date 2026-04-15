import asyncio
import json
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


app = FastAPI()
app.include_router(recordings.router)


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine") / f"call_intelligence_{uuid.uuid4().hex}.db"
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
    if test_db.exists():
        test_db.unlink()


def _auth_headers() -> dict[str, str]:
    return {"X-API-KEY": core.config.API_KEY}


def _seed_call_log_row(call_id: str) -> None:
    conn = sqlite3.connect(core.config.DB_PATH)
    conn.execute(
        """
        INSERT INTO call_log (
            id, lead_id, lead_address, user_id, outcome, connected, timestamp,
            call_duration_seconds, duration_seconds, note, operator, logged_at,
            logged_date, provider, provider_call_id, direction, from_number,
            to_number, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            call_id,
            "lead-1",
            "1 Test Street, Woonona NSW 2517",
            "Shahid",
            "connected",
            1,
            "2026-03-26T09:00:00+11:00",
            180,
            180,
            "Initial call",
            "Shahid",
            "2026-03-26T09:00:00+11:00",
            "2026-03-26",
            "manual",
            "provider-call-1",
            "outbound",
            "+61200000000",
            "+61400000000",
            "{}",
        ),
    )
    conn.commit()
    conn.close()


def test_extract_signals_detects_intent_next_step_booking_and_objections():
    from services.call_intelligence_service import extract_signals

    result = extract_signals(
        "I'm thinking of selling, but not now because the price feels too low. "
        "Call me next week and let's meet to book the appraisal."
    )

    assert result["intent_signal"] >= 0.7
    assert result["booking_attempted"] is True
    assert result["next_step_detected"] is True
    assert "not now" in result["objection_tags"]
    assert "price too low" in result["objection_tags"]


def test_extract_signals_detects_low_intent_researching():
    from services.call_intelligence_service import extract_signals

    result = extract_signals("We're just looking and researching options right now.")

    assert result["intent_signal"] <= 0.3
    assert "just researching" in result["objection_tags"]
    assert result["booking_attempted"] is False


@pytest.mark.asyncio
async def test_post_transcript_updates_call_log_with_structured_signals(isolated_db):
    _seed_call_log_row("call-1")

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/api/calls/call-1/transcript",
            headers=_auth_headers(),
            json={
                "transcript": (
                    "I'm thinking of selling but not now. "
                    "Call me next week and let's meet to book the appraisal."
                )
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["signals"]["intent_signal"] >= 0.7
    assert payload["signals"]["booking_attempted"] is True
    assert payload["signals"]["next_step_detected"] is True
    assert "not now" in payload["signals"]["objection_tags"]

    conn = sqlite3.connect(core.config.DB_PATH)
    row = conn.execute(
        """
        SELECT transcript, intent_signal, booking_attempted, objection_tags, next_step_detected, summary
        FROM call_log
        WHERE id = ?
        """,
        ("call-1",),
    ).fetchone()
    conn.close()

    assert row is not None
    assert "thinking of selling" in (row[0] or "").lower()
    assert float(row[1] or 0) >= 0.7
    assert int(row[2] or 0) == 1
    assert "not now" in json.loads(row[3] or "[]")
    assert int(row[4] or 0) == 1
    assert "seller" in (row[5] or "").lower()
