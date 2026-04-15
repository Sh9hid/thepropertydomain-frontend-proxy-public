import asyncio
import sqlite3
import uuid
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from api.routes import operator
import core.config
import core.database as db_module
from core.utils import now_sydney
from services.metrics_service import build_call_log_row, insert_call_log_row
from services.zoom_call_sync_service import ensure_call_log_schema


app = FastAPI()
app.include_router(operator.router)

TEST_ROOT = Path("D:/woonona-lead-machine/backend/test_dbs")


@pytest.fixture
def isolated_db(monkeypatch):
    TEST_ROOT.mkdir(parents=True, exist_ok=True)
    test_db = TEST_ROOT / f"call_runtime_{uuid.uuid4().hex}.db"
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


def _seed_lead(conn: sqlite3.Connection, *, lead_id: str, address: str, last_contacted_at: str | None = None, call_today_score: int = 50) -> None:
    now = now_sydney().isoformat()
    conn.execute(
        """
        INSERT INTO leads (
            id, address, suburb, postcode, owner_name, trigger_type, record_type, heat_score,
            confidence_score, contact_emails, contact_phones, lat, lng, est_value, created_at, updated_at,
            activity_log, stage_note_history, status, conversion_score, compliance_score, readiness_score,
            call_today_score, evidence_score, queue_bucket, lead_archetype, contactability_status,
            owner_verified, contact_role, cadence_name, cadence_step, next_action_type, next_action_channel,
            next_action_title, next_action_reason, next_message_template, last_outcome, objection_reason,
            preferred_channel, strike_zone, touches_14d, touches_30d, route_queue, days_on_market, signal_status,
            last_contacted_at, id4me_enriched, last_activity_type,
            preferred_contact_method, followup_frequency, followup_status
        ) VALUES (
            :id, :address, 'Woonona', '2517', 'Owner', 'Manual', 'manual_entry', 55,
            70, '[]', '[]', -34.3430, 150.9130, 975000, :created_at, :updated_at,
            '[]', '[]', 'active', 0, 0, 0,
            :call_today_score, 0, '', '', '',
            0, '', '', 0, '', '',
            '', '', '', '', '',
            '', '', 0, 0, '', 12, 'live',
            :last_contacted_at, 0, '',
            '', 'none', 'active'
        )
        """,
        {
            "id": lead_id,
            "address": address,
            "call_today_score": call_today_score,
            "last_contacted_at": last_contacted_at,
            "created_at": now,
            "updated_at": now,
        },
    )


@pytest.mark.asyncio
async def test_process_call_log_entry_populates_transcript_insights_and_next_action(isolated_db, monkeypatch):
    from services import call_runtime_service

    recording_path = Path("D:/woonona-lead-machine/.tmp") / f"runtime-{uuid.uuid4().hex}.mp3"
    recording_path.parent.mkdir(parents=True, exist_ok=True)
    recording_path.write_bytes(b"fake-audio")

    conn = sqlite3.connect(core.config.DB_PATH)
    _seed_lead(conn, lead_id="lead-1", address="1 Test Street, Woonona NSW 2517")
    conn.commit()
    conn.close()

    async with db_module._async_session_factory() as session:
        await ensure_call_log_schema(session)
        await insert_call_log_row(
            session,
            build_call_log_row(
                lead_id="lead-1",
                lead_address="1 Test Street, Woonona NSW 2517",
                outcome="connected",
                call_duration_seconds=120,
                note="",
                provider="manual",
                provider_call_id="manual-call-1",
                recording_url=str(recording_path),
                row_id="call-1",
            ),
        )
        await session.commit()

    class _FakeProvider:
        async def transcribe(self, *, call_id: str, audio_path: Path, context: dict):
            assert audio_path.exists()
            return {
                "full_text": "I am interested but the price is too low. Call me back next week.",
                "status": "completed",
            }

    monkeypatch.setattr(call_runtime_service, "get_transcription_provider", lambda: _FakeProvider())

    result = await call_runtime_service.process_call_log_entry("call-1")
    assert result["ok"] is True

    conn = sqlite3.connect(core.config.DB_PATH)
    call_row = conn.execute(
        "SELECT transcript, summary, next_step_detected FROM call_log WHERE id = 'call-1'"
    ).fetchone()
    insight_row = conn.execute(
        "SELECT summary, intent, objections, next_step_detected, appointment_booked FROM call_insights WHERE call_id = 'call-1'"
    ).fetchone()
    lead_row = conn.execute(
        "SELECT next_action_due, follow_up_due_at FROM leads WHERE id = 'lead-1'"
    ).fetchone()
    conn.close()

    assert "price is too low" in (call_row[0] or "")
    assert bool(call_row[2]) is True
    assert insight_row is not None
    assert "low-intent seller" in (insight_row[0] or "") or "interested seller" in (insight_row[0] or "")
    assert '"price too low"' in (insight_row[2] or "")
    assert lead_row[0]
    assert lead_row[1]
    recording_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_log_call_attempt_with_recording_schedules_postprocess(isolated_db, monkeypatch):
    from services import call_brief_service

    conn = sqlite3.connect(core.config.DB_PATH)
    _seed_lead(conn, lead_id="lead-2", address="2 Test Street, Woonona NSW 2517")
    conn.commit()
    conn.close()

    captured: dict[str, str] = {}

    def _fake_schedule(call_id: str) -> None:
        captured["call_id"] = call_id

    monkeypatch.setattr(call_brief_service, "schedule_call_postprocess", _fake_schedule)

    async with db_module._async_session_factory() as session:
        result = await call_brief_service.log_call_attempt(
            session,
            "lead-2",
            "connected",
            "Recorded call",
            95,
            "Shahid",
            None,
            "https://example.test/recording.mp3",
        )

    assert result["call_log_id"]
    assert captured["call_id"] == result["call_log_id"]


@pytest.mark.asyncio
async def test_command_next_lead_returns_stale_lead_and_context(isolated_db):
    stale_ts = (now_sydney() - timedelta(days=2)).isoformat()
    recent_ts = (now_sydney() - timedelta(minutes=30)).isoformat()

    conn = sqlite3.connect(core.config.DB_PATH)
    _seed_lead(conn, lead_id="lead-stale", address="10 Stale Street, Woonona NSW 2517", last_contacted_at=stale_ts, call_today_score=82)
    _seed_lead(conn, lead_id="lead-recent", address="20 Recent Street, Woonona NSW 2517", last_contacted_at=recent_ts, call_today_score=95)
    conn.execute(
        """
        INSERT INTO call_log (
            id, lead_id, lead_address, outcome, connected, duration_seconds, note, operator, logged_at, logged_date,
            provider, provider_call_id, direction, from_number, to_number, raw_payload, summary
        ) VALUES (
            'call-stale', 'lead-stale', '10 Stale Street, Woonona NSW 2517', 'connected_follow_up', 1, 120, '', 'Shahid',
            :logged_at, :logged_date, 'manual', 'provider-call-stale', 'outbound', '', '', '{}', 'Owner wants a follow-up next week.'
        )
        """,
        {"logged_at": stale_ts, "logged_date": stale_ts[:10]},
    )
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/api/command", headers=headers, json={"command": "NEXT_LEAD"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["command"] == "NEXT_LEAD"
    assert payload["lead"]["id"] == "lead-stale"
    assert "follow-up" in (payload["context"]["last_call_summary"] or "").lower()


@pytest.mark.asyncio
async def test_operator_command_center_unifies_revenue_execution_snapshot(isolated_db):
    now = now_sydney().replace(microsecond=0)
    today_ts = now.isoformat()
    stale_ts = (now - timedelta(days=2)).isoformat()
    overdue_due = (now - timedelta(days=1, hours=3)).isoformat()
    due_today = (now + timedelta(hours=2)).isoformat()
    appointment_at = (now + timedelta(hours=5)).isoformat()

    conn = sqlite3.connect(core.config.DB_PATH)
    _seed_lead(
        conn,
        lead_id="lead-command",
        address="30 Command Street, Woonona NSW 2517",
        last_contacted_at=stale_ts,
        call_today_score=88,
    )
    conn.execute(
        "UPDATE leads SET status = 'contacted', owner_name = 'Command Lead', heat_score = 82, est_value = 1350000 WHERE id = 'lead-command'"
    )
    conn.execute(
        """
        INSERT INTO call_log (
            id, lead_id, lead_address, outcome, connected, duration_seconds, call_duration_seconds, note, operator, user_id,
            logged_at, logged_date, timestamp, provider, provider_call_id, direction, from_number, to_number, raw_payload,
            summary, transcript, intent_signal, booking_attempted, next_step_detected, objection_tags
        ) VALUES (
            'call-command', 'lead-command', '30 Command Street, Woonona NSW 2517', 'connected_follow_up', 1, 240, 240,
            'Need to circle back on timing.', 'Shahid', 'Shahid', :logged_at, :logged_date, :logged_at, 'manual',
            'provider-call-command', 'outbound', '', '', '{}',
            'Owner wants a follow-up next week.', 'Call me back next week after I speak with my partner.', 0.82, 0, 1, '["bad timing"]'
        )
        """,
            {"logged_at": today_ts, "logged_date": today_ts[:10]},
        )
    conn.execute(
        """
        INSERT INTO tasks (
            id, lead_id, title, task_type, action_type, channel, due_at, status, notes, related_report_id,
            approval_status, message_subject, message_preview, rewrite_reason, superseded_by, cadence_name,
            cadence_step, auto_generated, priority_bucket, completed_at, created_at, updated_at
        ) VALUES (
            'task-overdue', 'lead-command', 'Call back about timing', 'call', 'call', 'call', :due_at, 'pending', '',
            '', 'not_required', '', '', '', '', 'follow_up', 1, 0, 'callback_due', NULL, :created_at, :updated_at
        )
        """,
        {"due_at": overdue_due, "created_at": now.isoformat(), "updated_at": now.isoformat()},
    )
    conn.execute(
        """
        INSERT INTO tasks (
            id, lead_id, title, task_type, action_type, channel, due_at, status, notes, related_report_id,
            approval_status, message_subject, message_preview, rewrite_reason, superseded_by, cadence_name,
            cadence_step, auto_generated, priority_bucket, completed_at, created_at, updated_at
        ) VALUES (
            'task-due', 'lead-command', 'Send follow-up SMS', 'sms', 'sms', 'sms', :due_at, 'pending', '',
            '', 'pending', '', 'Checking in after our call', '', '', 'follow_up', 2, 0, 'follow_up', NULL, :created_at, :updated_at
        )
        """,
        {"due_at": due_today, "created_at": now.isoformat(), "updated_at": now.isoformat()},
    )
    conn.execute(
        """
        INSERT INTO appointments (
            id, lead_id, title, starts_at, status, location, notes, cadence_name, auto_generated, created_at, updated_at
        ) VALUES (
            'appt-command', 'lead-command', 'Property appraisal', :starts_at, 'booked', 'On site', '',
            'booked_appraisal', 0, :created_at, :updated_at
        )
        """,
        {"starts_at": appointment_at, "created_at": now.isoformat(), "updated_at": now.isoformat()},
    )
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/operator/command-center", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["overdue_tasks"] == 1
    assert payload["summary"]["due_today_tasks"] == 1
    assert payload["summary"]["appointments_today"] == 1
    assert payload["summary"]["call_queue_count"] >= 1
    assert payload["summary"]["missed_deals_count"] >= 1
    assert payload["metrics"]["dial_count"] == 1
    assert payload["metrics"]["connect_count"] == 1
    assert payload["next_lead"]["lead"]["id"] == "lead-command"
    assert payload["call_queue"][0]["lead_id"] == "lead-command"
    assert payload["operator_day"]["overdue"][0]["id"] == "task-overdue"
    assert payload["operator_day"]["due_today"][0]["id"] == "task-due"
    assert payload["operator_day"]["appointments"][0]["id"] == "appt-command"
    assert payload["missed_deals"]["cards"][0]["lead_id"] == "lead-command"
