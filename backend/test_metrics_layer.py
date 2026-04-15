import asyncio
import sqlite3
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from api.routes import analytics, leads

import core.config
import core.database as db_module
from core.utils import now_sydney


app = FastAPI()
app.include_router(leads.router)
app.include_router(analytics.router)


TEST_ROOT = Path("D:/woonona-lead-machine/backend/test_dbs")


@pytest.fixture
def isolated_db(monkeypatch):
    TEST_ROOT.mkdir(parents=True, exist_ok=True)
    test_db = TEST_ROOT / f"metrics-v1-{uuid.uuid4().hex}.db"
    db_path = str(test_db)
    database_url = f"sqlite+aiosqlite:///{test_db}"
    monkeypatch.setattr(core.config, "DB_PATH", db_path)
    monkeypatch.setattr(core.config, "DATABASE_URL", database_url)

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
    try:
        test_db.unlink(missing_ok=True)
    except PermissionError:
        pass


def seed_lead(conn: sqlite3.Connection, lead_id: str, created_at: str = "2026-03-24T08:00:00+11:00"):
    conn.execute(
        """
        INSERT OR REPLACE INTO leads (
            id, address, suburb, postcode, owner_name, trigger_type, record_type, heat_score,
            confidence_score, contact_emails, contact_phones, lat, lng, est_value, created_at, updated_at,
            activity_log, stage_note_history, status, conversion_score, compliance_score, readiness_score,
            call_today_score, evidence_score, queue_bucket, lead_archetype, contactability_status,
            owner_verified, contact_role, cadence_name, cadence_step, next_action_type, next_action_channel,
            next_action_title, next_action_reason, next_message_template, last_outcome, objection_reason,
            preferred_channel, strike_zone, touches_14d, touches_30d, route_queue, days_on_market,
            preferred_contact_method, signal_status, followup_frequency, followup_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            f"{lead_id} Example Street, Woonona NSW 2517",
            "Woonona",
            "2517",
            f"Owner {lead_id}",
            "Manual",
            "manual_entry",
            55,
            70,
            '["owner@example.com"]',
            '["+61400000000"]',
            -34.3430,
            150.9130,
            950000,
            created_at,
            created_at,
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
            "",
            "none",
            "active",
        ),
    )


def seed_call(
    conn: sqlite3.Connection,
    *,
    row_id: str,
    lead_id: str,
    outcome: str,
    connected: int,
    duration_seconds: int,
    logged_at: str,
    logged_date: str = "2026-03-24",
    note: str = "",
    user_id: str = "Shahid",
    timestamp: str | None = None,
    next_action_due: str | None = None,
):
    conn.execute(
        """
        INSERT INTO call_log (
            id, lead_id, lead_address, user_id, outcome, connected, timestamp, call_duration_seconds,
            duration_seconds, note, operator, logged_at, logged_date, next_action_due, provider,
            provider_call_id, direction, from_number, to_number, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row_id,
            lead_id,
            f"{lead_id} Example Street, Woonona NSW 2517",
            user_id,
            outcome,
            connected,
            timestamp or logged_at,
            duration_seconds,
            duration_seconds,
            note,
            user_id,
            logged_at,
            logged_date,
            next_action_due,
            "manual",
            f"provider-{row_id}",
            "outbound",
            "+61200000000",
            "+61400000000",
            "{}",
        ),
    )


def auth_headers():
    return {"X-API-KEY": core.config.API_KEY}


@pytest.mark.asyncio
async def test_no_activity_day_returns_zero_sessions(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/daily?date=2026-03-24", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["dial_count"] == 0
    assert data["sessions_count"] == 0
    assert data["total_talk_time_seconds"] == 0
    assert data["total_session_time_seconds"] == 0
    assert data["idle_time_seconds"] == 0
    assert data["active_time_seconds"] == 0
    assert data["sessions"] == []
    assert data["timezone"]["name"] == "Australia/Sydney"
    assert data["timezone"]["date_interpreted_in_timezone"] == "2026-03-24"


@pytest.mark.asyncio
async def test_single_session_day(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=120,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-2",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:10:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/sessions?date=2026-03-24", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["sessions_count"] == 1
    assert len(data["sessions"]) == 1
    assert data["sessions"][0]["session_duration_seconds"] == 600
    assert data["sessions"][0]["session_active_time_seconds"] == 120
    assert data["sessions"][0]["session_idle_time_seconds"] == 480
    assert data["sessions"][0]["session_talk_time_seconds"] == 120
    assert data["sessions"][0]["session_dial_count"] == 2


@pytest.mark.asyncio
async def test_only_dials_return_zero_connects_and_zero_talk_time(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-2",
        outcome="voicemail",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:03:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/daily?date=2026-03-24", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["dial_count"] == 2
    assert data["connect_count"] == 0
    assert data["conversation_count"] == 0
    assert data["total_talk_time_seconds"] == 0
    assert data["total_talk_time_minutes"] == 0
    assert data["avg_talk_time_seconds"] == 0
    assert data["active_time_seconds"] == 0
    assert data["conversion_rate"] == 0


@pytest.mark.asyncio
async def test_gap_equal_to_threshold_stays_in_same_session(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-2",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:16:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/daily?date=2026-03-24", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["sessions_count"] == 1
    assert data["sessions"][0]["session_idle_time_seconds"] == 900
    assert data["sessions"][0]["session_duration_seconds"] == 960


@pytest.mark.asyncio
async def test_multiple_sessions_separated_by_gap_greater_than_threshold(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-2",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:16:01+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/daily?date=2026-03-24", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["sessions_count"] == 2
    assert data["avg_session_duration_seconds"] == 30
    assert data["longest_session_seconds"] == 60
    assert data["session_boundary_threshold_seconds"] == 900


@pytest.mark.asyncio
async def test_session_metrics_aggregation_and_idle_gap_correctness(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_lead(conn, "lead-3")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-2",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:10:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-3",
        lead_id="lead-3",
        outcome="booked_appraisal",
        connected=1,
        duration_seconds=120,
        logged_at="2026-03-24T09:20:00+11:00",
        next_action_due="2026-03-25T16:00:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/daily?date=2026-03-24", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["dial_count"] == 3
    assert data["connect_count"] == 2
    assert data["conversation_count"] == 2
    assert data["total_talk_time_seconds"] == 180
    assert data["total_talk_time_minutes"] == 3
    assert data["avg_talk_time_seconds"] == 90
    assert data["appointments_booked_count"] == 1
    assert data["appraisal_booked_count"] == 1
    assert data["total_session_time_seconds"] == 1320
    assert data["idle_time_seconds"] == 1140
    assert data["active_time_seconds"] == 180
    assert data["idle_blocks_count"] == 2
    assert round(data["conversion_rate"], 4) == 0.5


@pytest.mark.asyncio
async def test_timeline_session_assignment(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_lead(conn, "lead-3")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=90,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-2",
        outcome="not_interested",
        connected=1,
        duration_seconds=30,
        logged_at="2026-03-24T09:05:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-3",
        lead_id="lead-3",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:25:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/timeline?date=2026-03-24", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    activities = data["activities"]
    assert [activity["id"] for activity in activities] == ["call-1", "call-2", "call-3"]
    assert activities[0]["gap_from_previous_seconds"] == 0
    assert activities[1]["gap_from_previous_seconds"] == 210
    assert activities[1]["idle_gap_seconds"] == 210
    assert activities[0]["session_id"] == activities[1]["session_id"]
    assert activities[2]["session_id"] != activities[1]["session_id"]
    assert len(data["sessions"]) == 2


@pytest.mark.asyncio
async def test_sydney_date_handling_uses_local_timestamp(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_call(
        conn,
        row_id="call-utc",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=120,
        logged_at="2026-03-23T23:30:00+00:00",
        logged_date="2026-03-24",
        timestamp="2026-03-23T23:30:00+00:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/timeline?date=2026-03-24", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["activities"][0]["local_timestamp_sydney"].startswith("2026-03-24T10:30:00+11:00")


@pytest.mark.asyncio
async def test_range_aggregation_with_sessions(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1", created_at="2026-03-20T08:00:00+11:00")
    seed_lead(conn, "lead-2", created_at="2026-03-22T08:00:00+11:00")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-21T09:00:00+11:00",
        logged_date="2026-03-21",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-1",
        outcome="booked_appraisal",
        connected=1,
        duration_seconds=120,
        logged_at="2026-03-24T10:00:00+11:00",
        logged_date="2026-03-24",
        next_action_due="2026-03-25T16:00:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/range?start_date=2026-03-20&end_date=2026-03-24", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["dial_count"] == 2
    assert data["sessions_count"] == 2
    assert data["connect_count"] == 2
    assert data["conversation_count"] == 2
    assert data["total_talk_time_seconds"] == 180
    assert data["appointments_booked_count"] == 1
    assert data["avg_session_duration_seconds"] == 90
    assert data["longest_session_seconds"] == 120


@pytest.mark.asyncio
async def test_filtering_by_outcome(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-2",
        outcome="booked_appraisal",
        connected=1,
        duration_seconds=120,
        logged_at="2026-03-24T09:10:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/daily?date=2026-03-24&outcome=booked_appraisal", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["dial_count"] == 1
    assert data["connect_count"] == 1
    assert data["appointments_booked_count"] == 1
    assert data["appraisal_booked_count"] == 1
    assert data["filter_metadata"]["outcome"] == "booked_appraisal"


@pytest.mark.asyncio
async def test_filtering_by_user_id(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-24T09:00:00+11:00",
        user_id="Alice",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-2",
        outcome="booked_appraisal",
        connected=1,
        duration_seconds=120,
        logged_at="2026-03-24T09:10:00+11:00",
        user_id="Bob",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/timeline?date=2026-03-24&user_id=Alice", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert len(data["activities"]) == 1
    assert data["activities"][0]["user_id"] == "Alice"
    assert data["filter_metadata"]["user_id"] == "Alice"


@pytest.mark.asyncio
async def test_lead_scoped_daily_metrics_with_no_calls(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-2",
        outcome="connected",
        connected=1,
        duration_seconds=90,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/daily?date=2026-03-24&lead_id=lead-1", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["dial_count"] == 0
    assert data["connect_count"] == 0
    assert data["sessions_count"] == 0
    assert data["filter_metadata"]["lead_id"] == "lead-1"


@pytest.mark.asyncio
async def test_lead_scoped_only_dials_metrics(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-1",
        outcome="voicemail",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:04:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-3",
        lead_id="lead-2",
        outcome="connected",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-24T09:05:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/daily?date=2026-03-24&lead_id=lead-1", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["dial_count"] == 2
    assert data["call_attempt_count"] == 2
    assert data["attempts_per_lead"] == 2
    assert data["connect_count"] == 0
    assert data["conversation_count"] == 0
    assert data["total_talk_time_seconds"] == 0
    assert data["total_talk_time_minutes"] == 0
    assert data["conversion_rate"] == 0


@pytest.mark.asyncio
async def test_lead_scoped_connected_calls_and_conversion_metrics(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-24T09:00:00+11:00",
        note="Reached owner",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-1",
        outcome="booked_appraisal",
        connected=1,
        duration_seconds=120,
        logged_at="2026-03-24T09:10:00+11:00",
        note="Booked appraisal",
    )
    seed_call(
        conn,
        row_id="call-3",
        lead_id="lead-2",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:12:00+11:00",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/daily?date=2026-03-24&lead_id=lead-1", headers=auth_headers())

    assert response.status_code == 200
    data = response.json()
    assert data["dial_count"] == 2
    assert data["leads_touched_count"] == 1
    assert data["attempts_per_lead"] == 2
    assert data["connect_count"] == 2
    assert data["conversation_count"] == 2
    assert data["total_talk_time_seconds"] == 180
    assert data["avg_talk_time_seconds"] == 90
    assert data["avg_talk_time_per_connect_seconds"] == 90
    assert data["avg_talk_time_per_conversation_seconds"] == 90
    assert data["appointments_booked_count"] == 1
    assert data["appraisal_booked_count"] == 1
    assert round(data["conversion_rate"], 4) == 0.5
    assert round(data["booked_per_dial_rate"], 4) == 0.5
    assert round(data["booked_per_connect_rate"], 4) == 0.5
    assert round(data["booked_per_conversation_rate"], 4) == 0.5
    assert data["first_contact_at"] == "2026-03-24T09:00:00+11:00"
    assert data["last_contact_at"] == "2026-03-24T09:10:00+11:00"
    assert data["hourly_breakdown"][9]["dial_count"] == 2
    assert data["outcome_breakdown"][0]["count"] >= 1
    assert data["user_breakdown"][0]["dial_count"] == 2
    assert data["session_rankings"]["best_by_talk_time"]["session_id"] == data["sessions"][0]["session_id"]
    assert data["rundown"]["working"]


@pytest.mark.asyncio
async def test_lead_scoped_idle_gap_and_session_splitting(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-24T09:00:00+11:00",
        note="First connect",
    )
    seed_call(
        conn,
        row_id="call-2",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=30,
        logged_at="2026-03-24T09:04:00+11:00",
        note="Second connect",
    )
    seed_call(
        conn,
        row_id="call-3",
        lead_id="lead-2",
        outcome="connected",
        connected=1,
        duration_seconds=120,
        logged_at="2026-03-24T09:10:00+11:00",
    )
    seed_call(
        conn,
        row_id="call-4",
        lead_id="lead-1",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:25:00+11:00",
        note="No answer after long gap",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        daily_response = await ac.get("/api/metrics/daily?date=2026-03-24&lead_id=lead-1", headers=auth_headers())
        timeline_response = await ac.get("/api/metrics/timeline?date=2026-03-24&lead_id=lead-1", headers=auth_headers())

    assert daily_response.status_code == 200
    assert timeline_response.status_code == 200
    daily_data = daily_response.json()
    timeline_data = timeline_response.json()
    assert daily_data["sessions_count"] == 2
    assert daily_data["total_session_time_seconds"] == 270
    assert daily_data["idle_time_seconds"] == 180
    assert daily_data["active_time_seconds"] == 90
    assert daily_data["idle_blocks_count"] == 1
    assert daily_data["sessions"][0]["session_duration_seconds"] == 270
    assert daily_data["sessions"][0]["session_idle_time_seconds"] == 180
    assert daily_data["sessions"][1]["session_duration_seconds"] == 0
    assert daily_data["sessions"][1]["session_idle_time_seconds"] == 0
    assert timeline_data["activities"][1]["gap_from_previous_seconds"] == 180
    assert timeline_data["activities"][1]["idle_gap_seconds"] == 180
    assert timeline_data["activities"][2]["gap_from_previous_seconds"] == 1230
    assert timeline_data["activities"][2]["idle_gap_seconds"] == 0
    assert timeline_data["activities"][2]["note"] == "No answer after long gap"


@pytest.mark.asyncio
async def test_lead_scoped_sydney_date_and_filters(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    seed_lead(conn, "lead-2")
    seed_call(
        conn,
        row_id="call-utc-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=120,
        logged_at="2026-03-23T23:30:00+00:00",
        logged_date="2026-03-24",
        timestamp="2026-03-23T23:30:00+00:00",
        note="UTC edge",
    )
    seed_call(
        conn,
        row_id="call-utc-2",
        lead_id="lead-1",
        outcome="booked_appraisal",
        connected=1,
        duration_seconds=90,
        logged_at="2026-03-24T10:45:00+11:00",
        logged_date="2026-03-24",
        note="Booked on lead one",
    )
    seed_call(
        conn,
        row_id="call-utc-3",
        lead_id="lead-2",
        outcome="booked_appraisal",
        connected=1,
        duration_seconds=60,
        logged_at="2026-03-24T11:00:00+11:00",
        logged_date="2026-03-24",
    )
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        full_response = await ac.get("/api/metrics/timeline?date=2026-03-24&lead_id=lead-1", headers=auth_headers())

    assert full_response.status_code == 200
    full_data = full_response.json()
    first_session_id = full_data["activities"][0]["session_id"]
    assert full_data["activities"][0]["local_timestamp_sydney"].startswith("2026-03-24T10:30:00+11:00")
    assert full_data["filter_metadata"]["lead_id"] == "lead-1"
    assert len(full_data["activities"]) == 2

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        filtered_response = await ac.get(
            f"/api/metrics/timeline?date=2026-03-24&lead_id=lead-1&outcome=booked_appraisal&session_id={first_session_id}",
            headers=auth_headers(),
        )

    assert filtered_response.status_code == 200
    filtered_data = filtered_response.json()
    assert len(filtered_data["activities"]) == 1
    assert filtered_data["activities"][0]["lead_id"] == "lead-1"
    assert filtered_data["activities"][0]["outcome"] == "booked_appraisal"
    assert filtered_data["filter_metadata"]["lead_id"] == "lead-1"
    assert filtered_data["filter_metadata"]["outcome"] == "booked_appraisal"
    assert filtered_data["filter_metadata"]["session_id"] == first_session_id


@pytest.mark.asyncio
async def test_log_call_endpoint_persists_required_metrics_fields(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/api/leads/lead-1/log-call",
            json={"outcome": "connected", "note": "Reached owner", "duration_seconds": 75, "user_id": "Alice"},
            headers=auth_headers(),
        )

    assert response.status_code == 200
    conn = sqlite3.connect(core.config.DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM call_log WHERE lead_id = 'lead-1'").fetchone()
    conn.close()

    assert row is not None
    assert row["lead_id"] == "lead-1"
    assert row["outcome"] == "connected"
    assert row["connected"] == 1
    assert row["duration_seconds"] == 75
    assert row["logged_date"] == now_sydney().strftime("%Y-%m-%d")
    assert row["user_id"] == "Alice"
    assert row["timestamp"] == row["logged_at"]
    assert row["call_duration_seconds"] == 75


@pytest.mark.asyncio
async def test_outcome_endpoint_also_writes_metrics_call_log_row(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn, "lead-1")
    conn.commit()
    conn.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/api/leads/lead-1/outcome",
            json={
                "outcome": "booked_appraisal",
                "appointment_at": "2026-03-25T16:00:00+11:00",
                "user_id": "Alice",
            },
            headers=auth_headers(),
        )

    assert response.status_code == 200
    conn = sqlite3.connect(core.config.DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT outcome, connected, next_action_due, user_id FROM call_log WHERE lead_id = 'lead-1' ORDER BY logged_at DESC LIMIT 1"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["outcome"] == "booked_appraisal"
    assert row["connected"] == 1
    assert row["next_action_due"] == "2026-03-25T16:00:00+11:00"
    assert row["user_id"] == "Alice"
