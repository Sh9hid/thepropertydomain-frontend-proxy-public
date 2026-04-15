import sqlite3
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

import httpx
import pytest

import main
from main import app
from core.config import API_KEY, DB_PATH
from core.utils import now_iso, now_sydney
from models.schemas import LEAD_COLUMNS_SQL, TASK_COLUMNS_SQL, APPOINTMENT_COLUMNS_SQL, SOLD_EVENT_COLUMNS_SQL


import core.config
import core.database as db_module

# Ensure dev DB schema is up to date before running smoke tests
_DB_AVAILABLE = False
try:
    db_module.init_db()
    _DB_AVAILABLE = True
except Exception:
    pass  # DB may not be available in CI / non-Postgres environments

_skip_no_db = pytest.mark.skipif(not _DB_AVAILABLE, reason="Production DB not reachable")

@_skip_no_db
@pytest.mark.asyncio
async def test_read_leads():
    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/leads", headers=headers)
    assert response.status_code == 200
    leads = response.json()
    assert isinstance(leads, dict)
    assert "leads" in leads
    if leads["leads"]:
        lead = leads["leads"][0]
        assert "address" in lead
        assert "owner_name" in lead
        assert "call_today_score" in lead
        assert "contact_emails" in lead
        assert "status" in lead


@_skip_no_db
@pytest.mark.asyncio
async def test_read_analytics():
    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/analytics", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "active_leads" in data


@pytest.mark.asyncio
@_skip_no_db
async def test_local_dev_preflight_allows_vite_fallback_port():
    headers = {
        "Origin": "http://localhost:5175",
        "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "x-api-key",
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.options("/api/metrics/daily?date=2026-03-31", headers=headers)
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "http://localhost:5175"


@pytest.mark.asyncio
@_skip_no_db
async def test_pipeline_endpoint_shape():
    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/pipeline", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "funnel_order" in data
    assert "stage_counts" in data
    assert isinstance(data["stage_counts"], dict)


@pytest.mark.asyncio
@_skip_no_db
async def test_generate_outreach_for_existing_lead():
    conn = sqlite3.connect(core.config.DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM leads LIMIT 1")
    row = c.fetchone()
    conn.close()
    if not row:
        pytest.skip("No leads present in DB")

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(f"/api/leads/{row[0]}/generate_outreach", json={"tone": "professional"}, headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert "pack" in payload
    assert "call_opener" in payload["pack"]


@pytest.mark.asyncio
@_skip_no_db
async def test_advance_rejects_invalid_transition():
    conn = sqlite3.connect(core.config.DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, status FROM leads LIMIT 1")
    row = c.fetchone()
    conn.close()
    if not row:
        pytest.skip("No leads present in DB")

    lead_id, current_status = row
    # Force a likely invalid jump to converted from early stages.
    if current_status not in ("appt_booked", "mortgage_appt_booked", "converted"):
        headers = {"X-API-KEY": core.config.API_KEY}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.post(
                f"/api/leads/{lead_id}/advance",
                json={"status": "converted", "note": "test invalid jump"},
                headers=headers,
            )
        assert response.status_code == 400


@_skip_no_db
def test_no_duplicate_addresses():
    conn = sqlite3.connect(core.config.DB_PATH)
    c = conn.cursor()
    c.execute("SELECT address, COUNT(*) FROM leads GROUP BY address HAVING COUNT(*) > 1")
    dupes = c.fetchall()
    conn.close()
    assert len(dupes) == 0, f"Found duplicate addresses: {dupes}"


@_skip_no_db
def test_schema_integrity():
    expected_cols = [
        "id",
        "address",
        "suburb",
        "postcode",
        "owner_name",
        "trigger_type",
        "heat_score",
        "confidence_score",
        "status",
        "conversion_score",
        "compliance_score",
        "readiness_score",
        "next_actions",
        "source_evidence",
        "updated_at",
    ]

    conn = sqlite3.connect(core.config.DB_PATH)
    c = conn.cursor()
    c.execute("PRAGMA table_info(leads)")
    cols = [row[1] for row in c.fetchall()]
    conn.close()
    for col in expected_cols:
        assert col in cols, f"Missing column in leads table: {col}"


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    test_db = tmp_path / "sold_events.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db}")
    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    test_factory = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(db_module, "_async_session_factory", test_factory)

    async def override_get_session():
        async with test_factory() as session:
            yield session

    app.dependency_overrides[db_module.get_session] = override_get_session
    db_module.init_db()
    # Create supplementary tables not covered by init_db
    conn = sqlite3.connect(test_db)
    _create_test_table(conn, "appointments", APPOINTMENT_COLUMNS_SQL)
    _create_test_table(conn, "notes", {"id": "INTEGER PRIMARY KEY AUTOINCREMENT", "lead_id": "TEXT", "note_type": "TEXT", "content": "TEXT", "created_at": "TEXT"})
    conn.commit()
    conn.close()
    try:
        yield test_db
    finally:
        app.dependency_overrides.pop(db_module.get_session, None)
        asyncio.run(test_engine.dispose())


def _create_test_table(conn: sqlite3.Connection, table_name: str, columns: dict[str, str]):
    column_sql = ", ".join(f"{name} {definition}" for name, definition in columns.items())
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_sql})")


def seed_lead(conn: sqlite3.Connection, lead_id: str = "lead-1", owner_name: str = "Owner One"):
    conn.execute(
        """
        INSERT OR REPLACE INTO leads (
            id, address, suburb, postcode, owner_name, trigger_type, record_type, heat_score,
            scenario, strategic_value, contact_status, confidence_score, contact_emails, contact_phones,
            lat, lng, date_found, key_details, property_images, features, summary_points, source_evidence,
            linked_files, source_tags, risk_flags, next_actions, created_at, updated_at, activity_log, stage_note_history, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            "10 Example Street, Woonona NSW 2517",
            "Woonona",
            "2517",
            owner_name,
            "Manual",
            "manual_entry",
            55,
            "Seed lead",
            "",
            "",
            70,
            '["owner@example.com"]',
            '["+61400000000"]',
            -34.3430,
            150.9130,
            now_iso(),
            "[]",
            "[]",
            "[]",
            "[]",
            '["https://example.com/evidence"]',
            "[]",
            "[]",
            "[]",
            "[]",
            now_iso(),
            now_iso(),
            "[]",
            "[]",
            "captured",
        ),
    )


def seed_task(
    conn: sqlite3.Connection,
    task_id: str,
    lead_id: str,
    *,
    title: str,
    task_type: str,
    channel: str,
    due_at: str,
    approval_status: str = "not_required",
    priority_bucket: str = "follow_up",
    message_subject: str = "",
    message_preview: str = "",
    cadence_step: int = 3,
):
    now = now_iso()
    conn.execute(
        """
        INSERT OR REPLACE INTO tasks (
            id, lead_id, title, task_type, action_type, channel, due_at, status, notes, related_report_id,
            approval_status, message_subject, message_preview, rewrite_reason, superseded_by, cadence_name,
            cadence_step, auto_generated, priority_bucket, completed_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', '', '', ?, ?, ?, 'Test task', '', 'hot_seller_10_day', ?, 1, ?, NULL, ?, ?)
        """,
        (
            task_id,
            lead_id,
            title,
            task_type,
            task_type,
            channel,
            due_at,
            approval_status,
            message_subject,
            message_preview,
            cadence_step,
            priority_bucket,
            now,
            now,
        ),
    )


@pytest.mark.asyncio
async def test_create_and_list_sold_event(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    conn.execute(
        """
        INSERT OR REPLACE INTO leads (
            id, address, suburb, postcode, owner_name, trigger_type, record_type, heat_score,
            scenario, strategic_value, contact_status, confidence_score, contact_emails, contact_phones,
            lat, lng, date_found, key_details, property_images, features, summary_points, source_evidence,
            linked_files, source_tags, risk_flags, next_actions, created_at, updated_at, activity_log, stage_note_history, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "lead-1",
            "10 Example Street, Woonona NSW 2517",
            "Woonona",
            "2517",
            "Owner One",
            "Manual",
            "manual_entry",
            55,
            "Seed lead",
            "",
            "",
            70,
            "[]",
            "[]",
            -34.3430,
            150.9130,
            now_iso(),
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            now_iso(),
            now_iso(),
            "[]",
            "[]",
            "captured",
        ),
    )
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    payload = {
        "address": "10 Example Street, Woonona NSW 2517",
        "suburb": "Woonona",
        "postcode": "2517",
        "sale_date": "2026-03-12",
        "sale_price": "$1,250,000",
        "lat": -34.3430,
        "lng": 150.9130,
        "source_name": "agency_site",
        "source_url": "https://example.com/sold/10-example-street",
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/api/sold-events", json=payload, headers=headers)
        recent = await ac.get("/api/sold-events/recent", headers=headers)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "created"
    
    matched = body["event"]["matched_lead_ids"]
    if isinstance(matched, str):
        import json
        matched = json.loads(matched)
    assert matched == ["lead-1"]
    assert recent.status_code == 200
    assert recent.json()[0]["source_name"] == "agency_site"

    conn = sqlite3.connect(core.config.DB_PATH)
    note_row = conn.execute("SELECT note_type, content FROM notes WHERE lead_id = 'lead-1'").fetchone()
    conn.close()
    assert note_row is not None
    assert note_row[0] == "sold_event"
    assert "example.com/sold/10-example-street" in note_row[1]


@pytest.mark.asyncio
async def test_operator_calendar_month_and_day(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn)
    seed_task(
        conn,
        "task-call-1",
        "lead-1",
        title="Initial value review call",
        task_type="call",
        channel="call",
        due_at="2026-03-16T09:15:00+11:00",
        priority_bucket="call_now",
    )
    seed_task(
        conn,
        "task-email-1",
        "lead-1",
        title="Market update email",
        task_type="email",
        channel="email",
        due_at="2026-03-16T11:00:00+11:00",
        approval_status="pending",
        priority_bucket="send_now",
        message_subject="Quick property update",
        message_preview="Hello from the operator calendar",
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO appointments (id, lead_id, title, starts_at, status, location, notes, cadence_name, auto_generated, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "appt-1",
            "lead-1",
            "Property appraisal",
            "2026-03-16T16:00:00+11:00",
            "booked",
            "On site",
            "",
            "",
            0,
            now_iso(),
            now_iso(),
        ),
    )
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        month = await ac.get("/api/operator/calendar/month?start=2026-03-01", headers=headers)
        day = await ac.get("/api/operator/calendar/day?date=2026-03-16", headers=headers)

    assert month.status_code == 200
    assert day.status_code == 200
    month_body = month.json()
    day_body = day.json()
    march_16 = next(item for item in month_body["days"] if item["date"] == "2026-03-16")
    assert march_16["calls"] == 1
    assert march_16["emails"] == 1
    assert march_16["appointments"] == 1
    assert march_16["pending_approvals"] == 1
    assert day_body["counts"]["calls"] == 1
    assert day_body["counts"]["emails"] == 1
    assert day_body["counts"]["appointments"] == 1
    assert day_body["tasks"][0]["phase_label"] == "Hot Seller 10 Day · Day 3"


@pytest.mark.asyncio
async def test_approve_and_skip_task(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    import datetime
    due_email = (now_sydney() + datetime.timedelta(days=2)).isoformat()
    due_sms = (now_sydney() + datetime.timedelta(days=3)).isoformat()
    seed_lead(conn)
    seed_task(
        conn,
        "task-email-approve",
        "lead-1",
        title="Market update email",
        task_type="email",
        channel="email",
        due_at=due_email,
        approval_status="pending",
        priority_bucket="send_now",
        message_subject="Old subject",
        message_preview="Old body",
    )
    seed_task(
        conn,
        "task-sms-skip",
        "lead-1",
        title="Missed-call SMS",
        task_type="sms",
        channel="sms",
        due_at=due_sms,
        approval_status="pending",
        priority_bucket="send_now",
        message_preview="Original text",
    )
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        approve = await ac.post(
            "/api/tasks/task-email-approve/approve",
            json={"due_at": due_email, "subject": "Queued subject", "message": "Queued body"},
            headers=headers,
        )
        approvals = await ac.get("/api/operator/approvals?days=30", headers=headers)
        skip = await ac.post("/api/tasks/task-sms-skip/skip", json={"note": "No longer needed"}, headers=headers)
    assert approve.status_code == 200
    approved_body = approve.json()["task"]
    assert approved_body["approval_status"] == "approved"
    assert approved_body["message_subject"] == "Queued subject"
    assert approved_body["message_preview"] == "Queued body"
    assert approvals.status_code == 200
    approval_ids = {task["id"]: task["approval_status"] for task in approvals.json()["tasks"]}
    assert approval_ids["task-email-approve"] == "approved"
    assert skip.status_code == 200

    conn = sqlite3.connect(core.config.DB_PATH)
    skipped = conn.execute("SELECT status, superseded_by, notes FROM tasks WHERE id = 'task-sms-skip'").fetchone()
    conn.close()
    assert skipped == ("superseded", "operator_skip", "No longer needed")


@pytest.mark.asyncio
async def test_get_next_leads_to_call_returns_state_next_action_and_hints(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn)
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/leads/get_next_leads_to_call?limit=5", headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    lead = body["leads"][0]
    assert lead["lead_state"] == "ready_to_call"
    assert lead["next_action"]["channel"] == "call"
    assert lead["next_action"]["type"] == "call"
    assert lead["what_to_say"]
    assert lead["script_hints"]["if_no_answer"]
    assert lead["script_hints"]["if_objection"]


@pytest.mark.asyncio
async def test_apply_outcome_updates_follow_up_state_and_next_action(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn)
    seed_task(
        conn,
        "task-call-outcome",
        "lead-1",
        title="Initial value review call",
        task_type="call",
        channel="call",
        due_at="2026-03-16T09:15:00+11:00",
        priority_bucket="call_now",
        cadence_step=1,
    )
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/api/leads/lead-1/outcome",
            json={"outcome": "no_answer", "task_id": "task-call-outcome"},
            headers=headers,
        )

    assert response.status_code == 200
    lead = response.json()["lead"]
    assert lead["last_outcome"] == "no_answer"
    assert lead["lead_state"] == "follow_up_pending"
    assert lead["next_action"]["channel"] == "sms"
    assert lead["next_action"]["title"] == "Missed-call SMS"


@pytest.mark.asyncio
async def test_log_call_updates_last_outcome_and_enrichment_state(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    seed_lead(conn)
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/api/leads/lead-1/log-call",
            json={"outcome": "wrong_number", "note": "Bad number"},
            headers=headers,
        )

    assert response.status_code == 200
    lead = response.json()["lead"]
    assert lead["last_outcome"] == "wrong_number"
    assert lead["lead_state"] == "needs_enrichment"
    assert lead["next_action"]["type"] == "enrichment"
