import asyncio
import sqlite3
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from api.routes import analytics
import core.config
import core.database as db_module
from services.speech_pipeline_service import ensure_speech_schema


app = FastAPI()
app.include_router(analytics.router)

TEST_ROOT = Path("D:/woonona-lead-machine/backend/test_dbs")


@pytest.fixture
def isolated_db(monkeypatch):
    TEST_ROOT.mkdir(parents=True, exist_ok=True)
    test_db = TEST_ROOT / f"metrics_timeline_understanding_{uuid.uuid4().hex}.db"
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


def seed_call(
    conn: sqlite3.Connection,
    *,
    row_id: str,
    provider_call_id: str,
    lead_id: str,
    outcome: str,
    connected: int,
    duration_seconds: int,
    logged_at: str,
):
    conn.execute(
        """
        INSERT INTO call_log (
            id, lead_id, lead_address, outcome, connected, duration_seconds, note, operator, logged_at, logged_date,
            provider, provider_call_id, direction, from_number, to_number, raw_payload, user_id, timestamp, call_duration_seconds
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row_id,
            lead_id,
            f"{lead_id} Example Street, Woonona NSW 2517",
            outcome,
            connected,
            duration_seconds,
            "",
            "Shahid",
            logged_at,
            "2026-03-24",
            "zoom_phone",
            provider_call_id,
            "outbound",
            "+61200000000",
            "+61400000000",
            "{}",
            "Shahid",
            logged_at,
            duration_seconds,
        ),
    )


@pytest.mark.asyncio
async def test_metrics_timeline_includes_minimal_recorded_call_understanding(isolated_db):
    async with db_module._async_session_factory() as session:
        await ensure_speech_schema(session)
        await session.commit()

    conn = sqlite3.connect(core.config.DB_PATH)
    seed_call(
        conn,
        row_id="row-1",
        provider_call_id="provider-call-1",
        lead_id="lead-1",
        outcome="connected",
        connected=1,
        duration_seconds=180,
        logged_at="2026-03-24T09:00:00+11:00",
    )
    seed_call(
        conn,
        row_id="row-2",
        provider_call_id="provider-call-2",
        lead_id="lead-2",
        outcome="no_answer",
        connected=0,
        duration_seconds=0,
        logged_at="2026-03-24T09:25:00+11:00",
    )
    conn.execute(
        """
        INSERT INTO call_analysis (
            call_id, lead_id, provider, status, summary, outcome, key_topics, objections, next_step,
            suggested_follow_up_task, sentiment_label, sentiment_confidence, sentiment_reason, overall_confidence,
            error_message, raw_payload, analyzed_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "provider-call-1",
            "lead-1",
            "speech_pipeline_v1",
            "completed",
            "Asked discovery questions, handled a pricing objection, and moved toward a next step.",
            "connected",
            '["pricing"]',
            '["Need to think about the price"]',
            "Confirm the next meeting time.",
            "Send the confirmation.",
            "neutral",
            0.8,
            "Measured",
            0.84,
            "",
            '{"sales_analysis": {"booking_attempted": true, "next_step_defined": true}}',
            "2026-03-24T09:05:00+11:00",
            "2026-03-24T09:05:00+11:00",
            "2026-03-24T09:05:00+11:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO transcripts (
            id, call_id, provider, version_type, language, full_text, confidence, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "transcript-1",
            "provider-call-1",
            "stub",
            "canonical",
            "en-AU",
            "Agent asked about timing and pricing, then proposed locking in a time.",
            0.91,
            "completed",
            "2026-03-24T09:05:00+11:00",
            "2026-03-24T09:05:00+11:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO calls (
            id, external_call_id, source, lead_id, rep_id, call_type, direction, outcome,
            started_at, ended_at, duration_seconds, recording_id, audio_uri, audio_storage_status,
            analysis_status, transcript_status, diarization_status, metadata_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "provider-call-1",
            "provider-call-1",
            "zoom",
            "lead-1",
            "zoom",
            "recorded_call",
            "outbound",
            "connected",
            "2026-03-24T09:00:00+11:00",
            "2026-03-24T09:03:00+11:00",
            180,
            "recording-1",
            "recordings/provider-call-1.mp3",
            "stored",
            "completed",
            "completed",
            "completed",
            "{}",
            "2026-03-24T09:05:00+11:00",
            "2026-03-24T09:05:00+11:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO score_snapshots (
            id, entity_type, entity_id, call_id, rep_id, scenario_type, scoring_version, composite_score, confidence, computed_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "snapshot-1",
            "call",
            "provider-call-1",
            "provider-call-1",
            "zoom",
            "recorded_call",
            "v0",
            79.0,
            0.8,
            "2026-03-24T09:05:00+11:00",
            "2026-03-24T09:05:00+11:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO score_components (
            id, snapshot_id, call_id, score_name, score_value, raw_value, normalized_value, weight, stable_flag, evidence_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "component-1",
            "snapshot-1",
            "provider-call-1",
            "sales_control_score",
            81.0,
            0.81,
            0.81,
            0.25,
            1,
            "{}",
            "2026-03-24T09:05:00+11:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO objections (
            id, call_id, segment_id, objection_type, normalized_text, detected_at_ms, response_quality_score, resolved_flag, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "objection-1",
            "provider-call-1",
            "",
            "price",
            "Need to think about the price",
            34000,
            0.7,
            1,
            "2026-03-24T09:05:00+11:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO filler_events (
            id, call_id, segment_id, token, family, count, start_ms, duration_ms, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "filler-1",
            "provider-call-1",
            "",
            "um",
            "hesitation",
            3,
            12000,
            0,
            "2026-03-24T09:05:00+11:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO fluency_events (
            id, call_id, segment_id, event_type, start_ms, duration_ms, severity, evidence, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "fluency-1",
            "provider-call-1",
            "",
            "hesitation",
            12000,
            800,
            0.5,
            "Long pause before response",
            "2026-03-24T09:05:00+11:00",
        ),
    )
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/metrics/timeline?date=2026-03-24", headers=headers)

    assert response.status_code == 200
    data = response.json()
    assert len(data["activities"]) == 2

    first_activity = data["activities"][0]
    assert first_activity["call_understanding"]["transcript"].startswith("Agent asked about timing")
    assert first_activity["call_understanding"]["structured_summary"]["summary"].startswith("Asked discovery questions")
    assert first_activity["call_understanding"]["objections"] == ["Need to think about the price"]
    assert first_activity["call_understanding"]["booking_attempted"] is True
    assert first_activity["call_understanding"]["next_step_detected"] is True
    assert first_activity["call_understanding"]["filler_count"] == 3
    assert first_activity["call_understanding"]["pause_signals"][0]["event_type"] == "hesitation"
    assert first_activity["has_analysis"] is True
    assert first_activity["has_transcript"] is True
    assert first_activity["analysis_status"] == "completed"
    assert first_activity["transcript_status"] == "completed"
    assert first_activity["file_url"] == "/api/recordings/provider-call-1/stream"
    assert first_activity["score_summary"]["composite_score"] == 79.0
    assert first_activity["score_summary"]["sales_control_score"] == 81.0

    assert data["sessions"][0]["calls"][0]["call_id"] == "provider-call-1"
    assert data["rundown"]["working"]
    assert data["session_rankings"]["best_by_talk_time"]["session_id"] == data["sessions"][0]["session_id"]

    second_activity = data["activities"][1]
    assert second_activity["call_understanding"]["transcript"] == ""
    assert second_activity["call_understanding"]["objections"] == []
    assert second_activity["call_understanding"]["booking_attempted"] is False
