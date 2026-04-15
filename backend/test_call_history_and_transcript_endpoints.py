"""
Tests for the DB-backed call history and transcript retrieval logic used by:
  GET /api/leads/{lead_id}/call-history
  GET /api/calls/{call_id}/transcript
"""
import json
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from services.speech_pipeline_service import ensure_speech_schema


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _setup_db():
    """Return (engine, Session) with speech schema + call_log table created."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as session:
        await ensure_speech_schema(session)
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS call_log (
                    id TEXT PRIMARY KEY,
                    lead_id TEXT DEFAULT '',
                    lead_address TEXT DEFAULT '',
                    user_id TEXT DEFAULT 'Shahid',
                    outcome TEXT DEFAULT '',
                    connected INTEGER DEFAULT 0,
                    timestamp TEXT DEFAULT '',
                    call_duration_seconds INTEGER DEFAULT 0,
                    duration_seconds INTEGER DEFAULT 0,
                    note TEXT DEFAULT '',
                    operator TEXT DEFAULT 'Shahid',
                    logged_at TEXT DEFAULT '',
                    logged_date TEXT DEFAULT '',
                    next_action_due TEXT,
                    provider TEXT DEFAULT 'manual',
                    provider_call_id TEXT,
                    direction TEXT DEFAULT '',
                    from_number TEXT DEFAULT '',
                    to_number TEXT DEFAULT '',
                    raw_payload TEXT DEFAULT '{}'
                )
                """
            )
        )
        await session.commit()
    return engine, Session


async def _seed_call_log(session: AsyncSession, lead_id: str) -> str:
    row_id = uuid.uuid4().hex
    await session.execute(
        text(
            """
            INSERT INTO call_log (id, lead_id, lead_address, user_id, outcome, connected,
                timestamp, call_duration_seconds, duration_seconds, note, operator,
                logged_at, logged_date, provider, direction)
            VALUES (:id, :lead_id, '1 Test St', 'Shahid', 'qualified', 1,
                '2026-03-24T10:00:00+00:00', 90, 90, 'Interested in selling', 'Shahid',
                '2026-03-24T10:00:00+00:00', '2026-03-24', 'manual', 'outbound')
            """
        ),
        {"id": row_id, "lead_id": lead_id},
    )
    return row_id


async def _seed_call_analysis(session: AsyncSession, call_id: str, lead_id: str) -> None:
    now = _now()
    await session.execute(
        text(
            """
            INSERT INTO call_analysis (
                call_id, lead_id, provider, recording_id, ai_call_summary_id, status,
                summary, outcome, key_topics, objections, next_step, suggested_follow_up_task,
                sentiment_label, sentiment_confidence, sentiment_reason, overall_confidence,
                error_message, raw_payload, analyzed_at, created_at, updated_at
            ) VALUES (
                :call_id, :lead_id, 'zoom_phone', '', '', 'completed',
                'Seller expressed interest and asked about pricing.', 'qualified',
                '["pricing","booking"]', '["price objection"]', 'Send CMA', 'Call back Thursday',
                'positive', 0.82, 'Seller sounded keen', 0.9, '', '{}', :now, :now, :now
            )
            """
        ),
        {"call_id": call_id, "lead_id": lead_id, "now": now},
    )


async def _seed_transcript_and_segments(session: AsyncSession, call_id: str) -> str:
    now = _now()
    transcript_id = uuid.uuid4().hex
    speaker_id = uuid.uuid4().hex

    await session.execute(
        text(
            """
            INSERT INTO speakers (id, call_id, diarization_label, role, display_name, linked_rep_id, linked_contact_id, confidence, created_at)
            VALUES (:id, :call_id, 'speaker_0', 'agent', 'Shahid', '', '', 0.95, :now)
            """
        ),
        {"id": speaker_id, "call_id": call_id, "now": now},
    )
    await session.execute(
        text(
            """
            INSERT INTO transcripts (id, call_id, provider, version_type, language, full_text, confidence, status, created_at, updated_at)
            VALUES (:id, :call_id, 'deepgram', 'canonical', 'en-AU',
                'Would Thursday work for you? I need to think about the price first.',
                0.93, 'completed', :now, :now)
            """
        ),
        {"id": transcript_id, "call_id": call_id, "now": now},
    )
    for i, (text_val, start, end) in enumerate([
        ("Would Thursday work for you?", 0, 2200),
        ("I need to think about the price first.", 2600, 4500),
    ]):
        await session.execute(
            text(
                """
                INSERT INTO call_segments (id, call_id, speaker_id, turn_index, start_ms, end_ms,
                    text, overlap_flag, segment_type, confidence, created_at)
                VALUES (:id, :call_id, :spk, :idx, :start, :end, :txt, 0, 'turn', 0.92, :now)
                """
            ),
            {"id": uuid.uuid4().hex, "call_id": call_id, "spk": speaker_id,
             "idx": i, "start": start, "end": end, "txt": text_val, "now": now},
        )
    return transcript_id


# ---------------------------------------------------------------------------
# Call history tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_call_history_manual_log_shape():
    """call_log rows are returned for the lead with the correct fields."""
    engine, Session = await _setup_db()
    lead_id = "lead-hist-1"

    async with Session() as session:
        row_id = await _seed_call_log(session, lead_id)
        await session.commit()

        rows = (
            await session.execute(
                text("SELECT id, lead_id, outcome, connected, note, direction FROM call_log WHERE lead_id = :lid"),
                {"lid": lead_id},
            )
        ).mappings().all()

    await engine.dispose()

    assert len(rows) == 1
    row = dict(rows[0])
    assert row["outcome"] == "qualified"
    assert bool(row["connected"]) is True
    assert row["note"] == "Interested in selling"
    assert row["direction"] == "outbound"


@pytest.mark.asyncio
async def test_call_history_analysis_data_joinable():
    """call_analysis rows are fetchable by lead_id and have correct JSON fields."""
    engine, Session = await _setup_db()
    lead_id = "lead-hist-2"
    call_id = uuid.uuid4().hex

    async with Session() as session:
        await _seed_call_analysis(session, call_id, lead_id)
        await session.commit()

        rows = (
            await session.execute(
                text("SELECT call_id, summary, outcome, key_topics, objections, sentiment_label FROM call_analysis WHERE lead_id = :lid"),
                {"lid": lead_id},
            )
        ).mappings().all()

    await engine.dispose()

    assert len(rows) == 1
    row = dict(rows[0])
    assert row["outcome"] == "qualified"
    assert row["sentiment_label"] == "positive"
    assert "pricing" in json.loads(row["key_topics"])
    assert len(json.loads(row["objections"])) == 1


@pytest.mark.asyncio
async def test_call_history_empty_for_unknown_lead():
    """Query for nonexistent lead returns empty."""
    engine, Session = await _setup_db()

    async with Session() as session:
        rows = (
            await session.execute(
                text("SELECT id FROM call_log WHERE lead_id = :lid"),
                {"lid": "nonexistent-lead"},
            )
        ).mappings().all()

    await engine.dispose()
    assert rows == []


@pytest.mark.asyncio
async def test_call_history_transcript_flag_detected():
    """Completed transcripts are found by call_id; calls without transcript return nothing."""
    engine, Session = await _setup_db()
    call_id = uuid.uuid4().hex

    async with Session() as session:
        await _seed_transcript_and_segments(session, call_id)
        await session.commit()

        found = (
            await session.execute(
                text("SELECT call_id FROM transcripts WHERE call_id = :cid AND status = 'completed'"),
                {"cid": call_id},
            )
        ).mappings().all()

        missing = (
            await session.execute(
                text("SELECT call_id FROM transcripts WHERE call_id = :cid AND status = 'completed'"),
                {"cid": "no-such-call"},
            )
        ).mappings().all()

    await engine.dispose()

    assert {str(r["call_id"]) for r in found} == {call_id}
    assert missing == []


# ---------------------------------------------------------------------------
# Transcript tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_transcript_full_text_and_segment_count():
    """Transcript full_text and correct segment count persist after seeding."""
    engine, Session = await _setup_db()
    call_id = uuid.uuid4().hex

    async with Session() as session:
        await _seed_transcript_and_segments(session, call_id)
        await session.commit()

        transcript_row = (
            await session.execute(
                text("SELECT * FROM transcripts WHERE call_id = :cid AND status = 'completed'"),
                {"cid": call_id},
            )
        ).mappings().first()

        segment_rows = (
            await session.execute(
                text("SELECT * FROM call_segments WHERE call_id = :cid ORDER BY turn_index ASC"),
                {"cid": call_id},
            )
        ).mappings().all()

    await engine.dispose()

    assert transcript_row is not None
    assert transcript_row["provider"] == "deepgram"
    assert "Thursday" in transcript_row["full_text"]
    assert len(segment_rows) == 2
    assert segment_rows[0]["text"] == "Would Thursday work for you?"
    assert segment_rows[1]["text"] == "I need to think about the price first."


@pytest.mark.asyncio
async def test_transcript_missing_returns_none():
    """No transcript row returned for unknown call_id."""
    engine, Session = await _setup_db()

    async with Session() as session:
        row = (
            await session.execute(
                text("SELECT id FROM transcripts WHERE call_id = :cid AND status = 'completed'"),
                {"cid": "no-such-call"},
            )
        ).mappings().first()

    await engine.dispose()
    assert row is None


@pytest.mark.asyncio
async def test_transcript_segments_reference_valid_speaker():
    """Each segment's speaker_id points to a real speaker row with role='agent'."""
    engine, Session = await _setup_db()
    call_id = uuid.uuid4().hex

    async with Session() as session:
        await _seed_transcript_and_segments(session, call_id)
        await session.commit()

        speaker_rows = (
            await session.execute(
                text("SELECT id, role FROM speakers WHERE call_id = :cid"),
                {"cid": call_id},
            )
        ).mappings().all()
        speaker_map = {str(r["id"]): dict(r) for r in speaker_rows}

        segment_rows = (
            await session.execute(
                text("SELECT speaker_id FROM call_segments WHERE call_id = :cid"),
                {"cid": call_id},
            )
        ).mappings().all()

    await engine.dispose()

    assert len(speaker_rows) == 1
    for seg in segment_rows:
        spk = speaker_map.get(str(seg["speaker_id"]))
        assert spk is not None, f"speaker_id {seg['speaker_id']} not found"
        assert spk["role"] == "agent"


@pytest.mark.asyncio
async def test_transcript_segment_ordering():
    """Segments are stored with correct turn_index and chronological start_ms."""
    engine, Session = await _setup_db()
    call_id = uuid.uuid4().hex

    async with Session() as session:
        await _seed_transcript_and_segments(session, call_id)
        await session.commit()

        rows = (
            await session.execute(
                text("SELECT turn_index, start_ms, end_ms FROM call_segments WHERE call_id = :cid ORDER BY turn_index ASC"),
                {"cid": call_id},
            )
        ).mappings().all()

    await engine.dispose()

    assert rows[0]["turn_index"] == 0
    assert rows[0]["start_ms"] == 0
    assert rows[1]["turn_index"] == 1
    assert rows[1]["start_ms"] > rows[0]["end_ms"]


# ---------------------------------------------------------------------------
# DB-first recordings + call-summaries tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_db_first_recordings_returns_call_log_without_zoom():
    """Recording list is built from call_log rows with no Zoom API dependency."""
    engine, Session = await _setup_db()
    lead_id = "lead-rec-1"

    async with Session() as session:
        row_id = await _seed_call_log(session, lead_id)
        await session.commit()

        # Simulate the DB-first recordings query (call_log + call_analysis join)
        log_rows = (
            await session.execute(
                text(
                    """
                    SELECT id, outcome, timestamp, duration_seconds, direction,
                           provider, provider_call_id, operator
                    FROM call_log WHERE lead_id = :lead_id
                    ORDER BY timestamp DESC LIMIT 50
                    """
                ),
                {"lead_id": lead_id},
            )
        ).mappings().all()

        analysis_rows = (
            await session.execute(
                text("SELECT * FROM call_analysis WHERE lead_id = :lead_id"),
                {"lead_id": lead_id},
            )
        ).mappings().all()
        analysis_by_call_id = {str(r["call_id"]): dict(r) for r in analysis_rows}

    await engine.dispose()

    assert len(log_rows) == 1
    row = dict(log_rows[0])
    assert row["outcome"] == "qualified"
    assert row["direction"] == "outbound"
    call_id = str(row.get("provider_call_id") or row["id"])
    analysis = analysis_by_call_id.get(call_id)
    assert analysis is None  # No analysis seeded for this test


@pytest.mark.asyncio
async def test_call_lead_summaries_batch_query():
    """Batch summary query returns total_calls per lead and transcript flags."""
    engine, Session = await _setup_db()
    lead_a = "lead-summ-A"
    lead_b = "lead-summ-B"
    call_id_with_transcript = uuid.uuid4().hex

    async with Session() as session:
        # lead_a: 2 calls
        await _seed_call_log(session, lead_a)
        await _seed_call_log(session, lead_a)
        # lead_b: 1 call + a speech_call with transcript
        await _seed_call_log(session, lead_b)
        # Insert a SpeechCall and transcript for lead_b
        now = _now()
        await session.execute(
            text(
                """
                INSERT INTO calls (
                    id, external_call_id, source, lead_id, rep_id, call_type,
                    direction, outcome, started_at, ended_at, duration_seconds,
                    recording_id, audio_uri, audio_storage_status,
                    analysis_status, transcript_status, diarization_status,
                    metadata_json, created_at, updated_at
                ) VALUES (
                    :id, '', 'upload', :lead_id, 'Shahid', 'recorded_call',
                    'outbound', 'qualified', :now, :now, 90,
                    '', '', 'pending',
                    'pending', 'completed', 'pending',
                    '{}', :now, :now
                )
                """
            ),
            {"id": call_id_with_transcript, "lead_id": lead_b, "now": now},
        )
        await _seed_transcript_and_segments(session, call_id_with_transcript)
        await session.commit()

        # Simulate the batch summary query
        log_counts = (
            await session.execute(
                text("SELECT lead_id, COUNT(*) as total_calls FROM call_log GROUP BY lead_id")
            )
        ).mappings().all()

        transcript_leads = (
            await session.execute(
                text(
                    """
                    SELECT DISTINCT c.lead_id
                    FROM calls c
                    JOIN transcripts t ON t.call_id = c.id AND t.status = 'completed'
                    """
                )
            )
        ).mappings().all()

    await engine.dispose()

    counts_by_lead = {str(r["lead_id"]): int(r["total_calls"]) for r in log_counts}
    transcript_set = {str(r["lead_id"]) for r in transcript_leads}

    assert counts_by_lead.get(lead_a) == 2
    assert counts_by_lead.get(lead_b) == 1
    assert lead_b in transcript_set
    assert lead_a not in transcript_set
