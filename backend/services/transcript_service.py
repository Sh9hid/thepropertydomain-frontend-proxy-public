from __future__ import annotations

import json
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import _async_session_factory
from core.utils import now_iso
from services.call_intelligence_service import clean_transcript, extract_signals
from services.speech_pipeline_service import ensure_speech_schema
from services.zoom_call_sync_service import ensure_call_log_schema


async def ingest_transcript(call_id: str, raw_text: str) -> dict:
    async with _async_session_factory() as session:
        result = await ingest_transcript_with_session(session, call_id, raw_text)
        await session.commit()
        return result


async def ingest_transcript_with_session(session: AsyncSession, call_id: str, raw_text: str) -> dict:
    raw_text = str(raw_text or "").strip()
    if not raw_text:
        raise ValueError("Transcript is required")

    await ensure_call_log_schema(session)
    await ensure_speech_schema(session)

    call_row = (
        await session.execute(
            text(
                """
                SELECT id, lead_id, provider_call_id
                FROM call_log
                WHERE id = :call_id OR provider_call_id = :call_id
                ORDER BY CASE WHEN id = :call_id THEN 0 ELSE 1 END
                LIMIT 1
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().first()
    if not call_row:
        raise ValueError("Call log entry not found")

    now = now_iso()
    cleaned_text = clean_transcript(raw_text)
    signals = extract_signals(cleaned_text)
    transcript_call_id = str(call_row.get("provider_call_id") or call_row["id"])

    transcript_row = (
        await session.execute(
            text(
                """
                SELECT id
                FROM transcripts
                WHERE call_id = :call_id AND version_type = 'canonical'
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                """
            ),
            {"call_id": transcript_call_id},
        )
    ).mappings().first()

    if transcript_row:
        await session.execute(
            text(
                """
                UPDATE transcripts
                SET provider = :provider,
                    language = :language,
                    full_text = :full_text,
                    confidence = :confidence,
                    status = :status,
                    updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {
                "id": str(transcript_row["id"]),
                "provider": "manual_transcript",
                "language": "en-AU",
                "full_text": raw_text,
                "confidence": 1.0,
                "status": "completed",
                "updated_at": now,
            },
        )
    else:
        await session.execute(
            text(
                """
                INSERT INTO transcripts (
                    id, call_id, provider, version_type, language, full_text,
                    confidence, status, created_at, updated_at
                ) VALUES (
                    :id, :call_id, :provider, :version_type, :language, :full_text,
                    :confidence, :status, :created_at, :updated_at
                )
                """
            ),
            {
                "id": uuid.uuid4().hex,
                "call_id": transcript_call_id,
                "provider": "manual_transcript",
                "version_type": "canonical",
                "language": "en-AU",
                "full_text": raw_text,
                "confidence": 1.0,
                "status": "completed",
                "created_at": now,
                "updated_at": now,
            },
        )

    await session.execute(
        text(
            """
            UPDATE call_log
            SET transcript = :transcript,
                intent_signal = :intent_signal,
                booking_attempted = :booking_attempted,
                objection_tags = :objection_tags,
                next_step_detected = :next_step_detected,
                summary = :summary
            WHERE id = :id
            """
        ),
        {
            "id": str(call_row["id"]),
            "transcript": cleaned_text,
            "intent_signal": float(signals["intent_signal"]),
            "booking_attempted": 1 if signals["booking_attempted"] else 0,
            "objection_tags": json.dumps(signals["objection_tags"]),
            "next_step_detected": 1 if signals["next_step_detected"] else 0,
            "summary": str(signals["summary"]),
        },
    )

    return {
        "call_id": str(call_row["id"]),
        "provider_call_id": transcript_call_id,
        "transcript": cleaned_text,
        "raw_transcript": raw_text,
        "signals": {
            "intent_signal": float(signals["intent_signal"]),
            "booking_attempted": bool(signals["booking_attempted"]),
            "objection_tags": list(signals["objection_tags"]),
            "next_step_detected": bool(signals["next_step_detected"]),
            "next_step_text": str(signals["next_step_text"]),
            "summary": str(signals["summary"]),
        },
    }
