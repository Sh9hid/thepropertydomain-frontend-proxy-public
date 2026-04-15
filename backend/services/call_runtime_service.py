from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import _async_session_factory
from core.utils import now_iso, now_sydney
from services.call_intelligence_service import extract_signals
from services.transcript_service import ingest_transcript_with_session
from services.transcription_provider import get_transcription_provider
from services.zoom_call_sync_service import ensure_call_log_schema

_logger = logging.getLogger(__name__)
_RECENT_EVENTS: deque[dict[str, Any]] = deque(maxlen=100)


def _record_event(event_type: str, **payload: Any) -> None:
    entry = {
        "event_type": event_type,
        "timestamp": now_iso(),
        **payload,
    }
    _RECENT_EVENTS.appendleft(entry)
    _logger.info("call_runtime %s %s", event_type, payload)


def record_runtime_event(event_type: str, **payload: Any) -> None:
    _record_event(event_type, **payload)


def get_recent_events(limit: int = 50) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 50), 100))
    return list(_RECENT_EVENTS)[:safe_limit]


async def ensure_call_runtime_schema(session: AsyncSession) -> None:
    await ensure_call_log_schema(session)
    bind = session.get_bind()
    dialect = getattr(bind, "dialect", None)
    dialect_name = getattr(dialect, "name", "")
    if dialect_name == "sqlite":
        rows = await session.execute(text("PRAGMA table_info(leads)"))
        lead_columns = {str(row[1]) for row in rows.fetchall()}
        if "next_action_due" not in lead_columns:
            await session.execute(text("ALTER TABLE leads ADD COLUMN next_action_due TEXT"))
    else:
        await session.execute(text("ALTER TABLE leads ADD COLUMN IF NOT EXISTS next_action_due TEXT"))
    await session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS call_insights (
                call_id TEXT PRIMARY KEY,
                summary TEXT DEFAULT '',
                intent TEXT DEFAULT '',
                objections TEXT DEFAULT '[]',
                next_step_detected INTEGER DEFAULT 0,
                appointment_booked INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
    )
    await session.execute(
        text("CREATE INDEX IF NOT EXISTS idx_call_insights_next_step_detected ON call_insights(next_step_detected)")
    )


async def _resolve_local_audio_path(session: AsyncSession, call_row: dict[str, Any]) -> Path | None:
    direct_path = Path(str(call_row.get("recording_url") or "").strip())
    if direct_path.exists():
        return direct_path

    call_id = str(call_row.get("id") or "")
    provider_call_id = str(call_row.get("provider_call_id") or "")
    speech_row = (
        await session.execute(
            text(
                """
                SELECT audio_uri
                FROM calls
                WHERE id = :call_id OR external_call_id = :provider_call_id
                ORDER BY CASE WHEN id = :call_id THEN 0 ELSE 1 END
                LIMIT 1
                """
            ),
            {"call_id": call_id, "provider_call_id": provider_call_id},
        )
    ).mappings().first()
    if not speech_row:
        return None

    audio_uri = str(speech_row.get("audio_uri") or "").strip()
    if not audio_uri:
        return None

    audio_path = Path(audio_uri)
    if audio_path.exists():
        return audio_path

    project_relative = Path.cwd() / audio_uri
    if project_relative.exists():
        return project_relative
    return None


def _derive_next_action_due(call_row: dict[str, Any], transcript: str, signals: dict[str, Any]) -> str | None:
    existing_due = str(call_row.get("next_action_due") or "").strip()
    if existing_due:
        return existing_due

    lowered = transcript.lower()
    now = now_sydney()
    if signals.get("appointment_booked"):
        return None
    if "next week" in lowered:
        return (now + timedelta(days=7)).isoformat()
    if signals.get("next_step_detected") or signals.get("objections"):
        return (now + timedelta(days=1)).isoformat()
    if str(call_row.get("outcome") or "").strip().lower() in {"no_answer", "voicemail"}:
        return (now + timedelta(hours=4)).isoformat()
    return None


async def _upsert_call_insights(session: AsyncSession, call_id: str, signals: dict[str, Any]) -> None:
    now = now_iso()
    await session.execute(
        text(
            """
            INSERT INTO call_insights (
                call_id, summary, intent, objections, next_step_detected, appointment_booked, created_at, updated_at
            ) VALUES (
                :call_id, :summary, :intent, :objections, :next_step_detected, :appointment_booked, :created_at, :updated_at
            )
            ON CONFLICT(call_id) DO UPDATE SET
                summary = excluded.summary,
                intent = excluded.intent,
                objections = excluded.objections,
                next_step_detected = excluded.next_step_detected,
                appointment_booked = excluded.appointment_booked,
                updated_at = excluded.updated_at
            """
        ),
        {
            "call_id": call_id,
            "summary": str(signals.get("summary") or ""),
            "intent": str(signals.get("intent") or ""),
            "objections": json.dumps(signals.get("objections") or []),
            "next_step_detected": 1 if signals.get("next_step_detected") else 0,
            "appointment_booked": 1 if signals.get("appointment_booked") else 0,
            "created_at": now,
            "updated_at": now,
        },
    )


async def _apply_lead_updates(session: AsyncSession, lead_id: str, call_row: dict[str, Any], signals: dict[str, Any]) -> None:
    if not lead_id:
        return

    next_action_due = _derive_next_action_due(call_row, str(signals.get("transcript") or ""), signals)
    next_status = None
    if signals.get("appointment_booked"):
        next_status = "appt_booked"

    await session.execute(
        text(
            """
            UPDATE leads
            SET status = COALESCE(:status, status),
                follow_up_due_at = COALESCE(:next_action_due, follow_up_due_at),
                next_action_at = COALESCE(:next_action_due, next_action_at),
                next_action_due = COALESCE(:next_action_due, next_action_due),
                next_action_type = CASE WHEN :next_action_due IS NOT NULL THEN 'follow_up' ELSE next_action_type END,
                next_action_channel = CASE WHEN :next_action_due IS NOT NULL THEN 'phone' ELSE next_action_channel END,
                next_action_title = CASE WHEN :next_action_due IS NOT NULL THEN 'Scheduled follow-up' ELSE next_action_title END,
                next_action_reason = CASE
                    WHEN :status = 'appt_booked' THEN 'Appointment booked on the latest call.'
                    WHEN :next_action_due IS NOT NULL THEN 'Post-call follow-up scheduled from transcript and objection signals.'
                    ELSE next_action_reason
                END,
                updated_at = :updated_at
            WHERE id = :lead_id
            """
        ),
        {
            "lead_id": lead_id,
            "status": next_status,
            "next_action_due": next_action_due,
            "updated_at": now_iso(),
        },
    )


async def process_call_log_entry(call_id: str) -> dict[str, Any]:
    async with _async_session_factory() as session:
        await ensure_call_runtime_schema(session)
        call_row = (
            await session.execute(
                text(
                    """
                    SELECT *
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
            _record_event("call_missing", call_id=call_id)
            return {"ok": False, "reason": "call_not_found", "call_id": call_id}

        row = dict(call_row)
        _record_event("call_created", call_id=str(row.get("id") or call_id), lead_id=str(row.get("lead_id") or ""))

        transcript = str(row.get("transcript") or "").strip()
        if not transcript and str(row.get("recording_url") or "").strip():
            audio_path = await _resolve_local_audio_path(session, row)
            if audio_path:
                provider = get_transcription_provider()
                provider_payload = await provider.transcribe(
                    call_id=str(row.get("id") or call_id),
                    audio_path=audio_path,
                    context={
                        "direction": str(row.get("direction") or "outbound"),
                        "transcript_hint": str(row.get("summary") or ""),
                    },
                )
                transcript = str(provider_payload.get("full_text") or "").strip()
                if transcript:
                    await ingest_transcript_with_session(session, str(row.get("id") or call_id), transcript)
                    _record_event("transcript_processed", call_id=str(row.get("id") or call_id), provider=str(provider_payload.get("provider") or "unknown"))
                    refreshed = (
                        await session.execute(
                            text("SELECT transcript, summary, outcome, lead_id, next_action_due FROM call_log WHERE id = :call_id"),
                            {"call_id": str(row.get("id") or call_id)},
                        )
                    ).mappings().first()
                    if refreshed:
                        row.update(dict(refreshed))

        transcript = str(row.get("transcript") or transcript or "").strip()
        signals_raw = extract_signals(transcript or str(row.get("summary") or ""))
        objections = list(signals_raw.get("objection_tags") or [])
        appointment_booked = bool(signals_raw.get("booking_attempted")) or str(row.get("outcome") or "") in {"booked_appraisal", "booked_mortgage"}
        signals = {
            "summary": str(row.get("summary") or signals_raw.get("summary") or ""),
            "intent": str(signals_raw.get("summary") or ""),
            "objections": objections,
            "next_step_detected": bool(row.get("next_step_detected")) or bool(signals_raw.get("next_step_detected")),
            "appointment_booked": appointment_booked,
            "transcript": transcript,
        }

        await _upsert_call_insights(session, str(row.get("id") or call_id), signals)
        await _apply_lead_updates(session, str(row.get("lead_id") or ""), row, signals)
        await session.commit()
        _record_event(
            "extraction_completed",
            call_id=str(row.get("id") or call_id),
            objections=len(objections),
            appointment_booked=appointment_booked,
        )
        return {
            "ok": True,
            "call_id": str(row.get("id") or call_id),
            "lead_id": str(row.get("lead_id") or ""),
            "transcript": transcript,
            "summary": signals["summary"],
        }


def schedule_call_postprocess(call_id: str) -> None:
    async def _runner() -> None:
        try:
            await process_call_log_entry(call_id)
        except Exception as exc:
            _record_event("postprocess_failed", call_id=call_id, error=str(exc))
            _logger.exception("Post-call runtime failed for %s", call_id)

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_runner())
        _record_event("postprocess_scheduled", call_id=call_id)
    except RuntimeError:
        _record_event("postprocess_unscheduled", call_id=call_id, error="no_running_loop")


async def choose_next_lead(session: AsyncSession) -> dict[str, Any] | None:
    await ensure_call_runtime_schema(session)
    cutoff = (now_sydney() - timedelta(hours=1)).isoformat()
    row = (
        await session.execute(
            text(
                """
                SELECT
                    l.id,
                    l.address,
                    l.suburb,
                    l.owner_name,
                    l.status,
                    l.call_today_score,
                    l.last_contacted_at,
                    cl.summary AS last_call_summary,
                    cl.outcome AS last_call_outcome,
                    cl.logged_at AS last_call_logged_at
                FROM leads l
                LEFT JOIN (
                    SELECT c1.*
                    FROM call_log c1
                    INNER JOIN (
                        SELECT lead_id, MAX(COALESCE(timestamp, logged_at)) AS latest_ts
                        FROM call_log
                        WHERE COALESCE(lead_id, '') <> ''
                        GROUP BY lead_id
                    ) latest
                      ON latest.lead_id = c1.lead_id
                     AND latest.latest_ts = COALESCE(c1.timestamp, c1.logged_at)
                ) cl ON cl.lead_id = l.id
                WHERE COALESCE(l.status, 'captured') NOT IN ('converted', 'dropped', 'appt_booked', 'mortgage_appt_booked')
                  AND (l.last_contacted_at IS NULL OR l.last_contacted_at <= :cutoff)
                ORDER BY
                    CASE
                        WHEN l.follow_up_due_at IS NOT NULL AND l.follow_up_due_at <= :now_ts THEN 0
                        WHEN l.last_contacted_at IS NULL THEN 1
                        ELSE 2
                    END,
                    COALESCE(l.last_contacted_at, '') ASC,
                    COALESCE(l.call_today_score, 0) DESC,
                    l.id ASC
                LIMIT 1
                """
            ),
            {"cutoff": cutoff, "now_ts": now_iso()},
        )
    ).mappings().first()
    if not row:
        return None
    payload = dict(row)
    return {
        "lead": {
            "id": str(payload.get("id") or ""),
            "address": str(payload.get("address") or ""),
            "suburb": str(payload.get("suburb") or ""),
            "owner_name": str(payload.get("owner_name") or ""),
            "status": str(payload.get("status") or ""),
            "call_today_score": int(payload.get("call_today_score") or 0),
            "last_contacted_at": payload.get("last_contacted_at"),
        },
        "context": {
            "last_call_summary": str(payload.get("last_call_summary") or ""),
            "last_call_outcome": str(payload.get("last_call_outcome") or ""),
            "last_call_logged_at": payload.get("last_call_logged_at"),
        },
    }
