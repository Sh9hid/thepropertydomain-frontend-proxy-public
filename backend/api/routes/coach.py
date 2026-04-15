"""AI Coach API routes."""
import json
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session

logger = logging.getLogger(__name__)
router = APIRouter(tags=["coach"])


@router.get("/api/calls/{call_id}/coach")
async def get_coaching_session(call_id: int, db: AsyncSession = Depends(get_session)):
    """Get coaching session for a call. Returns cached if available."""
    row = (await db.execute(
        text("SELECT status, coach_data_json FROM coaching_sessions WHERE call_id = :cid ORDER BY id DESC LIMIT 1"),
        {"cid": call_id},
    )).fetchone()

    if row and row[0] == "ready" and row[1]:
        data = json.loads(row[1])
        data["session_status"] = "ready"
        data["cached"] = True
        return data
    elif row and row[0] == "processing":
        return {"session_status": "processing", "message": "Coach analysis in progress..."}

    return {"session_status": "not_started", "call_id": call_id}


@router.post("/api/calls/{call_id}/coach/run")
async def run_coaching(
    call_id: int,
    background_tasks: BackgroundTasks,
    force: bool = False,
    db: AsyncSession = Depends(get_session),
):
    """Trigger coaching analysis for a call."""
    from services.ai_coach_service import process_call_coaching

    call = (await db.execute(
        text("SELECT id, transcript FROM call_log WHERE id = :cid"),
        {"cid": call_id},
    )).fetchone()

    if not call:
        raise HTTPException(404, f"Call {call_id} not found")

    if not call[1]:
        raise HTTPException(400, "No transcript available for this call — cannot generate coaching")

    existing = (await db.execute(
        text("SELECT status FROM coaching_sessions WHERE call_id = :cid ORDER BY id DESC LIMIT 1"),
        {"cid": call_id},
    )).fetchone()

    if existing and existing[0] == "processing":
        return {"status": "already_processing"}

    if existing and existing[0] == "ready" and not force:
        return {"status": "already_ready", "message": "Use force=true to regenerate"}

    try:
        result = await process_call_coaching(db, call_id, force=force)
        return {"status": "completed", "data": result}
    except Exception as e:
        logger.error(f"Coach run failed: {e}")
        raise HTTPException(500, f"Coaching analysis failed: {str(e)}")


@router.post("/api/calls/coach/backfill")
async def backfill_coaching(limit: int = 10, db: AsyncSession = Depends(get_session)):
    """Backfill coaching for calls that have transcripts but no coaching sessions."""
    from services.ai_coach_service import process_call_coaching

    rows = (await db.execute(
        text(
            "SELECT cl.id FROM call_log cl "
            "WHERE cl.transcript IS NOT NULL AND cl.transcript != '' "
            "AND cl.id NOT IN (SELECT call_id FROM coaching_sessions WHERE status = 'ready') "
            "ORDER BY cl.id DESC LIMIT :lim"
        ),
        {"lim": limit},
    )).fetchall()

    results = []
    for row in rows:
        try:
            await process_call_coaching(db, row[0])
            results.append({"call_id": row[0], "status": "completed"})
        except Exception as e:
            results.append({"call_id": row[0], "status": "failed", "error": str(e)})

    return {"processed": len(results), "results": results}


@router.get("/api/calls/{call_id}/lessons")
async def get_lessons(call_id: int, db: AsyncSession = Depends(get_session)):
    """Get lesson events for a call's coaching session."""
    rows = (await db.execute(
        text(
            "SELECT ce.id, ce.session_id, ce.call_id, ce.timestamp_start_ms, ce.timestamp_end_ms, "
            "ce.subtitle_excerpt, ce.issue_type, ce.severity, ce.what_you_said, "
            "ce.stronger_option_1, ce.stronger_option_2, ce.stronger_option_3, ce.why_it_matters, "
            "ce.accent_note, ce.delivery_note, ce.confidence_note, ce.suggested_drill, "
            "ce.recurring, ce.lesson_order, "
            "lp.status, lp.preferred_option, lp.repeat_count "
            "FROM coaching_events ce "
            "LEFT JOIN lesson_progress lp ON lp.event_id = ce.id AND lp.user_id = 'shahid' "
            "WHERE ce.call_id = :cid "
            "ORDER BY ce.lesson_order"
        ),
        {"cid": call_id},
    )).fetchall()

    lessons = []
    for r in rows:
        lessons.append({
            "id": r[0],
            "session_id": r[1],
            "call_id": r[2],
            "timestamp_start_ms": r[3],
            "timestamp_end_ms": r[4],
            "subtitle_excerpt": r[5],
            "issue_type": r[6],
            "severity": r[7],
            "what_you_said": r[8],
            "stronger_option_1": r[9],
            "stronger_option_2": r[10],
            "stronger_option_3": r[11],
            "why_it_matters": r[12],
            "accent_note": r[13],
            "delivery_note": r[14],
            "confidence_note": r[15],
            "suggested_drill": r[16],
            "recurring": r[17],
            "lesson_order": r[18],
            "lesson_status": r[19] if r[19] is not None else "pending",
            "preferred_option": r[20],
            "repeat_count": r[21] if r[21] is not None else 0,
        })

    return {"call_id": call_id, "lessons": lessons, "total": len(lessons)}


@router.post("/api/lessons/{lesson_id}/complete")
async def complete_lesson(
    lesson_id: int,
    preferred_option: int = None,
    db: AsyncSession = Depends(get_session),
):
    """Mark a lesson as completed."""
    now = datetime.now(timezone.utc).isoformat()

    await db.execute(
        text(
            "INSERT INTO lesson_progress (event_id, session_id, user_id, status, preferred_option, completed_at) "
            "SELECT :eid, ce.session_id, 'shahid', 'completed', :po, :now "
            "FROM coaching_events ce WHERE ce.id = :eid "
            "ON CONFLICT (event_id, user_id) DO UPDATE SET status = 'completed', preferred_option = :po, completed_at = :now"
        ),
        {"eid": lesson_id, "po": preferred_option, "now": now},
    )

    await db.execute(
        text(
            "UPDATE coaching_sessions SET completed_lessons = ("
            "SELECT COUNT(*) FROM lesson_progress lp "
            "JOIN coaching_events ce ON ce.id = lp.event_id "
            "WHERE ce.session_id = coaching_sessions.id AND lp.status = 'completed'"
            "), updated_at = :now "
            "WHERE id = (SELECT session_id FROM coaching_events WHERE id = :eid)"
        ),
        {"eid": lesson_id, "now": now},
    )

    await db.commit()
    return {"status": "completed", "lesson_id": lesson_id}


@router.post("/api/lessons/{lesson_id}/skip")
async def skip_lesson(lesson_id: int, db: AsyncSession = Depends(get_session)):
    """Mark a lesson as skipped."""
    now = datetime.now(timezone.utc).isoformat()

    await db.execute(
        text(
            "INSERT INTO lesson_progress (event_id, session_id, user_id, status, completed_at) "
            "SELECT :eid, ce.session_id, 'shahid', 'skipped', :now "
            "FROM coaching_events ce WHERE ce.id = :eid "
            "ON CONFLICT (event_id, user_id) DO UPDATE SET status = 'skipped', completed_at = :now"
        ),
        {"eid": lesson_id, "now": now},
    )

    await db.execute(
        text(
            "UPDATE coaching_sessions SET skipped_lessons = ("
            "SELECT COUNT(*) FROM lesson_progress lp "
            "JOIN coaching_events ce ON ce.id = lp.event_id "
            "WHERE ce.session_id = coaching_sessions.id AND lp.status = 'skipped'"
            "), updated_at = :now "
            "WHERE id = (SELECT session_id FROM coaching_events WHERE id = :eid)"
        ),
        {"eid": lesson_id, "now": now},
    )

    await db.commit()
    return {"status": "skipped", "lesson_id": lesson_id}


@router.get("/api/speech/profile")
async def get_speech_profile(db: AsyncSession = Depends(get_session)):
    """Get speech habit profile with improvement tracking."""
    from services.ai_coach_service import get_speech_profile as _get_profile
    return await _get_profile(db)


@router.get("/api/speech/habits")
async def get_speech_habits(db: AsyncSession = Depends(get_session)):
    """Get all tracked speech habits."""
    rows = (await db.execute(
        text("SELECT * FROM speech_habits WHERE user_id = 'shahid' ORDER BY occurrence_count DESC")
    )).fetchall()

    return {"habits": [dict(r._mapping) for r in rows]}


@router.get("/api/speech/preferences")
async def get_coach_preferences(db: AsyncSession = Depends(get_session)):
    """Get coach preferences."""
    from services.ai_coach_service import get_preferences
    return await get_preferences(db)


@router.post("/api/speech/preferences")
async def update_coach_preferences(prefs: dict, db: AsyncSession = Depends(get_session)):
    """Update coach preferences."""
    from services.ai_coach_service import update_preferences
    return await update_preferences(db, "shahid", prefs)


@router.get("/api/calls/{call_id}/subtitles")
async def get_subtitles(call_id: int, db: AsyncSession = Depends(get_session)):
    """Get timestamped subtitle segments for a call."""
    segments = (await db.execute(
        text(
            "SELECT speaker_id, text, start_ms, end_ms, confidence "
            "FROM call_segments WHERE call_id = :cid ORDER BY turn_index"
        ),
        {"cid": call_id},
    )).fetchall()

    if segments:
        return {
            "call_id": call_id,
            "segments": [
                {"speaker": s[0], "text": s[1], "start_ms": s[2], "end_ms": s[3], "confidence": s[4]}
                for s in segments
            ],
        }

    # Fallback: split transcript text into pseudo-segments
    call = (await db.execute(
        text("SELECT transcript, duration_seconds FROM call_log WHERE id = :cid"),
        {"cid": call_id},
    )).fetchone()

    if call and call[0]:
        lines = [l.strip() for l in call[0].split("\n") if l.strip()]
        duration_ms = (call[1] or 60) * 1000
        segment_duration = duration_ms // max(len(lines), 1)
        return {
            "call_id": call_id,
            "segments": [
                {
                    "speaker": "unknown",
                    "text": line,
                    "start_ms": i * segment_duration,
                    "end_ms": (i + 1) * segment_duration,
                    "confidence": 0.5,
                }
                for i, line in enumerate(lines)
            ],
        }

    return {"call_id": call_id, "segments": []}


@router.get("/api/calls/{call_id}/timeline-events")
async def get_timeline_events(call_id: int, db: AsyncSession = Depends(get_session)):
    """Get coaching events as timeline markers for the audio player."""
    events = (await db.execute(
        text(
            "SELECT ce.timestamp_start_ms, ce.timestamp_end_ms, ce.issue_type, ce.severity, "
            "ce.subtitle_excerpt, ce.lesson_order "
            "FROM coaching_events ce "
            "JOIN coaching_sessions cs ON cs.id = ce.session_id "
            "WHERE ce.call_id = :cid "
            "ORDER BY ce.timestamp_start_ms"
        ),
        {"cid": call_id},
    )).fetchall()

    return {
        "call_id": call_id,
        "events": [
            {
                "start_ms": e[0],
                "end_ms": e[1],
                "type": e[2],
                "severity": e[3],
                "excerpt": e[4],
                "lesson_order": e[5],
            }
            for e in events
        ],
    }
