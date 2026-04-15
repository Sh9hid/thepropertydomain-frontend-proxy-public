import datetime
import json
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence

from fastapi import HTTPException
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import OWNIT1ST_OPERATOR_NAME, SYDNEY_TZ
from core.utils import _parse_iso_datetime, now_sydney
from services.zoom_call_sync_service import ensure_call_log_schema

SESSION_BOUNDARY_THRESHOLD_SECONDS = 15 * 60
CONVERSATION_MIN_DURATION_SECONDS = 10
DEFAULT_USER_ID = OWNIT1ST_OPERATOR_NAME or "Shahid"

OUTCOME_ALIASES = {
    "left_voicemail": "voicemail",
    "vm_sent": "voicemail",
    "bad_number": "wrong_number",
    "appointment_booked": "booked_appraisal",
}

CONNECTED_OUTCOMES = frozenset(
    {
        "connected",
        "connected_interested",
        "connected_follow_up",
        "connected_not_interested",
        "connected_do_not_call",
        "spoke",
        "booked_appraisal",
        "booked_mortgage",
        "not_interested",
        "soft_no",
        "hard_no",
        "call_back",
        "send_info",
        "question",
    }
)
APPOINTMENT_OUTCOMES = frozenset({"booked_appraisal", "booked_mortgage"})
APPRAISAL_OUTCOMES = frozenset({"booked_appraisal"})
STRICT_METRIC_DEFINITIONS = {
    "dial_count": {
        "source": "call_log",
        "rule": "Count of stored call_log rows in scope.",
    },
    "call_attempt_count": {
        "source": "call_log",
        "rule": "Same as dial_count.",
    },
    "leads_touched_count": {
        "source": "call_log",
        "rule": "Distinct non-empty lead_id values in scope.",
    },
    "connect_count": {
        "source": "call_log",
        "rule": "Rows where connected is true, or the stored outcome maps to a connected outcome.",
    },
    "unanswered_count": {
        "source": "call_log",
        "rule": "Calculated as dial_count - connect_count.",
    },
    "conversation_count": {
        "source": "call_log",
        "rule": f"Connected rows with call_duration_seconds >= {CONVERSATION_MIN_DURATION_SECONDS}.",
    },
    "appointments_booked_count": {
        "source": "call_log",
        "rule": f"Rows whose normalized outcome is in {sorted(APPOINTMENT_OUTCOMES)}.",
    },
    "idle_time_seconds": {
        "source": "call_log",
        "rule": "Sum of positive gaps between current activity start and previous activity end.",
    },
    "active_time_seconds": {
        "source": "call_log",
        "rule": "Equal to total_talk_time_seconds.",
    },
    "session_boundary_threshold_seconds": SESSION_BOUNDARY_THRESHOLD_SECONDS,
}


def normalize_outcome(value: Any) -> str:
    text_value = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    return OUTCOME_ALIASES.get(text_value, text_value)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _safe_div(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _safe_json_loads_local(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _empty_call_understanding() -> Dict[str, Any]:
    return {
        "summary": "",
        "transcript": "",
        "structured_summary": {
            "summary": "",
            "outcome": "",
            "next_step": "",
        },
        "objections": [],
        "booking_attempted": False,
        "next_step_detected": False,
        "filler_count": 0,
        "pause_signals": [],
    }


def _build_minimal_call_understanding(
    *,
    call_id: str,
    analysis_by_call: Dict[str, Dict[str, Any]],
    transcript_by_call: Dict[str, Dict[str, Any]],
    objections_by_call: Dict[str, List[Dict[str, Any]]],
    filler_count_by_call: Dict[str, int],
    pause_signals_by_call: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    payload = _empty_call_understanding()
    analysis_row = analysis_by_call.get(call_id, {})
    transcript_row = transcript_by_call.get(call_id, {})
    raw_payload = _safe_json_loads_local(analysis_row.get("raw_payload"), {})
    sales_analysis = raw_payload.get("sales_analysis") if isinstance(raw_payload, dict) else {}
    if not isinstance(sales_analysis, dict):
        sales_analysis = {}

    objections = [
        str(item.get("normalized_text") or "").strip()
        for item in objections_by_call.get(call_id, [])
        if str(item.get("normalized_text") or "").strip()
    ]
    summary = str(analysis_row.get("summary") or "").strip()
    next_step = str(analysis_row.get("next_step") or "").strip()
    outcome = str(analysis_row.get("outcome") or "").strip()
    transcript = str(
        transcript_row.get("full_text")
        or transcript_row.get("transcript_text")
        or ""
    ).strip()

    payload["summary"] = summary
    payload["transcript"] = transcript
    payload["structured_summary"] = {
        "summary": summary,
        "outcome": outcome,
        "next_step": next_step,
    }
    payload["objections"] = objections
    payload["booking_attempted"] = bool(sales_analysis.get("booking_attempted")) or outcome in APPOINTMENT_OUTCOMES
    payload["next_step_detected"] = bool(sales_analysis.get("next_step_defined")) or bool(next_step)
    payload["filler_count"] = int(filler_count_by_call.get(call_id) or 0)
    payload["pause_signals"] = list(pause_signals_by_call.get(call_id, []))
    return payload


async def build_minimal_call_understanding_payloads(
    session: AsyncSession,
    call_ids: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    clean_call_ids = [
        call_id
        for call_id in dict.fromkeys(str(value or "").strip() for value in call_ids)
        if call_id
    ]
    if not clean_call_ids:
        return {}

    analysis_by_call: Dict[str, Dict[str, Any]] = {}
    transcript_by_call: Dict[str, Dict[str, Any]] = {}
    objections_by_call: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    filler_count_by_call: Dict[str, int] = {}
    pause_signals_by_call: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    call_ids_param = bindparam("call_ids", expanding=True)

    analysis_rows = await session.execute(
        text("SELECT * FROM call_analysis WHERE call_id IN :call_ids").bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    for row in analysis_rows.mappings().all():
        analysis_by_call[str(row["call_id"])] = dict(row)

    transcript_rows = await session.execute(
        text("SELECT * FROM transcripts WHERE call_id IN :call_ids").bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    for row in transcript_rows.mappings().all():
        transcript_by_call[str(row["call_id"])] = dict(row)

    objection_rows = await session.execute(
        text("SELECT * FROM objections WHERE call_id IN :call_ids ORDER BY detected_at_ms ASC, created_at ASC").bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    for row in objection_rows.mappings().all():
        objections_by_call[str(row["call_id"])].append(dict(row))

    filler_rows = await session.execute(
        text(
            """
            SELECT call_id, SUM(COALESCE(count, 0)) AS filler_count
            FROM filler_events
            WHERE call_id IN :call_ids
            GROUP BY call_id
            """
        ).bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    for row in filler_rows.mappings().all():
        filler_count_by_call[str(row["call_id"])] = int(row.get("filler_count") or 0)

    fluency_rows = await session.execute(
        text(
            """
            SELECT call_id, event_type, start_ms, duration_ms, severity, evidence
            FROM fluency_events
            WHERE call_id IN :call_ids
            ORDER BY start_ms ASC, created_at ASC
            """
        ).bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    for row in fluency_rows.mappings().all():
        pause_signals_by_call[str(row["call_id"])].append(
            {
                "event_type": str(row.get("event_type") or ""),
                "start_ms": int(row.get("start_ms") or 0),
                "duration_ms": int(row.get("duration_ms") or 0),
                "severity": float(row.get("severity") or 0.0),
                "evidence": str(row.get("evidence") or ""),
            }
        )

    return {
        call_id: _build_minimal_call_understanding(
            call_id=call_id,
            analysis_by_call=analysis_by_call,
            transcript_by_call=transcript_by_call,
            objections_by_call=objections_by_call,
            filler_count_by_call=filler_count_by_call,
            pause_signals_by_call=pause_signals_by_call,
        )
        for call_id in clean_call_ids
    }


def _coerce_datetime(value: Any) -> Optional[datetime.datetime]:
    if isinstance(value, datetime.datetime):
        parsed = value
    else:
        parsed = _parse_iso_datetime(str(value or ""))
    if not parsed:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SYDNEY_TZ)
    return parsed.astimezone(SYDNEY_TZ).replace(microsecond=0)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def outcome_implies_connected(outcome: str) -> bool:
    normalized = normalize_outcome(outcome)
    return normalized in CONNECTED_OUTCOMES or normalized.startswith("connected")


def build_call_log_row(
    *,
    lead_id: str,
    lead_address: str = "",
    outcome: str,
    connected: Optional[bool] = None,
    call_duration_seconds: int = 0,
    note: str = "",
    user_id: Optional[str] = None,
    timestamp: Optional[str] = None,
    next_action_due: Optional[str] = None,
    provider: str = "manual",
    provider_call_id: Optional[str] = None,
    direction: str = "outbound",
    from_number: str = "",
    to_number: str = "",
    raw_payload: str = "{}",
    row_id: Optional[str] = None,
    recording_url: Optional[str] = None,
    transcript: Optional[str] = None,
    summary: Optional[str] = None,
    intent_signal: float = 0.0,
    booking_attempted: bool = False,
    next_step_detected: bool = False,
    objection_tags: Optional[List[str]] = None,
) -> Dict[str, Any]:
    normalized_outcome = normalize_outcome(outcome)
    logged_at_dt = _coerce_datetime(timestamp) or now_sydney()
    duration_seconds = _safe_int(call_duration_seconds)
    resolved_user_id = str(user_id or DEFAULT_USER_ID).strip() or DEFAULT_USER_ID
    resolved_connected = outcome_implies_connected(normalized_outcome) if connected is None else bool(connected)
    next_action_due_dt = _coerce_datetime(next_action_due)
    logged_at = logged_at_dt.isoformat()
    return {
        "id": row_id or str(uuid.uuid4()),
        "lead_id": lead_id,
        "lead_address": lead_address or "",
        "outcome": normalized_outcome,
        "connected": 1 if resolved_connected else 0,
        "duration_seconds": duration_seconds,
        "note": str(note or ""),
        "operator": resolved_user_id,
        "logged_at": logged_at,
        "logged_date": logged_at_dt.strftime("%Y-%m-%d"),
        "provider": provider or "manual",
        "provider_call_id": provider_call_id,
        "direction": direction or "outbound",
        "from_number": from_number or "",
        "to_number": to_number or "",
        "raw_payload": raw_payload or "{}",
        "user_id": resolved_user_id,
        "timestamp": logged_at,
        "call_duration_seconds": duration_seconds,
        "next_action_due": next_action_due_dt.isoformat() if next_action_due_dt else None,
        "recording_url": recording_url or None,
        "transcript": transcript or None,
        "summary": summary or None,
        "intent_signal": float(intent_signal or 0.0),
        "booking_attempted": 1 if booking_attempted else 0,
        "next_step_detected": 1 if next_step_detected else 0,
        "objection_tags": json.dumps(objection_tags or []),
    }


async def insert_call_log_row(session: AsyncSession, row_payload: Dict[str, Any]) -> None:
    await ensure_call_log_schema(session)
    await session.execute(
        text(
            """
            INSERT INTO call_log (
                id,
                lead_id,
                lead_address,
                outcome,
                connected,
                duration_seconds,
                note,
                operator,
                logged_at,
                logged_date,
                provider,
                provider_call_id,
                direction,
                from_number,
                to_number,
                raw_payload,
                user_id,
                timestamp,
                call_duration_seconds,
                next_action_due,
                recording_url,
                transcript,
                summary,
                intent_signal,
                booking_attempted,
                next_step_detected,
                objection_tags
            ) VALUES (
                :id,
                :lead_id,
                :lead_address,
                :outcome,
                :connected,
                :duration_seconds,
                :note,
                :operator,
                :logged_at,
                :logged_date,
                :provider,
                :provider_call_id,
                :direction,
                :from_number,
                :to_number,
                :raw_payload,
                :user_id,
                :timestamp,
                :call_duration_seconds,
                :next_action_due,
                :recording_url,
                :transcript,
                :summary,
                :intent_signal,
                :booking_attempted,
                :next_step_detected,
                :objection_tags
            )
            """
        ),
        row_payload,
    )


def _resolve_date_range(
    *,
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> tuple[datetime.date, datetime.date]:
    if date and (start_date or end_date):
        raise HTTPException(status_code=400, detail="Use either date or start_date/end_date.")
    if date:
        try:
            resolved = datetime.date.fromisoformat(date)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid date: {date}") from exc
        return resolved, resolved
    start_value = start_date or end_date or now_sydney().date().isoformat()
    end_value = end_date or start_date or now_sydney().date().isoformat()
    try:
        start_resolved = datetime.date.fromisoformat(start_value)
        end_resolved = datetime.date.fromisoformat(end_value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid start_date/end_date.") from exc
    if end_resolved < start_resolved:
        raise HTTPException(status_code=400, detail="end_date must be on or after start_date.")
    return start_resolved, end_resolved


def _scope_metadata(
    *,
    start_date: datetime.date,
    end_date: datetime.date,
    selected_lead_id: Optional[str],
    selected_user_id: Optional[str],
    selected_outcome: Optional[str],
    selected_session_id: Optional[str],
    available_user_ids: Sequence[str],
    available_outcomes: Sequence[str],
) -> Dict[str, Any]:
    return {
        "date": start_date.isoformat() if start_date == end_date else None,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "lead_id": selected_lead_id,
        "user_id": selected_user_id,
        "outcome": normalize_outcome(selected_outcome) if selected_outcome else None,
        "session_id": selected_session_id,
        "available_user_ids": list(available_user_ids),
        "available_outcomes": list(available_outcomes),
        "lead_id_supported": True,
        "user_id_supported": True,
    }


def _timezone_metadata(anchor_date: datetime.date) -> Dict[str, Any]:
    anchor_dt = datetime.datetime.combine(anchor_date, datetime.time.min, tzinfo=SYDNEY_TZ)
    return {
        "name": "Australia/Sydney",
        "date_interpreted_in_timezone": anchor_date.isoformat(),
        "utc_offset": anchor_dt.strftime("%z"),
        "current_time": now_sydney().isoformat(),
    }


def _normalize_call_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    start_dt = _coerce_datetime(row.get("timestamp") or row.get("logged_at"))
    if not start_dt:
        return None
    duration_seconds = _safe_int(row.get("call_duration_seconds", row.get("duration_seconds")))
    normalized_outcome = normalize_outcome(row.get("outcome"))
    connected = _coerce_bool(row.get("connected")) or outcome_implies_connected(normalized_outcome)
    user_id = str(row.get("user_id") or row.get("operator") or DEFAULT_USER_ID).strip() or DEFAULT_USER_ID
    return {
        "id": str(row.get("id") or ""),
        "call_id": str(row.get("provider_call_id") or row.get("id") or ""),
        "user_id": user_id,
        "lead_id": str(row.get("lead_id") or ""),
        "lead_address": str(row.get("lead_address") or ""),
        "timestamp": start_dt.isoformat(),
        "local_timestamp_sydney": start_dt.isoformat(),
        "start_dt": start_dt,
        "end_dt": start_dt + datetime.timedelta(seconds=duration_seconds),
        "outcome": normalized_outcome,
        "connected": connected,
        "call_duration_seconds": duration_seconds,
        "note": str(row.get("note") or ""),
        "next_action_due": row.get("next_action_due"),
        "provider": str(row.get("provider") or "manual"),
        "to_number": str(row.get("to_number") or ""),
        "from_number": str(row.get("from_number") or ""),
    }


async def _fetch_call_rows(
    session: AsyncSession,
    *,
    start_date: datetime.date,
    end_date: datetime.date,
) -> List[Dict[str, Any]]:
    await ensure_call_log_schema(session)
    result = await session.execute(
        text(
            """
            SELECT *
            FROM call_log
            WHERE logged_date >= :start_date
              AND logged_date <= :end_date
            ORDER BY COALESCE(timestamp, logged_at) ASC, id ASC
            """
        ),
        {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
    )
    return [dict(row) for row in result.mappings().all()]


def _attach_sessions(
    rows: Sequence[Dict[str, Any]],
    *,
    threshold_seconds: int = SESSION_BOUNDARY_THRESHOLD_SECONDS,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    normalized_rows = [row.copy() for row in rows]
    normalized_rows.sort(key=lambda row: (row["start_dt"], row["id"]))
    sessions: List[Dict[str, Any]] = []
    current_session: Optional[Dict[str, Any]] = None

    for index, row in enumerate(normalized_rows):
        previous = normalized_rows[index - 1] if index > 0 else None
        if previous:
            gap_seconds = max(0, int((row["start_dt"] - previous["end_dt"]).total_seconds()))
        else:
            gap_seconds = 0

        starts_new_session = current_session is None or gap_seconds > threshold_seconds
        row["gap_from_previous_seconds"] = gap_seconds
        row["idle_gap_seconds"] = 0 if starts_new_session else gap_seconds
        if starts_new_session:
            session_id = f"session-{row['start_dt'].strftime('%Y%m%d')}-{len(sessions) + 1:03d}"
            current_session = {
                "session_id": session_id,
                "session_start": row["start_dt"],
                "session_end": row["end_dt"],
                "session_idle_time_seconds": 0,
                "session_talk_time_seconds": 0,
                "session_dial_count": 0,
                "session_connect_count": 0,
                "session_conversation_count": 0,
                "session_appointments_booked_count": 0,
                "events": [],
            }
            sessions.append(current_session)
        else:
            current_session["session_end"] = max(current_session["session_end"], row["end_dt"])
            current_session["session_idle_time_seconds"] += gap_seconds

        row["session_id"] = current_session["session_id"]
        current_session["events"].append(row)
        current_session["session_talk_time_seconds"] += row["call_duration_seconds"] if row["connected"] else 0
        current_session["session_dial_count"] += 1
        current_session["session_connect_count"] += 1 if row["connected"] else 0
        current_session["session_conversation_count"] += 1 if row["connected"] and row["call_duration_seconds"] >= CONVERSATION_MIN_DURATION_SECONDS else 0
        current_session["session_appointments_booked_count"] += 1 if row["outcome"] in APPOINTMENT_OUTCOMES else 0

    session_payloads: List[Dict[str, Any]] = []
    for session_row in sessions:
        duration_seconds = int((session_row["session_end"] - session_row["session_start"]).total_seconds()) if session_row["events"] else 0
        session_payloads.append(
            {
                "session_id": session_row["session_id"],
                "session_start": session_row["session_start"].isoformat(),
                "session_end": session_row["session_end"].isoformat(),
                "session_duration_seconds": duration_seconds,
                "session_active_time_seconds": session_row["session_talk_time_seconds"],
                "session_idle_time_seconds": session_row["session_idle_time_seconds"],
                "session_talk_time_seconds": session_row["session_talk_time_seconds"],
                "session_dial_count": session_row["session_dial_count"],
                "session_connect_count": session_row["session_connect_count"],
                "session_unanswered_count": session_row["session_dial_count"] - session_row["session_connect_count"],
                "session_conversation_count": session_row["session_conversation_count"],
                "session_appointments_booked_count": session_row["session_appointments_booked_count"],
            }
        )

    return normalized_rows, session_payloads


def _summarize_filtered_sessions(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped_rows[row["session_id"]].append(row)

    session_payloads: List[Dict[str, Any]] = []
    for current_session_id, session_rows in sorted(grouped_rows.items(), key=lambda item: min(row["start_dt"] for row in item[1])):
        ordered_rows = sorted(session_rows, key=lambda row: (row["start_dt"], row["id"]))
        session_start = ordered_rows[0]["start_dt"]
        session_end = max(row["end_dt"] for row in ordered_rows)
        idle_time_seconds = sum(int(row["idle_gap_seconds"]) for row in ordered_rows[1:]) if len(ordered_rows) > 1 else 0
        duration_seconds = int((session_end - session_start).total_seconds())
        talk_time_seconds = sum(row["call_duration_seconds"] for row in ordered_rows if row["connected"])
        session_payloads.append(
            {
                "session_id": current_session_id,
                "session_start": session_start.isoformat(),
                "session_end": session_end.isoformat(),
                "session_duration_seconds": duration_seconds,
                "session_active_time_seconds": talk_time_seconds,
                "session_idle_time_seconds": idle_time_seconds,
                "session_talk_time_seconds": talk_time_seconds,
                "session_dial_count": len(ordered_rows),
                "session_connect_count": sum(1 for row in ordered_rows if row["connected"]),
                "session_unanswered_count": len(ordered_rows) - sum(1 for row in ordered_rows if row["connected"]),
                "session_conversation_count": sum(1 for row in ordered_rows if row["connected"] and row["call_duration_seconds"] >= CONVERSATION_MIN_DURATION_SECONDS),
                "session_appointments_booked_count": sum(1 for row in ordered_rows if row["outcome"] in APPOINTMENT_OUTCOMES),
            }
        )
    return session_payloads


def _aggregate_metrics(
    rows: Sequence[Dict[str, Any]],
    sessions: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    ordered_rows = sorted(rows, key=lambda row: (row["start_dt"], row["id"]))
    dial_count = len(rows)
    call_attempt_count = dial_count
    touched_leads = {row["lead_id"] for row in rows if row.get("lead_id")}
    leads_touched_count = len(touched_leads)
    connect_count = sum(1 for row in rows if row["connected"])
    conversation_count = sum(1 for row in rows if row["connected"] and row["call_duration_seconds"] >= CONVERSATION_MIN_DURATION_SECONDS)
    unanswered_count = dial_count - connect_count
    total_talk_time_seconds = sum(row["call_duration_seconds"] for row in rows if row["connected"])
    total_talk_time_minutes = total_talk_time_seconds / 60 if total_talk_time_seconds else 0.0
    appointments_booked_count = sum(1 for row in rows if row["outcome"] in APPOINTMENT_OUTCOMES)
    appraisal_booked_count = sum(1 for row in rows if row["outcome"] in APPRAISAL_OUTCOMES)
    voicemail_count = sum(1 for row in rows if row["outcome"] == "voicemail")
    longest_idle_gap_seconds = max((int(row["idle_gap_seconds"]) for row in ordered_rows), default=0)

    total_session_time_seconds = sum(int(session["session_duration_seconds"]) for session in sessions)
    idle_time_seconds = sum(int(session["session_idle_time_seconds"]) for session in sessions)
    active_time_seconds = total_talk_time_seconds
    idle_blocks_count = sum(1 for row in ordered_rows if row["idle_gap_seconds"] > 0)

    sessions_count = len(sessions)
    avg_session_duration_seconds = sum(session["session_duration_seconds"] for session in sessions) / sessions_count if sessions_count else 0.0
    avg_talk_time_per_session_seconds = sum(session["session_talk_time_seconds"] for session in sessions) / sessions_count if sessions_count else 0.0
    longest_session_seconds = max((session["session_duration_seconds"] for session in sessions), default=0)
    first_contact_at = ordered_rows[0]["timestamp"] if ordered_rows else None
    last_contact_at = ordered_rows[-1]["timestamp"] if ordered_rows else None

    return {
        "dial_count": dial_count,
        "call_attempt_count": call_attempt_count,
        "leads_touched_count": leads_touched_count,
        "attempts_per_lead": _safe_div(call_attempt_count, leads_touched_count),
        "connect_count": connect_count,
        "unanswered_count": unanswered_count,
        "conversation_count": conversation_count,
        "connect_rate": _safe_div(connect_count, dial_count),
        "conversation_rate": _safe_div(conversation_count, connect_count),
        "total_talk_time_seconds": total_talk_time_seconds,
        "total_talk_time_minutes": total_talk_time_minutes,
        "avg_talk_time_seconds": _safe_div(total_talk_time_seconds, connect_count),
        "avg_talk_time": _safe_div(total_talk_time_seconds, connect_count),
        "avg_talk_time_per_connect_seconds": _safe_div(total_talk_time_seconds, connect_count),
        "avg_talk_time_per_conversation_seconds": _safe_div(total_talk_time_seconds, conversation_count),
        "first_activity_timestamp": first_contact_at,
        "last_activity_timestamp": last_contact_at,
        "first_contact_at": first_contact_at,
        "last_contact_at": last_contact_at,
        "total_session_time_seconds": total_session_time_seconds,
        "idle_time_seconds": idle_time_seconds,
        "active_time_seconds": active_time_seconds,
        "idle_blocks_count": idle_blocks_count,
        "longest_idle_gap_seconds": longest_idle_gap_seconds,
        "appointments_booked_count": appointments_booked_count,
        "appraisal_booked_count": appraisal_booked_count,
        "voicemail_count": voicemail_count,
        "conversion_rate": _safe_div(appointments_booked_count, connect_count),
        "booked_per_dial_rate": _safe_div(appointments_booked_count, dial_count),
        "booked_per_connect_rate": _safe_div(appointments_booked_count, connect_count),
        "booked_per_conversation_rate": _safe_div(appointments_booked_count, conversation_count),
        "sessions_count": sessions_count,
        "avg_session_duration_seconds": avg_session_duration_seconds,
        "avg_talk_time_per_session_seconds": avg_talk_time_per_session_seconds,
        "longest_session_seconds": longest_session_seconds,
    }


def _build_hourly_breakdown(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for hour in range(24):
        label = f"{hour:02d}:00"
        buckets[label] = {
            "label": label,
            "dial_count": 0,
            "connect_count": 0,
            "conversation_count": 0,
            "total_talk_time_seconds": 0,
            "appointments_booked_count": 0,
        }
    for row in rows:
        label = row["start_dt"].strftime("%H:00")
        bucket = buckets[label]
        bucket["dial_count"] += 1
        if row["connected"]:
            bucket["connect_count"] += 1
            bucket["total_talk_time_seconds"] += int(row["call_duration_seconds"])
            if int(row["call_duration_seconds"]) >= CONVERSATION_MIN_DURATION_SECONDS:
                bucket["conversation_count"] += 1
        if row["outcome"] in APPOINTMENT_OUTCOMES:
            bucket["appointments_booked_count"] += 1
    return list(buckets.values())


def _build_outcome_breakdown(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"outcome": "", "count": 0, "talk_time_seconds": 0})
    for row in rows:
        outcome = row["outcome"]
        bucket = grouped[outcome]
        bucket["outcome"] = outcome
        bucket["count"] += 1
        bucket["talk_time_seconds"] += int(row["call_duration_seconds"])
    return sorted(grouped.values(), key=lambda item: (-int(item["count"]), item["outcome"]))


def _build_user_breakdown(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "user_id": "",
            "dial_count": 0,
            "connect_count": 0,
            "conversation_count": 0,
            "appointments_booked_count": 0,
            "total_talk_time_seconds": 0,
        }
    )
    for row in rows:
        key = str(row.get("user_id") or DEFAULT_USER_ID)
        bucket = grouped[key]
        bucket["user_id"] = key
        bucket["dial_count"] += 1
        if row["connected"]:
            bucket["connect_count"] += 1
            bucket["total_talk_time_seconds"] += int(row["call_duration_seconds"])
            if int(row["call_duration_seconds"]) >= CONVERSATION_MIN_DURATION_SECONDS:
                bucket["conversation_count"] += 1
        if row["outcome"] in APPOINTMENT_OUTCOMES:
            bucket["appointments_booked_count"] += 1
    for bucket in grouped.values():
        bucket["connect_rate"] = _safe_div(bucket["connect_count"], bucket["dial_count"])
        bucket["booked_per_connect_rate"] = _safe_div(bucket["appointments_booked_count"], bucket["connect_count"])
    return sorted(grouped.values(), key=lambda item: (-int(item["appointments_booked_count"]), -int(item["connect_count"]), item["user_id"]))


def _build_session_rankings(sessions: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not sessions:
        return {
            "best_by_talk_time": None,
            "best_by_bookings": None,
            "worst_by_idle_ratio": None,
        }

    def _idle_ratio(session_row: Dict[str, Any]) -> float:
        return _safe_div(int(session_row.get("session_idle_time_seconds") or 0), int(session_row.get("session_duration_seconds") or 0))

    best_by_talk_time = max(sessions, key=lambda item: (int(item.get("session_talk_time_seconds") or 0), int(item.get("session_connect_count") or 0)))
    best_by_bookings = max(sessions, key=lambda item: (int(item.get("session_appointments_booked_count") or 0), int(item.get("session_connect_count") or 0)))
    worst_by_idle_ratio = max(sessions, key=_idle_ratio)
    return {
        "best_by_talk_time": best_by_talk_time,
        "best_by_bookings": best_by_bookings,
        "worst_by_idle_ratio": {
            **worst_by_idle_ratio,
            "idle_ratio": _idle_ratio(worst_by_idle_ratio),
        },
    }


def _build_rundown(
    metrics: Dict[str, Any],
    *,
    hourly_breakdown: Sequence[Dict[str, Any]],
    user_breakdown: Sequence[Dict[str, Any]],
    outcome_breakdown: Sequence[Dict[str, Any]],
) -> Dict[str, List[str]]:
    working: List[str] = []
    underperforming: List[str] = []
    iterate_next: List[str] = []

    connect_rate = float(metrics.get("connect_rate") or 0.0)
    booked_per_connect_rate = float(metrics.get("booked_per_connect_rate") or 0.0)
    idle_ratio = _safe_div(float(metrics.get("idle_time_seconds") or 0.0), float(metrics.get("total_session_time_seconds") or 0.0))

    best_hour = max(hourly_breakdown, key=lambda item: (int(item.get("appointments_booked_count") or 0), int(item.get("connect_count") or 0)), default=None)
    if best_hour and (int(best_hour.get("appointments_booked_count") or 0) > 0 or int(best_hour.get("connect_count") or 0) > 0):
        working.append(
            f"{best_hour['label']} is the strongest hour so far with {best_hour['connect_count']} connect(s) and {best_hour['appointments_booked_count']} booking(s)."
        )
    if connect_rate >= 0.35:
        working.append(f"Connect rate is healthy at {round(connect_rate * 100)}%.")
    if booked_per_connect_rate >= 0.2:
        working.append(f"Booking conversion from connects is strong at {round(booked_per_connect_rate * 100)}%.")

    if connect_rate < 0.2:
        underperforming.append(f"Connect rate is weak at {round(connect_rate * 100)}%; list quality or opener quality needs attention.")
    if float(metrics.get("booked_per_connect_rate") or 0.0) < 0.1 and int(metrics.get("connect_count") or 0) >= 3:
        underperforming.append("Connects are happening, but booking conversion is weak once conversations start.")
    if idle_ratio > 0.45:
        underperforming.append(f"Idle ratio is high at {round(idle_ratio * 100)}%; session momentum is leaking.")
    if int(metrics.get("longest_idle_gap_seconds") or 0) >= 1800:
        underperforming.append(f"Longest idle gap hit {int(metrics['longest_idle_gap_seconds']) // 60} minutes.")

    no_answer_count = next((int(item["count"]) for item in outcome_breakdown if item.get("outcome") == "no_answer"), 0)
    if no_answer_count > int(metrics.get("connect_count") or 0):
        iterate_next.append("The no-answer volume is dominating connects; test call windows before changing scripts.")
    if int(metrics.get("appointments_booked_count") or 0) == 0 and int(metrics.get("conversation_count") or 0) > 0:
        iterate_next.append("Add a harder next-step ask on the next live conversations; the current flow is not converting into bookings.")
    if user_breakdown:
        top_user = user_breakdown[0]
        iterate_next.append(
            f"Double down on the highest-output operator pattern: {top_user['user_id']} has {top_user['connect_count']} connect(s) and {top_user['appointments_booked_count']} booking(s)."
        )

    if not working:
        working.append("Volume is being captured, but there is no standout performance pattern yet.")
    if not underperforming:
        underperforming.append("No critical failure band detected in the current deterministic metrics.")
    if not iterate_next:
        iterate_next.append("Keep pushing for more connects and review the strongest recorded calls first.")

    return {
        "working": working[:3],
        "underperforming": underperforming[:3],
        "iterate_next": iterate_next[:3],
    }


async def _build_call_review_metadata(
    session: AsyncSession,
    call_ids: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    clean_call_ids = [call_id for call_id in dict.fromkeys(str(value or "").strip() for value in call_ids) if call_id]
    if not clean_call_ids:
        return {}

    metadata_by_call: Dict[str, Dict[str, Any]] = {
        call_id: {
            "has_analysis": False,
            "has_transcript": False,
            "analysis_status": "",
            "transcript_status": "",
            "file_url": None,
            "recording_url": None,
            "recording_duration_seconds": None,
            "has_recording": False,
            "score_summary": None,
        }
        for call_id in clean_call_ids
    }

    call_ids_param = bindparam("call_ids", expanding=True)

    analysis_rows = await session.execute(
        text("SELECT call_id, status FROM call_analysis WHERE call_id IN :call_ids").bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    for row in analysis_rows.mappings().all():
        call_id = str(row["call_id"])
        metadata_by_call.setdefault(call_id, {}).update(
            {
                "has_analysis": True,
                "analysis_status": str(row.get("status") or ""),
            }
        )

    transcript_rows = await session.execute(
        text("SELECT call_id, status FROM transcripts WHERE call_id IN :call_ids").bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    for row in transcript_rows.mappings().all():
        call_id = str(row["call_id"])
        metadata_by_call.setdefault(call_id, {}).update(
            {
                "has_transcript": True,
                "transcript_status": str(row.get("status") or ""),
            }
        )

    call_log_rows = await session.execute(
        text(
            """
            SELECT id, provider_call_id, recording_url, recording_status, recording_duration_seconds
            FROM call_log
            WHERE id IN :call_ids OR provider_call_id IN :call_ids
            """
        ).bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    for row in call_log_rows.mappings().all():
        payload = dict(row)
        row_id = str(payload.get("id") or "")
        ext_id = str(payload.get("provider_call_id") or "")
        playback_key = ext_id or row_id
        playback_url = (
            f"/api/recordings/{playback_key}/stream"
            if playback_key and str(payload.get("recording_url") or "").strip()
            else None
        )
        review_payload = {
            "recording_url": playback_url,
            "recording_duration_seconds": (
                int(payload["recording_duration_seconds"])
                if payload.get("recording_duration_seconds") not in (None, "")
                else None
            ),
            "has_recording": bool(playback_url),
        }
        if row_id:
            metadata_by_call.setdefault(row_id, {}).update(review_payload)
        if ext_id:
            metadata_by_call.setdefault(ext_id, {}).update(review_payload)

    speech_rows = await session.execute(
        text(
            """
            SELECT id, external_call_id, audio_uri, audio_storage_status, analysis_status, transcript_status, source
            FROM calls
            WHERE id IN :call_ids OR external_call_id IN :call_ids
            """
        ).bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    speech_call_by_key: Dict[str, Dict[str, Any]] = {}
    for row in speech_rows.mappings().all():
        payload = dict(row)
        row_id = str(payload.get("id") or "")
        ext_id = str(payload.get("external_call_id") or "")
        if row_id:
            speech_call_by_key[row_id] = payload
        if ext_id:
            speech_call_by_key[ext_id] = payload

    snapshot_ids: List[str] = []
    snapshot_by_key: Dict[str, Dict[str, Any]] = {}
    component_scores_by_snapshot: Dict[str, Dict[str, float]] = defaultdict(dict)
    snapshot_rows = await session.execute(
        text(
            """
            SELECT *
            FROM score_snapshots
            WHERE entity_type = 'call'
              AND (entity_id IN :call_ids OR call_id IN :call_ids)
            ORDER BY computed_at DESC, created_at DESC
            """
        ).bindparams(call_ids_param),
        {"call_ids": clean_call_ids},
    )
    for row in snapshot_rows.mappings().all():
        payload = dict(row)
        snapshot_id = str(payload.get("id") or "")
        entity_id = str(payload.get("entity_id") or "")
        call_id = str(payload.get("call_id") or "")
        if snapshot_id:
            snapshot_ids.append(snapshot_id)
        if entity_id and entity_id not in snapshot_by_key:
            snapshot_by_key[entity_id] = payload
        if call_id and call_id not in snapshot_by_key:
            snapshot_by_key[call_id] = payload

    if snapshot_ids:
        snapshot_ids_param = bindparam("snapshot_ids", expanding=True)
        component_rows = await session.execute(
            text(
                """
                SELECT snapshot_id, score_name, score_value
                FROM score_components
                WHERE snapshot_id IN :snapshot_ids
                """
            ).bindparams(snapshot_ids_param),
            {"snapshot_ids": snapshot_ids},
        )
        for row in component_rows.mappings().all():
            component_scores_by_snapshot[str(row["snapshot_id"])][str(row["score_name"])] = float(row.get("score_value") or 0.0)

    for call_id in clean_call_ids:
        speech_call = speech_call_by_key.get(call_id) or {}
        snapshot = snapshot_by_key.get(call_id) or {}
        snapshot_components = component_scores_by_snapshot.get(str(snapshot.get("id") or ""), {})
        metadata_by_call.setdefault(call_id, {}).update(
            {
                "provider": str(speech_call.get("source") or ""),
                "analysis_status": str(speech_call.get("analysis_status") or metadata_by_call.get(call_id, {}).get("analysis_status") or ""),
                "transcript_status": str(speech_call.get("transcript_status") or metadata_by_call.get(call_id, {}).get("transcript_status") or ""),
                "file_url": (
                    f"/api/recordings/{str(speech_call.get('id') or call_id)}/stream"
                    if str(speech_call.get("audio_storage_status") or "") == "stored" and str(speech_call.get("audio_uri") or "")
                    else None
                ),
                "recording_url": (
                    f"/api/recordings/{str(speech_call.get('id') or call_id)}/stream"
                    if str(speech_call.get("audio_storage_status") or "") == "stored" and str(speech_call.get("audio_uri") or "")
                    else metadata_by_call.get(call_id, {}).get("recording_url")
                ),
                "has_recording": bool(
                    (
                        f"/api/recordings/{str(speech_call.get('id') or call_id)}/stream"
                        if str(speech_call.get("audio_storage_status") or "") == "stored" and str(speech_call.get("audio_uri") or "")
                        else metadata_by_call.get(call_id, {}).get("recording_url")
                    )
                ),
                "score_summary": (
                    {
                        "composite_score": float(snapshot.get("composite_score") or 0.0),
                        "scoring_version": str(snapshot.get("scoring_version") or ""),
                        "fluency_score": float(snapshot_components.get("fluency_score") or 0.0),
                        "confidence_score": float(snapshot_components.get("confidence_score") or 0.0),
                        "sales_control_score": float(snapshot_components.get("sales_control_score") or 0.0),
                        "booking_closing_score": float(snapshot_components.get("booking_closing_score") or 0.0),
                    }
                    if snapshot
                    else None
                ),
            }
        )

    return metadata_by_call


async def build_metrics_dataset(
    session: AsyncSession,
    *,
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lead_id: Optional[str] = None,
    user_id: Optional[str] = None,
    outcome: Optional[str] = None,
    session_id: Optional[str] = None,
    threshold_seconds: int = SESSION_BOUNDARY_THRESHOLD_SECONDS,
    include_call_understanding: bool = False,
) -> Dict[str, Any]:
    scope_start, scope_end = _resolve_date_range(date=date, start_date=start_date, end_date=end_date)
    raw_rows = await _fetch_call_rows(session, start_date=scope_start, end_date=scope_end)
    normalized_rows = [row for row in (_normalize_call_row(raw_row) for raw_row in raw_rows) if row is not None]

    selected_lead_id = str(lead_id or "").strip() or None
    selected_user_id = str(user_id or "").strip() or None
    selected_outcome = normalize_outcome(outcome) if outcome else None

    if selected_lead_id:
        normalized_rows = [row for row in normalized_rows if row["lead_id"] == selected_lead_id]

    available_user_ids = sorted({row["user_id"] for row in normalized_rows if row.get("user_id")})
    available_outcomes = sorted({row["outcome"] for row in normalized_rows if row.get("outcome")})

    if selected_user_id:
        normalized_rows = [row for row in normalized_rows if row["user_id"] == selected_user_id]

    timeline_rows, _ = _attach_sessions(normalized_rows, threshold_seconds=threshold_seconds)

    filtered_rows = timeline_rows
    if selected_outcome:
        filtered_rows = [row for row in filtered_rows if row["outcome"] == selected_outcome]
    if session_id:
        filtered_rows = [row for row in filtered_rows if row["session_id"] == session_id]

    filtered_sessions = _summarize_filtered_sessions(filtered_rows)

    aggregated = _aggregate_metrics(filtered_rows, filtered_sessions)
    call_understanding_by_call: Dict[str, Dict[str, Any]] = {}
    call_review_metadata_by_call: Dict[str, Dict[str, Any]] = {}
    if include_call_understanding and filtered_rows:
        call_understanding_by_call = await build_minimal_call_understanding_payloads(
            session,
            [row["call_id"] for row in filtered_rows],
        )
        call_review_metadata_by_call = await _build_call_review_metadata(
            session,
            [row["call_id"] for row in filtered_rows],
        )

    hourly_breakdown = _build_hourly_breakdown(filtered_rows)
    outcome_breakdown = _build_outcome_breakdown(filtered_rows)
    user_breakdown = _build_user_breakdown(filtered_rows)
    session_rankings = _build_session_rankings(filtered_sessions)
    rundown = _build_rundown(
        aggregated,
        hourly_breakdown=hourly_breakdown,
        user_breakdown=user_breakdown,
        outcome_breakdown=outcome_breakdown,
    )

    return {
        **aggregated,
        "date": scope_start.isoformat() if scope_start == scope_end else None,
        "start_date": scope_start.isoformat(),
        "end_date": scope_end.isoformat(),
        "timezone": _timezone_metadata(scope_start),
        "filter_metadata": _scope_metadata(
            start_date=scope_start,
            end_date=scope_end,
            selected_lead_id=selected_lead_id,
            selected_user_id=selected_user_id,
            selected_outcome=selected_outcome,
            selected_session_id=session_id,
            available_user_ids=available_user_ids,
            available_outcomes=available_outcomes,
        ),
        "definitions": STRICT_METRIC_DEFINITIONS,
        "session_boundary_threshold_seconds": threshold_seconds,
        "hourly_breakdown": hourly_breakdown,
        "outcome_breakdown": outcome_breakdown,
        "user_breakdown": user_breakdown,
        "session_rankings": session_rankings,
        "rundown": rundown,
        "sessions": filtered_sessions,
        "activities": [
            {
                "id": row["id"],
                "call_id": row["call_id"],
                "user_id": row["user_id"],
                "lead_id": row["lead_id"],
                "lead_address": row["lead_address"],
                "to_number": row["to_number"],
                "from_number": row["from_number"],
                "provider": row["provider"],
                "timestamp": row["timestamp"],
                "local_timestamp_sydney": row["local_timestamp_sydney"],
                "outcome": row["outcome"],
                "connected": row["connected"],
                "call_duration_seconds": row["call_duration_seconds"],
                "talk_time_seconds": row["call_duration_seconds"] if row["connected"] else 0,
                "note": row["note"],
                "gap_from_previous_seconds": row["gap_from_previous_seconds"],
                "idle_gap_seconds": row["idle_gap_seconds"],
                "session_id": row["session_id"],
                "next_action_due": row["next_action_due"],
                **(
                    {
                        "call_understanding": call_understanding_by_call.get(
                            row["call_id"],
                            _empty_call_understanding(),
                        )
                    }
                    if include_call_understanding
                    else {}
                ),
                **(call_review_metadata_by_call.get(row["call_id"], {}) if include_call_understanding else {}),
            }
            for row in filtered_rows
        ],
    }


async def get_daily_metrics(
    session: AsyncSession,
    *,
    date: Optional[str],
    lead_id: Optional[str],
    user_id: Optional[str],
    outcome: Optional[str],
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    data = await build_metrics_dataset(session, date=date, lead_id=lead_id, user_id=user_id, outcome=outcome, session_id=session_id)
    data.pop("activities", None)
    return data


async def get_range_metrics(
    session: AsyncSession,
    *,
    start_date: Optional[str],
    end_date: Optional[str],
    lead_id: Optional[str],
    user_id: Optional[str],
    outcome: Optional[str],
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    data = await build_metrics_dataset(
        session,
        start_date=start_date,
        end_date=end_date,
        lead_id=lead_id,
        user_id=user_id,
        outcome=outcome,
        session_id=session_id,
    )
    data.pop("activities", None)
    return data


async def get_timeline_metrics(
    session: AsyncSession,
    *,
    date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    lead_id: Optional[str],
    user_id: Optional[str],
    outcome: Optional[str],
    session_id: Optional[str],
) -> Dict[str, Any]:
    data = await build_metrics_dataset(
        session,
        date=date,
        start_date=start_date,
        end_date=end_date,
        lead_id=lead_id,
        user_id=user_id,
        outcome=outcome,
        session_id=session_id,
        include_call_understanding=True,
    )
    session_calls: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for activity in data.get("activities") or []:
        session_calls[str(activity.get("session_id") or "")].append(activity)

    return {
        "date": data.get("date"),
        "start_date": data.get("start_date"),
        "end_date": data.get("end_date"),
        "timezone": data.get("timezone"),
        "filter_metadata": data.get("filter_metadata"),
        "definitions": data.get("definitions"),
        "session_boundary_threshold_seconds": data.get("session_boundary_threshold_seconds"),
        "rundown": data.get("rundown"),
        "session_rankings": data.get("session_rankings"),
        "activities": data.get("activities"),
        "sessions": [
            {**session_row, "calls": session_calls.get(str(session_row.get("session_id") or ""), [])}
            for session_row in (data.get("sessions") or [])
        ],
        "total_count": len(data.get("activities") or []),
    }


async def get_sessions_metrics(
    session: AsyncSession,
    *,
    date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    lead_id: Optional[str],
    user_id: Optional[str],
    outcome: Optional[str],
) -> Dict[str, Any]:
    data = await build_metrics_dataset(
        session,
        date=date,
        start_date=start_date,
        end_date=end_date,
        lead_id=lead_id,
        user_id=user_id,
        outcome=outcome,
    )
    return {
        "date": data.get("date"),
        "start_date": data.get("start_date"),
        "end_date": data.get("end_date"),
        "timezone": data.get("timezone"),
        "filter_metadata": data.get("filter_metadata"),
        "definitions": data.get("definitions"),
        "session_boundary_threshold_seconds": data.get("session_boundary_threshold_seconds"),
        "sessions": data.get("sessions"),
        "sessions_count": data.get("sessions_count"),
        "avg_session_duration_seconds": data.get("avg_session_duration_seconds"),
        "avg_talk_time_per_session_seconds": data.get("avg_talk_time_per_session_seconds"),
        "longest_session_seconds": data.get("longest_session_seconds"),
    }

async def get_historical_call_data(session: AsyncSession, start_date: str = "2026-03-13"):
    """Fetch all call logs from start_date to now for trend analysis."""
    res = await session.execute(text("""
        SELECT logged_date, 
               timestamp,
               connected, 
               outcome,
               call_duration_seconds
        FROM call_log 
        WHERE logged_date >= :start 
        ORDER BY timestamp ASC
    """), {"start": start_date})
    return [dict(r) for r in res.mappings().all()]

def aggregate_by_timeframe(rows: List[Dict], timeframe: str):
    """
    Groups call data into chart-ready points.
    - daily: by hour (00:00 - 23:00)
    - weekly: by day (Mon - Sun)
    - monthly: by week (Week 1 - Week 4)
    """
    if timeframe == 'daily':
        # 24 hours
        data = {f"{h:02d}:00": {"dials": 0, "connects": 0, "booked": 0, "voicemails": 0, "unanswered": 0} for h in range(24)}
        for r in rows:
            dt = _parse_iso_datetime(r.get("timestamp"))
            if dt:
                key = dt.strftime("%H:00")
                if key in data:
                    data[key]["dials"] += 1
                    outcome = normalize_outcome(r.get("outcome", ""))
                    if r.get("connected"): data[key]["connects"] += 1
                    if outcome in APPOINTMENT_OUTCOMES: data[key]["booked"] += 1
                    if outcome == "voicemail": data[key]["voicemails"] += 1
                    if not r.get("connected") and outcome != "voicemail": data[key]["unanswered"] += 1
        return [{"label": k, **v} for k, v in data.items()]

    elif timeframe == 'weekly':
        # Last 7 days
        days = []
        for i in range(6, -1, -1):
            d = (now_sydney() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            days.append(d)
        
        data = {d: {"dials": 0, "connects": 0, "booked": 0, "voicemails": 0, "unanswered": 0} for d in days}
        for r in rows:
            d = r.get("logged_date")
            if d in data:
                data[d]["dials"] += 1
                outcome = normalize_outcome(r.get("outcome", ""))
                if r.get("connected"): data[d]["connects"] += 1
                if outcome in APPOINTMENT_OUTCOMES: data[d]["booked"] += 1
                if outcome == "voicemail": data[d]["voicemails"] += 1
                if not r.get("connected") and outcome != "voicemail": data[d]["unanswered"] += 1
        return [{"label": k[5:], **v} for k, v in data.items()]

    return []

def aggregate_monthly(rows: List[Dict]):
    """Group by week for the last 4 weeks."""
    # Last 28 days
    data = {f"Week {i}": {"dials": 0, "connects": 0, "booked": 0, "voicemails": 0, "unanswered": 0} for i in range(1, 5)}
    now = now_sydney()
    for r in rows:
        d = r.get("logged_date")
        if not d: continue
        dt = datetime.date.fromisoformat(d)
        days_ago = (now.date() - dt).days
        if days_ago < 28:
            week_idx = 4 - (days_ago // 7)
            if 1 <= week_idx <= 4:
                key = f"Week {week_idx}"
                data[key]["dials"] += 1
                outcome = normalize_outcome(r.get("outcome", ""))
                if r.get("connected"): data[key]["connects"] += 1
                if outcome in APPOINTMENT_OUTCOMES: data[key]["booked"] += 1
                if outcome == "voicemail": data[key]["voicemails"] += 1
                if not r.get("connected") and outcome != "voicemail": data[key]["unanswered"] += 1
    return [{"label": k, **v} for k, v in data.items()]

def aggregate_historical(rows: List[Dict]):
    """Group by date for all time since March 13."""
    counts = defaultdict(lambda: {"dials": 0, "connects": 0, "booked": 0, "voicemails": 0, "unanswered": 0})
    for r in rows:
        d = r.get("logged_date")
        if not d: continue
        counts[d]["dials"] += 1
        outcome = normalize_outcome(r.get("outcome", ""))
        if r.get("connected"): counts[d]["connects"] += 1
        if outcome in APPOINTMENT_OUTCOMES: counts[d]["booked"] += 1
        if outcome == "voicemail": counts[d]["voicemails"] += 1
        if not r.get("connected") and outcome != "voicemail": counts[d]["unanswered"] += 1
    
    sorted_days = sorted(counts.keys())
    return [{"label": d[5:], **counts[d]} for d in sorted_days]
