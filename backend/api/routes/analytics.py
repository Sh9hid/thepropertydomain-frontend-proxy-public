import datetime
from collections import defaultdict
import logging
import html
import asyncio
import hmac
import hashlib
import json
import os
import re
import smtplib
from base64 import b64encode
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx
from fastapi import APIRouter, Depends, HTTPException, Security, Request, BackgroundTasks, File, UploadFile, Form
from pydantic import BaseModel
from sqlalchemy import text, bindparam
from sqlalchemy.ext.asyncio import AsyncSession

from api.routes._deps import APIKeyDep, SessionDep
from core.config import (
    API_KEY, api_key_header, APP_TITLE, SYDNEY_TZ, STOCK_ROOT, 
    PROJECT_ROOT, PROJECT_LOG_PATH, BRAND_NAME, BRAND_AREA, BRAND_LOGO_URL, 
    PRINCIPAL_NAME, PRINCIPAL_EMAIL, PRINCIPAL_PHONE, PROJECT_MEMORY_RULE, 
    BACKGROUND_SEND_POLL_SECONDS, PRIMARY_STRIKE_SUBURB, SECONDARY_STRIKE_SUBURBS,
    USE_POSTGRES
)
from core.utils import (
    now_sydney, now_iso, format_sydney, parse_client_datetime, 
    _first_non_empty, _safe_int, _format_moneyish, _parse_json_list, 
    _encode_value, _decode_row, _dedupe_text_list, _normalize_phone, 
    _dedupe_by_phone, _parse_iso_datetime, _parse_calendar_date, 
    _month_range_from_date, _bool_db
)
from services.scoring import _trigger_bonus, _status_penalty, _score_lead
from models.schemas import *
from core.logic import *

from core.database import get_session
from services.automations import _schedule_task, _refresh_lead_next_action
from services.signal_engine import compute_live_signals
try:
    from services.lead_hygiene import apply_precall_hygiene
except Exception:  # pragma: no cover - boot safety fallback
    async def apply_precall_hygiene(*args, **kwargs):
        return {"processed": 0, "updated": 0, "errors": 0, "status": "fallback"}
from core.security import get_api_key

router = APIRouter()
_log = logging.getLogger("api.analytics")

# ── 30-second in-memory cache for heavy analytics endpoints ──────────────────
import time as _time
_cache: dict = {}  # key → (timestamp, result)
_CACHE_TTL = 30  # seconds

def _cache_get(key: str):
    entry = _cache.get(key)
    if entry and (_time.monotonic() - entry[0]) < _CACHE_TTL:
        return entry[1]
    return None

def _cache_set(key: str, value):
    _cache[key] = (_time.monotonic(), value)

# ── Metric Constants ──
_CALL_SESSION_GAP_SECONDS = 15 * 60
_BOOKED_CALL_OUTCOMES = {"booked_appraisal", "booked_mortgage", "booked_meeting"}

def _parse_call_timestamp(ts_str: Optional[str]) -> Optional[datetime.datetime]:
    if not ts_str: return None
    try:
        # standardise to naive for subtraction safety in session gap logic
        dt = datetime.datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        return dt.replace(tzinfo=None)
    except:
        return None

def _call_end_timestamp(start: Optional[datetime.datetime], duration: int) -> Optional[datetime.datetime]:
    if not start: return None
    # ensure naive for subtraction safety
    res = start + datetime.timedelta(seconds=max(int(duration or 0), 0))
    return res.replace(tzinfo=None)

def _safe_json_loads_local(val: Any, default: Any = None) -> Any:
    if not val: return default
    if isinstance(val, (dict, list)): return val
    try: return json.loads(val)
    except: return default


def _recording_source_url_local(row: Dict[str, Any], raw_payload: Dict[str, Any]) -> str:
    return str(
        row.get("recording_url")
        or raw_payload.get("file_url")
        or raw_payload.get("download_url")
        or ""
    ).strip()

def _build_minimal_call_understanding(
    *,
    call_id: str,
    analysis_by_call: Dict[str, Dict[str, Any]],
    transcript_by_call: Dict[str, Dict[str, Any]],
    objections_by_call: Dict[str, List[Dict[str, Any]]],
    filler_count_by_call: Dict[str, int],
    pause_signals_by_call: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    analysis_row = analysis_by_call.get(call_id, {})
    transcript_row = transcript_by_call.get(call_id, {})
    raw_payload = _safe_json_loads_local(analysis_row.get("raw_payload"), {})
    sales_analysis = {}
    if isinstance(raw_payload, dict):
        sales_analysis = raw_payload.get("sales_analysis") or {}
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

    return {
        "summary": summary,
        "transcript": str(transcript_row.get("transcript_text") or "").strip(),
        "structured_summary": {
            "summary": summary,
            "outcome": outcome,
            "next_step": next_step,
        },
        "objections": objections,
        "booking_attempted": bool(sales_analysis.get("booking_attempted")) or outcome in _BOOKED_CALL_OUTCOMES,
        "next_step_detected": bool(sales_analysis.get("next_step_defined")) or bool(next_step),
        "filler_count": int(filler_count_by_call.get(call_id) or 0),
        "pause_signals": pause_signals_by_call.get(call_id, []),
    }

@router.get("/api/call-today")
async def get_call_today(limit: int = 25, api_key: APIKeyDep = "", session: SessionDep = None):
    from services.call_brief_service import get_todays_call_list
    return await get_todays_call_list(session, limit=limit)

@router.get("/api/operator/daily-brief")
async def get_daily_brief(api_key: APIKeyDep = "", session: SessionDep = None):
    from services.call_brief_service import get_operator_brief_text
    from core.config import OWNIT1ST_OPERATOR_NAME
    brief = await get_operator_brief_text(session)
    return {"brief": brief, "operator_name": OWNIT1ST_OPERATOR_NAME}


@router.get("/api/operator/mortgage-brief")
async def get_mortgage_brief(api_key: APIKeyDep = "", session: SessionDep = None):
    from services.call_brief_service import get_mortgage_market_brief_text
    return {"brief": await get_mortgage_market_brief_text(session)}

@router.get("/api/analytics/historical")
async def get_historical_report(
    timeframe: str = "weekly", # daily, weekly, monthly, historical
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session)
):
    from services.metrics_service import get_historical_call_data, aggregate_by_timeframe, aggregate_monthly, aggregate_historical
    # Default to 30 days of history for the 'historical' view if we wanted, 
    # but aggregate_by_timeframe uses all rows returned.
    start_date = (now_sydney() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    rows = await get_historical_call_data(session, start_date=start_date)
    if timeframe == 'monthly':
        chart_data = aggregate_monthly(rows)
    elif timeframe == 'historical':
        chart_data = aggregate_historical(rows)
    else:
        chart_data = aggregate_by_timeframe(rows, timeframe)
    return {
        "timeframe": timeframe,
        "chart_data": chart_data,
        "total_rows": len(rows),
        "start_date": start_date
    }

@router.get("/api/analytics/calls")
async def get_call_analytics(
    date: Optional[str] = None,
    days: int = 14,
    sync_zoom: bool = False,
    background_tasks: BackgroundTasks = None,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """Daily call log summary. date=YYYY-MM-DD. days max 30."""
    from services.zoom_call_sync_service import sync_zoom_calls_for_date

    sydney_now = datetime.datetime.now(ZoneInfo("Australia/Sydney"))
    sydney_today = sydney_now.strftime("%Y-%m-%d")
    target_date = date if date else sydney_today
    day_count = max(1, min(days, 30))

    if sync_zoom and background_tasks:
        background_tasks.add_task(sync_zoom_calls_for_date, session, target_date, force=True)

    # 1. 14-day history
    cutoff_date = (sydney_now - datetime.timedelta(days=day_count)).strftime("%Y-%m-%d")
    res_hist = await session.execute(text("""
        SELECT logged_date as date, COUNT(*) as total_dialed, SUM(connected) as connected
        FROM call_log
        WHERE logged_date >= :cutoff
        GROUP BY logged_date
        ORDER BY logged_date ASC
    """), {"cutoff": cutoff_date})
    history = [dict(r) for r in res_hist.mappings().all()]

    # 2. Target Date Details
    res = await session.execute(
        text("SELECT * FROM call_log WHERE logged_date = :d ORDER BY logged_at ASC"),
        {"d": target_date},
    )
    rows = [dict(r) for r in res.mappings().all()]

    call_ids = [str(r.get("provider_call_id") or r.get("id") or "") for r in rows if str(r.get("provider_call_id") or r.get("id") or "").strip()]
    analysis_by_call: Dict[str, Dict[str, Any]] = {}
    transcript_by_call: Dict[str, Dict[str, Any]] = {}
    objections_by_call: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    filler_count_by_call: Dict[str, int] = {}
    pause_signals_by_call: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    score_summary_by_call: Dict[str, Dict[str, Any]] = {}

    if call_ids:
        a_res = await session.execute(text("SELECT * FROM call_analysis WHERE call_id IN :call_ids").bindparams(bindparam("call_ids", expanding=True)), {"call_ids": call_ids})
        for r in a_res.mappings().all(): analysis_by_call[str(r["call_id"])] = dict(r)
        
        t_res = await session.execute(text("SELECT * FROM transcripts WHERE call_id IN :call_ids").bindparams(bindparam("call_ids", expanding=True)), {"call_ids": call_ids})
        for r in t_res.mappings().all(): transcript_by_call[str(r["call_id"])] = dict(r)

    speech_call_by_call_id: Dict[str, Dict[str, Any]] = {}
    if call_ids:
        c_res = await session.execute(
            text(
                """
                SELECT id, external_call_id, audio_uri, audio_storage_status, analysis_status, transcript_status, source
                FROM calls
                WHERE id IN :call_ids OR external_call_id IN :call_ids
                """
            ).bindparams(bindparam("call_ids", expanding=True)),
            {"call_ids": call_ids},
        )
        for row in c_res.mappings().all():
            payload = dict(row)
            row_id = str(payload.get("id") or "")
            ext_id = str(payload.get("external_call_id") or "")
            if row_id:
                speech_call_by_call_id[row_id] = payload
            if ext_id:
                speech_call_by_call_id[ext_id] = payload

    snapshot_ids: List[str] = []
    snapshot_by_key: Dict[str, Dict[str, Any]] = {}
    components_by_snapshot: Dict[str, Dict[str, float]] = defaultdict(dict)
    if call_ids:
        snapshot_res = await session.execute(
            text(
                """
                SELECT *
                FROM score_snapshots
                WHERE entity_type = 'call'
                  AND (entity_id IN :call_ids OR call_id IN :call_ids)
                ORDER BY computed_at DESC, created_at DESC
                """
            ).bindparams(bindparam("call_ids", expanding=True)),
            {"call_ids": call_ids},
        )
        for row in snapshot_res.mappings().all():
            payload = dict(row)
            snapshot_id = str(payload.get("id") or "")
            entity_id = str(payload.get("entity_id") or "")
            snapshot_call_id = str(payload.get("call_id") or "")
            if snapshot_id:
                snapshot_ids.append(snapshot_id)
            if entity_id and entity_id not in snapshot_by_key:
                snapshot_by_key[entity_id] = payload
            if snapshot_call_id and snapshot_call_id not in snapshot_by_key:
                snapshot_by_key[snapshot_call_id] = payload

    if snapshot_ids:
        component_res = await session.execute(
            text(
                """
                SELECT snapshot_id, score_name, score_value
                FROM score_components
                WHERE snapshot_id IN :snapshot_ids
                """
            ).bindparams(bindparam("snapshot_ids", expanding=True)),
            {"snapshot_ids": snapshot_ids},
        )
        for row in component_res.mappings().all():
            components_by_snapshot[str(row["snapshot_id"])][str(row["score_name"])] = float(row.get("score_value") or 0.0)

    for call_key, snapshot in snapshot_by_key.items():
        snapshot_id = str(snapshot.get("id") or "")
        component_scores = components_by_snapshot.get(snapshot_id, {})
        score_summary_by_call[call_key] = {
            "composite_score": float(snapshot.get("composite_score") or 0.0),
            "scoring_version": str(snapshot.get("scoring_version") or ""),
            "fluency_score": float(component_scores.get("fluency_score") or 0.0),
            "confidence_score": float(component_scores.get("confidence_score") or 0.0),
            "sales_control_score": float(component_scores.get("sales_control_score") or 0.0),
            "booking_closing_score": float(component_scores.get("booking_closing_score") or 0.0),
        }

    from services.metrics_service import (
        build_minimal_call_understanding_payloads,
        outcome_implies_connected,
        normalize_outcome,
    )

    total_dialed = len(rows)
    connected_count = 0
    conversation_count = 0
    outcomes: dict = {}
    total_duration = 0
    voicemail_count = 0
    call_understanding_by_call: Dict[str, Dict[str, Any]] = {}

    if call_ids:
        call_understanding_by_call = await build_minimal_call_understanding_payloads(session, call_ids)

    for r in rows:
        outcome = r.get("outcome", "")
        norm_o = normalize_outcome(outcome)
        is_conn = bool(r.get("connected")) or outcome_implies_connected(outcome)
        duration = int(r.get("duration_seconds") or 0)

        if is_conn:
            connected_count += 1
            if duration >= 10:
                conversation_count += 1

        outcomes[norm_o] = outcomes.get(norm_o, 0) + 1
        total_duration += duration
        if norm_o == "voicemail":
            voicemail_count += 1

    connection_rate = round(connected_count / total_dialed, 2) if total_dialed else 0.0
    avg_duration = round(total_duration / total_dialed) if total_dialed else 0
    appointments_booked_count = sum(1 for r in rows if str(r.get("outcome") or "") in _BOOKED_CALL_OUTCOMES)

    calls_out: List[Dict[str, Any]] = []
    sessions: List[Dict[str, Any]] = []
    current_session: Optional[Dict[str, Any]] = None
    first_start_at: Optional[datetime.datetime] = None
    last_end_at: Optional[datetime.datetime] = None
    previous_end_at: Optional[datetime.datetime] = None

    for row in rows:
        call_id = str(row.get("provider_call_id") or row.get("id") or "")
        raw_payload = _safe_json_loads_local(row.get("raw_payload"), {}) or {}
        remote_recording_url = _recording_source_url_local(row, raw_payload)
        logged_at = str(row.get("logged_at") or "")
        duration_seconds = int(row.get("duration_seconds") or 0)
        started_at = _parse_call_timestamp(logged_at)
        ended_at = _call_end_timestamp(started_at, duration_seconds)
        if first_start_at is None and started_at is not None: first_start_at = started_at
        if ended_at is not None: last_end_at = ended_at

        gap_from_previous_seconds = 0
        if started_at is not None and previous_end_at is not None:
            try:
                gap_from_previous_seconds = max(int((started_at - previous_end_at).total_seconds()), 0)
            except TypeError:
                # safety fallback for mixed naive/aware
                s_naive = started_at.replace(tzinfo=None)
                p_naive = previous_end_at.replace(tzinfo=None)
                gap_from_previous_seconds = max(int((s_naive - p_naive).total_seconds()), 0)

        if current_session is None or gap_from_previous_seconds > _CALL_SESSION_GAP_SECONDS:
            current_session = {
                "session_id": f"session-{len(sessions) + 1}",
                "started_at": logged_at,
                "ended_at": logged_at,
                "call_count": 0,
                "connect_count": 0,
                "total_talk_time": 0,
                "calls": [],
            }
            sessions.append(current_session)

        call_payload = {
            "id": row.get("id"),
            "call_id": call_id,
            "lead_id": row.get("lead_id", ""),
            "address": row.get("lead_address", ""),
            "provider": row.get("provider", ""),
            "to_number": row.get("to_number", ""),
            "outcome": row.get("outcome", ""),
            "connected": bool(row.get("connected")),
            "duration_seconds": duration_seconds,
            "talk_time_seconds": duration_seconds,
            "logged_at": logged_at,
            "session_id": current_session["session_id"],
            "has_analysis": call_id in analysis_by_call,
            "has_transcript": call_id in transcript_by_call,
            "analysis_status": str((speech_call_by_call_id.get(call_id) or {}).get("analysis_status") or ""),
            "transcript_status": str((speech_call_by_call_id.get(call_id) or {}).get("transcript_status") or ""),
            "recording_duration_seconds": (
                int(row.get("recording_duration_seconds"))
                if row.get("recording_duration_seconds") not in (None, "")
                else None
            ),
            "zoom_source_endpoint": str(raw_payload.get("zoom_source_endpoint") or ""),
            "zoom_object_type": str(raw_payload.get("zoom_object_type") or ""),
            "zoom_call_log_id": str(raw_payload.get("zoom_call_log_id") or ""),
            "zoom_call_history_id": str(raw_payload.get("zoom_call_history_id") or ""),
            "zoom_owner_name": str(raw_payload.get("zoom_owner_name") or ""),
            "zoom_owner_extension": str(raw_payload.get("zoom_owner_extension") or ""),
            "zoom_ai_call_summary_id": str(raw_payload.get("ai_call_summary_id") or ""),
            "zoom_recording_download_url": str(raw_payload.get("download_url") or raw_payload.get("file_url") or ""),
            "zoom_recording_present": bool(raw_payload.get("has_recording") or remote_recording_url),
            "zoom_ai_summary_present": bool(raw_payload.get("has_ai_summary")),
            "zoom_transcript_present": bool(raw_payload.get("has_transcript")),
            "score_summary": score_summary_by_call.get(call_id),
            "file_url": (
                f"/api/recordings/{str((speech_call_by_call_id.get(call_id) or {}).get('id') or call_id)}/stream"
                if (
                    (
                        str((speech_call_by_call_id.get(call_id) or {}).get("audio_storage_status") or "") == "stored"
                        and str((speech_call_by_call_id.get(call_id) or {}).get("audio_uri") or "")
                    )
                    or remote_recording_url
                )
                else None
            ),
            "call_understanding": call_understanding_by_call.get(
                call_id,
                {
                    "summary": "",
                    "transcript": "",
                    "structured_summary": {"summary": "", "outcome": "", "next_step": ""},
                    "objections": [],
                    "booking_attempted": False,
                    "next_step_detected": False,
                    "filler_count": 0,
                    "pause_signals": [],
                },
            ),
        }
        call_payload["recording_url"] = call_payload["file_url"]
        call_payload["has_recording"] = bool(call_payload["recording_url"])
        calls_out.append(call_payload)
        current_session["calls"].append(call_payload)
        current_session["call_count"] += 1
        current_session["connect_count"] += 1 if bool(row.get("connected")) else 0
        current_session["total_talk_time"] += duration_seconds
        current_session["ended_at"] = (ended_at.isoformat() if ended_at else logged_at)
        previous_end_at = ended_at or started_at or previous_end_at

    total_session_time = 0
    if first_start_at is not None and last_end_at is not None:
        total_session_time = max(int((last_end_at - first_start_at).total_seconds()), 0)
    
    metrics = {
        "dial_count": total_dialed,
        "connect_count": connected_count,
        "conversation_count": conversation_count,
        "total_talk_time": total_duration,
        "active_time": total_duration,
        "appointments_booked_count": appointments_booked_count,
        "sessions_count": len(sessions),
        "idle_time": max(total_session_time - total_duration, 0),
    }

    return {
        "date": target_date,
        "metrics": metrics,
        "total_dialed": total_dialed,
        "connected": connected_count,
        "connection_rate": connection_rate,
        "outcomes": outcomes,
        "history": history,
        "calls": calls_out,
        "sessions": sessions
    }

@router.get("/api/metrics/daily")
async def get_metrics_daily(
    date: Optional[str] = None,
    lead_id: Optional[str] = None,
    user_id: Optional[str] = None,
    outcome: Optional[str] = None,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    from services.metrics_service import get_daily_metrics
    return await get_daily_metrics(session, date=date, lead_id=lead_id, user_id=user_id, outcome=outcome)

@router.get("/api/metrics/range")
async def get_metrics_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lead_id: Optional[str] = None,
    user_id: Optional[str] = None,
    outcome: Optional[str] = None,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    from services.metrics_service import get_range_metrics
    return await get_range_metrics(
        session,
        start_date=start_date,
        end_date=end_date,
        lead_id=lead_id,
        user_id=user_id,
        outcome=outcome,
    )

@router.get("/api/metrics/timeline")
async def get_metrics_timeline(
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lead_id: Optional[str] = None,
    user_id: Optional[str] = None,
    outcome: Optional[str] = None,
    session_id: Optional[str] = None,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    from services.metrics_service import get_timeline_metrics
    return await get_timeline_metrics(session, date=date, start_date=start_date, end_date=end_date, lead_id=lead_id, user_id=user_id, outcome=outcome, session_id=session_id)

@router.get("/api/metrics/sessions")
async def get_metrics_sessions(
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lead_id: Optional[str] = None,
    user_id: Optional[str] = None,
    outcome: Optional[str] = None,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    from services.metrics_service import get_sessions_metrics
    return await get_sessions_metrics(session, date=date, start_date=start_date, end_date=end_date, lead_id=lead_id, user_id=user_id, outcome=outcome)

@router.get("/api/pipeline")
async def get_pipeline(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    stage_counts = {status: 0 for status in LEAD_STATUS_ORDER}
    res = await session.execute(text("SELECT status, COUNT(*) as count FROM leads GROUP BY status"))
    for row in res.mappings().all():
        stage_counts[row["status"]] = row["count"]
    return {"funnel_order": LEAD_STATUS_ORDER, "stage_counts": stage_counts}

@router.get("/api/analytics")
async def get_analytics(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    cached = _cache_get("analytics")
    if cached is not None: return cached
    res_active = await session.execute(text("SELECT COUNT(*) as c, SUM(est_value) as s, AVG(call_today_score) as a FROM leads WHERE status != 'dropped'"))
    active_stats = res_active.mappings().first()
    res_withdrawn = await session.execute(text("SELECT COUNT(*) FROM leads WHERE status != 'dropped' AND (trigger_type LIKE '%withdrawn%' OR lead_archetype LIKE '%withdrawn%')"))
    res_delta = await session.execute(text("SELECT COUNT(*) FROM leads WHERE status != 'dropped' AND trigger_type IN ('delta_engine', 'reaxml', 'sitemap')"))
    res_cliff = await session.execute(text("SELECT COUNT(*) FROM leads WHERE status != 'dropped' AND lead_archetype = 'mortgage_cliff'"))
    recent_cutoff = (now_sydney() - datetime.timedelta(hours=24)).isoformat()
    res_fresh = await session.execute(
        text(
            """
            SELECT COUNT(*)
            FROM leads
            WHERE status != 'dropped'
              AND (
                created_at >= :cutoff
                OR (
                    updated_at >= :cutoff
                    AND lower(COALESCE(last_activity_type, '')) LIKE 'door_knock%'
                )
              )
            """
        ),
        {"cutoff": recent_cutoff},
    )
    res_latest = await session.execute(text("SELECT created_at FROM leads WHERE status != 'dropped' ORDER BY created_at DESC LIMIT 1"))
    latest_signal_at = res_latest.scalar_one_or_none()
    result = {
        "active_leads": active_stats["c"] or 0,
        "withdrawn_count": res_withdrawn.scalar_one(),
        "delta_count": res_delta.scalar_one(),
        "mortgage_cliff_count": res_cliff.scalar_one(),
        "fresh_count": res_fresh.scalar_one(),
        "latest_signal_at": latest_signal_at,
    }
    _cache_set("analytics", result)
    return result


@router.get("/api/analytics/events")
async def get_signal_events(
    hours: int = 168,
    limit: int = 40,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """Legacy endpoint used by ticker UI; maps live signals into event cards."""
    capped_limit = max(1, min(int(limit or 40), 200))
    try:
        live_signals = await compute_live_signals(session, limit=capped_limit)
    except Exception:
        # Keep the endpoint stable for dashboards even if signal computation fails.
        _log.exception("Failed to compute live signals for analytics events")
        return {"events": [], "total": 0, "since": now_sydney().isoformat(), "degraded": True}
    since = (now_sydney() - datetime.timedelta(hours=max(1, int(hours or 168)))).isoformat()
    events: List[Dict[str, Any]] = []
    for signal in live_signals:
        events.append(
            {
                "id": str(signal.get("id") or ""),
                "type": str(signal.get("type") or "SIGNAL"),
                "address": str(signal.get("address") or ""),
                "suburb": str(signal.get("suburb") or ""),
                "agency": str(signal.get("source") or ""),
                "heat_score": int(round(float(signal.get("score") or 0))),
                "signal_date": str(signal.get("detected_at") or ""),
                "color": str(signal.get("color") or "#30d158"),
                "icon": str(signal.get("icon") or "SIG"),
            }
        )
    return {"events": events, "total": len(events), "since": since}


# ── Accountability endpoint ──────────────────────────────────────────────────

def _accountability_date_range(period: str) -> tuple[datetime.date, datetime.date]:
    """Return (date_from, date_to) for the selected reporting period."""
    today = now_sydney().date()
    if period == "week":
        date_from = today - datetime.timedelta(days=today.weekday())
    elif period == "month":
        date_from = today.replace(day=1)
    else:  # today (default)
        date_from = today
    return date_from, today


@router.get("/api/analytics/accountability")
async def get_accountability(
    period: str = "today",
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """Team activity metrics. period: today | week | month."""
    date_from, date_to = _accountability_date_range(period)
    if isinstance(date_from, str):
        date_from = datetime.date.fromisoformat(date_from)
    if isinstance(date_to, str):
        date_to = datetime.date.fromisoformat(date_to)

    # ── call_log aggregates ──
    date_filter = "logged_date BETWEEN :date_from AND :date_to"
    appt_filter = "DATE(last_outcome_at) BETWEEN :date_from AND :date_to"
    overdue_expr = "NULLIF(follow_up_due_at, '')::timestamptz < NOW()"
    overdue_1d_expr = "NULLIF(follow_up_due_at, '')::timestamptz < NOW() - INTERVAL '1 day'"

    params = {"date_from": date_from, "date_to": date_to}

    totals_res = await session.execute(
        text(
            f"""
            SELECT
                COUNT(*) AS dials,
                SUM(CASE WHEN connected THEN 1 ELSE 0 END) AS connected_calls,
                COALESCE(SUM(duration_seconds), 0) AS total_talk_time_seconds,
                COALESCE(AVG(CASE WHEN connected THEN duration_seconds ELSE NULL END), 0) AS avg_talk_time_seconds,
                COUNT(DISTINCT lead_id) AS leads_touched
            FROM call_log
            WHERE {date_filter}
            """
        ),
        params,
    )
    t = dict(totals_res.mappings().first() or {})
    dials = int(t.get("dials") or 0)
    connected_calls = int(t.get("connected_calls") or 0)
    connection_rate = round(connected_calls / dials, 3) if dials else 0.0

    appts_res = await session.execute(
        text(
            f"""
            SELECT COUNT(*) FROM leads
            WHERE status IN ('appt_booked', 'mortgage_appt_booked')
              AND {appt_filter}
            """
        ),
        params,
    )
    appointments_booked = int(appts_res.scalar_one() or 0)

    fu_due_res = await session.execute(
        text(
            f"""
            SELECT COUNT(*) FROM leads
            WHERE follow_up_due_at IS NOT NULL
              AND follow_up_due_at != ''
              AND {overdue_expr}
              AND status NOT IN ('converted', 'dropped')
            """
        )
    )
    follow_ups_due = int(fu_due_res.scalar_one() or 0)

    fu_overdue_res = await session.execute(
        text(
            f"""
            SELECT COUNT(*) FROM leads
            WHERE follow_up_due_at IS NOT NULL
              AND follow_up_due_at != ''
              AND {overdue_1d_expr}
              AND status NOT IN ('converted', 'dropped')
            """
        )
    )
    follow_ups_overdue = int(fu_overdue_res.scalar_one() or 0)

    # ── per-operator breakdown ──
    op_res = await session.execute(
        text(
            f"""
            SELECT
                operator,
                COUNT(*) AS dials,
                SUM(CASE WHEN connected THEN 1 ELSE 0 END) AS connected_calls,
                COALESCE(SUM(duration_seconds), 0) AS total_talk_time_seconds,
                COALESCE(AVG(CASE WHEN connected THEN duration_seconds ELSE NULL END), 0) AS avg_talk_time_seconds,
                COUNT(DISTINCT lead_id) AS leads_touched
            FROM call_log
            WHERE {date_filter}
            GROUP BY operator
            """
        ),
        params,
    )
    by_operator = []
    for row in op_res.mappings().all():
        op_dials = int(row.get("dials") or 0)
        op_connected = int(row.get("connected_calls") or 0)
        by_operator.append(
            {
                "operator": str(row.get("operator") or "unknown"),
                "dials": op_dials,
                "connected_calls": op_connected,
                "connection_rate": round(op_connected / op_dials, 3) if op_dials else 0.0,
                "total_talk_time_seconds": int(row.get("total_talk_time_seconds") or 0),
                "avg_talk_time_seconds": round(float(row.get("avg_talk_time_seconds") or 0)),
                "leads_touched": int(row.get("leads_touched") or 0),
            }
        )

    return {
        "period": period,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "totals": {
            "dials": dials,
            "connected_calls": connected_calls,
            "connection_rate": connection_rate,
            "total_talk_time_seconds": int(t.get("total_talk_time_seconds") or 0),
            "avg_talk_time_seconds": round(float(t.get("avg_talk_time_seconds") or 0)),
            "leads_touched": int(t.get("leads_touched") or 0),
            "appointments_booked": appointments_booked,
            "follow_ups_due": follow_ups_due,
            "follow_ups_overdue": follow_ups_overdue,
        },
        "by_operator": by_operator,
    }


# ── Call Queue endpoint ──────────────────────────────────────────────────────

@router.get("/api/analytics/call-queue")
async def get_call_queue(
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """Prioritized list of leads to call now. Deterministic ranking — no AI."""
    await apply_precall_hygiene(session, limit=1200)
    now_expr = "NOW()"
    now_plus_24_expr = "NOW() + INTERVAL '24 hours'"

    res = await session.execute(
        text(
            f"""
            SELECT
                l.id AS lead_id,
                l.address,
                l.suburb,
                l.owner_name,
                l.status,
                l.heat_score,
                l.call_today_score,
                l.follow_up_due_at,
                l.next_action_at,
                l.last_outcome,
                l.last_outcome_at,
                l.do_not_contact_until,
                l.contact_phones,
                cl.last_call_at,
                cl.last_call_outcome,
                cl.total_calls
            FROM leads l
            LEFT JOIN (
                SELECT
                    lead_id,
                    MAX(logged_at) AS last_call_at,
                    COUNT(*) AS total_calls,
                    (SELECT outcome FROM call_log ci
                     WHERE ci.lead_id = co.lead_id
                     ORDER BY ci.logged_at DESC LIMIT 1) AS last_call_outcome
                FROM call_log co
                GROUP BY lead_id
            ) cl ON cl.lead_id = l.id
            WHERE l.status NOT IN ('converted', 'dropped')
              AND (l.do_not_contact_until IS NULL OR l.do_not_contact_until < {now_expr})
            """
        )
    )
    rows = [dict(r) for r in res.mappings().all()]

    import json as _json

    now_dt = now_sydney()
    entries = []

    for row in rows:
        follow_up_due_at_str = row.get("follow_up_due_at")
        last_call_at_str = row.get("last_call_at")
        heat_score = int(row.get("heat_score") or 0)
        call_today_score = int(row.get("call_today_score") or 0)
        total_calls = int(row.get("total_calls") or 0)

        # Days since last call
        days_since_last_call = 0
        if last_call_at_str:
            try:
                lc = datetime.fromisoformat(str(last_call_at_str).replace("Z", "+00:00"))
                if lc.tzinfo is None:
                    lc = lc.replace(tzinfo=now_dt.tzinfo)
                days_since_last_call = max(0, (now_dt - lc).days)
            except Exception:
                days_since_last_call = 0

        # Follow-up status
        is_overdue = False
        is_due_today = False
        if follow_up_due_at_str:
            try:
                fu = datetime.fromisoformat(str(follow_up_due_at_str).replace("Z", "+00:00"))
                if fu.tzinfo is None:
                    fu = fu.replace(tzinfo=now_dt.tzinfo)
                if fu < now_dt:
                    is_overdue = True
                elif fu <= now_dt + datetime.timedelta(hours=24):
                    is_due_today = True
            except Exception:
                pass

        # Compute rank score
        rank_score = 0
        if is_overdue:
            rank_score += 100
        elif is_due_today:
            rank_score += 50
        no_call_days_beyond_3 = max(0, days_since_last_call - 3)
        rank_score += min(no_call_days_beyond_3 * 10, 50)
        rank_score += min(heat_score // 2, 50)
        rank_score += min(call_today_score // 2, 50)

        # Urgency label
        if is_overdue:
            urgency = "overdue"
        elif is_due_today:
            urgency = "due"
        elif days_since_last_call <= 3:
            urgency = "upcoming"
        else:
            urgency = "cold"

        # Reasons
        reasons = []
        if is_overdue and follow_up_due_at_str:
            try:
                fu = datetime.fromisoformat(str(follow_up_due_at_str).replace("Z", "+00:00"))
                if fu.tzinfo is None:
                    fu = fu.replace(tzinfo=now_dt.tzinfo)
                overdue_days = max(1, (now_dt - fu).days)
                reasons.append(f"Follow-up overdue by {overdue_days} day{'s' if overdue_days != 1 else ''}")
            except Exception:
                reasons.append("Follow-up overdue")
        elif is_due_today:
            reasons.append("Follow-up due today")
        if days_since_last_call > 3:
            reasons.append(f"No call in {days_since_last_call} days")
        if heat_score > 70:
            reasons.append(f"High heat score ({heat_score})")
        last_call_outcome = str(row.get("last_call_outcome") or "")
        next_action_at = row.get("next_action_at")
        if last_call_outcome == "connected" and not next_action_at:
            reasons.append("Connected call — action pending")

        # Parse phones
        raw_phones = row.get("contact_phones") or "[]"
        if isinstance(raw_phones, list):
            phones = raw_phones
        else:
            try:
                phones = _json.loads(raw_phones)
            except Exception:
                phones = []

        entries.append(
            {
                "lead_id": str(row.get("lead_id") or ""),
                "address": str(row.get("address") or ""),
                "suburb": row.get("suburb"),
                "owner_name": row.get("owner_name"),
                "urgency": urgency,
                "reasons": reasons,
                "heat_score": heat_score,
                "call_today_score": call_today_score,
                "last_call_at": last_call_at_str,
                "last_call_outcome": last_call_outcome or None,
                "next_action_at": next_action_at,
                "days_since_last_call": days_since_last_call,
                "total_calls": total_calls,
                "contact_phones": phones if isinstance(phones, list) else [],
                "follow_up_due_at": follow_up_due_at_str,
                "status": str(row.get("status") or ""),
                "_rank_score": rank_score,
            }
        )

    entries.sort(key=lambda x: -x["_rank_score"])
    for idx, entry in enumerate(entries[:50], start=1):
        entry["rank"] = idx
        del entry["_rank_score"]

    return entries[:50]


# ── Suburb Velocity endpoint ─────────────────────────────────────────────────

@router.get("/api/analytics/suburb-velocity")
async def get_suburb_velocity(
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """30/90 day sold event counts and median prices per suburb."""
    cutoff_30 = "(timezone('Australia/Sydney', NOW()) - INTERVAL '30 days')::date"
    cutoff_90 = "(timezone('Australia/Sydney', NOW()) - INTERVAL '90 days')::date"

    res = await session.execute(
        text(
            f"""
            SELECT
                suburb,
                SUM(CASE WHEN sale_date >= {cutoff_30} THEN 1 ELSE 0 END) AS sales_30d,
                SUM(CASE WHEN sale_date >= {cutoff_90} THEN 1 ELSE 0 END) AS sales_90d,
                MAX(CASE WHEN sale_date >= {cutoff_30} THEN sale_date ELSE NULL END) AS latest_sale_date,
                COUNT(*) AS total_sales
            FROM sold_events
            WHERE suburb IS NOT NULL
            GROUP BY suburb
            ORDER BY sales_30d DESC
            """
        )
    )
    rows = [dict(r) for r in res.mappings().all()]

    # Median price per suburb (30d) — fetch prices separately to avoid complex SQL
    price_res = await session.execute(
        text(
            f"""
            SELECT suburb, sale_price
            FROM sold_events
            WHERE suburb IS NOT NULL
              AND sale_date >= {cutoff_30}
              AND sale_price IS NOT NULL
              AND sale_price != ''
            """
        )
    )
    price_rows = price_res.mappings().all()

    # Build suburb → prices map
    suburb_prices: Dict[str, List[float]] = {}
    for pr in price_rows:
        sub = str(pr.get("suburb") or "")
        raw_price = pr.get("sale_price")
        try:
            price_val = float(str(raw_price).replace(",", "").replace("$", ""))
            if price_val > 0:
                suburb_prices.setdefault(sub, []).append(price_val)
        except Exception:
            pass

    def _median(vals: List[float]) -> Optional[int]:
        if not vals:
            return None
        sorted_vals = sorted(vals)
        n = len(sorted_vals)
        mid = n // 2
        if n % 2 == 0:
            return int((sorted_vals[mid - 1] + sorted_vals[mid]) / 2)
        return int(sorted_vals[mid])

    suburbs_out = []
    for row in rows:
        sub = str(row.get("suburb") or "")
        suburbs_out.append(
            {
                "suburb": sub,
                "sales_30d": int(row.get("sales_30d") or 0),
                "sales_90d": int(row.get("sales_90d") or 0),
                "median_price_30d": _median(suburb_prices.get(sub, [])),
                "latest_sale_date": row.get("latest_sale_date"),
            }
        )

    return {
        "suburbs": suburbs_out,
        "generated_at": now_sydney().isoformat(),
    }
