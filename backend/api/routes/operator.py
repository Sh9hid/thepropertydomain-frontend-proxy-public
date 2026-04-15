import datetime
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

import httpx
from fastapi import APIRouter, Depends, HTTPException, Security, Request, BackgroundTasks, File, UploadFile, Form
from pydantic import BaseModel
from zoneinfo import ZoneInfo
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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
from runtime.loops import get_loop_heartbeats
from core.security import get_api_key

router = APIRouter()


class CommandRequest(BaseModel):
    command: str


@router.post("/api/command")
async def execute_command(
    body: CommandRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    from services.call_runtime_service import choose_next_lead, record_runtime_event

    command = str(body.command or "").strip().upper()
    if command != "NEXT_LEAD":
        raise HTTPException(status_code=400, detail="Unsupported command")

    payload = await choose_next_lead(session)
    record_runtime_event("command_executed", command=command, found=bool(payload))
    if not payload:
        return {"command": command, "lead": None, "context": {}}
    return {"command": command, **payload}


@router.get("/api/debug/recent-events")
async def get_recent_debug_events(
    limit: int = 50,
    api_key: str = Depends(get_api_key),
):
    from services.call_runtime_service import get_recent_events

    return {"events": get_recent_events(limit=limit)}

def _operator_counts(tasks: List[Dict[str, Any]], appointments: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "total_tasks": len(tasks),
        "calls": 0,
        "sms": 0,
        "emails": 0,
        "callbacks": 0,
        "manual": 0,
        "pending_approvals": 0,
        "queued_sends": 0,
        "failed_sends": 0,
        "appointments": len(appointments),
    }
    for task in tasks:
        channel = task.get("channel") or task.get("task_type") or "manual"
        bucket = task.get("priority_bucket") or ""
        approval_status = task.get("approval_status") or "not_required"
        if channel == "call":
            counts["calls"] += 1
        elif channel == "sms":
            counts["sms"] += 1
        elif channel == "email":
            counts["emails"] += 1
        else:
            counts["manual"] += 1
        if bucket == "callback_due":
            counts["callbacks"] += 1
        if approval_status == "pending":
            counts["pending_approvals"] += 1
        elif approval_status == "approved":
            counts["queued_sends"] += 1
        elif approval_status == "failed":
            counts["failed_sends"] += 1
    return counts


async def _get_operator_today_payload(session: AsyncSession) -> Dict[str, Any]:
    now = now_sydney()
    start = now.replace(hour=0, minute=0, second=0)
    end = start + datetime.timedelta(days=1)

    res_tasks = await session.execute(
        text("""
        SELECT tasks.*, leads.address, leads.owner_name, leads.trigger_type, leads.status AS lead_status,
               leads.contact_emails, leads.contact_phones, leads.next_action_reason, leads.last_outcome,
               leads.queue_bucket, leads.lead_archetype
        FROM tasks
        LEFT JOIN leads ON leads.id = tasks.lead_id
        WHERE tasks.status = 'pending'
          AND COALESCE(tasks.superseded_by, '') = ''
          AND tasks.due_at < :end
        ORDER BY tasks.due_at ASC, tasks.created_at ASC
        """),
        {"end": end.isoformat()}
    )
    task_rows = res_tasks.mappings().all()

    res_appts = await session.execute(
        text("""
        SELECT appointments.*, leads.address, leads.owner_name
        FROM appointments
        LEFT JOIN leads ON leads.id = appointments.lead_id
        WHERE appointments.status NOT IN ('cancelled', 'completed')
          AND appointments.starts_at >= :start AND appointments.starts_at < :end
        ORDER BY appointments.starts_at ASC
        """),
        {"start": start.isoformat(), "end": end.isoformat()}
    )
    appointment_rows = res_appts.mappings().all()

    overdue: List[Dict[str, Any]] = []
    due_today: List[Dict[str, Any]] = []
    for row in task_rows:
        task = _operator_task_payload(row)
        due_at = _parse_iso_datetime(task.get("due_at"))
        if due_at and due_at < start:
            overdue.append(task)
        else:
            due_today.append(task)
    appointments = [_appointment_to_dict(row) for row in appointment_rows]
    return {
        "date": start.date().isoformat(),
        "overdue": overdue,
        "due_today": due_today,
        "appointments": appointments,
        "counts": {
            "overdue": len(overdue),
            "due_today": len(due_today),
            "appointments": len(appointments),
            **_operator_counts(due_today, appointments),
        },
    }


def _command_center_focus(
    *,
    overdue_count: int,
    missed_deals_count: int,
    queue_count: int,
    appointments_count: int,
    bookings_today: int,
) -> Dict[str, str]:
    if overdue_count > 0:
        return {
            "headline": f"{overdue_count} overdue actions are leaking throughput.",
            "subheadline": "Clear the overdue lane first so callbacks, follow-ups, and send approvals stop compounding drag.",
        }
    if missed_deals_count > 0:
        return {
            "headline": f"{missed_deals_count} neglected opportunities need immediate recovery.",
            "subheadline": "Prioritize warm conversations and high-intent leads that stalled without a booked next step.",
        }
    if queue_count > 0:
        return {
            "headline": f"{queue_count} leads are ranked and ready to call.",
            "subheadline": "Use the queue as the live execution runway and keep session momentum high.",
        }
    if appointments_count > 0 or bookings_today > 0:
        return {
            "headline": "Protect booked revenue and tighten follow-through.",
            "subheadline": "Execution today is about keeping appointments clean, prepared, and conversion-ready.",
        }
    return {
        "headline": "No immediate fires. Build fresh pipeline now.",
        "subheadline": "Use the command center to move from idle state into ranked outreach and deterministic activity.",
    }

@router.get("/api/operator/today")
async def get_operator_today(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    return await _get_operator_today_payload(session)


@router.get("/api/operator/command-center")
async def get_operator_command_center(
    queue_limit: int = 8,
    missed_limit: int = 6,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    from api.routes.analytics import get_accountability, get_call_queue
    from services.call_brief_service import get_operator_brief_text
    from services.call_runtime_service import choose_next_lead
    from core.config import OWNIT1ST_OPERATOR_NAME
    from services.hermes_lead_ops_service import get_hermes_rep_brief, get_hermes_today
    from services.metrics_service import get_daily_metrics
    from services.missed_deals_service import get_missed_deals, summarize_missed_deals

    safe_queue_limit = max(1, min(int(queue_limit or 8), 20))
    safe_missed_limit = max(1, min(int(missed_limit or 6), 20))

    today_payload = await _get_operator_today_payload(session)
    metrics = await get_daily_metrics(
        session,
        date=today_payload["date"],
        lead_id=None,
        user_id=None,
        outcome=None,
        session_id=None,
    )
    accountability = await get_accountability(period="today", api_key=api_key, session=session)
    call_queue = await get_call_queue(api_key=api_key, session=session)
    missed_cards = await get_missed_deals(session, date_range="today")
    missed_summary = summarize_missed_deals(missed_cards)
    next_lead = await choose_next_lead(session)
    brief = await get_operator_brief_text(session)
    hermes_today = await get_hermes_today(session, business_context_key="real_estate", limit=safe_queue_limit, auto_refresh=True)
    hermes_rep_brief = await get_hermes_rep_brief(
        session,
        OWNIT1ST_OPERATOR_NAME or "Shahid",
        business_context="real_estate",
        target_date=today_payload["date"],
    )
    runtime_warnings: List[str] = []
    runtime_role = (os.getenv("RUNTIME_ROLE") or "web").strip().lower()
    if runtime_role == "web":
        runtime_warnings.append("This process is web-only; scheduler and worker roles must run in dedicated processes for automation.")
    heartbeats = get_loop_heartbeats()
    if runtime_role == "scheduler" and not heartbeats.get("followup_scheduler"):
        runtime_warnings.append("Follow-up scheduler heartbeat missing.")
    if runtime_role == "worker" and not heartbeats.get("followup_worker"):
        runtime_warnings.append("Follow-up worker heartbeat missing.")

    focus = _command_center_focus(
        overdue_count=len(today_payload["overdue"]),
        missed_deals_count=missed_summary["total_missed_deals"],
        queue_count=len(call_queue),
        appointments_count=len(today_payload["appointments"]),
        bookings_today=int(metrics.get("appointments_booked_count") or 0),
    )

    return {
        "date": today_payload["date"],
        "generated_at": now_iso(),
        "brief": brief,
        "focus": focus,
        "summary": {
            "dial_count": int(metrics.get("dial_count") or 0),
            "connect_count": int(metrics.get("connect_count") or 0),
            "conversation_count": int(metrics.get("conversation_count") or 0),
            "talk_time_seconds": int(metrics.get("total_talk_time_seconds") or 0),
            "bookings_today": int(metrics.get("appointments_booked_count") or 0),
            "overdue_tasks": len(today_payload["overdue"]),
            "due_today_tasks": len(today_payload["due_today"]),
            "appointments_today": len(today_payload["appointments"]),
            "call_queue_count": len(call_queue),
            "missed_deals_count": missed_summary["total_missed_deals"],
        },
        "metrics": metrics,
        "accountability": accountability,
        "operator_day": today_payload,
        "next_lead": next_lead,
        "call_queue": call_queue[:safe_queue_limit],
        "hermes_today": hermes_today,
        "hermes_rep_brief": hermes_rep_brief,
        "runtime": {
            "role": runtime_role,
            "warnings": runtime_warnings,
            "heartbeats": heartbeats,
        },
        "missed_deals": {
            "summary": missed_summary,
            "cards": missed_cards[:safe_missed_limit],
        },
    }


@router.get("/api/operator/approvals")
async def get_operator_approvals(days: int = 21, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    horizon_days = min(max(days, 1), 90)
    start = now_sydney()
    end = start + datetime.timedelta(days=horizon_days)
    res = await session.execute(
        text("""
        SELECT tasks.*, leads.address, leads.owner_name, leads.trigger_type, leads.status AS lead_status,
               leads.contact_emails, leads.contact_phones, leads.next_action_reason, leads.last_outcome,
               leads.queue_bucket, leads.lead_archetype
        FROM tasks
        LEFT JOIN leads ON leads.id = tasks.lead_id
        WHERE tasks.status = 'pending'
          AND COALESCE(tasks.superseded_by, '') = ''
          AND tasks.channel IN ('sms', 'email')
          AND tasks.approval_status IN ('pending', 'approved', 'failed')
          AND tasks.due_at >= :start AND tasks.due_at < :end
        ORDER BY tasks.due_at ASC, tasks.created_at ASC
        """),
        {"start": start.isoformat(), "end": end.isoformat()}
    )
    rows = res.mappings().all()
    tasks = [_operator_task_payload(row) for row in rows]
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "tasks": tasks,
        "counts": _operator_counts(tasks, []),
    }


@router.get("/api/operator/calendar/month")
async def get_operator_calendar_month(start: Optional[str] = None, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    anchor = _parse_calendar_date(start)
    month_start_dt, month_end_dt, month_start_date = _month_range_from_date(anchor)
    grid_start_date = month_start_date - datetime.timedelta(days=month_start_date.weekday())
    month_end_date = (month_end_dt - datetime.timedelta(days=1)).date()
    grid_end_date = month_end_date + datetime.timedelta(days=(6 - month_end_date.weekday()))
    grid_start = datetime.datetime.combine(grid_start_date, datetime.time.min, tzinfo=SYDNEY_TZ)
    grid_end = datetime.datetime.combine(grid_end_date + datetime.timedelta(days=1), datetime.time.min, tzinfo=SYDNEY_TZ)

    res_tasks = await session.execute(
        text("""
        SELECT id, title, channel, approval_status, due_at, priority_bucket
        FROM tasks
        WHERE status = 'pending'
          AND COALESCE(superseded_by, '') = ''
          AND due_at >= :start AND due_at < :end
        ORDER BY due_at ASC
        """),
        {"start": grid_start.isoformat(), "end": grid_end.isoformat()}
    )
    task_rows = res_tasks.mappings().all()
    
    res_appts = await session.execute(
        text("""
        SELECT id, title, starts_at
        FROM appointments
        WHERE status NOT IN ('cancelled', 'completed')
          AND starts_at >= :start AND starts_at < :end
        ORDER BY starts_at ASC
        """),
        {"start": grid_start.isoformat(), "end": grid_end.isoformat()}
    )
    appointment_rows = res_appts.mappings().all()

    day_map: Dict[str, Dict[str, Any]] = {}
    current = grid_start_date
    while current <= grid_end_date:
        day_map[current.isoformat()] = {
            "date": current.isoformat(),
            "is_current_month": current.month == month_start_date.month,
            "is_today": current == now_sydney().date(),
            "total": 0,
            "calls": 0,
            "sms": 0,
            "emails": 0,
            "callbacks": 0,
            "manual": 0,
            "pending_approvals": 0,
            "queued_sends": 0,
            "failed_sends": 0,
            "appointments": 0,
            "sample_titles": [],
        }
        current += datetime.timedelta(days=1)

    for row in task_rows:
        due_at = _parse_iso_datetime(row["due_at"])
        if not due_at:
            continue
        key = due_at.date().isoformat()
        bucket = day_map.get(key)
        if not bucket:
            continue
        bucket["total"] += 1
        channel = row["channel"] or "manual"
        if channel == "call":
            bucket["calls"] += 1
        elif channel == "sms":
            bucket["sms"] += 1
        elif channel == "email":
            bucket["emails"] += 1
        else:
            bucket["manual"] += 1
        if row["priority_bucket"] == "callback_due":
            bucket["callbacks"] += 1
        if row["approval_status"] == "pending":
            bucket["pending_approvals"] += 1
        elif row["approval_status"] == "approved":
            bucket["queued_sends"] += 1
        elif row["approval_status"] == "failed":
            bucket["failed_sends"] += 1
        if len(bucket["sample_titles"]) < 3:
            bucket["sample_titles"].append(row["title"])

    for row in appointment_rows:
        starts_at = _parse_iso_datetime(row["starts_at"])
        if not starts_at:
            continue
        key = starts_at.date().isoformat()
        bucket = day_map.get(key)
        if not bucket:
            continue
        bucket["appointments"] += 1
        bucket["total"] += 1
        if len(bucket["sample_titles"]) < 3:
            bucket["sample_titles"].append(row["title"])

    return {
        "month_start": month_start_date.isoformat(),
        "month_end": month_end_date.isoformat(),
        "grid_start": grid_start_date.isoformat(),
        "grid_end": grid_end_date.isoformat(),
        "days": [day_map[key] for key in sorted(day_map.keys())],
    }


@router.get("/api/operator/calendar/day")
async def get_operator_calendar_day(date: str, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    selected_date = _parse_calendar_date(date)
    start = datetime.datetime.combine(selected_date, datetime.time.min, tzinfo=SYDNEY_TZ)
    end = start + datetime.timedelta(days=1)
    res_tasks = await session.execute(
        text("""
        SELECT tasks.*, leads.address, leads.owner_name, leads.trigger_type, leads.status AS lead_status,
               leads.contact_emails, leads.contact_phones, leads.next_action_reason, leads.last_outcome,
               leads.queue_bucket, leads.lead_archetype
        FROM tasks
        LEFT JOIN leads ON leads.id = tasks.lead_id
        WHERE tasks.status = 'pending'
          AND COALESCE(tasks.superseded_by, '') = ''
          AND tasks.due_at >= :start AND tasks.due_at < :end
        ORDER BY tasks.due_at ASC, tasks.created_at ASC
        """),
        {"start": start.isoformat(), "end": end.isoformat()}
    )
    task_rows = res_tasks.mappings().all()
    
    res_appts = await session.execute(
        text("""
        SELECT appointments.*, leads.address, leads.owner_name
        FROM appointments
        LEFT JOIN leads ON leads.id = appointments.lead_id
        WHERE appointments.status NOT IN ('cancelled', 'completed')
          AND appointments.starts_at >= :start AND appointments.starts_at < :end
        ORDER BY appointments.starts_at ASC
        """),
        {"start": start.isoformat(), "end": end.isoformat()}
    )
    appointment_rows = res_appts.mappings().all()
    
    tasks = [_operator_task_payload(row) for row in task_rows]
    appointments = [_appointment_to_dict(row) for row in appointment_rows]
    return {
        "date": selected_date.isoformat(),
        "tasks": tasks,
        "appointments": appointments,
        "counts": _operator_counts(tasks, appointments),
    }
