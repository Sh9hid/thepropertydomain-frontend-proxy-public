import datetime
import html
import asyncio
import hmac
import hashlib
import json
import os
import re
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
from core.database import _async_session_factory
from core.config import (
    API_KEY, api_key_header, APP_TITLE, SYDNEY_TZ, STOCK_ROOT, 
    PROJECT_ROOT, PROJECT_LOG_PATH, BRAND_NAME, BRAND_AREA, BRAND_LOGO_URL, 
    PRINCIPAL_NAME, PRINCIPAL_EMAIL, PRINCIPAL_PHONE, PROJECT_MEMORY_RULE, 
    BACKGROUND_SEND_POLL_SECONDS, PRIMARY_STRIKE_SUBURB, SECONDARY_STRIKE_SUBURBS
)
from core.utils import (
    now_sydney, now_iso, format_sydney, parse_client_datetime, 
    _first_non_empty, _safe_int, _format_moneyish, _parse_json_list, 
    _encode_value, _decode_row, _dedupe_text_list, _normalize_phone, 
    _dedupe_by_phone, _parse_iso_datetime, _parse_calendar_date, 
    _month_range_from_date, _bool_db
)
from services.scoring import _trigger_bonus, _status_penalty, _score_lead
try:
    from services.lead_hygiene import apply_precall_hygiene
except Exception:  # pragma: no cover - boot safety fallback
    async def apply_precall_hygiene(*args, **kwargs):
        return {"processed": 0, "updated": 0, "errors": 0, "status": "fallback"}
from models.schemas import *

from core.logic import _infer_contactability_status, _normalize_token, _task_id, _parse_iso_datetime, _next_business_slot, _lead_has_phone, _lead_has_email, _message_bundle, _hydrate_lead, _infer_lead_archetype, _infer_strike_zone, _default_preferred_channel, _recent_touch_count, _queue_bucket_for_lead, _append_note_text, _append_activity, _build_activity_entry


async def _refresh_lead_next_action(session: AsyncSession, lead_id: str) -> None:
    res = await session.execute(
        text("""
        SELECT * FROM tasks
        WHERE lead_id = :lead_id AND status = 'pending' AND COALESCE(superseded_by, '') = ''
        ORDER BY due_at ASC, created_at ASC
        LIMIT 1
        """),
        {"lead_id": lead_id},
    )
    task = res.mappings().first()
    
    res2 = await session.execute(
        text("""
        SELECT * FROM appointments
        WHERE lead_id = :lead_id AND status NOT IN ('cancelled', 'completed')
        ORDER BY starts_at ASC, created_at ASC
        LIMIT 1
        """),
        {"lead_id": lead_id},
    )
    appointment = res2.mappings().first()
    
    next_task_dt = _parse_iso_datetime(task["due_at"]) if task else None
    next_appt_dt = _parse_iso_datetime(appointment["starts_at"]) if appointment else None
    
    if appointment and (not task or (next_appt_dt and next_task_dt and next_appt_dt <= next_task_dt)):
        await session.execute(
            text("""
            UPDATE leads
            SET next_action_at = :starts_at, next_action_type = 'appointment', next_action_channel = 'appointment',
                next_action_title = :title, next_action_reason = :reason, updated_at = :updated_at
            WHERE id = :id
            """),
            {
                "starts_at": appointment["starts_at"],
                "title": appointment["title"],
                "reason": appointment["notes"] or "Upcoming booked appraisal",
                "updated_at": now_iso(),
                "id": lead_id,
            },
        )
        return
    
    if task:
        task_data = dict(task)
        await session.execute(
            text("""
            UPDATE leads
            SET next_action_at = :due_at, next_action_type = :task_type, next_action_channel = :channel, next_action_title = :title,
                next_action_reason = :reason, next_message_template = :template, cadence_name = CASE WHEN cadence_name = '' THEN :cadence_name ELSE cadence_name END,
                cadence_step = CASE WHEN cadence_step = 0 THEN :cadence_step ELSE cadence_step END, updated_at = :updated_at
            WHERE id = :id
            """),
            {
                "due_at": task["due_at"],
                "task_type": task["task_type"] or task_data.get("action_type") or "follow_up",
                "channel": task["channel"] or "",
                "title": task["title"] or "",
                "reason": task["rewrite_reason"] or task["notes"] or "",
                "template": task["message_preview"] or "",
                "cadence_name": task_data.get("cadence_name") or "",
                "cadence_step": int(task_data.get("cadence_step") or 0),
                "updated_at": now_iso(),
                "id": lead_id,
            },
        )
        return
        
    await session.execute(
        text("""
        UPDATE leads
        SET next_action_at = NULL, next_action_type = '', next_action_channel = '', next_action_title = '',
            next_action_reason = '', next_message_template = '', updated_at = :updated_at
        WHERE id = :id
        """),
        {"updated_at": now_iso(), "id": lead_id},
    )


async def _schedule_task(
    session: AsyncSession,
    lead: Dict[str, Any],
    *,
    title: str,
    due_at: datetime.datetime,
    task_type: str,
    channel: str = "",
    notes: str = "",
    cadence_name: str = "",
    cadence_step: int = 0,
    auto_generated: bool = True,
    priority_bucket: str = "follow_up",
    approval_status: Optional[str] = None,
    message_subject: str = "",
    message_preview: str = "",
    rewrite_reason: str = "",
) -> str:
    due_at_iso = due_at.astimezone(SYDNEY_TZ).replace(microsecond=0).isoformat()
    task_id = _task_id(lead["id"], cadence_name or task_type, cadence_step, task_type, due_at_iso)
    
    res = await session.execute(text("SELECT id, status FROM tasks WHERE id = :id"), {"id": task_id})
    existing = res.mappings().first()
    if existing and existing["status"] in {"pending", "completed"}:
        return task_id
        
    now = now_iso()
    await session.execute(
        text("""
        INSERT INTO tasks (
            id, lead_id, title, task_type, action_type, channel, due_at, status, notes, related_report_id,
            approval_status, message_subject, message_preview, rewrite_reason, superseded_by, cadence_name,
            cadence_step, auto_generated, priority_bucket, completed_at, created_at, updated_at
        ) VALUES (:id, :lead_id, :title, :task_type, :action_type, :channel, :due_at, :status, :notes, :related_report_id,
            :approval_status, :message_subject, :message_preview, :rewrite_reason, :superseded_by, :cadence_name,
            :cadence_step, :auto_generated, :priority_bucket, :completed_at, :created_at, :updated_at)
        ON CONFLICT(id) DO UPDATE SET 
            status=EXCLUDED.status, notes=EXCLUDED.notes, updated_at=EXCLUDED.updated_at
        """),
        {
            "id": task_id,
            "lead_id": lead["id"],
            "title": title,
            "task_type": task_type,
            "action_type": task_type,
            "channel": channel,
            "due_at": due_at_iso,
            "status": "pending",
            "notes": notes,
            "related_report_id": "",
            "approval_status": approval_status or ("pending" if channel in {"sms", "email"} else "not_required"),
            "message_subject": message_subject,
            "message_preview": message_preview,
            "rewrite_reason": rewrite_reason,
            "superseded_by": "",
            "cadence_name": cadence_name,
            "cadence_step": cadence_step,
            "auto_generated": 1 if auto_generated else 0,
            "priority_bucket": priority_bucket,
            "completed_at": None,
            "created_at": now,
            "updated_at": now,
        },
    )
    return task_id


async def _rebuild_queue(session: AsyncSession, *, horizon_days: int = 60, force: bool = False) -> Dict[str, Any]:
    await apply_precall_hygiene(session, limit=1200)
    res = await session.execute(
        text("SELECT COUNT(*) FROM tasks WHERE status = 'pending' AND auto_generated = 1 AND COALESCE(superseded_by, '') = ''")
    )
    existing_pending_total = res.scalar_one()
    if existing_pending_total and not force:
        return {"processed": 0, "created_tasks": 0, "cancelled_tasks": 0, "skipped_existing": existing_pending_total}
        
    actionable_sql = """
        SELECT * FROM leads
        WHERE status NOT IN ('converted', 'dropped')
          AND (
            (contact_phones IS NOT NULL AND contact_phones != '[]')
            OR (contact_emails IS NOT NULL AND contact_emails != '[]')
            OR LOWER(COALESCE(trigger_type, '')) LIKE '%probate%'
            OR LOWER(COALESCE(lifecycle_stage, '')) LIKE '%documented%'
            OR COALESCE(call_today_score, 0) >= 60
            OR COALESCE(evidence_score, 0) >= 65
          )
        ORDER BY
            CASE WHEN LOWER(COALESCE(suburb, '')) = :suburb THEN 0 ELSE 1 END,
            COALESCE(call_today_score, 0) DESC,
            COALESCE(evidence_score, 0) DESC,
            COALESCE(updated_at, created_at, date_found) DESC
        LIMIT 800
    """
    res_rows = await session.execute(text(actionable_sql), {"suburb": _normalize_token(PRIMARY_STRIKE_SUBURB)})
    rows = res_rows.mappings().all()
    
    summary = {"processed": 0, "created_tasks": 0, "cancelled_tasks": 0}
    for row in rows:
        result = await _plan_lead_automation(session, row, horizon_days=horizon_days, force=force)
        summary["processed"] += 1
        summary["created_tasks"] += result["created"]
        summary["cancelled_tasks"] += result["cancelled"]
    await session.commit()
    return summary


async def _mark_task_send_failure(task_id: str, lead_id: str, detail: str) -> None:
    async with _async_session_factory() as session:
        res = await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})
        task = res.mappings().first()
        if not task:
            return
            
        now = now_iso()
        notes = _append_note_text(dict(task).get("notes") or "", f"Send failure: {detail}")
        await session.execute(
            text("UPDATE tasks SET approval_status = 'failed', notes = :notes, updated_at = :upd WHERE id = :id"),
            {"notes": notes, "upd": now, "id": task_id},
        )
        
        res_lead = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
        row_lead = res_lead.mappings().first()
        if row_lead:
            lead = _decode_row(row_lead)
            activity_log = _append_activity(
                lead.get("activity_log"),
                _build_activity_entry("message_send_failed", detail, lead.get("status"), dict(task).get("channel"), dict(task).get("title")),
            )
            await session.execute(
                text("UPDATE leads SET activity_log = :log, updated_at = :upd WHERE id = :id"),
                {"log": json.dumps(activity_log), "upd": now, "id": lead_id}
            )
            await _refresh_lead_next_action(session, lead_id)
        await session.commit()



def _get_dynamic_email_warmup_cap() -> int:
    """
    Global daily email ceiling that ramps linearly over time.
    Defaults: day 1 = 20, reaches max (500) by day 30.
    """
    start_cap = max(1, int(os.getenv("EMAIL_WARMUP_START_CAP", "20")))
    max_cap = max(start_cap, int(os.getenv("EMAIL_WARMUP_MAX_CAP", "500")))
    ramp_days = max(1, int(os.getenv("EMAIL_WARMUP_RAMP_DAYS", "30")))
    start_date_raw = (os.getenv("EMAIL_WARMUP_START_DATE") or "").strip()
    today = now_sydney().date()
    if not start_date_raw:
        day_index = 1
    else:
        parsed = _parse_calendar_date(start_date_raw)
        anchor = parsed or today
        day_index = max(1, (today - anchor).days + 1)
    if day_index >= ramp_days:
        return max_cap
    span = max_cap - start_cap
    if span <= 0:
        return max_cap
    progress = (day_index - 1) / max(1, ramp_days - 1)
    return max(start_cap, min(max_cap, int(round(start_cap + span * progress))))


async def _count_completed_email_sends_today(session: AsyncSession) -> int:
    today = now_sydney().date().isoformat()
    res = await session.execute(
        text(
            """
            SELECT COUNT(*)
            FROM tasks
            WHERE channel = 'email'
              AND status = 'completed'
              AND COALESCE(substr(completed_at, 1, 10), '') = :today
            """
        ),
        {"today": today},
    )
    return int(res.scalar_one() or 0)


async def _process_due_approved_messages(limit: int = 20) -> int:
    import random
    async with _async_session_factory() as session:
        res = await session.execute(
            text("""
            SELECT id, lead_id, channel
            FROM tasks
            WHERE status = 'pending'
              AND COALESCE(superseded_by, '') = ''
              AND channel IN ('sms', 'email')
              AND approval_status = 'approved'
              AND due_at <= :due
            ORDER BY due_at ASC
            LIMIT :limit
            """),
            {"due": now_sydney().isoformat(), "limit": limit},
        )
        rows = res.mappings().all()
        sent_emails_today = await _count_completed_email_sends_today(session)
        warmup_cap = _get_dynamic_email_warmup_cap()

    processed = 0
    # Import inside to avoid circular dependency
    from api.routes.tasks import execute_task

    for row in rows:
        if row.get("channel") == "email" and sent_emails_today >= warmup_cap:
            continue
        try:
            await execute_task(row["id"], TaskExecuteRequest(), API_KEY)
            processed += 1
            if row.get("channel") == "email":
                sent_emails_today += 1
                # Random delay between email sends to avoid burst patterns.
                await asyncio.sleep(random.uniform(45, 90))
        except HTTPException as exc:
            await _mark_task_send_failure(row["id"], row["lead_id"], str(exc.detail))
        except Exception as exc:
            await _mark_task_send_failure(row["id"], row["lead_id"], str(exc))
    return processed

async def _background_sender_loop() -> None:
    while True:
        try:
            await _process_due_approved_messages()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(BACKGROUND_SEND_POLL_SECONDS)


async def _write_lead_workflow_fields(session: AsyncSession, lead_id: str, lead: Dict[str, Any]) -> None:
    await session.execute(
        text("""
        UPDATE leads
        SET queue_bucket = :queue_bucket, lead_archetype = :lead_archetype, contactability_status = :contactability_status, owner_verified = :owner_verified, contact_role = :contact_role,
            cadence_name = :cadence_name, cadence_step = :cadence_step, last_outcome = :last_outcome, last_outcome_at = :last_outcome_at, objection_reason = :objection_reason,
            preferred_channel = :preferred_channel, strike_zone = :strike_zone, touches_14d = :touches_14d, touches_30d = :touches_30d, do_not_contact_until = :do_not_contact_until,
            updated_at = :updated_at
        WHERE id = :id
        """),
        {
            "queue_bucket": lead.get("queue_bucket") or "",
            "lead_archetype": lead.get("lead_archetype") or "",
            "contactability_status": lead.get("contactability_status") or "",
            "owner_verified": 1 if lead.get("owner_verified") else 0,
            "contact_role": lead.get("contact_role") or "",
            "cadence_name": lead.get("cadence_name") or "",
            "cadence_step": int(lead.get("cadence_step") or 0),
            "last_outcome": lead.get("last_outcome") or "",
            "last_outcome_at": lead.get("last_outcome_at"),
            "objection_reason": lead.get("objection_reason") or "",
            "preferred_channel": lead.get("preferred_channel") or "",
            "strike_zone": lead.get("strike_zone") or "",
            "touches_14d": int(lead.get("touches_14d") or 0),
            "touches_30d": int(lead.get("touches_30d") or 0),
            "do_not_contact_until": lead.get("do_not_contact_until"),
            "updated_at": now_iso(),
            "id": lead_id,
        },
    )


async def _supersede_auto_tasks(session: AsyncSession, lead_id: str, reason: str, *, preserve_task_id: Optional[str] = None) -> int:
    res = await session.execute(
        text("""
        SELECT id FROM tasks
        WHERE lead_id = :lead_id AND status = 'pending' AND auto_generated = 1 AND COALESCE(superseded_by, '') = ''
        """),
        {"lead_id": lead_id},
    )
    rows = res.mappings().all()
    count = 0
    for row in rows:
        if preserve_task_id and row["id"] == preserve_task_id:
            continue
        await session.execute(
            text("UPDATE tasks SET status = 'superseded', superseded_by = :reason, updated_at = :upd WHERE id = :id"),
            {"reason": reason, "upd": now_iso(), "id": row["id"]},
        )
        count += 1
    return count


async def _schedule_hot_cadence(session: AsyncSession, lead: Dict[str, Any], horizon_end: datetime.datetime) -> int:
    created = 0
    cadence_name = "hot_seller_10_day"
    pattern = [
        (1, 0, 9, 15, "Initial value review call", "call_now"),
        (2, 1, 16, 10, "Second attempt at a different time", "call_now"),
        (3, 3, 9, 30, "Market-angle callback", "call_now"),
        (4, 5, 16, 0, "Voice message follow-up call", "call_now"),
        (5, 8, 9, 15, "Late-cycle appraisal check-in", "call_now"),
        (6, 10, 15, 45, "Crossroads call", "callback_due"),
    ]
    for step, offset, hour, minute, label, bucket in pattern:
        due_at = _next_business_slot(offset, hour, minute)
        if due_at > horizon_end:
            continue
        await _schedule_task(
            session,
            lead,
            title=label,
            due_at=due_at,
            task_type="call",
            channel="call",
            notes=lead.get("what_to_say") or lead.get("recommended_next_step") or "",
            cadence_name=cadence_name,
            cadence_step=step,
            priority_bucket=bucket,
            rewrite_reason="Active 10-day strike cadence",
        )
        created += 1
    return created


async def _schedule_warm_cadence(session: AsyncSession, lead: Dict[str, Any], horizon_end: datetime.datetime) -> int:
    created = 0
    cadence_name = "warm_seller_30_day"
    pattern = [
        (1, 0, "call", "call", "Warm introduction call", "call_now"),
        (2, 4, "sms" if _lead_has_phone(lead) else "email", "sms" if _lead_has_phone(lead) else "email", "Value touch", "send_now"),
        (3, 10, "email" if _lead_has_email(lead) else "sms", "email" if _lead_has_email(lead) else "sms", "Market update", "send_now"),
        (4, 21, "call", "call", "Warm callback", "callback_due"),
        (5, 30, "call" if _lead_has_phone(lead) else "email", "call" if _lead_has_phone(lead) else "email", "Thirty-day review", "follow_up"),
    ]
    for step, offset, task_type, channel, label, bucket in pattern:
        if channel == "email" and not _lead_has_email(lead):
            continue
        if channel == "sms" and not _lead_has_phone(lead):
            continue
        due_at = _next_business_slot(offset, 10 if step in {1, 3, 5} else 15, 0)
        if due_at > horizon_end:
            continue
        bundle = _message_bundle(lead, "market_email" if channel == "email" else "nurture_sms")
        await _schedule_task(
            session,
            lead,
            title=label,
            due_at=due_at,
            task_type=task_type,
            channel=channel,
            notes=lead.get("recommended_next_step") or "",
            cadence_name=cadence_name,
            cadence_step=step,
            priority_bucket=bucket,
            message_subject=bundle["subject"] if channel == "email" else "",
            message_preview=bundle["body"] if channel in {"email", "sms"} else "",
            rewrite_reason="Warm 30-day nurture cadence",
        )
        created += 1
    return created


def _is_door_knock_lead(lead: Dict[str, Any]) -> bool:
    source_tags = json.dumps(lead.get("source_tags") or [])
    activity_log = json.dumps(lead.get("activity_log") or [])
    trigger_type = str(lead.get("trigger_type") or "").lower()
    route_queue = str(lead.get("route_queue") or "").lower()
    return (
        "door_knock" in source_tags
        or "\"door_knock\"" in activity_log
        or "door_knock" in trigger_type
        or "door_knock" in route_queue
    )


def _resolve_nurture_cadence(lead: Dict[str, Any]) -> str:
    override = str(lead.get("followup_frequency_override") or "").strip().lower()
    if override in {"monthly", "quarterly"}:
        return override
    if _is_door_knock_lead(lead):
        return "quarterly"
    lead_state = str(lead.get("lead_state") or "").strip().lower()
    if lead_state in {"new", "hot", "warm"}:
        return "monthly"
    if lead_state in {"cold", "dormant", "unqualified"}:
        return "quarterly"
    return "monthly"


async def _schedule_nurture_cadence(
    session: AsyncSession,
    lead: Dict[str, Any],
    horizon_end: datetime.datetime,
    *,
    cadence_kind: str = "monthly",
) -> int:
    created = 0
    normalized = str(cadence_kind or "monthly").strip().lower()
    cadence_name = "quarterly_nurture" if normalized == "quarterly" else "monthly_nurture"
    if normalized == "quarterly":
        pattern = [
            (1, 90, "email" if _lead_has_email(lead) else "sms", "email" if _lead_has_email(lead) else "sms", "Quarterly market pulse", "nurture"),
            (2, 180, "call" if _lead_has_phone(lead) else "email", "call" if _lead_has_phone(lead) else "email", "Semi-annual review", "nurture"),
        ]
    else:
        pattern = [
            (1, 30, "email" if _lead_has_email(lead) else "sms", "email" if _lead_has_email(lead) else "sms", "Monthly market pulse", "nurture"),
            (2, 60, "call" if _lead_has_phone(lead) else "email", "call" if _lead_has_phone(lead) else "email", "Sixty-day review", "nurture"),
        ]
    for step, offset, task_type, channel, label, bucket in pattern:
        if channel == "email" and not _lead_has_email(lead):
            continue
        if channel == "sms" and not _lead_has_phone(lead):
            continue
        due_at = _next_business_slot(offset, 11, 0)
        if due_at > horizon_end:
            continue
        bundle = _message_bundle(lead, "market_email" if channel == "email" else "nurture_sms")
        await _schedule_task(
            session,
            lead,
            title=label,
            due_at=due_at,
            task_type=task_type,
            channel=channel,
            cadence_name=cadence_name,
            cadence_step=step,
            priority_bucket=bucket,
            message_subject=bundle["subject"] if channel == "email" else "",
            message_preview=bundle["body"] if channel in {"email", "sms"} else "",
            rewrite_reason="Quarterly nurture cadence" if normalized == "quarterly" else "Slow nurture cadence",
        )
        created += 1
    return created


async def _schedule_enrichment_task(session: AsyncSession, lead: Dict[str, Any], due_at: Optional[datetime.datetime] = None, reason: str = "Contact details need enrichment") -> int:
    due = due_at or _next_business_slot(0, 13, 15)
    await _schedule_task(
        session,
        lead,
        title="Enrichment sweep",
        due_at=due,
        task_type="enrichment",
        channel="manual",
        notes="Use ID4me or operator research to confirm owner phone/email.",
        cadence_name="enrichment_queue",
        cadence_step=1,
        priority_bucket="enrichment",
        rewrite_reason=reason,
    )
    return 1


async def _schedule_callback_cadence(session: AsyncSession, lead: Dict[str, Any], callback_at: datetime.datetime, horizon_end: datetime.datetime, reason: str) -> int:
    created = 0
    preflight = callback_at - datetime.timedelta(days=7)
    if preflight > now_sydney() and preflight <= horizon_end:
        await _schedule_task(
            session,
            lead,
            title="Pre-callback warm touch",
            due_at=preflight,
            task_type="email" if _lead_has_email(lead) else "sms" if _lead_has_phone(lead) else "enrichment",
            channel="email" if _lead_has_email(lead) else "sms" if _lead_has_phone(lead) else "manual",
            cadence_name="callback_plan",
            cadence_step=1,
            priority_bucket="callback_due",
            message_subject=_message_bundle(lead, "market_email")["subject"] if _lead_has_email(lead) else "",
            message_preview=_message_bundle(lead, "market_email")["body"] if _lead_has_email(lead) else _message_bundle(lead, "nurture_sms")["body"] if _lead_has_phone(lead) else "",
            rewrite_reason=reason,
        )
        created += 1
    if callback_at <= horizon_end:
        await _schedule_task(
            session,
            lead,
            title="Promised callback",
            due_at=callback_at,
            task_type="call" if _lead_has_phone(lead) else "email",
            channel="call" if _lead_has_phone(lead) else "email",
            notes=lead.get("what_to_say") or "Callback promised by owner.",
            cadence_name="callback_plan",
            cadence_step=2,
            priority_bucket="callback_due",
            rewrite_reason=reason,
        )
        created += 1
    return created


async def _schedule_booked_followthrough(session: AsyncSession, lead: Dict[str, Any], appointment_at: datetime.datetime, location: str) -> int:
    appointment_id = hashlib.md5(f"{lead['id']}:appointment:{appointment_at.isoformat()}".encode()).hexdigest()
    now = now_iso()
    await session.execute(
        text("""
        INSERT INTO appointments (id, lead_id, title, starts_at, status, location, notes, cadence_name, auto_generated, created_at, updated_at)
        VALUES (:id, :lead_id, :title, :starts_at, :status, :location, :notes, :cadence_name, :auto_generated, :created_at, :updated_at)
        ON CONFLICT(id) DO UPDATE SET
            status=EXCLUDED.status, starts_at=EXCLUDED.starts_at, updated_at=EXCLUDED.updated_at
        """),
        {
            "id": appointment_id,
            "lead_id": lead["id"],
            "title": "Property appraisal",
            "starts_at": appointment_at.astimezone(SYDNEY_TZ).replace(microsecond=0).isoformat(),
            "status": "booked",
            "location": location,
            "notes": "Booked from workflow outcome",
            "cadence_name": "booked_appraisal",
            "auto_generated": 1,
            "created_at": now,
            "updated_at": now,
        },
    )
    bundle_confirm = _message_bundle(lead, "appointment_confirmation")
    bundle_reminder = _message_bundle(lead, "appointment_reminder")
    if _lead_has_phone(lead):
        for step, hours_before in ((1, 24), (2, 2)):
            reminder_time = appointment_at - datetime.timedelta(hours=hours_before)
            if reminder_time > now_sydney():
                await _schedule_task(
                    session,
                    lead,
                    title=f"Appointment reminder ({hours_before}h)",
                    due_at=reminder_time,
                    task_type="sms",
                    channel="sms",
                    cadence_name="booked_appraisal",
                    cadence_step=step,
                    priority_bucket="send_now",
                    message_preview=bundle_confirm["body"] if hours_before == 24 else bundle_reminder["body"],
                    rewrite_reason="Booked appraisal reminder",
                )
    if _lead_has_email(lead):
        confirm_time = max(now_sydney() + datetime.timedelta(minutes=2), appointment_at - datetime.timedelta(hours=24))
        await _schedule_task(
            session,
            lead,
            title="Appointment confirmation",
            due_at=confirm_time,
            task_type="email",
            channel="email",
            cadence_name="booked_appraisal",
            cadence_step=3,
            priority_bucket="send_now",
            message_subject=bundle_confirm["subject"],
            message_preview=bundle_confirm["body"],
            rewrite_reason="Booked appraisal confirmation",
        )
    return 1


async def _plan_lead_automation(session: AsyncSession, lead_row: Any, *, horizon_days: int = 60, force: bool = False) -> Dict[str, Any]:
    lead = _hydrate_lead(lead_row)
    lead["contactability_status"] = _infer_contactability_status(lead)
    lead["lead_archetype"] = lead.get("lead_archetype") or _infer_lead_archetype(lead)
    lead["strike_zone"] = lead.get("strike_zone") or _infer_strike_zone(lead)
    lead["preferred_channel"] = lead.get("preferred_channel") or _default_preferred_channel(lead)
    lead["touches_14d"] = _recent_touch_count(lead.get("activity_log"), 14)
    lead["touches_30d"] = _recent_touch_count(lead.get("activity_log"), 30)
    lead["queue_bucket"] = _queue_bucket_for_lead(lead)
    horizon_end = now_sydney() + datetime.timedelta(days=max(7, min(horizon_days, 90)))
    
    cancelled = await _supersede_auto_tasks(session, lead["id"], "queue_rebuild") if force else 0
    
    res = await session.execute(
        text("""
        SELECT COUNT(*) FROM tasks
        WHERE lead_id = :lead_id AND status = 'pending' AND auto_generated = 1 AND COALESCE(superseded_by, '') = ''
        """),
        {"lead_id": lead["id"]},
    )
    existing_pending = res.scalar_one()
    created = 0
    
    if force or existing_pending == 0:
        if lead["queue_bucket"] == "enrichment":
            lead["cadence_name"] = "enrichment_queue"
            created += await _schedule_enrichment_task(session, lead)
        elif lead["queue_bucket"] == "active":
            lead["cadence_name"] = "hot_seller_10_day" if lead["strike_zone"] == "primary" else "warm_seller_30_day"
            if lead["cadence_name"] == "hot_seller_10_day":
                created += await _schedule_hot_cadence(session, lead, horizon_end)
            else:
                created += await _schedule_warm_cadence(session, lead, horizon_end)
        elif lead["queue_bucket"] == "nurture":
            cadence_kind = _resolve_nurture_cadence(lead)
            lead["cadence_name"] = "quarterly_nurture" if cadence_kind == "quarterly" else "monthly_nurture"
            created += await _schedule_nurture_cadence(session, lead, horizon_end, cadence_kind=cadence_kind)
        elif lead["queue_bucket"] == "callback_due":
            lead["cadence_name"] = "callback_plan"
            callback_at = _parse_iso_datetime(lead.get("do_not_contact_until")) or _next_business_slot(21, 10, 0)
            created += await _schedule_callback_cadence(session, lead, callback_at, horizon_end, "Owner asked for later follow-up")
        elif lead["queue_bucket"] == "booked":
            res_appt = await session.execute(
                text("SELECT * FROM appointments WHERE lead_id = :lead_id AND status NOT IN ('cancelled', 'completed') ORDER BY starts_at ASC LIMIT 1"),
                {"lead_id": lead["id"]},
            )
            appointment_row = res_appt.mappings().first()
            if appointment_row:
                created += await _schedule_booked_followthrough(
                    session,
                    lead,
                    _parse_iso_datetime(appointment_row["starts_at"]) or _next_business_slot(1, 16, 0),
                    appointment_row["location"] or "Phone / on-site",
                )
    
    await _write_lead_workflow_fields(session, lead["id"], lead)
    await _refresh_lead_next_action(session, lead["id"])
    return {"lead_id": lead["id"], "created": created, "cancelled": cancelled, "queue_bucket": lead["queue_bucket"], "cadence_name": lead.get("cadence_name") or ""}


async def _bootstrap_automation_if_needed(session: AsyncSession, *, horizon_days: int = 60) -> None:
    res = await session.execute(text("SELECT COUNT(*) FROM leads WHERE status NOT IN ('converted', 'dropped')"))
    active_leads = res.scalar_one()
    
    res2 = await session.execute(
        text("SELECT COUNT(*) FROM tasks WHERE status = 'pending' AND auto_generated = 1 AND COALESCE(superseded_by, '') = ''")
    )
    pending_auto = res2.scalar_one()
    
    if active_leads and pending_auto == 0:
        await _rebuild_queue(session, horizon_days=horizon_days, force=False)

