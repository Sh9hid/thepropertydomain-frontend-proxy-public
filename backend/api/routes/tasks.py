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
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from zoneinfo import ZoneInfo

from api.routes._deps import APIKeyDep, SessionDep
from core.config import (
    API_KEY, api_key_header, APP_TITLE, SYDNEY_TZ, STOCK_ROOT,
    PROJECT_ROOT, PROJECT_LOG_PATH, BRAND_NAME, BRAND_AREA, BRAND_LOGO_URL,
    PRINCIPAL_NAME, PRINCIPAL_EMAIL, PRINCIPAL_PHONE, PROJECT_MEMORY_RULE,
    BACKGROUND_SEND_POLL_SECONDS, PRIMARY_STRIKE_SUBURB, SECONDARY_STRIKE_SUBURBS,
    USE_POSTGRES, build_public_url
)
from core.utils import (
    now_sydney, now_iso, format_sydney, parse_client_datetime,
    _first_non_empty, _safe_int, _format_moneyish, _parse_json_list,
    _encode_value, _decode_row, _dedupe_text_list, _normalize_phone,
    _dedupe_by_phone, _parse_iso_datetime, _parse_calendar_date, _is_sms_mobile_au,
    _month_range_from_date, _bool_db
)
from services.scoring import _trigger_bonus, _status_penalty, _score_lead
from models.schemas import *
from core.logic import *

from core.database import get_session, _get_lead_or_404, _fetch_joined_task, _task_to_dict
from services.automations import _schedule_task, _refresh_lead_next_action
from services.integrations import send_email_service, _zoom_request, _send_http_text
from core.security import get_api_key
from services.funnel_service import assert_outreach_allowed, resolve_outreach_purpose
from services.hermes_lead_ops_service import refresh_hermes_for_lead
from services.audit_log_service import write_lead_audit_log

router = APIRouter()

def _first_sms_mobile_or_none(values: Any) -> Optional[str]:
    for phone in _dedupe_by_phone(values):
        if _is_sms_mobile_au(phone):
            return phone
    return None


class KillSequenceRequest(BaseModel):
    reason: Optional[str] = "Manual operator override"
    suppress_until: Optional[str] = None
    mark_do_not_call: bool = True


class RestoreLeadFromAuditRequest(BaseModel):
    restore_to: str = "before"  # before | after
    reason: Optional[str] = "Manual rollback"


@router.post("/api/tasks/{task_id}/approve")
async def approve_task(task_id: str, body: TaskApprovalRequest, api_key: APIKeyDep, session: SessionDep):
    row = (await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Task not found")
    task_dict = dict(row)
    if (task_dict.get("channel") or "") not in {"sms", "email"}:
        raise HTTPException(status_code=400, detail="Only SMS and email tasks can be approved for scheduled send")
    due_at = parse_client_datetime(body.due_at) if body.due_at else task_dict.get("due_at")
    notes = _append_note_text(task_dict.get("notes") or "", body.note or "")
    subject = body.subject if body.subject is not None else task_dict.get("message_subject") or ""
    message_preview = body.message if body.message is not None else task_dict.get("message_preview") or ""
    now = now_iso()
    await session.execute(
        text("""
        UPDATE tasks
        SET due_at = :due_at, notes = :notes, approval_status = 'approved', status = 'pending',
            message_subject = :subject, message_preview = :message_preview, updated_at = :updated_at
        WHERE id = :id
        """),
        {"due_at": due_at, "notes": notes, "subject": subject, "message_preview": message_preview, "updated_at": now, "id": task_id},
    )
    lead = _decode_row(await _get_lead_or_404(session, task_dict["lead_id"]))
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry("task_approved", f"{task_dict['title']} approved for scheduled send", lead.get("status"), task_dict.get("channel"), task_dict["title"]),
    )
    await session.execute(text("UPDATE leads SET activity_log = :log, updated_at = :now WHERE id = :id"), {"log": json.dumps(activity_log), "now": now, "id": task_dict["lead_id"]})
    await _refresh_lead_next_action(session, task_dict["lead_id"])
    await session.commit()
    await refresh_hermes_for_lead(session, task_dict["lead_id"], actor="task_approve")

    # Fire SMS immediately if task_type is sms and now approved
    sms_result = None
    if task_dict.get("channel") == "sms" and message_preview:
        try:
            outreach_purpose = resolve_outreach_purpose(lead, cadence_name=str(task_dict.get("cadence_name") or ""))
            await assert_outreach_allowed(
                session,
                lead["id"],
                "sms",
                purpose=outreach_purpose,
                cadence_name=str(task_dict.get("cadence_name") or ""),
            )
            from services.sms_service import sms_service
            phone = _first_sms_mobile_or_none(lead.get("contact_phones") or "[]")
            if phone:
                sms_result = await sms_service.send_sms(phone, message_preview, task_dict["lead_id"])
            else:
                sms_result = {"ok": False, "error": "No SMS-capable AU mobile number on lead"}
        except Exception as _sms_err:
            sms_result = {"ok": False, "error": str(_sms_err)}

    updated_task = await _fetch_joined_task(session, task_id)
    updated_lead = _hydrate_lead(await _get_lead_or_404(session, task_dict["lead_id"]))
    return {"status": "ok", "task": _operator_task_payload(updated_task), "lead": updated_lead, "sms": sms_result}


@router.post("/api/tasks/{task_id}/skip")
async def skip_task(task_id: str, body: TaskSkipRequest, api_key: APIKeyDep, session: SessionDep):
    row = (await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Task not found")
    task_dict = dict(row)
    notes = _append_note_text(task_dict.get("notes") or "", body.note or "Skipped by operator")
    now = now_iso()
    await session.execute(
        text("""
        UPDATE tasks
        SET status = 'superseded', superseded_by = 'operator_skip', notes = :notes, updated_at = :now
        WHERE id = :id
        """),
        {"notes": notes, "now": now, "id": task_id},
    )
    lead = _decode_row(await _get_lead_or_404(session, task_dict["lead_id"]))
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry("task_skipped", f"{task_dict['title']} skipped", lead.get("status"), task_dict.get("channel"), task_dict["title"]),
    )
    await session.execute(text("UPDATE leads SET activity_log = :log, updated_at = :now WHERE id = :id"), {"log": json.dumps(activity_log), "now": now, "id": task_dict["lead_id"]})
    await _refresh_lead_next_action(session, task_dict["lead_id"])
    await session.commit()
    await refresh_hermes_for_lead(session, task_dict["lead_id"], actor="task_skip")
    updated_lead = _hydrate_lead(await _get_lead_or_404(session, task_dict["lead_id"]))
    return {"status": "ok", "skipped_task_id": task_id, "lead": updated_lead}


@router.post("/api/tasks/{task_id}/complete")
async def complete_task(task_id: str, body: TaskCompletionRequest, api_key: APIKeyDep, session: SessionDep):
    row = (await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Task not found")
    task_dict = dict(row)
    now = now_iso()
    notes = task_dict.get("notes") or ""
    if body.note:
        notes = f"{notes}\n{body.note}".strip()
    await session.execute(
        text("UPDATE tasks SET status = 'completed', notes = :notes, completed_at = :now, updated_at = :now WHERE id = :id"),
        {"notes": notes, "now": now, "id": task_id},
    )
    lead = _decode_row(await _get_lead_or_404(session, task_dict["lead_id"]))
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry("task_completed", f"{task_dict['title']} completed", lead.get("status"), task_dict.get("channel"), task_dict["title"]),
    )
    await session.execute(text("UPDATE leads SET activity_log = :log, updated_at = :now WHERE id = :id"), {"log": json.dumps(activity_log), "now": now, "id": task_dict["lead_id"]})
    await _refresh_lead_next_action(session, task_dict["lead_id"])
    await session.commit()
    await refresh_hermes_for_lead(session, task_dict["lead_id"], actor="task_complete")
    updated_row = (await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})).mappings().first()
    updated_lead = _hydrate_lead(await _get_lead_or_404(session, task_dict["lead_id"]))
    return {"status": "ok", "task": _task_to_dict(dict(updated_row) if updated_row else {}), "lead": updated_lead}


@router.post("/api/tasks/{task_id}/reschedule")
async def reschedule_task(task_id: str, body: TaskRescheduleRequest, api_key: APIKeyDep, session: SessionDep):
    row = (await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Task not found")
    due_at = parse_client_datetime(body.due_at)
    now = now_iso()
    task_dict = dict(row)
    notes = task_dict.get("notes") or ""
    if body.note:
        notes = f"{notes}\n{body.note}".strip()
    await session.execute(
        text("UPDATE tasks SET due_at = :due_at, notes = :notes, status = 'pending', updated_at = :now WHERE id = :id"),
        {"due_at": due_at, "notes": notes, "now": now, "id": task_id},
    )
    await _refresh_lead_next_action(session, task_dict["lead_id"])
    await session.commit()
    await refresh_hermes_for_lead(session, task_dict["lead_id"], actor="task_reschedule")
    updated_row = (await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})).mappings().first()
    updated_lead = _hydrate_lead(await _get_lead_or_404(session, task_dict["lead_id"]))
    return {"status": "ok", "task": _task_to_dict(dict(updated_row) if updated_row else {}), "lead": updated_lead}


@router.post("/api/tasks/{task_id}/execute")
async def execute_task(task_id: str, body: TaskExecuteRequest, api_key: APIKeyDep, session: SessionDep):
    task_row = (await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})).mappings().first()
    if not task_row:
        raise HTTPException(status_code=404, detail="Task not found")
    task = dict(task_row)
    lead = _decode_row(await _get_lead_or_404(session, task["lead_id"]))
    transport: Dict[str, Any] = {"status": "completed_without_transport"}
    recipient = (body.recipient or "").strip()
    now = now_iso()
    outreach_purpose = resolve_outreach_purpose(lead, cadence_name=str(task.get("cadence_name") or ""))
    if task.get("channel") == "email":
        try:
            await assert_outreach_allowed(session, lead["id"], "email", purpose=outreach_purpose, cadence_name=str(task.get("cadence_name") or ""))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        account_id = body.email_account_id
        if not account_id:
            from api.routes.communications import _get_account_for_rotation
            account_id = await _get_account_for_rotation(session)
        account_row = (await session.execute(
            text("SELECT * FROM email_accounts WHERE id = :id"), {"id": account_id}
        )).mappings().first() if account_id else None
        if not account_row:
            raise HTTPException(status_code=400, detail="No email account configured for task execution")
        recipient = recipient or (_dedupe_text_list(lead.get("contact_emails"))[0] if _dedupe_text_list(lead.get("contact_emails")) else "")
        if not recipient:
            raise HTTPException(status_code=400, detail="No email recipient available")
        subject = body.subject or task.get("message_subject") or _message_bundle(lead, "market_email")["subject"]
        message_body = body.message or task.get("message_preview") or _message_bundle(lead, "market_email")["body"]
        account_data = dict(account_row)
        # Append Australian Spam Act unsubscribe footer
        from api.routes.communications import _generate_unsub_token
        _unsub_token = _generate_unsub_token(lead["id"], recipient)
        _unsub_url = build_public_url(f"/unsubscribe/{_unsub_token}")
        _unsub_footer = (
            f'\n\n<p style="margin-top:32px;font-size:11px;color:#aaa;border-top:1px solid #eee;padding-top:12px">'
            f'You are receiving this email because your property has been identified through market intelligence activity. '
            f'<a href="{_unsub_url}" style="color:#aaa">Unsubscribe</a></p>'
        )
        message_body_with_footer = message_body + _unsub_footer
        from models.schemas import SendEmailRequest
        email_body = SendEmailRequest(
            account_id=account_id,
            recipient=recipient,
            subject=subject,
            body=message_body_with_footer,
        )
        try:
            # Use centralized send_email_service (Graph priority)
            await asyncio.to_thread(send_email_service, account_data, email_body)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Email send failed: {exc}") from exc
        # Track sends for inbox rotation
        if account_id:
            from api.routes.communications import _increment_account_sends
            await _increment_account_sends(session, account_id)
        task["message_subject"] = subject
        task["message_preview"] = message_body
        transport = {"status": "sent", "channel": "email", "recipient": recipient}
        note_history = _append_stage_note(lead.get("stage_note_history"), message_body, lead.get("status") or "captured", "email", subject, recipient)
        activity_log = _append_activity(lead.get("activity_log"), _build_activity_entry("email_sent", message_body, lead.get("status"), "email", subject, recipient))
        await session.execute(
            text("UPDATE leads SET stage_note_history = :nh, activity_log = :al, last_outbound_at = :now, updated_at = :now WHERE id = :id"),
            {"nh": json.dumps(note_history), "al": json.dumps(activity_log), "now": now, "id": lead["id"]},
        )
    elif task.get("channel") == "sms":
        try:
            await assert_outreach_allowed(session, lead["id"], "sms", purpose=outreach_purpose, cadence_name=str(task.get("cadence_name") or ""))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        account_id = body.text_account_id
        if not account_id:
            account_id_row = (await session.execute(
                text("SELECT id FROM communication_accounts WHERE text_enabled = 1 ORDER BY updated_at DESC, created_at DESC LIMIT 1")
            )).mappings().first()
            account_id = account_id_row["id"] if account_id_row else None
        account_row = (await session.execute(
            text("SELECT * FROM communication_accounts WHERE id = :id"), {"id": account_id}
        )).mappings().first() if account_id else None
        if not account_row:
            raise HTTPException(status_code=400, detail="No text account configured for task execution")
        recipient = recipient or (_dedupe_by_phone(lead.get("contact_phones"))[0] if _dedupe_by_phone(lead.get("contact_phones")) else "")
        if not recipient:
            raise HTTPException(status_code=400, detail="No phone recipient available")
        if not _first_sms_mobile_or_none([recipient]):
            raise HTTPException(status_code=400, detail="Recipient is not a valid AU mobile (must be 04xxxxxxxx or +614xxxxxxxx)")
        message_body = body.message or task.get("message_preview") or _message_bundle(lead, "nurture_sms")["body"]
        account_data = dict(account_row)
        if account_data.get("provider") == "zoom":
            if body.dry_run or not _bool_db(account_data.get("send_enabled")):
                transport = {"status": "dry_run", "channel": "sms", "recipient": recipient, "message_preview": message_body[:160]}
            else:
                transport = _zoom_request(
                    account_data,
                    "POST",
                    account_data.get("send_path") or "/phone/messages",
                    {"to": recipient, "from": account_data.get("from_number"), "message": message_body},
                )
                if not transport.get("ok"):
                    raise HTTPException(status_code=400, detail=f"Zoom text send failed: {transport.get('error', 'Unknown error')}")
        else:
            transport = _send_http_text(account_data, recipient, message_body)
        note_history = _append_stage_note(
            lead.get("stage_note_history"),
            message_body,
            lead.get("status") or "captured",
            account_data.get("provider") or "sms",
            f"Text via {account_data.get('label')}",
            recipient,
        )
        activity_log = _append_activity(
            lead.get("activity_log"),
            _build_activity_entry("text_dry_run" if transport.get("status") == "dry_run" else "text_sent", message_body, lead.get("status"), "sms", task.get("title"), recipient),
        )
        await session.execute(
            text("UPDATE leads SET stage_note_history = :nh, activity_log = :al, last_outbound_at = :now, updated_at = :now WHERE id = :id"),
            {"nh": json.dumps(note_history), "al": json.dumps(activity_log), "now": now, "id": lead["id"]},
        )
    else:
        activity_log = _append_activity(
            lead.get("activity_log"),
            _build_activity_entry("task_completed", f"{task['title']} completed", lead.get("status"), task.get("channel"), task["title"]),
        )
        await session.execute(text("UPDATE leads SET activity_log = :log, updated_at = :now WHERE id = :id"), {"log": json.dumps(activity_log), "now": now, "id": lead["id"]})

    await session.execute(
        text("UPDATE tasks SET status = 'completed', approval_status = 'approved', completed_at = :now, updated_at = :now, message_subject = :subject, message_preview = :preview WHERE id = :id"),
        {"now": now, "subject": task.get("message_subject") or "", "preview": body.message or task.get("message_preview") or "", "id": task_id},
    )
    await _refresh_lead_next_action(session, lead["id"])
    await session.commit()
    await refresh_hermes_for_lead(session, lead["id"], actor="task_execute")
    updated_row = (await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})).mappings().first()
    updated_lead = _hydrate_lead(await _get_lead_or_404(session, lead["id"]))
    return {"status": "ok", "transport": transport, "task": _task_to_dict(dict(updated_row) if updated_row else {}), "lead": updated_lead}


@router.post("/api/leads/{lead_id}/kill-sequence")
async def kill_lead_sequence(lead_id: str, body: KillSequenceRequest, api_key: APIKeyDep, session: SessionDep):
    lead_row = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    if not lead_row:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead = _decode_row(lead_row)
    before_state = dict(lead_row)
    now = now_iso()
    suppress_until = body.suppress_until or "2099-12-31T23:59:59+11:00"
    reason = (body.reason or "Manual operator override").strip()

    cancelled = await session.execute(
        text(
            """
            UPDATE tasks
            SET status = 'superseded',
                superseded_by = 'kill_sequence',
                notes = CASE WHEN COALESCE(notes, '') = '' THEN :note ELSE notes || CHAR(10) || :note END,
                updated_at = :now
            WHERE lead_id = :lead_id
              AND status = 'pending'
              AND COALESCE(superseded_by, '') = ''
            """
        ),
        {"lead_id": lead_id, "now": now, "note": f"Sequence killed: {reason}"},
    )

    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry("kill_sequence", f"{reason}; pending_tasks_cancelled={cancelled.rowcount}", lead.get("status"), "system", "Kill Sequence"),
    )
    await session.execute(
        text(
            """
            UPDATE leads
            SET do_not_contact_until = :until_ts,
                do_not_call = CASE WHEN :mark_dnc = 1 THEN 1 ELSE COALESCE(do_not_call, 0) END,
                followup_status = 'paused',
                stage_note = CASE
                    WHEN COALESCE(stage_note, '') = '' THEN :note
                    ELSE stage_note || CHAR(10) || :note
                END,
                activity_log = :activity_log,
                updated_at = :now
            WHERE id = :id
            """
        ),
        {
            "until_ts": suppress_until,
            "mark_dnc": 1 if body.mark_do_not_call else 0,
            "note": f"[KILL SEQUENCE] {reason}",
            "activity_log": json.dumps(activity_log),
            "now": now,
            "id": lead_id,
        },
    )
    after_row = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    if after_row:
        await write_lead_audit_log(
            session,
            lead_id=lead_id,
            action="kill_sequence",
            source="operator_command",
            actor="operator",
            before_state=before_state,
            after_state=dict(after_row),
            payload={"reason": reason, "cancelled_tasks": int(cancelled.rowcount or 0)},
        )
    await _refresh_lead_next_action(session, lead_id)
    await session.commit()
    await refresh_hermes_for_lead(session, lead_id, actor="kill_sequence")
    updated_lead = _hydrate_lead(await _get_lead_or_404(session, lead_id))
    return {"status": "ok", "lead": updated_lead, "cancelled_tasks": int(cancelled.rowcount or 0), "suppressed_until": suppress_until}


@router.get("/api/leads/{lead_id}/audit-log")
async def get_lead_audit_log(lead_id: str, api_key: APIKeyDep, session: SessionDep, limit: int = 50):
    limit = max(1, min(int(limit or 50), 200))
    rows = (
        await session.execute(
            text(
                """
                SELECT id, action, source, actor, batch_id, payload, created_at
                FROM lead_audit_log
                WHERE lead_id = :lead_id
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            {"lead_id": lead_id, "limit": limit},
        )
    ).mappings().all()
    return [dict(r) for r in rows]


@router.post("/api/leads/{lead_id}/restore-from-audit/{audit_id}")
async def restore_lead_from_audit(
    lead_id: str,
    audit_id: str,
    body: RestoreLeadFromAuditRequest,
    api_key: APIKeyDep,
    session: SessionDep,
):
    audit = (
        await session.execute(
            text("SELECT * FROM lead_audit_log WHERE id = :id AND lead_id = :lead_id"),
            {"id": audit_id, "lead_id": lead_id},
        )
    ).mappings().first()
    if not audit:
        raise HTTPException(status_code=404, detail="Audit entry not found")

    target_key = "after_state" if (body.restore_to or "").lower() == "after" else "before_state"
    try:
        snapshot = json.loads(audit.get(target_key) or "{}")
    except json.JSONDecodeError:
        snapshot = {}
    if not isinstance(snapshot, dict) or not snapshot:
        raise HTTPException(status_code=400, detail="Audit snapshot is empty; cannot restore")

    current = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    if not current:
        raise HTTPException(status_code=404, detail="Lead not found")
    before_state = dict(current)

    allowed_keys = [k for k in snapshot.keys() if k not in {"id", "created_at"}]
    if not allowed_keys:
        raise HTTPException(status_code=400, detail="No restorable fields found in snapshot")

    params: Dict[str, Any] = {"id": lead_id, "updated_at": now_iso()}
    assignments: List[str] = []
    for key in allowed_keys:
        assignments.append(f"{key} = :{key}")
        params[key] = snapshot.get(key)
    assignments.append("updated_at = :updated_at")
    await session.execute(text(f"UPDATE leads SET {', '.join(assignments)} WHERE id = :id"), params)

    after = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    if after:
        await write_lead_audit_log(
            session,
            lead_id=lead_id,
            action="restore_from_audit",
            source="manual_recovery",
            actor="operator",
            before_state=before_state,
            after_state=dict(after),
            payload={"audit_id": audit_id, "restore_to": target_key, "reason": body.reason or ""},
        )
    await _refresh_lead_next_action(session, lead_id)
    await session.commit()
    await refresh_hermes_for_lead(session, lead_id, actor="audit_restore")
    updated_lead = _hydrate_lead(await _get_lead_or_404(session, lead_id))
    return {"status": "ok", "lead": updated_lead, "restored_from": audit_id, "restore_to": target_key}

