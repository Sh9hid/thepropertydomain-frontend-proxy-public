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
from core.security import get_api_key
from services.zoom_call_analysis_service import analyze_zoom_call_for_lead
from services.zoom_recording_sync_service import (
    build_zoom_endpoint_validation,
    get_zoom_capabilities,
    infer_zoom_product,
    log_zoom_runtime_status,
    sync_zoom_recordings,
    verify_zoom_webhook_request,
)
from services.hermes_lead_ops_service import refresh_hermes_for_lead
from services.integrations import _send_email_graph
from services.inbound_email_sync_service import (
    _load_imap_accounts,
    _parse_aliases,
    poll_inbound_email_imap,
)

router = APIRouter()


def _env_csv(name: str) -> List[str]:
    raw = os.getenv(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


_OOO_PATTERNS = [
    re.compile(r"\bout of office\b", re.I),
    re.compile(r"\bauto(?:matic)?\s*reply\b", re.I),
    re.compile(r"\baway from (?:the )?(?:office|desk)\b", re.I),
    re.compile(r"\bi(?:'| a)m away\b", re.I),
    re.compile(r"\bannual leave\b", re.I),
    re.compile(r"\bon leave\b", re.I),
    re.compile(r"\bback on\b", re.I),
    re.compile(r"\breturn(?:ing)?\s+(?:on|at)\b", re.I),
]


def _looks_like_ooo(message: str) -> bool:
    text_value = str(message or "").strip()
    if not text_value:
        return False
    return any(pattern.search(text_value) for pattern in _OOO_PATTERNS)


def _ooo_pause_until_iso(message: str) -> str:
    """
    Conservative pause window for autoresponses:
    - if message includes a concrete dd/mm(/yyyy) date, use that date 10:00 Sydney.
    - otherwise default to +7 days.
    """
    now = now_sydney()
    text_value = str(message or "")
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\b", text_value)
    if m:
        day = int(m.group(1))
        month = int(m.group(2))
        year_raw = m.group(3)
        year = int(year_raw) if year_raw else now.year
        if year < 100:
            year += 2000
        try:
            dt = datetime.datetime(year, month, day, 10, 0, tzinfo=SYDNEY_TZ)
            if dt < now:
                dt = dt + datetime.timedelta(days=365)
            return dt.replace(microsecond=0).isoformat()
        except Exception:
            pass
    return (now + datetime.timedelta(days=7)).replace(microsecond=0).isoformat()


async def _send_inbound_escalation_email(
    *,
    recipients: List[str],
    lead: Dict[str, Any],
    inbound: InboundCommunicationRequest,
) -> int:
    if not recipients:
        return 0
    lead_name = str(lead.get("owner_name") or "Unknown lead")
    lead_address = str(lead.get("address") or "Unknown address")
    subject = f"[Speed-to-Lead] Inbound reply: {lead_name} ({lead_address})"
    body = (
        f"<p><strong>Inbound reply received.</strong></p>"
        f"<p><strong>Lead:</strong> {html.escape(lead_name)}<br/>"
        f"<strong>Address:</strong> {html.escape(lead_address)}<br/>"
        f"<strong>From:</strong> {html.escape(inbound.from_number)}<br/>"
        f"<strong>Message:</strong> {html.escape(inbound.message)}</p>"
        f"<p>Target callback SLA: <strong>within 5 minutes</strong>.</p>"
    )
    sent = 0
    for recipient in recipients:
        ok = await asyncio.to_thread(_send_email_graph, recipient, subject, body, False)
        if ok:
            sent += 1
    return sent


async def _create_inbound_draft_reply_task(
    session: AsyncSession,
    *,
    lead_id: str,
    inbound_message: str,
) -> Optional[str]:
    task_id = hashlib.md5(f"inbound-draft:{lead_id}:{now_iso()}".encode()).hexdigest()
    preview = (
        "Thanks for your message. I can help with a quick update on your property options. "
        "Are you free for a 10-minute call today?"
    )
    await session.execute(
        text(
            """
            INSERT INTO tasks (
                id, lead_id, title, task_type, action_type, channel, due_at, status, notes,
                related_report_id, approval_status, message_subject, message_preview, rewrite_reason,
                superseded_by, cadence_name, cadence_step, auto_generated, priority_bucket, payload_json,
                attempt_count, last_error, completed_at, created_at, updated_at
            ) VALUES (
                :id, :lead_id, :title, 'follow_up', 'follow_up', 'email', :due_at, 'pending', :notes,
                '', 'pending', :subject, :preview, 'Inbound reply triage draft', '', 'inbound_reply',
                0, 1, 'follow_up', :payload_json, 0, NULL, NULL, :created_at, :updated_at
            )
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {
            "id": task_id,
            "lead_id": lead_id,
            "title": "Reply to inbound lead",
            "due_at": now_iso(),
            "notes": "Draft created from inbound reply. Review and approve before send.",
            "subject": "Re: Your message",
            "preview": preview,
            "payload_json": json.dumps(
                {
                    "source": "inbound_reply",
                    "inbound_message": inbound_message[:2000],
                    "delivery_status": "drafted",
                    "requires_speed_to_lead": True,
                }
            ),
            "created_at": now_iso(),
            "updated_at": now_iso(),
        },
    )
    return task_id

@router.post("/api/recordings/{call_id}/analyze")
async def analyze_recording(call_id: str, lead_id: str, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    try:
        return await analyze_zoom_call_for_lead(session, lead_id, call_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"External API error: {exc.response.text}") from exc
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"External request failed: {exc}") from exc


@router.get("/api/email-accounts")
async def get_email_accounts(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    res = await session.execute(text("SELECT * FROM email_accounts ORDER BY updated_at DESC, created_at DESC"))
    return [dict(row) for row in res.mappings().all()]


@router.post("/api/email-accounts")
async def save_email_account(body: EmailAccount, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    account_id = body.id or hashlib.md5(f"{body.label}:{body.smtp_username}".encode()).hexdigest()
    now = now_iso()
    await session.execute(
        text("""
        INSERT INTO email_accounts (id, label, smtp_host, smtp_port, smtp_username, smtp_password, from_name, from_email, use_tls, daily_cap, is_warmup_mode, warmup_day, is_active, created_at, updated_at)
        VALUES (:id, :label, :smtp_host, :smtp_port, :smtp_username, :smtp_password, :from_name, :from_email, :use_tls, :daily_cap, :is_warmup_mode, :warmup_day, :is_active, :created_at, :updated_at)
        ON CONFLICT(id) DO UPDATE SET
            label=EXCLUDED.label, smtp_host=EXCLUDED.smtp_host, smtp_port=EXCLUDED.smtp_port,
            smtp_username=EXCLUDED.smtp_username, smtp_password=EXCLUDED.smtp_password, from_name=EXCLUDED.from_name,
            from_email=EXCLUDED.from_email, use_tls=EXCLUDED.use_tls, daily_cap=EXCLUDED.daily_cap,
            is_warmup_mode=EXCLUDED.is_warmup_mode, warmup_day=EXCLUDED.warmup_day, is_active=EXCLUDED.is_active,
            updated_at=EXCLUDED.updated_at
        """),
        {
            "id": account_id,
            "label": body.label,
            "smtp_host": body.smtp_host,
            "smtp_port": body.smtp_port,
            "smtp_username": body.smtp_username,
            "smtp_password": body.smtp_password,
            "from_name": body.from_name or "",
            "from_email": body.from_email or body.smtp_username,
            "use_tls": 1 if body.use_tls else 0,
            "daily_cap": body.daily_cap,
            "is_warmup_mode": 1 if body.is_warmup_mode else 0,
            "warmup_day": body.warmup_day,
            "is_active": 1 if body.is_active else 0,
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.commit()
    return {"status": "ok", "id": account_id}


@router.get("/api/communication-accounts")
async def get_communication_accounts(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    res = await session.execute(text("SELECT * FROM communication_accounts ORDER BY updated_at DESC, created_at DESC"))
    # _serialize_communication_account not defined, simple dict
    return [dict(row) for row in res.mappings().all()]


@router.post("/api/communication-accounts")
async def save_communication_account(body: CommunicationAccount, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    account_id = body.id or hashlib.md5(f"{body.provider}:{body.label}:{body.from_number}".encode()).hexdigest()
    now = now_iso()
    await session.execute(
        text("""
        INSERT INTO communication_accounts (
            id, label, provider, api_base, access_token, send_path, from_number, webhook_secret,
            client_id, client_secret, account_id, token_url, webhook_url, use_account_credentials,
            send_enabled, call_enabled, text_enabled, verify_ssl, created_at, updated_at
        )
        VALUES (:id, :label, :provider, :api_base, :access_token, :send_path, :from_number, :webhook_secret,
            :client_id, :client_secret, :account_id, :token_url, :webhook_url, :use_account_credentials,
            :send_enabled, :call_enabled, :text_enabled, :verify_ssl, :created_at, :updated_at)
        ON CONFLICT(id) DO UPDATE SET
            label=EXCLUDED.label, provider=EXCLUDED.provider, api_base=EXCLUDED.api_base, access_token=EXCLUDED.access_token,
            send_path=EXCLUDED.send_path, from_number=EXCLUDED.from_number, webhook_secret=EXCLUDED.webhook_secret,
            client_id=EXCLUDED.client_id, client_secret=EXCLUDED.client_secret, account_id=EXCLUDED.account_id,
            token_url=EXCLUDED.token_url, webhook_url=EXCLUDED.webhook_url, use_account_credentials=EXCLUDED.use_account_credentials,
            send_enabled=EXCLUDED.send_enabled, call_enabled=EXCLUDED.call_enabled, text_enabled=EXCLUDED.text_enabled,
            verify_ssl=EXCLUDED.verify_ssl, updated_at=EXCLUDED.updated_at
        """),
        {
            "id": account_id,
            "label": body.label,
            "provider": body.provider,
            "api_base": body.api_base,
            "access_token": body.access_token,
            "send_path": body.send_path,
            "from_number": body.from_number,
            "webhook_secret": body.webhook_secret or "",
            "client_id": body.client_id,
            "client_secret": body.client_secret,
            "account_id": body.account_id,
            "token_url": body.token_url,
            "webhook_url": body.webhook_url,
            "use_account_credentials": 1 if body.use_account_credentials else 0,
            "send_enabled": 1 if body.send_enabled else 0,
            "call_enabled": 1 if body.call_enabled else 0,
            "text_enabled": 1 if body.text_enabled else 0,
            "verify_ssl": 1 if body.verify_ssl else 0,
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.commit()
    return {"status": "ok", "id": account_id}


@router.post("/api/communications/inbound")
async def receive_inbound_communication(body: InboundCommunicationRequest, session: AsyncSession = Depends(get_session)):
    res = await session.execute(
        text("SELECT * FROM communication_accounts WHERE provider = :provider AND (:ws = '' OR webhook_secret = :ws)"),
        {"provider": body.provider, "ws": body.webhook_secret or ""}
    )
    account = res.mappings().first()
    if body.webhook_secret and not account:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")
        
    normalized_from = _normalize_phone(body.from_number)
    # Use last 9 digits so +61412345678 matches 0412345678 stored either way
    phone_digits = re.sub(r'\D', '', normalized_from)[-9:]
    res_lead = await session.execute(
        text("SELECT * FROM leads WHERE contact_phones LIKE :p LIMIT 1"),
        {"p": f"%{phone_digits}%"},
    )
    matched_row = res_lead.mappings().first()
    if not matched_row:
        return {"status": "ignored", "reason": "No matching lead"}

    lead = _decode_row(matched_row)
    is_ooo = _looks_like_ooo(body.message)
    if is_ooo:
        pause_until = _ooo_pause_until_iso(body.message)
        activity_log = _append_activity(
            lead.get("activity_log"),
            _build_activity_entry(
                "auto_reply_ooo",
                f"Autoresponder detected; paused cadence until {pause_until}. Message: {body.message[:240]}",
                lead.get("status"),
                body.provider,
                "OOO autoresponse",
                body.from_number,
            ),
        )
        await session.execute(
            text(
                """
                UPDATE leads
                SET do_not_contact_until = :pause_until,
                    activity_log = :activity_log,
                    stage_note = :stage_note,
                    updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {
                "pause_until": pause_until,
                "activity_log": json.dumps(activity_log),
                "stage_note": f"Auto-reply detected (OOO). Paused until {pause_until}.",
                "updated_at": now_iso(),
                "id": lead["id"],
            },
        )
        await session.commit()
        await refresh_hermes_for_lead(session, lead["id"], actor="inbound_ooo")
        return {
            "status": "ignored_auto_reply",
            "lead_id": lead["id"],
            "paused_until": pause_until,
            "reason": "Out-of-office autoresponse detected",
        }

    note_history = _append_stage_note(
        lead.get("stage_note_history"),
        body.message,
        lead.get("status") or "captured",
        f"{body.provider}_inbound",
        "Inbound text",
        body.from_number,
    )
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry("text_received", body.message, lead.get("status"), body.provider, "Inbound text", body.from_number),
    )
    
    await session.execute(
        text("UPDATE leads SET stage_note_history = :nh, activity_log = :al, last_inbound_at = :lia, updated_at = :upd WHERE id = :id"),
        {"nh": json.dumps(note_history), "al": json.dumps(activity_log), "lia": now_iso(), "upd": now_iso(), "id": lead["id"]}
    )
    draft_task_id = await _create_inbound_draft_reply_task(session, lead_id=lead["id"], inbound_message=body.message)
    escalation_recipients = _env_csv("INBOUND_ESCALATION_EMAILS") or _env_csv("OPERATOR_ALERT_EMAILS")
    escalation_sent = await _send_inbound_escalation_email(
        recipients=escalation_recipients,
        lead=lead,
        inbound=body,
    )
    await session.commit()
    await refresh_hermes_for_lead(session, lead["id"], actor="inbound_communication")
    return {
        "status": "ok",
        "lead_id": lead["id"],
        "draft_task_id": draft_task_id,
        "escalation_alerts_sent": escalation_sent,
    }


def _twilio_signature_valid(request_url: str, params: Dict[str, str], signature: str) -> bool:
    auth_token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    if not auth_token:
        # Keep local/dev operable when token is not configured.
        return True
    try:
        from twilio.request_validator import RequestValidator

        validator = RequestValidator(auth_token)
        return bool(validator.validate(request_url, params, signature or ""))
    except Exception:
        return False


@router.post("/api/twilio/webhook/sms")
async def receive_twilio_inbound_sms(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    form = await request.form()
    payload = {str(k): str(v) for k, v in form.items()}
    signature = request.headers.get("X-Twilio-Signature", "")
    if not _twilio_signature_valid(str(request.url), payload, signature):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    inbound = InboundCommunicationRequest(
        provider="twilio",
        from_number=payload.get("From", ""),
        to_number=payload.get("To", ""),
        message=payload.get("Body", ""),
        direction="inbound",
    )
    result = await receive_inbound_communication(inbound, session)
    lead_id = str((result or {}).get("lead_id") or "")
    if lead_id:
        try:
            await session.execute(
                text(
                    """
                    UPDATE leads
                    SET queue_bucket = CASE
                            WHEN COALESCE(queue_bucket, '') IN ('booked', 'active')
                                THEN queue_bucket
                            ELSE 'callback_due'
                        END,
                        next_action_type = 'follow_up',
                        next_action_channel = 'sms',
                        next_action_reason = 'Inbound SMS reply received',
                        updated_at = :updated_at
                    WHERE id = :lead_id
                    """
                ),
                {"updated_at": now_iso(), "lead_id": lead_id},
            )
            await session.commit()
            await refresh_hermes_for_lead(session, lead_id, actor="twilio_inbound_sms")
        except Exception:
            pass
    return {"ok": True, "result": result}


@router.post("/api/twilio/webhook/status")
async def receive_twilio_status_callback(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    form = await request.form()
    payload = {str(k): str(v) for k, v in form.items()}
    signature = request.headers.get("X-Twilio-Signature", "")
    if not _twilio_signature_valid(str(request.url), payload, signature):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    from_number = _normalize_phone(payload.get("From", ""))
    phone_digits = re.sub(r"\D", "", from_number)[-9:]
    lead_row = (
        await session.execute(
            text("SELECT * FROM leads WHERE contact_phones LIKE :p LIMIT 1"),
            {"p": f"%{phone_digits}%"},
        )
    ).mappings().first()
    if not lead_row:
        return {"ok": True, "matched": False}

    lead = _decode_row(lead_row)
    status = str(payload.get("MessageStatus", "")).strip().lower()
    sid = str(payload.get("MessageSid", "")).strip()
    error_code = str(payload.get("ErrorCode", "")).strip()
    note = f"Twilio status update: {status or 'unknown'} sid={sid}"
    if error_code:
        note += f" error={error_code}"

    note_history = _append_stage_note(
        lead.get("stage_note_history"),
        note,
        lead.get("status") or "captured",
        "twilio_status",
        "Twilio status callback",
        payload.get("To", ""),
    )
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry("sms_status", note, lead.get("status"), "twilio", "Twilio status callback", payload.get("To", "")),
    )
    await session.execute(
        text("UPDATE leads SET stage_note_history = :nh, activity_log = :al, updated_at = :upd WHERE id = :id"),
        {"nh": json.dumps(note_history), "al": json.dumps(activity_log), "upd": now_iso(), "id": lead["id"]},
    )
    await session.commit()
    return {"ok": True, "matched": True, "lead_id": lead["id"]}


class ZoomSMSRequest(BaseModel):
    lead_id: Optional[str] = None
    recipient: str
    message: str


class InboundEmailWebhookRequest(BaseModel):
    provider: str = "imap"
    from_email: str
    to_email: str
    subject: str = ""
    body: str = ""


class InboundImapPollRequest(BaseModel):
    limit_per_account: int = 25


@router.post("/api/email/webhooks/inbound")
async def receive_inbound_email(body: InboundEmailWebhookRequest, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    monitored_aliases = {v.lower() for v in (_env_csv("INBOUND_EMAIL_ALIASES") or []) if v}
    reply_to = (os.getenv("OUTBOUND_REPLY_TO") or "info@thepropertydomain.com.au").strip().lower()
    if reply_to:
        monitored_aliases.add(reply_to)

    sender = (body.from_email or "").strip().lower()
    recipient = (body.to_email or "").strip().lower()
    if not sender:
        raise HTTPException(status_code=400, detail="from_email is required")

    if monitored_aliases and recipient and recipient not in monitored_aliases:
        # Accept and log anyway, but indicate alias is not currently monitored.
        alias_warning = True
    else:
        alias_warning = False

    lead_row = (
        await session.execute(
            text(
                """
                SELECT * FROM leads
                WHERE LOWER(CAST(contact_emails AS TEXT)) LIKE :needle
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ),
            {"needle": f"%{sender}%"},
        )
    ).mappings().first()
    if not lead_row:
        return {"status": "ignored", "reason": "No lead matched sender email", "alias_warning": alias_warning}

    lead = _decode_row(lead_row)
    note = (body.body or body.subject or "").strip()
    note_history = _append_stage_note(
        lead.get("stage_note_history"),
        note,
        lead.get("status") or "captured",
        f"{body.provider}_inbound_email",
        body.subject or "Inbound email",
        sender,
    )
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry("email_received", note[:500], lead.get("status"), "email", body.subject or "Inbound email", sender),
    )
    await session.execute(
        text("UPDATE leads SET stage_note_history = :nh, activity_log = :al, last_inbound_at = :lia, updated_at = :upd WHERE id = :id"),
        {"nh": json.dumps(note_history), "al": json.dumps(activity_log), "lia": now_iso(), "upd": now_iso(), "id": lead["id"]},
    )
    draft_task_id = await _create_inbound_draft_reply_task(session, lead_id=lead["id"], inbound_message=note or "(inbound email)")
    await session.commit()
    await refresh_hermes_for_lead(session, lead["id"], actor="inbound_email")
    return {"status": "ok", "lead_id": lead["id"], "draft_task_id": draft_task_id, "alias_warning": alias_warning}


@router.get("/api/email/imap/status")
async def get_inbound_imap_status(api_key: str = Depends(get_api_key)):
    accounts = _load_imap_accounts()
    aliases = sorted(_parse_aliases())
    return {
        "enabled": len(accounts) > 0,
        "accounts_count": len(accounts),
        "accounts": [
            {
                "host": account.get("host"),
                "port": int(account.get("port") or 993),
                "username": account.get("username"),
                "folder": account.get("folder") or "INBOX",
                "email": account.get("email"),
            }
            for account in accounts
        ],
        "monitored_aliases": aliases,
        "poll_seconds": max(30, int(os.getenv("INBOUND_EMAIL_IMAP_POLL_SECONDS", "60"))),
    }


@router.post("/api/email/imap/poll-now")
async def poll_inbound_imap_now(body: InboundImapPollRequest, api_key: str = Depends(get_api_key)):
    limit_per_account = max(1, min(int(body.limit_per_account or 25), 100))
    result = await poll_inbound_email_imap(limit_per_account=limit_per_account)
    return {"status": "ok", "result": result, "limit_per_account": limit_per_account}


class ZoomRecordingSyncRequest(BaseModel):
    meeting_id: Optional[str] = None
    meeting_uuid: Optional[str] = None
    call_id: Optional[str] = None
    user: Optional[str] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    dry_run: bool = False
    verbose: bool = False


class ZoomRecentPhoneSyncRequest(BaseModel):
    lead_id: Optional[str] = None
    lookback_days: int = 7
    dry_run: bool = True


@router.post("/api/zoom/sms")
async def send_zoom_sms(body: ZoomSMSRequest, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """Send SMS via Zoom Phone REST API using Server-to-Server OAuth."""
    recipient_digits = re.sub(r"\D+", "", body.recipient or "")
    if not re.fullmatch(r"(?:04\d{8}|614\d{8})", recipient_digits):
        raise HTTPException(status_code=400, detail="Recipient is not an AU SMS-capable mobile (must be 04xxxxxxxx or +614xxxxxxxx)")

    client_id = os.getenv("ZOOM_CLIENT_ID", "")
    client_secret = os.getenv("ZOOM_CLIENT_SECRET", "")
    account_id = os.getenv("ZOOM_ACCOUNT_ID", "")
    from_number = os.getenv("ZOOM_FROM_NUMBER", os.getenv("PRINCIPAL_PHONE", ""))

    if not (client_id and client_secret and account_id):
        raise HTTPException(status_code=400, detail="Zoom credentials not configured (ZOOM_CLIENT_ID/SECRET/ACCOUNT_ID)")

    # Exchange credentials for access token
    token_url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={account_id}"
    auth_header = b64encode(f"{client_id}:{client_secret}".encode()).decode("ascii")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            token_resp = await client.post(
                token_url,
                headers={"Authorization": f"Basic {auth_header}", "Content-Type": "application/x-www-form-urlencoded"},
            )
        if token_resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Zoom token error: {token_resp.text}")
        access_token = token_resp.json().get("access_token")
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Zoom token request failed: {exc}")

    # Send SMS via Zoom Phone API
    sms_payload = {
        "from": from_number,
        "to_contact": [{"phone_number": body.recipient}],
        "message": body.message,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            sms_resp = await client.post(
                "https://api.zoom.us/v2/phone/sms",
                headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                json=sms_payload,
            )
        if sms_resp.status_code not in (200, 201, 204):
            raise HTTPException(status_code=502, detail=f"Zoom SMS error {sms_resp.status_code}: {sms_resp.text}")
        resp_data = sms_resp.json() if sms_resp.content else {}
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Zoom SMS request failed: {exc}")

    sid = resp_data.get("id") or resp_data.get("message_id") or ""

    # Log to outreach_log
    if body.lead_id:
        try:
            await session.execute(
                text("""
                INSERT INTO outreach_log
                    (lead_id, channel, provider, recipient, subject, sent_at, status, provider_message_id)
                VALUES (:lead_id, 'sms', 'zoom', :recipient, :subject, :sent_at, 'sent', :provider_id)
                """),
                {
                    "lead_id": body.lead_id,
                    "recipient": body.recipient,
                    "subject": body.message[:160],
                    "sent_at": now_iso(),
                    "provider_id": sid,
                },
            )
            await session.commit()
        except Exception:
            pass  # outreach_log table may not exist yet
        lead_row = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": body.lead_id})).mappings().first()
        if lead_row:
            lead = _decode_row(lead_row)
            note_history = _append_stage_note(
                lead.get("stage_note_history"),
                body.message,
                lead.get("status") or "captured",
                "sms",
                "SMS via Zoom",
                body.recipient,
            )
            activity_log = _append_activity(
                lead.get("activity_log"),
                _build_activity_entry("text_sent", body.message, lead.get("status"), "sms", "SMS via Zoom", body.recipient),
            )
            await session.execute(
                text("UPDATE leads SET stage_note_history = :stage_note_history, activity_log = :activity_log, last_outbound_at = :last_outbound_at, updated_at = :updated_at WHERE id = :id"),
                {
                    "stage_note_history": json.dumps(note_history),
                    "activity_log": json.dumps(activity_log),
                    "last_outbound_at": now_iso(),
                    "updated_at": now_iso(),
                    "id": body.lead_id,
                },
            )
            await session.commit()
    updated = None
    if body.lead_id:
        updated_row = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": body.lead_id})).mappings().first()
        if updated_row:
            updated = _hydrate_lead(updated_row)

    return {"ok": True, "sid": sid, "lead": updated}


@router.post("/api/zoom/verify")
async def verify_zoom_account(body: ZoomVerificationRequest, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    res = await session.execute(
        text("SELECT * FROM communication_accounts WHERE id = :id AND provider = 'zoom'"),
        {"id": body.account_id}
    )
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Zoom account not found")
    account = dict(row)
    return {"status": "ok", "product": infer_zoom_product(account), "capabilities": get_zoom_capabilities(account)}


@router.get("/api/zoom/status")
async def get_zoom_status(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    account = await _resolve_zoom_account(session)
    capabilities = get_zoom_capabilities(account)
    log_zoom_runtime_status(account)
    return {
        "configured": True,
        "product": capabilities["product"],
        "recordings_supported": capabilities["recordings_supported"],
        "ai_summary_supported": capabilities["ai_summary_supported"],
        "reason": capabilities["reason"],
    }


@router.post("/api/zoom/sync")
async def sync_zoom_recordings_endpoint(
    body: ZoomRecordingSyncRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    return await sync_zoom_recordings(
        session,
        {
            "meeting_id": body.meeting_id,
            "meeting_uuid": body.meeting_uuid,
            "call_id": body.call_id,
            "user": body.user,
            "from": body.from_date,
            "to": body.to_date,
            "dry_run": body.dry_run,
            "verbose": body.verbose,
        },
    )


@router.post("/api/zoom/phone/sync-recent")
async def sync_recent_zoom_phone_activity(
    body: ZoomRecentPhoneSyncRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    from services.zoom_call_sync_service import sync_zoom_calls_for_date

    lookback_days = max(1, min(int(body.lookback_days or 7), 14))
    today = now_sydney().date()
    day_results: List[Dict[str, Any]] = []
    imported = 0
    updated = 0
    skipped = 0
    fetched = 0

    for offset in range(lookback_days):
        target_date = (today - datetime.timedelta(days=offset)).isoformat()
        result = await sync_zoom_calls_for_date(session, target_date, force=True)
        day_results.append({"date": target_date, **result})
        if result.get("ok"):
            imported += int(result.get("imported") or 0)
            updated += int(result.get("updated") or 0)
            skipped += int(result.get("skipped") or 0)
            fetched += int(result.get("fetched") or 0)

    recording_result = await sync_zoom_recordings(
        session,
        {
            "from": (today - datetime.timedelta(days=lookback_days - 1)).isoformat(),
            "to": today.isoformat(),
            "dry_run": body.dry_run,
            "verbose": True,
        },
    )

    matched_count = 0
    unmatched_count = 0
    if body.lead_id:
        matched_count = int(
            (
                await session.execute(
                    text("SELECT COUNT(*) FROM call_log WHERE provider = 'zoom' AND lead_id = :lead_id"),
                    {"lead_id": body.lead_id},
                )
            ).scalar_one()
            or 0
        )
    unmatched_count = int(
        (
            await session.execute(
                text("SELECT COUNT(*) FROM call_log WHERE provider = 'zoom' AND COALESCE(lead_id, '') = ''"),
            )
        ).scalar_one()
        or 0
    )

    return {
        "ok": True,
        "lookback_days": lookback_days,
        "dry_run": body.dry_run,
        "lead_id": body.lead_id,
        "fetched": fetched,
        "imported": imported,
        "updated": updated,
        "skipped": skipped,
        "matched_count": matched_count,
        "unmatched_count": unmatched_count,
        "day_results": day_results,
        "recording_sync": recording_result,
    }


@router.post("/api/zoom/webhook")
async def receive_zoom_webhook(request: Request, session: AsyncSession = Depends(get_session)):
    body_bytes = await request.body()
    envelope = ZoomWebhookEnvelope(**json.loads(body_bytes.decode("utf-8") or "{}"))
    res = await session.execute(text("SELECT * FROM communication_accounts WHERE provider = 'zoom' ORDER BY updated_at DESC, created_at DESC"))
    rows = res.mappings().all()
    account_row = None
    for row in rows:
        candidate = dict(row)
        if candidate.get("webhook_secret") and (_has_zoom_credentials(candidate) or _has_zoom_credentials(_get_default_zoom_account())):
            account_row = candidate
            break
            
    if envelope.event == "endpoint.url_validation":
        if not account_row or not account_row.get("webhook_secret"):
            raise HTTPException(status_code=400, detail="Zoom webhook secret is not configured")
        signature = request.headers.get("x-zm-signature", "")
        timestamp = request.headers.get("x-zm-request-timestamp", "")
        if not verify_zoom_webhook_request(str(account_row.get("webhook_secret") or ""), body_bytes, timestamp, signature):
            raise HTTPException(status_code=403, detail="Invalid Zoom webhook signature")
        return build_zoom_endpoint_validation(json.loads(body_bytes.decode("utf-8") or "{}"), str(account_row.get("webhook_secret") or ""))

    if account_row and account_row.get("webhook_secret"):
        signature = request.headers.get("x-zm-signature", "")
        timestamp = request.headers.get("x-zm-request-timestamp", "")
        if signature or timestamp:
            if not verify_zoom_webhook_request(str(account_row.get("webhook_secret") or ""), body_bytes, timestamp, signature):
                raise HTTPException(status_code=403, detail="Invalid Zoom webhook signature")

    event_name = str(envelope.event or "").lower()
    object_payload = (envelope.payload or {}).get("object", {}) if isinstance(envelope.payload, dict) else {}
    if "recording" in event_name:
        return await sync_zoom_recordings(
            session,
            {
                "meeting_uuid": object_payload.get("uuid") or object_payload.get("meeting_uuid"),
                "meeting_id": object_payload.get("id") or object_payload.get("meeting_id"),
                "call_id": object_payload.get("call_id") or object_payload.get("id"),
                "from": object_payload.get("recording_start"),
                "to": object_payload.get("recording_end"),
                "verbose": True,
            },
        )

    payload = envelope.payload or {}
    from_number = str(object_payload.get("from") or object_payload.get("caller_number") or object_payload.get("phone_number") or "")
    to_number = str(object_payload.get("to") or object_payload.get("callee_number") or "")
    message = str(object_payload.get("message") or object_payload.get("body") or envelope.event or "Zoom event")
    direction = "inbound" if "received" in (envelope.event or "").lower() else "event"
    return await receive_inbound_communication(
        InboundCommunicationRequest(
            provider="zoom",
            from_number=from_number,
            to_number=to_number,
            message=message,
            direction=direction,
        ),
        session=session
    )


# ---------------------------------------------------------------------------
# Inbox rotation: daily send counts + cap management
# ---------------------------------------------------------------------------

class EmailCapUpdate(BaseModel):
    daily_cap: Optional[int] = None
    is_warmup_mode: Optional[bool] = None
    warmup_day: Optional[int] = None
    is_active: Optional[bool] = None


@router.get("/api/outreach/daily-send-counts")
async def get_daily_send_counts(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """Return sends_today, daily_cap, and warmup status for each email account."""
    today = now_sydney().date().isoformat()
    res = await session.execute(text("SELECT * FROM email_accounts ORDER BY label ASC"))
    accounts = [dict(row) for row in res.mappings().all()]
    result = []
    for acct in accounts:
        sends = acct.get("sends_today", 0) if acct.get("sends_today_date") == today else 0
        effective_cap = _effective_email_cap(acct, 80)
        result.append({
            "id": acct["id"],
            "label": acct["label"],
            "from_email": acct.get("from_email") or acct.get("smtp_username", ""),
            "sends_today": sends,
            "daily_cap": effective_cap,
            "is_warmup_mode": bool(acct.get("is_warmup_mode")),
            "warmup_day": acct.get("warmup_day", 0),
            "is_active": bool(acct.get("is_active", 1)),
            "remaining": max(0, effective_cap - sends),
        })
    return result


@router.patch("/api/email-accounts/{account_id}/caps")
async def update_email_account_caps(account_id: str, body: EmailCapUpdate, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """Update daily_cap, warmup mode, or active status for an email account."""
    res = await session.execute(text("SELECT id FROM email_accounts WHERE id = :id"), {"id": account_id})
    if not res.mappings().first():
        raise HTTPException(status_code=404, detail="Email account not found")
    updates = {}
    if body.daily_cap is not None:
        updates["daily_cap"] = body.daily_cap
    if body.is_warmup_mode is not None:
        updates["is_warmup_mode"] = 1 if body.is_warmup_mode else 0
    if body.warmup_day is not None:
        updates["warmup_day"] = body.warmup_day
    if body.is_active is not None:
        updates["is_active"] = 1 if body.is_active else 0
    if not updates:
        return {"status": "no_change"}
    updates["updated_at"] = now_iso()
    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["id"] = account_id
    await session.execute(text(f"UPDATE email_accounts SET {set_clause} WHERE id = :id"), updates)
    await session.commit()
    return {"status": "ok"}


def _effective_email_cap(acct: Dict[str, Any], fallback_cap: int = 80) -> int:
    base_cap = max(1, int(os.getenv("EMAIL_WARMUP_START_CAP", "20")))
    ramp_days = max(1, int(os.getenv("EMAIL_WARMUP_RAMP_DAYS", "30")))
    max_cap = max(base_cap, int(acct.get("daily_cap") or fallback_cap))
    if not acct.get("is_warmup_mode"):
        return max_cap
    warmup_day = max(1, int(acct.get("warmup_day") or 1))
    if warmup_day >= ramp_days:
        return max_cap
    span = max_cap - base_cap
    if span <= 0:
        return max_cap
    progress = (warmup_day - 1) / max(1, ramp_days - 1)
    return max(base_cap, min(max_cap, int(round(base_cap + span * progress))))


async def _get_account_for_rotation(session: AsyncSession, cap_per_account: int = 80) -> Optional[str]:
    """
    Return the email_account id with fewest sends today that hasn't hit its cap.
    Resets sends_today counter if date has changed.
    Returns None if all active accounts are capped.
    """
    today = now_sydney().date().isoformat()
    res = await session.execute(
        text("SELECT * FROM email_accounts WHERE COALESCE(is_active, 1) = 1 ORDER BY label ASC")
    )
    accounts = [dict(row) for row in res.mappings().all()]
    if not accounts:
        return None

    best_id = None
    best_sends = 999999

    for acct in accounts:
        # Reset counter if stale
        if acct.get("sends_today_date") != today:
            update_sql = "UPDATE email_accounts SET sends_today = 0, sends_today_date = :today"
            params: Dict[str, Any] = {"today": today, "id": acct["id"]}
            if acct.get("is_warmup_mode"):
                update_sql += ", warmup_day = COALESCE(warmup_day, 0) + 1"
                acct["warmup_day"] = int(acct.get("warmup_day") or 0) + 1
            update_sql += " WHERE id = :id"
            await session.execute(text(update_sql), params)
            acct["sends_today"] = 0
            acct["sends_today_date"] = today

        sends = acct.get("sends_today", 0)
        effective_cap = _effective_email_cap(acct, cap_per_account)
        if sends < effective_cap and sends < best_sends:
            best_sends = sends
            best_id = acct["id"]

    return best_id


async def _increment_account_sends(session: AsyncSession, account_id: str) -> None:
    """Increment sends_today for the given email account."""
    today = now_sydney().date().isoformat()
    await session.execute(
        text("""
        UPDATE email_accounts
        SET sends_today = COALESCE(sends_today, 0) + 1,
            sends_today_date = :today,
            updated_at = :now
        WHERE id = :id
        """),
        {"today": today, "now": now_iso(), "id": account_id},
    )


class DeliverabilityEvent(BaseModel):
    email: str
    event_type: str  # hard_bounce, soft_bounce, complaint, delivered
    provider: str = "smtp"
    account_id: Optional[str] = None
    message_id: Optional[str] = None
    reason: Optional[str] = None


class DeliverabilityWebhookPayload(BaseModel):
    events: List[DeliverabilityEvent] = []


@router.post("/api/email/webhooks/deliverability")
async def receive_deliverability_events(
    body: DeliverabilityWebhookPayload,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """Process provider bounce/complaint webhooks and apply suppression."""
    from models.funnel_schemas import SuppressionUpsertRequest
    from services.funnel_service import apply_lead_suppression, ensure_lead_funnels

    suppressed = 0
    complaints = 0
    hard_bounces = 0
    throttled_accounts = 0

    for event in body.events:
        email = (event.email or "").strip().lower()
        event_type = (event.event_type or "").strip().lower()
        if not email:
            continue
        try:
            await session.execute(
                text(
                    """
                    INSERT INTO email_delivery_events (
                        id, provider, account_id, recipient_email, event_type, message_id, reason, payload_json, created_at
                    ) VALUES (
                        :id, :provider, :account_id, :recipient_email, :event_type, :message_id, :reason, :payload_json, :created_at
                    )
                    """
                ),
                {
                    "id": str(uuid.uuid4()),
                    "provider": event.provider or "smtp",
                    "account_id": event.account_id,
                    "recipient_email": email,
                    "event_type": event_type,
                    "message_id": event.message_id,
                    "reason": event.reason or "",
                    "payload_json": json.dumps(
                        {
                            "provider": event.provider or "smtp",
                            "account_id": event.account_id,
                            "message_id": event.message_id,
                            "reason": event.reason or "",
                        }
                    ),
                    "created_at": now_iso(),
                },
            )
        except Exception:
            # Do not block suppression flow if event table is unavailable.
            pass

        if event_type in {"hard_bounce", "complaint"}:
            lead_row = (
                await session.execute(
                    text(
                        """
                        SELECT id FROM leads
                        WHERE LOWER(CAST(contact_emails AS TEXT)) LIKE :needle
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """
                    ),
                    {"needle": f"%{email}%"},
                )
            ).mappings().first()
            if lead_row:
                lead_id = str(lead_row["id"])
                await ensure_lead_funnels(session, lead_id)
                await apply_lead_suppression(
                    session,
                    lead_id,
                    SuppressionUpsertRequest(
                        channel="email",
                        status="active",
                        reason="Complaint from mailbox provider" if event_type == "complaint" else "Hard bounce from mailbox provider",
                        source=f"{event.provider}_webhook",
                        note=f"{email} | {event.reason or 'No reason provided'}",
                        created_by="deliverability_webhook",
                    ),
                )
                suppressed += 1

        if event.account_id and event_type in {"hard_bounce", "complaint"}:
            try:
                await session.execute(
                    text(
                        """
                        UPDATE email_accounts
                        SET risk_score = COALESCE(risk_score, 0) + :delta,
                            updated_at = :updated_at
                        WHERE id = :id
                        """
                    ),
                    {
                        "delta": 10 if event_type == "complaint" else 6,
                        "updated_at": now_iso(),
                        "id": event.account_id,
                    },
                )
            except Exception:
                # Legacy schemas may not have risk_score yet.
                pass

            # Protect sender reputation: automatically pause inboxes crossing high risk.
            try:
                risk_row = (
                    await session.execute(
                        text("SELECT COALESCE(risk_score, 0) AS risk_score FROM email_accounts WHERE id = :id"),
                        {"id": event.account_id},
                    )
                ).mappings().first()
                risk_score = int((risk_row or {}).get("risk_score") or 0)
                risk_pause_threshold = int(os.getenv("EMAIL_RISK_PAUSE_THRESHOLD", "80"))
                if risk_score >= risk_pause_threshold:
                    await session.execute(
                        text(
                            """
                            UPDATE email_accounts
                            SET is_active = 0,
                                updated_at = :updated_at
                            WHERE id = :id
                            """
                        ),
                        {"updated_at": now_iso(), "id": event.account_id},
                    )
                    throttled_accounts += 1
            except Exception:
                pass

        if event_type == "hard_bounce":
            hard_bounces += 1
        elif event_type == "complaint":
            complaints += 1

    await session.commit()
    return {
        "ok": True,
        "received": len(body.events),
        "suppressed": suppressed,
        "hard_bounces": hard_bounces,
        "complaints": complaints,
        "throttled_accounts": throttled_accounts,
    }


# ---------------------------------------------------------------------------
# Unsubscribe: tokenised one-click suppression (Australian Spam Act compliance)
# ---------------------------------------------------------------------------

from base64 import urlsafe_b64encode, urlsafe_b64decode


def _generate_unsub_token(lead_id: str, email: str) -> str:
    """Generate a URL-safe unsubscribe token. Format: base64(lead_id:email).hmac"""
    secret = os.getenv("UNSUB_SECRET", os.getenv("API_KEY", "HILLS_SECURE_2026_CORE"))
    payload = f"{lead_id}:{email}"
    sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()[:16]
    encoded = urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    return f"{encoded}.{sig}"


def _verify_unsub_token(token: str) -> Optional[tuple]:
    """Return (lead_id, email) if token is valid, else None."""
    try:
        encoded, sig = token.rsplit(".", 1)
        padding = 4 - len(encoded) % 4
        payload = urlsafe_b64decode(encoded + "=" * (padding % 4)).decode()
        lead_id, email = payload.split(":", 1)
        expected = _generate_unsub_token(lead_id, email)
        if not hmac.compare_digest(token, expected):
            return None
        return lead_id, email
    except Exception:
        return None


@router.get("/unsubscribe/{token}")
async def handle_unsubscribe(token: str, session: AsyncSession = Depends(get_session)):
    """One-click unsubscribe. Applies email suppression and returns a plain HTML confirmation."""
    from fastapi.responses import HTMLResponse
    result = _verify_unsub_token(token)
    if not result:
        return HTMLResponse(
            "<html><body style='font-family:sans-serif;padding:40px'>"
            "<h2>Invalid or expired unsubscribe link.</h2>"
            "<p>Please contact us directly to update your preferences.</p>"
            "</body></html>",
            status_code=400
        )
    lead_id, email = result
    res_lead = await session.execute(text("SELECT id, owner_name FROM leads WHERE id = :id"), {"id": lead_id})
    lead_row = res_lead.mappings().first()
    if not lead_row:
        return HTMLResponse(
            "<html><body style='font-family:sans-serif;padding:40px'>"
            "<h2>You have been unsubscribed.</h2></body></html>"
        )
    # Apply email suppression via funnel service
    try:
        from services.funnel_service import apply_lead_suppression, ensure_lead_funnels
        from models.funnel_schemas import SuppressionUpsertRequest
        await ensure_lead_funnels(session, lead_id)
        await apply_lead_suppression(session, lead_id, SuppressionUpsertRequest(
            channel="email",
            status="active",
            reason="Unsubscribed via email footer link",
            source="unsubscribe_link",
            note=f"Email: {email}",
            created_by="system",
        ))
    except Exception:
        # Log but don't fail — suppression is best-effort
        import logging
        logging.getLogger(__name__).warning("Unsubscribe suppression failed for lead %s", lead_id)
    return HTMLResponse(
        "<html><body style='font-family:sans-serif;padding:40px;max-width:480px;margin:0 auto'>"
        "<h2 style='color:#111'>You have been unsubscribed.</h2>"
        f"<p style='color:#555'>You will no longer receive emails from {BRAND_NAME} "
        "at this address.</p>"
        "<p style='color:#888;font-size:13px'>If this was a mistake, please reply to any previous email "
        f"or call <a href='tel:{PRINCIPAL_PHONE}'>{PRINCIPAL_PHONE}</a>.</p>"
        "</body></html>"
    )
