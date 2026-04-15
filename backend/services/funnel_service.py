from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.logic import _append_activity, _append_stage_note, _build_activity_entry
from core.utils import _decode_row, _dedupe_by_phone, _dedupe_text_list, now_iso, parse_client_datetime
from models.funnel_models import LeadChannelConsent, LeadFunnel, LeadFunnelEvent, LeadSuppression
from models.funnel_schemas import (
    ConsentPayload,
    ConsentUpsertRequest,
    FunnelBookingRequest,
    FunnelEventPayload,
    FunnelOutreachTaskRequest,
    FunnelPayload,
    FunnelStageUpdateRequest,
    FunnelTaskResponse,
    LeadFunnelsResponse,
    OutreachGuardPayload,
    SuppressionPayload,
    SuppressionUpsertRequest,
)
from services.automations import _refresh_lead_next_action
from services.pipeline_guard import assert_status_transition_allowed

SELLER_FUNNEL = "seller_appraisal"
MORTGAGE_FUNNEL = "mortgage"
SELLER_PURPOSE = "seller_marketing"
MORTGAGE_PURPOSE = "mortgage_marketing"
_CROSS_BRAND_LOCK_DAYS = 7


def _purpose_for_funnel(funnel_type: str) -> str:
    return SELLER_PURPOSE if funnel_type == SELLER_FUNNEL else MORTGAGE_PURPOSE


def resolve_outreach_purpose(lead: Dict[str, Any], cadence_name: str = "", funnel_type: str = "") -> str:
    trigger = str(lead.get("trigger_type") or "").lower()
    route_queue = str(lead.get("route_queue") or "").lower()
    cadence = str(cadence_name or "").lower()
    funnel = str(funnel_type or "").lower()
    if "mortgage" in trigger or "mortgage" in route_queue or "mortgage" in cadence or funnel == MORTGAGE_FUNNEL:
        return MORTGAGE_PURPOSE
    return SELLER_PURPOSE


def _seller_stage_from_state(lead: Dict[str, Any], workflow: Optional[Dict[str, Any]]) -> str:
    if workflow and workflow.get("authority_pack_status") == "signed":
        return "authority_signed"
    if workflow and (workflow.get("pack_sent_at") or workflow.get("authority_pack_status") in {"ready", "sent"}):
        return "authority_sent"
    if workflow and (workflow.get("pack_document_id") or workflow.get("stage") in {"authority_pack", "send_sign", "signed"}):
        return "authority_requested"
    if str(lead.get("status") or "") == "appt_booked" or str(lead.get("last_outcome") or "") == "booked_appraisal":
        return "appraisal_booked"
    if str(lead.get("status") or "") in {"contacted", "qualified", "outreach_ready"}:
        return "outreach_active"
    return "lead_captured"


def _mortgage_stage_from_state(lead: Dict[str, Any]) -> str:
    if str(lead.get("status") or "") == "mortgage_appt_booked":
        return "callback_booked"
    if str(lead.get("route_queue") or "") == "mortgage_ownit1st" or "mortgage" in str(lead.get("trigger_type") or "").lower():
        return "outreach_active" if str(lead.get("status") or "") in {"contacted", "qualified"} else "lead_captured"
    return "monitor"


async def _load_lead(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    row = (
        await session.execute(text("SELECT * FROM leads WHERE id = :lead_id"), {"lead_id": lead_id})
    ).mappings().first()
    if not row:
        raise ValueError("Lead not found")
    return _decode_row(dict(row))


async def _load_listing_workflow(session: AsyncSession, lead_id: str) -> Optional[Dict[str, Any]]:
    row = (
        await session.execute(text("SELECT * FROM listing_workflows WHERE lead_id = :lead_id"), {"lead_id": lead_id})
    ).mappings().first()
    return dict(row) if row else None


async def _append_lead_activity(
    session: AsyncSession,
    lead: Dict[str, Any],
    note: str,
    status: Optional[str],
    channel: str,
    subject: str,
    recipient: Optional[str] = None,
) -> None:
    now = now_iso()
    stage_history = _append_stage_note(lead.get("stage_note_history"), note, status or (lead.get("status") or "captured"), channel, subject, recipient)
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry("funnel_update", note, status or lead.get("status"), channel, subject, recipient),
    )
    await session.execute(
        text(
            """
            UPDATE leads
            SET stage_note = :stage_note,
                stage_note_history = :stage_history,
                activity_log = :activity_log,
                updated_at = :updated_at
            WHERE id = :lead_id
            """
        ),
        {
            "stage_note": note,
            "stage_history": json.dumps(stage_history),
            "activity_log": json.dumps(activity_log),
            "updated_at": now,
            "lead_id": lead["id"],
        },
    )


async def _create_funnel_event(
    session: AsyncSession,
    funnel: LeadFunnel,
    event_type: str,
    title: str,
    detail: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    session.add(
        LeadFunnelEvent(
            id=uuid.uuid4().hex,
            lead_id=funnel.lead_id,
            funnel_id=funnel.id,
            funnel_type=funnel.funnel_type,
            event_type=event_type,
            title=title,
            detail=detail,
            payload=payload or {},
            created_at=now_iso(),
        )
    )


async def _ensure_single_funnel(
    session: AsyncSession,
    lead_id: str,
    funnel_type: str,
    default_stage: str,
) -> LeadFunnel:
    existing = (
        await session.execute(
            select(LeadFunnel).where(LeadFunnel.lead_id == lead_id, LeadFunnel.funnel_type == funnel_type)
        )
    ).scalars().first()
    if existing:
        return existing
    now = now_iso()
    funnel = LeadFunnel(
        id=uuid.uuid4().hex,
        lead_id=lead_id,
        funnel_type=funnel_type,
        stage=default_stage,
        status="active" if default_stage != "monitor" else "idle",
        owner="operator",
        summary="Generated from live lead state.",
        next_step_title="Review compliance state and queue the next outreach move.",
        created_at=now,
        updated_at=now,
    )
    session.add(funnel)
    await _create_funnel_event(
        session,
        funnel,
        "created",
        f"{funnel_type.replace('_', ' ').title()} funnel initialized",
        f"Default stage set to {default_stage.replace('_', ' ')}.",
    )
    return funnel


async def ensure_lead_funnels(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    lead = await _load_lead(session, lead_id)
    workflow = await _load_listing_workflow(session, lead_id)
    await _ensure_single_funnel(session, lead_id, SELLER_FUNNEL, _seller_stage_from_state(lead, workflow))
    await _ensure_single_funnel(session, lead_id, MORTGAGE_FUNNEL, _mortgage_stage_from_state(lead))
    await session.commit()
    return lead


def _guard_from_state(
    lead: Dict[str, Any],
    consents: List[LeadChannelConsent],
    suppressions: List[LeadSuppression],
    channel: str,
    purpose: str,
) -> OutreachGuardPayload:
    active_suppression = any(
        suppression.status == "active" and suppression.channel in {"all", channel}
        for suppression in suppressions
    )
    consent = next(
        (
            item
            for item in consents
            if item.channel == channel and item.purpose == purpose
        ),
        None,
    )
    consent_status = consent.status if consent else "unknown"
    reasons: List[str] = []
    allowed = True

    if active_suppression:
        allowed = False
        reasons.append("Active suppression is recorded for this channel.")
    if channel in {"sms", "email"} and consent_status != "granted":
        allowed = False
        reasons.append("SMS and email queueing require granted consent in this beta.")
    if channel == "call" and consent_status == "denied":
        allowed = False
        reasons.append("Call consent is marked denied.")
    if str(lead.get("do_not_contact_until") or "").strip():
        reasons.append(f"Lead has do-not-contact-until set to {lead.get('do_not_contact_until')}.")

    return OutreachGuardPayload(
        channel=channel,
        purpose=purpose,
        allowed=allowed,
        consent_status=consent_status,
        active_suppression=active_suppression,
        reasons=reasons,
    )


def _purpose_brand_bucket(purpose: str) -> str:
    return "mortgage" if "mortgage" in str(purpose or "").lower() else "seller"


async def _cross_brand_lock_reason(
    session: AsyncSession,
    lead: Dict[str, Any],
    *,
    purpose: str,
    channel: str,
) -> Optional[str]:
    if channel not in {"sms", "email", "call"}:
        return None
    lead_phones = set(_dedupe_by_phone(lead.get("contact_phones")))
    lead_emails = {email.lower() for email in _dedupe_text_list(lead.get("contact_emails"))}
    if not lead_phones and not lead_emails:
        return None

    current_bucket = _purpose_brand_bucket(purpose)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_CROSS_BRAND_LOCK_DAYS)).replace(microsecond=0).isoformat()
    rows = (
        await session.execute(
            text(
                """
                SELECT
                    t.id,
                    t.channel,
                    t.cadence_name,
                    t.completed_at,
                    l.id AS lead_id,
                    l.owner_name,
                    l.trigger_type,
                    l.route_queue,
                    l.contact_phones,
                    l.contact_emails
                FROM tasks t
                JOIN leads l ON l.id = t.lead_id
                WHERE t.status = 'completed'
                  AND COALESCE(t.channel, '') IN ('sms', 'email', 'call')
                  AND COALESCE(t.completed_at, '') >= :cutoff
                ORDER BY t.completed_at DESC
                LIMIT 1000
                """
            ),
            {"cutoff": cutoff},
        )
    ).mappings().all()

    for row in rows:
        row_phones = set(_dedupe_by_phone(row.get("contact_phones")))
        row_emails = {email.lower() for email in _dedupe_text_list(row.get("contact_emails"))}
        if not (lead_phones.intersection(row_phones) or lead_emails.intersection(row_emails)):
            continue
        historical_purpose = resolve_outreach_purpose(
            {"trigger_type": row.get("trigger_type"), "route_queue": row.get("route_queue")},
            cadence_name=str(row.get("cadence_name") or ""),
        )
        historical_bucket = _purpose_brand_bucket(historical_purpose)
        if historical_bucket != current_bucket:
            owner_name = str(row.get("owner_name") or "matched contact")
            completed_at = str(row.get("completed_at") or "")
            return (
                f"Cross-brand lock active: {historical_bucket} outreach was sent to {owner_name} at {completed_at}. "
                f"Hold opposite-brand outreach for {_CROSS_BRAND_LOCK_DAYS} days."
            )
    return None


def _funnel_payload(funnel: LeadFunnel) -> FunnelPayload:
    return FunnelPayload(**funnel.model_dump())


async def get_lead_funnels(session: AsyncSession, lead_id: str) -> LeadFunnelsResponse:
    lead = await ensure_lead_funnels(session, lead_id)
    funnels = (
        await session.execute(select(LeadFunnel).where(LeadFunnel.lead_id == lead_id).order_by(LeadFunnel.funnel_type.asc()))
    ).scalars().all()
    consents = (
        await session.execute(
            select(LeadChannelConsent).where(LeadChannelConsent.lead_id == lead_id).order_by(LeadChannelConsent.purpose.asc(), LeadChannelConsent.channel.asc())
        )
    ).scalars().all()
    suppressions = (
        await session.execute(
            select(LeadSuppression).where(LeadSuppression.lead_id == lead_id).order_by(LeadSuppression.created_at.desc())
        )
    ).scalars().all()
    events = (
        await session.execute(
            select(LeadFunnelEvent).where(LeadFunnelEvent.lead_id == lead_id).order_by(LeadFunnelEvent.created_at.desc()).limit(30)
        )
    ).scalars().all()

    guards = [
        _guard_from_state(lead, consents, suppressions, "call", SELLER_PURPOSE),
        _guard_from_state(lead, consents, suppressions, "email", SELLER_PURPOSE),
        _guard_from_state(lead, consents, suppressions, "sms", SELLER_PURPOSE),
        _guard_from_state(lead, consents, suppressions, "call", MORTGAGE_PURPOSE),
        _guard_from_state(lead, consents, suppressions, "email", MORTGAGE_PURPOSE),
        _guard_from_state(lead, consents, suppressions, "sms", MORTGAGE_PURPOSE),
    ]

    return LeadFunnelsResponse(
        lead_id=lead_id,
        funnels=[_funnel_payload(funnel) for funnel in funnels],
        consents=[ConsentPayload(**item.model_dump()) for item in consents],
        suppressions=[SuppressionPayload(**item.model_dump()) for item in suppressions],
        events=[FunnelEventPayload(**item.model_dump()) for item in events],
        guards=guards,
    )


async def get_outreach_guard(
    session: AsyncSession,
    lead_id: str,
    channel: str,
    purpose: Optional[str] = None,
    cadence_name: str = "",
    funnel_type: str = "",
) -> OutreachGuardPayload:
    lead = await _load_lead(session, lead_id)
    consents = (
        await session.execute(select(LeadChannelConsent).where(LeadChannelConsent.lead_id == lead_id))
    ).scalars().all()
    suppressions = (
        await session.execute(select(LeadSuppression).where(LeadSuppression.lead_id == lead_id))
    ).scalars().all()
    resolved_purpose = purpose or resolve_outreach_purpose(lead, cadence_name=cadence_name, funnel_type=funnel_type)
    guard = _guard_from_state(lead, consents, suppressions, channel, resolved_purpose)
    lock_reason = await _cross_brand_lock_reason(session, lead, purpose=resolved_purpose, channel=channel)
    if lock_reason:
        guard.allowed = False
        reasons = list(guard.reasons or [])
        reasons.append(lock_reason)
        guard.reasons = reasons
    return guard


async def assert_outreach_allowed(
    session: AsyncSession,
    lead_id: str,
    channel: str,
    purpose: Optional[str] = None,
    cadence_name: str = "",
    funnel_type: str = "",
) -> OutreachGuardPayload:
    guard = await get_outreach_guard(session, lead_id, channel, purpose=purpose, cadence_name=cadence_name, funnel_type=funnel_type)
    if not guard.allowed:
        raise ValueError(" / ".join(guard.reasons) or "Channel is blocked by compliance guard")
    return guard


async def record_lead_consent(session: AsyncSession, lead_id: str, body: ConsentUpsertRequest) -> LeadFunnelsResponse:
    lead = await ensure_lead_funnels(session, lead_id)
    now = now_iso()
    existing = (
        await session.execute(
            select(LeadChannelConsent).where(
                LeadChannelConsent.lead_id == lead_id,
                LeadChannelConsent.channel == body.channel,
                LeadChannelConsent.purpose == body.purpose,
            )
        )
    ).scalars().first()
    if existing:
        existing.status = body.status
        existing.basis = body.basis
        existing.source = body.source
        existing.note = body.note
        existing.recipient = body.recipient
        existing.recorded_by = body.recorded_by
        existing.recorded_at = now
        existing.expires_at = body.expires_at
        existing.updated_at = now
        consent = existing
    else:
        consent = LeadChannelConsent(
            id=uuid.uuid4().hex,
            lead_id=lead_id,
            channel=body.channel,
            purpose=body.purpose,
            status=body.status,
            basis=body.basis,
            source=body.source,
            note=body.note,
            recipient=body.recipient,
            recorded_by=body.recorded_by,
            recorded_at=now,
            expires_at=body.expires_at,
            updated_at=now,
        )
        session.add(consent)

    funnels = (
        await session.execute(select(LeadFunnel).where(LeadFunnel.lead_id == lead_id))
    ).scalars().all()
    target_purpose = body.purpose
    for funnel in funnels:
        if _purpose_for_funnel(funnel.funnel_type) == target_purpose:
            await _create_funnel_event(
                session,
                funnel,
                "consent_recorded",
                f"{body.channel.upper()} consent {body.status}",
                body.note or f"Consent status for {body.channel} updated to {body.status}.",
                {"purpose": body.purpose, "basis": body.basis, "source": body.source},
            )

    await _append_lead_activity(
        session,
        lead,
        f"{body.channel.upper()} consent for {body.purpose} recorded as {body.status}.",
        lead.get("status"),
        body.channel,
        "Consent update",
        body.recipient or None,
    )
    await session.commit()
    return await get_lead_funnels(session, lead_id)


async def apply_lead_suppression(session: AsyncSession, lead_id: str, body: SuppressionUpsertRequest) -> LeadFunnelsResponse:
    lead = await ensure_lead_funnels(session, lead_id)
    now = now_iso()
    existing = (
        await session.execute(
            select(LeadSuppression).where(
                LeadSuppression.lead_id == lead_id,
                LeadSuppression.channel == body.channel,
                LeadSuppression.status == "active",
            )
        )
    ).scalars().first()
    if existing:
        existing.reason = body.reason
        existing.source = body.source
        existing.note = body.note
        existing.created_by = body.created_by
        existing.updated_at = now
        suppression = existing
    else:
        suppression = LeadSuppression(
            id=uuid.uuid4().hex,
            lead_id=lead_id,
            channel=body.channel,
            status="active",
            reason=body.reason,
            source=body.source,
            note=body.note,
            created_by=body.created_by,
            created_at=now,
            updated_at=now,
        )
        session.add(suppression)

    if body.channel == "all":
        await session.execute(
            text("UPDATE leads SET do_not_contact_until = :until, updated_at = :updated_at WHERE id = :lead_id"),
            {"until": "2099-12-31T00:00:00+11:00", "updated_at": now, "lead_id": lead_id},
        )

    funnels = (
        await session.execute(select(LeadFunnel).where(LeadFunnel.lead_id == lead_id))
    ).scalars().all()
    for funnel in funnels:
        await _create_funnel_event(
            session,
            funnel,
            "suppression_applied",
            f"{body.channel.upper()} suppression applied",
            body.reason,
            {"note": body.note or "", "source": body.source},
        )

    await _append_lead_activity(
        session,
        lead,
        f"{body.channel.upper()} suppression applied: {body.reason}.",
        lead.get("status"),
        body.channel,
        "Suppression update",
    )
    await session.commit()
    return await get_lead_funnels(session, lead_id)


async def release_lead_suppression(session: AsyncSession, lead_id: str, channel: str, released_by: str = "operator") -> LeadFunnelsResponse:
    lead = await ensure_lead_funnels(session, lead_id)
    now = now_iso()
    suppressions = (
        await session.execute(
            select(LeadSuppression).where(
                LeadSuppression.lead_id == lead_id,
                LeadSuppression.channel == channel,
                LeadSuppression.status == "active",
            )
        )
    ).scalars().all()
    if not suppressions:
        raise ValueError("No active suppression found for that channel")
    for item in suppressions:
        item.status = "released"
        item.released_at = now
        item.updated_at = now

    if channel == "all":
        remaining_all = (
            await session.execute(
                select(LeadSuppression).where(
                    LeadSuppression.lead_id == lead_id,
                    LeadSuppression.channel == "all",
                    LeadSuppression.status == "active",
                )
            )
        ).scalars().all()
        if not remaining_all:
            await session.execute(
                text("UPDATE leads SET do_not_contact_until = NULL, updated_at = :updated_at WHERE id = :lead_id"),
                {"updated_at": now, "lead_id": lead_id},
            )

    funnels = (
        await session.execute(select(LeadFunnel).where(LeadFunnel.lead_id == lead_id))
    ).scalars().all()
    for funnel in funnels:
        await _create_funnel_event(
            session,
            funnel,
            "suppression_released",
            f"{channel.upper()} suppression released",
            f"Released by {released_by}.",
            {"released_by": released_by},
        )

    await _append_lead_activity(
        session,
        lead,
        f"{channel.upper()} suppression released by {released_by}.",
        lead.get("status"),
        channel,
        "Suppression released",
    )
    await session.commit()
    return await get_lead_funnels(session, lead_id)


async def update_lead_funnel_stage(
    session: AsyncSession,
    lead_id: str,
    funnel_type: str,
    body: FunnelStageUpdateRequest,
) -> LeadFunnelsResponse:
    lead = await ensure_lead_funnels(session, lead_id)
    funnel = (
        await session.execute(
            select(LeadFunnel).where(LeadFunnel.lead_id == lead_id, LeadFunnel.funnel_type == funnel_type)
        )
    ).scalars().first()
    if not funnel:
        raise ValueError("Funnel not found")

    now = now_iso()
    funnel.stage = body.stage
    funnel.owner = body.owner or funnel.owner
    funnel.next_step_title = body.next_step_title or funnel.next_step_title
    funnel.next_step_due_at = parse_client_datetime(body.next_step_due_at) if body.next_step_due_at else funnel.next_step_due_at
    funnel.updated_at = now
    if body.stage in {"authority_signed", "settled", "closed_won"}:
        funnel.status = "completed"
        funnel.completed_at = now
    elif body.stage in {"closed_lost"}:
        funnel.status = "closed_lost"
        funnel.completed_at = now
    else:
        funnel.status = "active"

    await _create_funnel_event(
        session,
        funnel,
        "stage_changed",
        f"{funnel_type.replace('_', ' ').title()} stage set to {body.stage.replace('_', ' ')}",
        body.note or "Stage updated by operator.",
        {"owner": body.owner, "next_step_title": body.next_step_title or "", "next_step_due_at": funnel.next_step_due_at or ""},
    )
    await _append_lead_activity(
        session,
        lead,
        f"{funnel_type.replace('_', ' ').title()} stage changed to {body.stage.replace('_', ' ')}.",
        lead.get("status"),
        "workflow",
        "Funnel stage updated",
    )
    await session.commit()
    return await get_lead_funnels(session, lead_id)


async def _create_appointment(
    session: AsyncSession,
    lead_id: str,
    title: str,
    starts_at: str,
    location: str,
    notes: str,
) -> Dict[str, Any]:
    appointment_id = uuid.uuid4().hex
    now = now_iso()
    await session.execute(
        text(
            """
            INSERT INTO appointments (id, lead_id, title, starts_at, status, location, notes, cadence_name, auto_generated, created_at, updated_at)
            VALUES (:id, :lead_id, :title, :starts_at, 'scheduled', :location, :notes, '', 0, :created_at, :updated_at)
            """
        ),
        {
            "id": appointment_id,
            "lead_id": lead_id,
            "title": title,
            "starts_at": starts_at,
            "location": location,
            "notes": notes,
            "created_at": now,
            "updated_at": now,
        },
    )
    row = (
        await session.execute(text("SELECT * FROM appointments WHERE id = :id"), {"id": appointment_id})
    ).mappings().first()
    return dict(row or {})


async def book_funnel_appointment(
    session: AsyncSession,
    lead_id: str,
    funnel_type: str,
    body: FunnelBookingRequest,
) -> LeadFunnelsResponse:
    lead = await ensure_lead_funnels(session, lead_id)
    funnel = (
        await session.execute(
            select(LeadFunnel).where(LeadFunnel.lead_id == lead_id, LeadFunnel.funnel_type == funnel_type)
        )
    ).scalars().first()
    if not funnel:
        raise ValueError("Funnel not found")

    starts_at = parse_client_datetime(body.starts_at)
    target_status = "appt_booked" if funnel_type == SELLER_FUNNEL else "mortgage_appt_booked"
    assert_status_transition_allowed(
        lead,
        target_status,
        source="funnel_booking",
        appointment_at=starts_at,
    )
    title = (
        f"Property appraisal - {lead.get('address') or lead.get('owner_name') or 'lead'}"
        if funnel_type == SELLER_FUNNEL
        else f"Ownit1st callback - {lead.get('owner_name') or lead.get('address') or 'lead'}"
    )
    stage = "appraisal_booked" if funnel_type == SELLER_FUNNEL else "callback_booked"
    next_step = "Complete appraisal and request authority" if funnel_type == SELLER_FUNNEL else "Run fact find and capture lending goals"
    appointment = await _create_appointment(session, lead_id, title, starts_at, body.location, body.note or "")

    now = now_iso()
    funnel.stage = stage
    funnel.status = "active"
    funnel.booked_at = starts_at
    funnel.next_step_title = next_step
    funnel.next_step_due_at = starts_at
    funnel.updated_at = now

    await session.execute(
        text(
            """
            UPDATE leads
            SET status = :status,
                queue_bucket = 'booked',
                cadence_name = :cadence_name,
                last_contacted_at = :last_contacted_at,
                updated_at = :updated_at
            WHERE id = :lead_id
            """
        ),
        {
            "status": target_status,
            "cadence_name": "booked_appraisal" if funnel_type == SELLER_FUNNEL else "mortgage_callback",
            "last_contacted_at": now,
            "updated_at": now,
            "lead_id": lead_id,
        },
    )
    await _create_funnel_event(
        session,
        funnel,
        "appointment_booked",
        title,
        body.note or f"{title} booked for {starts_at}.",
        {"starts_at": starts_at, "location": body.location, "appointment_id": appointment.get("id")},
    )
    await _append_lead_activity(
        session,
        lead,
        f"{title} booked for {starts_at}.",
        "appt_booked" if funnel_type == SELLER_FUNNEL else "mortgage_appt_booked",
        "appointment",
        title,
    )
    await _refresh_lead_next_action(session, lead_id)
    await session.commit()
    return await get_lead_funnels(session, lead_id)


def _default_task_copy(lead: Dict[str, Any], funnel_type: str, channel: str) -> Dict[str, str]:
    first = str((lead.get("owner_name") or "there").split(" ")[0]).strip() or "there"
    address = str(lead.get("address") or "your property").strip()
    if funnel_type == SELLER_FUNNEL:
        subject = f"Appraisal timing for {address}"
        message = (
            f"Hi {first}, I wanted to reach out regarding {address}. "
            "If helpful, we can lock in a short appraisal conversation and walk through likely value, buyer depth, and timing."
        )
    else:
        subject = f"Ownit1st callback for {address}"
        message = (
            f"Hi {first}, I can book a short Ownit1st callback to run through lending position, rate pressure, and the next steps if you want clarity."
        )
    if channel == "call":
        message = message.replace("Hi ", "Call opener for ").replace(",", "")
    return {"subject": subject, "message": message}


async def queue_funnel_outreach_task(
    session: AsyncSession,
    lead_id: str,
    body: FunnelOutreachTaskRequest,
) -> FunnelTaskResponse:
    lead = await ensure_lead_funnels(session, lead_id)
    consents = (
        await session.execute(select(LeadChannelConsent).where(LeadChannelConsent.lead_id == lead_id))
    ).scalars().all()
    suppressions = (
        await session.execute(select(LeadSuppression).where(LeadSuppression.lead_id == lead_id))
    ).scalars().all()
    purpose = _purpose_for_funnel(body.funnel_type)
    guard = _guard_from_state(lead, consents, suppressions, body.channel, purpose)
    if not guard.allowed:
        raise ValueError(" / ".join(guard.reasons) or "Channel is blocked by compliance guard")

    funnel = (
        await session.execute(
            select(LeadFunnel).where(LeadFunnel.lead_id == lead_id, LeadFunnel.funnel_type == body.funnel_type)
        )
    ).scalars().first()
    if not funnel:
        raise ValueError("Funnel not found")

    copy = _default_task_copy(lead, body.funnel_type, body.channel)
    due_at = parse_client_datetime(body.due_at)
    recipient = body.recipient or (
        (_dedupe_text_list(lead.get("contact_emails"))[0] if body.channel == "email" and _dedupe_text_list(lead.get("contact_emails")) else "")
        or (_dedupe_by_phone(lead.get("contact_phones"))[0] if body.channel == "sms" and _dedupe_by_phone(lead.get("contact_phones")) else "")
    )
    task_id = uuid.uuid4().hex
    now = now_iso()
    await session.execute(
        text(
            """
            INSERT INTO tasks (
                id, lead_id, title, task_type, action_type, channel, due_at, status, notes, related_report_id,
                approval_status, message_subject, message_preview, rewrite_reason, superseded_by, cadence_name,
                cadence_step, auto_generated, priority_bucket, completed_at, created_at, updated_at
            ) VALUES (
                :id, :lead_id, :title, :task_type, :action_type, :channel, :due_at, 'pending', :notes, '',
                :approval_status, :message_subject, :message_preview, :rewrite_reason, '', :cadence_name,
                0, 0, :priority_bucket, NULL, :created_at, :updated_at
            )
            """
        ),
        {
            "id": task_id,
            "lead_id": lead_id,
            "title": body.title or ("Queue appraisal outreach" if body.funnel_type == SELLER_FUNNEL else "Queue mortgage outreach"),
            "task_type": body.channel,
            "action_type": body.channel,
            "channel": body.channel,
            "due_at": due_at,
            "notes": body.note or f"Recipient: {recipient}" if recipient else (body.note or ""),
            "approval_status": "pending" if body.channel in {"sms", "email"} else "not_required",
            "message_subject": body.subject or copy["subject"],
            "message_preview": body.message or copy["message"],
            "rewrite_reason": f"Queued through {body.funnel_type} funnel with consent guard satisfied.",
            "cadence_name": body.funnel_type,
            "priority_bucket": "send_now" if body.channel in {"sms", "email"} else "follow_up",
            "created_at": now,
            "updated_at": now,
        },
    )

    funnel.next_step_title = body.title or ("Await task approval and send" if body.channel in {"sms", "email"} else "Complete call task")
    funnel.next_step_due_at = due_at
    funnel.updated_at = now

    await _create_funnel_event(
        session,
        funnel,
        "task_queued",
        body.title or f"{body.channel.upper()} task queued",
        body.note or "Outreach task queued with compliance guard satisfied.",
        {"channel": body.channel, "due_at": due_at, "recipient": recipient, "purpose": purpose},
    )
    await _append_lead_activity(
        session,
        lead,
        f"{body.channel.upper()} outreach queued for {due_at}.",
        lead.get("status"),
        body.channel,
        "Outreach queued",
        recipient or None,
    )
    await _refresh_lead_next_action(session, lead_id)
    await session.commit()

    task = (
        await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})
    ).mappings().first()
    return FunnelTaskResponse(status="ok", guard=guard, task=dict(task or {}), funnel=_funnel_payload(funnel))
