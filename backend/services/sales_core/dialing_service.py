from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.sales_core_models import ContactAttempt, LeadContact, LeadState, TaskQueue
from models.sql_models import Lead
from services.sales_core.state_engine import build_lead_state_snapshot


def _resolved_now(now: Optional[datetime]) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    return now if now.tzinfo else now.replace(tzinfo=timezone.utc)


def _task_type_for_outcome(outcome: str) -> str:
    normalized = (outcome or "").strip().lower()
    if normalized in {"call_back", "follow_up", "not_now"}:
        return "follow_up_call"
    if normalized in {"send_info", "email_back"}:
        return "follow_up_email"
    return "follow_up"


async def _load_contact_with_lead(session: AsyncSession, lead_contact_id: str) -> tuple[LeadContact, Optional[Lead]]:
    contact = await session.get(LeadContact, lead_contact_id)
    if not contact:
        raise ValueError(f"Lead contact not found: {lead_contact_id}")
    lead = await session.get(Lead, contact.lead_id) if contact.lead_id else None
    return contact, lead


async def sync_lead_state(session: AsyncSession, lead_contact_id: str, *, now: Optional[datetime] = None) -> LeadState:
    resolved_now = _resolved_now(now)
    contact, lead = await _load_contact_with_lead(session, lead_contact_id)
    attempts = (
        await session.execute(
            select(ContactAttempt).where(ContactAttempt.lead_contact_id == lead_contact_id).order_by(ContactAttempt.attempted_at.desc())
        )
    ).scalars().all()
    existing = await session.get(LeadState, lead_contact_id)

    snapshot = build_lead_state_snapshot(
        {
            "lead_contact_id": contact.id,
            "business_context_key": contact.business_context_key,
            "lead_status": getattr(lead, "status", "captured"),
            "primary_phone": contact.primary_phone,
            "phone_verification_status": contact.phone_verification_status,
            "phone_verified": contact.phone_verification_status,
            "primary_email": contact.primary_email,
            "do_not_call": getattr(lead, "do_not_call", False),
            "lead_est_value": getattr(lead, "est_value", 0),
            "lead_heat_score": getattr(lead, "heat_score", 0),
            "lead_evidence_score": getattr(lead, "evidence_score", 0),
            "enrichment_status": getattr(lead, "enrichment_status", None),
            "next_action_due_at": getattr(lead, "follow_up_due_at", None),
            "attempts": [
                {
                    "attempted_at": attempt.attempted_at,
                    "outcome": attempt.outcome,
                    "connected": attempt.connected,
                    "next_action_due_at": attempt.next_action_due_at,
                }
                for attempt in attempts
            ],
        },
        now=resolved_now,
    )

    if existing is None:
        state = LeadState(
            lead_contact_id=contact.id,
            business_context_key=contact.business_context_key,
            total_attempts=snapshot["total_attempts"],
            attempts_last_7d=snapshot["attempts_last_7d"],
            last_attempt_at=_maybe_datetime(snapshot["last_attempt_at"]),
            last_attempt_outcome=snapshot["last_attempt_outcome"],
            last_contact_at=_maybe_datetime(snapshot["last_contact_at"]),
            last_response_at=_maybe_datetime(snapshot["last_response_at"]),
            best_contact_window=snapshot["best_contact_window"],
            fatigue_band=snapshot["fatigue_band"],
            callable_now=snapshot["callable_now"],
            next_action=snapshot["next_action"],
            next_action_due_at=_maybe_datetime(snapshot["next_action_due_at"]),
            queue_score=snapshot["queue_score"],
            needs_enrichment=snapshot["needs_enrichment"],
            stale_enrichment=snapshot["stale_enrichment"],
            summary_json=snapshot,
            updated_at=resolved_now,
        )
        session.add(state)
    else:
        state = existing
        state.business_context_key = contact.business_context_key
        state.total_attempts = snapshot["total_attempts"]
        state.attempts_last_7d = snapshot["attempts_last_7d"]
        state.last_attempt_at = _maybe_datetime(snapshot["last_attempt_at"])
        state.last_attempt_outcome = snapshot["last_attempt_outcome"]
        state.last_contact_at = _maybe_datetime(snapshot["last_contact_at"])
        state.last_response_at = _maybe_datetime(snapshot["last_response_at"])
        state.best_contact_window = snapshot["best_contact_window"]
        state.fatigue_band = snapshot["fatigue_band"]
        state.callable_now = snapshot["callable_now"]
        state.next_action = snapshot["next_action"]
        state.next_action_due_at = _maybe_datetime(snapshot["next_action_due_at"])
        state.queue_score = snapshot["queue_score"]
        state.needs_enrichment = snapshot["needs_enrichment"]
        state.stale_enrichment = snapshot["stale_enrichment"]
        state.summary_json = snapshot
        state.updated_at = resolved_now

    await session.commit()
    await session.refresh(state)
    return state


def _maybe_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", 0):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


async def log_contact_attempt(session: AsyncSession, payload: Dict[str, Any], *, now: Optional[datetime] = None) -> Dict[str, Any]:
    resolved_now = _resolved_now(now)
    contact_id = str(payload["lead_contact_id"])
    contact, lead = await _load_contact_with_lead(session, contact_id)

    attempt = ContactAttempt(
        business_context_key=str(payload["business_context_key"]),
        lead_contact_id=contact_id,
        attempted_at=resolved_now,
        channel=str(payload.get("channel") or "call"),
        outcome=str(payload.get("outcome") or "unknown"),
        connected=bool(payload.get("connected")),
        duration_seconds=int(payload.get("duration_seconds") or 0),
        voicemail_left=bool(payload.get("voicemail_left")),
        note=str(payload.get("note") or "") or None,
        transcript_id=payload.get("transcript_id"),
        recording_id=payload.get("recording_id"),
        next_action_due_at=_maybe_datetime(payload.get("next_action_due_at")),
        created_by=str(payload.get("created_by") or "system"),
        created_at=resolved_now,
    )
    session.add(attempt)
    await session.flush()

    task = None
    if attempt.next_action_due_at is not None:
        await session.execute(
            delete(TaskQueue)
            .where(TaskQueue.lead_contact_id == contact_id)
            .where(TaskQueue.status == "pending")
            .where(TaskQueue.task_type.in_(["follow_up", "follow_up_call", "follow_up_email"]))
        )
        task = TaskQueue(
            business_context_key=contact.business_context_key,
            lead_contact_id=contact_id,
            task_type=_task_type_for_outcome(attempt.outcome),
            due_at=attempt.next_action_due_at,
            status="pending",
            priority=80,
            reason=str(payload.get("note") or attempt.outcome or "Follow up required"),
            payload_json={
                "channel": attempt.channel,
                "outcome": attempt.outcome,
                "attempt_id": attempt.id,
            },
            created_by=str(payload.get("created_by") or "system"),
            created_at=resolved_now,
            updated_at=resolved_now,
        )
        session.add(task)

    if lead is not None:
        lead.last_contacted_at = resolved_now.isoformat()
        lead.last_called_date = resolved_now.date().isoformat()
        lead.last_outcome = attempt.outcome
        lead.last_outcome_at = resolved_now.isoformat()
        if attempt.next_action_due_at is not None:
            lead.follow_up_due_at = attempt.next_action_due_at.isoformat()
            lead.next_action_at = attempt.next_action_due_at.isoformat()
            lead.next_action_type = "follow_up"
            lead.next_action_channel = attempt.channel
            lead.next_action_title = "Scheduled follow-up"
            lead.next_action_reason = str(payload.get("note") or f"{attempt.outcome} requires follow-up")
        lead.updated_at = resolved_now.isoformat()

    await session.commit()
    state = await sync_lead_state(session, contact_id, now=resolved_now)
    if task is not None:
        await session.refresh(task)
    await session.refresh(attempt)
    return {"attempt": attempt, "task": task, "state": state}


async def get_lead_context(session: AsyncSession, lead_contact_id: str) -> Dict[str, Any]:
    contact, lead = await _load_contact_with_lead(session, lead_contact_id)
    state = await session.get(LeadState, lead_contact_id)
    open_tasks = (
        await session.execute(
            select(TaskQueue)
            .where(TaskQueue.lead_contact_id == lead_contact_id)
            .where(TaskQueue.status == "pending")
            .order_by(TaskQueue.due_at.asc())
        )
    ).scalars().all()
    attempts = (
        await session.execute(
            select(ContactAttempt)
            .where(ContactAttempt.lead_contact_id == lead_contact_id)
            .order_by(ContactAttempt.attempted_at.desc())
            .limit(10)
        )
    ).scalars().all()
    return {
        "contact": contact,
        "lead": lead,
        "state": state,
        "tasks": open_tasks,
        "attempts": attempts,
    }


async def fetch_next_callable_record(session: AsyncSession, business_context_key: str, *, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    resolved_now = _resolved_now(now)
    rows = (
        await session.execute(
            select(LeadState)
            .where(LeadState.business_context_key == business_context_key)
            .where(LeadState.callable_now.is_(True))
            .where((LeadState.next_action_due_at.is_(None)) | (LeadState.next_action_due_at <= resolved_now))
            .order_by(LeadState.queue_score.desc(), LeadState.updated_at.asc())
        )
    ).scalars().all()
    for row in rows:
        context = await get_lead_context(session, row.lead_contact_id)
        if context["contact"] is not None:
            return context
    return None
