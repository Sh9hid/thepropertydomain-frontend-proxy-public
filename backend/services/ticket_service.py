"""
Ticket service — create, transition, and query org tickets.

Tickets are the source of truth for all agent work.
Every state change emits a WebSocket event.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.events import event_manager
from models.org_models import Ticket, TicketStatus, TicketSeverity, TicketDept, TicketKind


# ─── Event helpers ────────────────────────────────────────────────────────────

async def _emit(event_type: str, ticket: Ticket, extra: Optional[Dict] = None):
    payload = {
        "type": event_type,
        "data": {
            "ticket_id": ticket.id,
            "title": ticket.title,
            "status": ticket.status,
            "department": ticket.department,
            "priority": ticket.priority,
            "severity": ticket.severity,
            "assigned_agent_id": ticket.assigned_agent_id,
            "ts": datetime.utcnow().isoformat(),
            **(extra or {}),
        }
    }
    await event_manager.broadcast(payload)


# ─── CRUD ─────────────────────────────────────────────────────────────────────

async def create_ticket(
    session: AsyncSession,
    title: str,
    description: Optional[str],
    department: str,
    kind: str,
    workspace_key: str = "real_estate",
    priority: int = 5,
    severity: str = TicketSeverity.MEDIUM,
    created_by_type: str = "system",
    created_by_id: Optional[str] = None,
    assigned_agent_id: Optional[str] = None,
    related_lead_id: Optional[str] = None,
    related_job_id: Optional[str] = None,
    evidence_json: Optional[Dict] = None,
    metadata_json: Optional[Dict] = None,
    tags: Optional[List[str]] = None,
    parent_ticket_id: Optional[str] = None,
) -> Ticket:
    ticket = Ticket(
        title=title,
        description=description,
        workspace_key=workspace_key,
        department=department,
        kind=kind,
        priority=priority,
        severity=severity,
        status=TicketStatus.OPEN,
        created_by_type=created_by_type,
        created_by_id=created_by_id,
        assigned_agent_id=assigned_agent_id,
        related_lead_id=related_lead_id,
        related_job_id=related_job_id,
        evidence_json=evidence_json or {},
        metadata_json=metadata_json or {},
        tags=tags or [],
        parent_ticket_id=parent_ticket_id,
    )
    session.add(ticket)
    await session.commit()
    await session.refresh(ticket)
    await _emit("TICKET_CREATED", ticket)
    return ticket


async def get_ticket(session: AsyncSession, ticket_id: str) -> Optional[Ticket]:
    r = await session.execute(select(Ticket).where(Ticket.id == ticket_id))
    return r.scalars().first()


async def list_tickets(
    session: AsyncSession,
    workspace_key: Optional[str] = None,
    department: Optional[str] = None,
    status: Optional[str] = None,
    kind: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[Ticket]:
    q = select(Ticket).order_by(Ticket.priority, Ticket.created_at.desc()).limit(limit).offset(offset)
    if workspace_key:
        q = q.where(Ticket.workspace_key == workspace_key)
    if department:
        q = q.where(Ticket.department == department)
    if status:
        q = q.where(Ticket.status == status)
    if kind:
        q = q.where(Ticket.kind == kind)
    return (await session.execute(q)).scalars().all()


async def update_ticket(
    session: AsyncSession,
    ticket_id: str,
    **fields,
) -> Optional[Ticket]:
    ticket = await get_ticket(session, ticket_id)
    if not ticket:
        return None
    for k, v in fields.items():
        if hasattr(ticket, k):
            setattr(ticket, k, v)
    ticket.updated_at = datetime.utcnow()
    await session.commit()
    await session.refresh(ticket)
    await _emit("TICKET_UPDATED", ticket)
    return ticket


# ─── State transitions ────────────────────────────────────────────────────────

async def accept_ticket(
    session: AsyncSession,
    ticket_id: str,
    agent_id: str,
    reason: str = "",
) -> Optional[Ticket]:
    ticket = await get_ticket(session, ticket_id)
    if not ticket or ticket.status not in (TicketStatus.OPEN,):
        return ticket
    ticket.status = TicketStatus.ACCEPTED
    ticket.assigned_agent_id = agent_id
    ticket.acceptance_reason = reason
    ticket.updated_at = datetime.utcnow()
    await session.commit()
    await session.refresh(ticket)
    await _emit("TICKET_ACCEPTED", ticket, {"agent_id": agent_id, "reason": reason})
    return ticket


async def reject_ticket(
    session: AsyncSession,
    ticket_id: str,
    agent_id: str,
    reason: str,
) -> Optional[Ticket]:
    ticket = await get_ticket(session, ticket_id)
    if not ticket:
        return None
    ticket.status = TicketStatus.REJECTED
    ticket.rejection_reason = reason
    ticket.updated_at = datetime.utcnow()
    ticket.closed_at = datetime.utcnow()
    await session.commit()
    await session.refresh(ticket)
    await _emit("TICKET_REJECTED", ticket, {"agent_id": agent_id, "reason": reason})
    return ticket


async def escalate_ticket(
    session: AsyncSession,
    ticket_id: str,
    agent_id: str,
    new_severity: str = TicketSeverity.HIGH,
    note: str = "",
) -> Optional[Ticket]:
    ticket = await get_ticket(session, ticket_id)
    if not ticket:
        return None
    ticket.severity = new_severity
    ticket.metadata_json = {**(ticket.metadata_json or {}), "escalation_note": note, "escalated_by": agent_id}
    ticket.updated_at = datetime.utcnow()
    await session.commit()
    await session.refresh(ticket)
    await _emit("TICKET_ESCALATED", ticket, {"agent_id": agent_id, "new_severity": new_severity})
    return ticket


async def complete_ticket(
    session: AsyncSession,
    ticket_id: str,
    agent_id: str,
    resolution_notes: str = "",
) -> Optional[Ticket]:
    ticket = await get_ticket(session, ticket_id)
    if not ticket:
        return None
    ticket.status = TicketStatus.DONE
    ticket.resolution_notes = resolution_notes
    ticket.closed_at = datetime.utcnow()
    ticket.updated_at = datetime.utcnow()
    await session.commit()
    await session.refresh(ticket)
    await _emit("TICKET_COMPLETED", ticket, {"agent_id": agent_id})
    return ticket


# ─── Deduplication guard for auto-generated tickets ──────────────────────────

async def ticket_exists_for_pattern(
    session: AsyncSession,
    title_pattern: str,
    department: str,
    hours_back: int = 24,
) -> bool:
    """Prevent duplicate auto-tickets for the same pattern within a time window."""
    from datetime import timedelta
    cutoff = datetime.utcnow() - timedelta(hours=hours_back)
    q = select(Ticket).where(
        Ticket.department == department,
        Ticket.created_at >= cutoff,
        Ticket.status.notin_([TicketStatus.REJECTED, TicketStatus.CANCELLED]),
    )
    existing = (await session.execute(q)).scalars().all()
    pattern_low = title_pattern.lower()
    return any(pattern_low in (t.title or "").lower() for t in existing)
