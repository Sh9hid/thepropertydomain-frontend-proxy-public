"""Tickets API."""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.database import get_session
from models.org_models import Ticket, TicketStatus, TicketSeverity, TicketDept, TicketKind
from services.ticket_service import (
    create_ticket, get_ticket, list_tickets, update_ticket,
    accept_ticket, reject_ticket, escalate_ticket, complete_ticket,
)

router = APIRouter(prefix="/tickets", tags=["tickets"])


class CreateTicketRequest(BaseModel):
    title: str
    description: Optional[str] = None
    workspace_key: str = "real_estate"
    department: str
    kind: str
    priority: int = 5
    severity: str = TicketSeverity.MEDIUM
    created_by_type: str = "user"
    created_by_id: Optional[str] = None
    assigned_agent_id: Optional[str] = None
    related_lead_id: Optional[str] = None
    related_job_id: Optional[str] = None
    evidence_json: Dict[str, Any] = {}
    metadata_json: Dict[str, Any] = {}
    tags: List[str] = []
    parent_ticket_id: Optional[str] = None


def _ticket_dict(t: Ticket) -> Dict:
    return {
        "id": t.id,
        "title": t.title,
        "description": t.description,
        "department": t.department,
        "kind": t.kind,
        "priority": t.priority,
        "severity": t.severity,
        "status": t.status,
        "created_by_type": t.created_by_type,
        "created_by_id": t.created_by_id,
        "assigned_agent_id": t.assigned_agent_id,
        "parent_ticket_id": t.parent_ticket_id,
        "related_lead_id": t.related_lead_id,
        "related_job_id": t.related_job_id,
        "acceptance_reason": t.acceptance_reason,
        "rejection_reason": t.rejection_reason,
        "resolution_notes": t.resolution_notes,
        "evidence_json": t.evidence_json,
        "metadata_json": t.metadata_json,
        "tags": t.tags,
        "created_at": t.created_at.isoformat(),
        "updated_at": t.updated_at.isoformat(),
        "closed_at": t.closed_at.isoformat() if t.closed_at else None,
    }


@router.post("", response_model=Dict)
async def post_create_ticket(body: CreateTicketRequest, session: AsyncSession = Depends(get_session)):
    t = await create_ticket(session, **body.model_dump())
    return _ticket_dict(t)


@router.get("", response_model=List[Dict])
async def get_tickets(
    workspace_key: Optional[str] = None,
    department: Optional[str] = None,
    status: Optional[str] = None,
    kind: Optional[str] = None,
    limit: int = Query(default=100, le=500),
    offset: int = 0,
    session: AsyncSession = Depends(get_session),
):
    tickets = await list_tickets(session, workspace_key, department, status, kind, limit, offset)
    return [_ticket_dict(t) for t in tickets]


@router.get("/{ticket_id}", response_model=Dict)
async def get_single_ticket(ticket_id: str, session: AsyncSession = Depends(get_session)):
    t = await get_ticket(session, ticket_id)
    if not t:
        raise HTTPException(404, "Ticket not found")
    return _ticket_dict(t)


@router.patch("/{ticket_id}", response_model=Dict)
async def patch_ticket(ticket_id: str, body: Dict[str, Any], session: AsyncSession = Depends(get_session)):
    t = await update_ticket(session, ticket_id, **body)
    if not t:
        raise HTTPException(404, "Ticket not found")
    return _ticket_dict(t)


@router.post("/{ticket_id}/accept")
async def post_accept(
    ticket_id: str,
    body: Dict[str, Any],
    session: AsyncSession = Depends(get_session),
):
    t = await accept_ticket(session, ticket_id, body.get("agent_id", "system"), body.get("reason", ""))
    if not t:
        raise HTTPException(404, "Ticket not found")
    return _ticket_dict(t)


@router.post("/{ticket_id}/reject")
async def post_reject(
    ticket_id: str,
    body: Dict[str, Any],
    session: AsyncSession = Depends(get_session),
):
    t = await reject_ticket(session, ticket_id, body.get("agent_id", "system"), body.get("reason", ""))
    if not t:
        raise HTTPException(404, "Ticket not found")
    return _ticket_dict(t)


@router.post("/{ticket_id}/escalate")
async def post_escalate(
    ticket_id: str,
    body: Dict[str, Any],
    session: AsyncSession = Depends(get_session),
):
    t = await escalate_ticket(
        session, ticket_id,
        body.get("agent_id", "system"),
        body.get("new_severity", TicketSeverity.HIGH),
        body.get("note", ""),
    )
    if not t:
        raise HTTPException(404, "Ticket not found")
    return _ticket_dict(t)


@router.post("/{ticket_id}/complete")
async def post_complete(
    ticket_id: str,
    body: Dict[str, Any],
    session: AsyncSession = Depends(get_session),
):
    t = await complete_ticket(
        session, ticket_id,
        body.get("agent_id", "system"),
        body.get("resolution_notes", ""),
    )
    if not t:
        raise HTTPException(404, "Ticket not found")
    return _ticket_dict(t)


@router.get("/stats/summary")
async def ticket_stats(session: AsyncSession = Depends(get_session)):
    from sqlalchemy import text
    rows = (await session.execute(text("""
        SELECT department, status, count(*) as cnt
        FROM org_tickets
        GROUP BY department, status
    """))).mappings().all()
    return [dict(r) for r in rows]
