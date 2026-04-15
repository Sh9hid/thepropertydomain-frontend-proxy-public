"""
Outreach Queue API — browse, approve, edit, reject agent-drafted outreach.

Reads from hermes_campaigns table where agents have queued drafts.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import text

from api.routes._deps import APIKeyDep, SessionDep

router = APIRouter()


@router.get("/api/outreach/queue")
async def list_outreach_queue(
    api_key: APIKeyDep,
    session: SessionDep,
    status: Optional[str] = None,
    channel: Optional[str] = None,
    limit: int = 50,
):
    """List outreach drafts. Filter by status (pending_approval, approved, sent, send_failed)."""
    where_clauses = ["1=1"]
    params = {"limit": limit}

    if status:
        where_clauses.append("c.status = :status")
        params["status"] = status
    if channel:
        where_clauses.append("c.channel = :channel")
        params["channel"] = channel

    where_sql = " AND ".join(where_clauses)

    rows = (await session.execute(text(f"""
        SELECT c.id, c.campaign_type, c.audience, c.channel, c.stage,
               c.subject, c.message, c.goal, c.status, c.related_lead_id,
               c.created_at, c.sent_at,
               l.address, l.suburb, l.contact_phones, l.contact_emails
        FROM hermes_campaigns c
        LEFT JOIN leads l ON l.id = c.related_lead_id
        WHERE {where_sql}
        ORDER BY
            CASE c.status
                WHEN 'pending_approval' THEN 0
                WHEN 'approved' THEN 1
                WHEN 'send_failed' THEN 2
                WHEN 'sent' THEN 3
                ELSE 4
            END,
            c.created_at DESC
        LIMIT :limit
    """), params)).mappings().all()

    return {"count": len(rows), "drafts": [dict(r) for r in rows]}


class ApproveBody(BaseModel):
    approved_by: str = "operator"


@router.post("/api/outreach/queue/{draft_id}/approve")
async def approve_draft(draft_id: str, body: ApproveBody, api_key: APIKeyDep, session: SessionDep):
    """Approve a pending draft for sending."""
    result = await session.execute(text("""
        UPDATE hermes_campaigns SET status = 'approved'
        WHERE id = :id AND status = 'pending_approval'
    """), {"id": draft_id})
    await session.commit()
    if result.rowcount == 0:
        return {"ok": False, "error": "not_found_or_already_processed"}
    return {"ok": True, "draft_id": draft_id, "status": "approved"}


class EditBody(BaseModel):
    subject: Optional[str] = None
    message: Optional[str] = None


@router.put("/api/outreach/queue/{draft_id}/edit")
async def edit_draft(draft_id: str, body: EditBody, api_key: APIKeyDep, session: SessionDep):
    """Edit a pending draft's subject/message."""
    updates = []
    params = {"id": draft_id}
    if body.subject is not None:
        updates.append("subject = :subject")
        params["subject"] = body.subject
    if body.message is not None:
        updates.append("message = :message")
        params["message"] = body.message
    if not updates:
        return {"ok": False, "error": "nothing_to_update"}

    set_clause = ", ".join(updates)
    result = await session.execute(text(f"""
        UPDATE hermes_campaigns SET {set_clause}
        WHERE id = :id AND status = 'pending_approval'
    """), params)
    await session.commit()
    if result.rowcount == 0:
        return {"ok": False, "error": "not_found_or_already_processed"}
    return {"ok": True, "draft_id": draft_id}


@router.post("/api/outreach/queue/{draft_id}/reject")
async def reject_draft(draft_id: str, api_key: APIKeyDep, session: SessionDep):
    """Reject a pending draft."""
    result = await session.execute(text("""
        UPDATE hermes_campaigns SET status = 'rejected'
        WHERE id = :id AND status IN ('pending_approval', 'approved')
    """), {"id": draft_id})
    await session.commit()
    if result.rowcount == 0:
        return {"ok": False, "error": "not_found_or_already_processed"}
    return {"ok": True, "draft_id": draft_id, "status": "rejected"}


@router.post("/api/outreach/queue/approve-all")
async def approve_all_pending(api_key: APIKeyDep, session: SessionDep):
    """Approve all pending drafts at once."""
    result = await session.execute(text("""
        UPDATE hermes_campaigns SET status = 'approved'
        WHERE status = 'pending_approval'
    """))
    await session.commit()
    return {"ok": True, "approved_count": result.rowcount}
