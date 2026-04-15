"""
Operator Review Queue endpoints.
Phase 2: triage the 13K imported leads — confirm, merge duplicates, or suppress.
"""
import json
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session, _get_lead_or_404
from core.logic import _hydrate_lead
from core.security import get_api_key
from core.utils import now_iso

router = APIRouter()


class ReviewConfirmBody(BaseModel):
    note: Optional[str] = None


class ReviewMergeBody(BaseModel):
    duplicate_id: str
    note: Optional[str] = None


class ReviewSuppressBody(BaseModel):
    reason: Optional[str] = None


class BatchConfirmBody(BaseModel):
    lead_ids: List[str]


@router.get("/api/review/queue")
async def get_review_queue(
    page: int = 1,
    page_size: int = 50,
    search: Optional[str] = None,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """
    Returns leads that need operator triage:
    no phone, no email, or low confidence score.
    """
    offset = (page - 1) * page_size
    base_where = """
        (COALESCE(contact_phones,'[]') IN ('[]','') AND COALESCE(contact_emails,'[]') IN ('[]',''))
        OR COALESCE(confidence_score, 0) < 40
    """
    if search:
        q = f"%{search.lower()}%"
        rows = (await session.execute(
            text(
                f"SELECT id, address, suburb, postcode, owner_name, trigger_type, contact_phones, contact_emails, confidence_score, evidence_score, status FROM leads WHERE ({base_where}) AND (LOWER(address) LIKE :q OR LOWER(owner_name) LIKE :q OR LOWER(suburb) LIKE :q) ORDER BY COALESCE(evidence_score,0) DESC LIMIT :limit OFFSET :offset"
            ),
            {"q": q, "limit": page_size, "offset": offset},
        )).mappings().all()
        total_row = (await session.execute(
            text(
                f"SELECT COUNT(*) as c FROM leads WHERE ({base_where}) AND (LOWER(address) LIKE :q OR LOWER(owner_name) LIKE :q OR LOWER(suburb) LIKE :q)"
            ),
            {"q": q},
        )).mappings().first()
    else:
        rows = (await session.execute(
            text(
                f"SELECT id, address, suburb, postcode, owner_name, trigger_type, contact_phones, contact_emails, confidence_score, evidence_score, status FROM leads WHERE {base_where} ORDER BY COALESCE(evidence_score,0) DESC LIMIT :limit OFFSET :offset"
            ),
            {"limit": page_size, "offset": offset},
        )).mappings().all()
        total_row = (await session.execute(
            text(f"SELECT COUNT(*) as c FROM leads WHERE {base_where}")
        )).mappings().first()
    total = total_row["c"] if total_row else 0
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "leads": [dict(row) for row in rows],
    }


@router.post("/api/review/{lead_id}/confirm")
async def confirm_lead(lead_id: str, body: Optional[ReviewConfirmBody] = None, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """Mark a lead as operator-verified."""
    await _get_lead_or_404(session, lead_id)
    now = now_iso()
    await session.execute(
        text("UPDATE leads SET owner_verified = 1, contact_status = 'verified', updated_at = :now WHERE id = :id"),
        {"now": now, "id": lead_id},
    )
    if body and body.note:
        await session.execute(
            text("INSERT OR IGNORE INTO notes (lead_id, note_type, content, created_at) VALUES (:lead_id, 'review_confirmed', :note, :now)"),
            {"lead_id": lead_id, "note": body.note, "now": now},
        )
    await session.commit()
    return {"status": "confirmed", "lead_id": lead_id}


@router.post("/api/review/{lead_id}/merge")
async def merge_lead(lead_id: str, body: ReviewMergeBody, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """Mark a lead as duplicate of duplicate_id."""
    await _get_lead_or_404(session, lead_id)
    await _get_lead_or_404(session, body.duplicate_id)
    now = now_iso()
    await session.execute(
        text("UPDATE leads SET status = 'dropped', contact_status = 'merged', stage_note = :note, updated_at = :now WHERE id = :id"),
        {"note": f"Merged into {body.duplicate_id}. {body.note or ''}".strip(), "now": now, "id": lead_id},
    )
    await session.commit()
    return {"status": "merged", "lead_id": lead_id, "merged_into": body.duplicate_id}


@router.post("/api/review/{lead_id}/suppress")
async def suppress_lead(lead_id: str, body: Optional[ReviewSuppressBody] = None, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """Mark a lead as do-not-contact."""
    await _get_lead_or_404(session, lead_id)
    now = now_iso()
    await session.execute(
        text("UPDATE leads SET status = 'dropped', contact_status = 'suppressed', stage_note = :note, updated_at = :now WHERE id = :id"),
        {"note": (body.reason if body else None) or "Operator suppressed", "now": now, "id": lead_id},
    )
    await session.commit()
    return {"status": "suppressed", "lead_id": lead_id}


@router.post("/api/review/batch-confirm")
async def batch_confirm(body: BatchConfirmBody, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """Bulk confirm up to 50 leads."""
    if len(body.lead_ids) > 50:
        raise HTTPException(status_code=400, detail="Max 50 leads per batch")
    now = now_iso()
    confirmed = 0
    for lead_id in body.lead_ids:
        try:
            await session.execute(
                text("UPDATE leads SET owner_verified = 1, contact_status = 'verified', updated_at = :now WHERE id = :id"),
                {"now": now, "id": lead_id},
            )
            confirmed += 1
        except Exception:
            pass
    await session.commit()
    return {"status": "ok", "confirmed": confirmed}
