from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session
from core.security import get_api_key
from services.followup_engine import apply_follow_up
from services.missed_deals_service import get_missed_deals, summarize_missed_deals

router = APIRouter(tags=["missed-deals"])


class FollowUpRequest(BaseModel):
    reason: str


@router.get("/api/missed-deals")
async def list_missed_deals(
    range: str = Query(default="today"),
    user_id: Optional[str] = Query(default=None),
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    cards = await get_missed_deals(session, date_range=range, user_id=user_id)
    return sorted(cards, key=lambda item: item["priority_score"], reverse=True)


@router.get("/api/missed-deals/summary")
async def missed_deals_summary(
    range: str = Query(default="today"),
    user_id: Optional[str] = Query(default=None),
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    cards = await get_missed_deals(session, date_range=range, user_id=user_id)
    return summarize_missed_deals(cards)


@router.post("/api/missed-deals/{lead_id}/follow-up")
async def set_missed_deal_follow_up(
    lead_id: str,
    body: FollowUpRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        payload = await apply_follow_up(session, lead_id, body.reason)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok", **payload}
