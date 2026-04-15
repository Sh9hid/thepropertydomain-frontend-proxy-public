from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session
from core.security import get_api_key
from models.funnel_schemas import (
    ConsentUpsertRequest,
    FunnelBookingRequest,
    FunnelOutreachTaskRequest,
    FunnelStageUpdateRequest,
    LeadFunnelsResponse,
    SuppressionUpsertRequest,
)
from services.funnel_service import (
    apply_lead_suppression,
    book_funnel_appointment,
    get_lead_funnels,
    queue_funnel_outreach_task,
    record_lead_consent,
    release_lead_suppression,
    update_lead_funnel_stage,
)

router = APIRouter(tags=["Funnels"])


@router.get("/api/funnels/{lead_id}", response_model=LeadFunnelsResponse)
async def get_lead_funnel_bundle(
    lead_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await get_lead_funnels(session, lead_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/funnels/{lead_id}/consent", response_model=LeadFunnelsResponse)
async def upsert_lead_consent(
    lead_id: str,
    body: ConsentUpsertRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await record_lead_consent(session, lead_id, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/funnels/{lead_id}/suppression", response_model=LeadFunnelsResponse)
async def upsert_lead_suppression(
    lead_id: str,
    body: SuppressionUpsertRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await apply_lead_suppression(session, lead_id, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/funnels/{lead_id}/suppression/release", response_model=LeadFunnelsResponse)
async def release_suppression(
    lead_id: str,
    channel: str = Query(...),
    released_by: str = Query(default="operator"),
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await release_lead_suppression(session, lead_id, channel, released_by=released_by)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/funnels/{lead_id}/{funnel_type}/stage", response_model=LeadFunnelsResponse)
async def set_funnel_stage(
    lead_id: str,
    funnel_type: str,
    body: FunnelStageUpdateRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await update_lead_funnel_stage(session, lead_id, funnel_type, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/funnels/{lead_id}/{funnel_type}/book", response_model=LeadFunnelsResponse)
async def book_funnel_step(
    lead_id: str,
    funnel_type: str,
    body: FunnelBookingRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await book_funnel_appointment(session, lead_id, funnel_type, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/funnels/{lead_id}/queue-outreach")
async def queue_funnel_outreach(
    lead_id: str,
    body: FunnelOutreachTaskRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await queue_funnel_outreach_task(session, lead_id, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
