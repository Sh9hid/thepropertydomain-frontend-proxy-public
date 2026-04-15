from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session
from core.security import get_api_key
from models.sales_core_models import ContactAttempt, EnrichmentState, LeadContact, LeadState, TaskQueue
from models.sales_core_schemas import (
    ContactAttemptPayload,
    DialingContextResponse,
    EnrichmentQueueRequest,
    LeadContactPayload,
    LeadStatePayload,
    LogContactAttemptRequest,
    LogContactAttemptResponse,
    TaskQueuePayload,
)
from services.sales_core.dialing_service import fetch_next_callable_record, get_lead_context, log_contact_attempt, sync_lead_state
from services.sales_core.enrichment_service import claim_next_enrichment_job, enqueue_enrichment_job


router = APIRouter()


def _resolved_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


@router.get("/api/dialing/next", response_model=DialingContextResponse)
async def get_next_callable_record(
    business_context: str,
    session: AsyncSession = Depends(get_session),
    api_key: str = Depends(get_api_key),
):
    context = await fetch_next_callable_record(session, business_context_key=business_context, now=_resolved_now())
    if context is None:
        raise HTTPException(status_code=404, detail="No callable record available")
    return DialingContextResponse(
        contact=LeadContactPayload.model_validate(context["contact"].model_dump()),
        lead=context["lead"].model_dump() if context["lead"] else None,
        state=LeadStatePayload.model_validate(context["state"].model_dump()) if context["state"] else None,
        tasks=[TaskQueuePayload.model_validate(task.model_dump()) for task in context["tasks"]],
        attempts=[ContactAttemptPayload.model_validate(item.model_dump()) for item in context["attempts"]],
    )


@router.get("/api/dialing/contacts/{lead_contact_id}", response_model=DialingContextResponse)
async def get_dialing_context(
    lead_contact_id: str,
    session: AsyncSession = Depends(get_session),
    api_key: str = Depends(get_api_key),
):
    try:
        context = await get_lead_context(session, lead_contact_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return DialingContextResponse(
        contact=LeadContactPayload.model_validate(context["contact"].model_dump()),
        lead=context["lead"].model_dump() if context["lead"] else None,
        state=LeadStatePayload.model_validate(context["state"].model_dump()) if context["state"] else None,
        tasks=[TaskQueuePayload.model_validate(task.model_dump()) for task in context["tasks"]],
        attempts=[ContactAttemptPayload.model_validate(item.model_dump()) for item in context["attempts"]],
    )


@router.post("/api/dialing/attempts/log", response_model=LogContactAttemptResponse)
async def create_contact_attempt(
    body: LogContactAttemptRequest,
    session: AsyncSession = Depends(get_session),
    api_key: str = Depends(get_api_key),
):
    result = await log_contact_attempt(session, body.model_dump(), now=_resolved_now())
    return LogContactAttemptResponse(
        attempt=ContactAttemptPayload.model_validate(result["attempt"].model_dump()),
        state=LeadStatePayload.model_validate(result["state"].model_dump()),
        task=TaskQueuePayload.model_validate(result["task"].model_dump()) if result["task"] else None,
    )


@router.post("/api/dialing/contacts/{lead_contact_id}/sync-state", response_model=dict)
async def sync_contact_state_endpoint(
    lead_contact_id: str,
    session: AsyncSession = Depends(get_session),
    api_key: str = Depends(get_api_key),
):
    state = await sync_lead_state(session, lead_contact_id, now=_resolved_now())
    return {"status": "ok", "state": LeadStatePayload.model_validate(state.model_dump()).model_dump(mode="json")}


@router.post("/api/sales/enrichment/queue", response_model=dict)
async def queue_sales_enrichment(
    body: EnrichmentQueueRequest,
    session: AsyncSession = Depends(get_session),
    api_key: str = Depends(get_api_key),
):
    result = await enqueue_enrichment_job(
        session,
        business_context_key=body.business_context_key,
        lead_contact_id=body.lead_contact_id,
        source=body.source,
        reason=body.reason,
        now=_resolved_now(),
        target_type=body.target_type,
        target_id=body.target_id,
    )
    return {
        "status": "queued" if result["enqueued"] else "skipped",
        "reason": result["reason"],
        "job": result["job"].model_dump(mode="json"),
    }


@router.post("/api/sales/enrichment/claim", response_model=dict)
async def claim_sales_enrichment(
    business_context: str | None = None,
    source: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    job = await claim_next_enrichment_job(
        session,
        business_context_key=business_context,
        source=source,
        now=_resolved_now(),
    )
    if job is None:
        return {"job": None}
    return {"job": job.model_dump(mode="json")}
