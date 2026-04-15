from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.routes._deps import APIKeyDep, SessionDep
from services.growth_engine import build_growth_digest
from services.revenue_growth_service import (
    generate_daily_content_bundle,
    queue_basic_sequence,
    record_email_event,
    send_outreach_email,
    summarize_email_performance,
)

router = APIRouter(prefix="/api/growth", tags=["growth"])


@router.get("/digest")
async def get_growth_digest(api_key: APIKeyDep = "", session: SessionDep = None):
    return await build_growth_digest(session)


class SendGrowthEmailRequest(BaseModel):
    business_context_key: str
    lead_contact_id: str
    sequence_step: int = 0
    account_id: Optional[str] = None
    force_variant: Optional[str] = None
    created_by: str = "system"
    queue_follow_ups: bool = True


class QueueSequenceRequest(BaseModel):
    business_context_key: str
    lead_contact_id: str
    created_by: str = "system"


class EmailEventRequest(BaseModel):
    event_type: str
    metadata: Dict[str, Any] = {}


class ContentGenerationRequest(BaseModel):
    business_context_key: str
    posts_per_day: int = 5
    blog_count: int = 1
    newsletter_count: int = 1
    created_by: str = "system"


@router.post("/outreach/send-email")
async def send_growth_email(body: SendGrowthEmailRequest, api_key: APIKeyDep = "", session: SessionDep = None):
    try:
        result = await send_outreach_email(
            session,
            business_context_key=body.business_context_key,
            lead_contact_id=body.lead_contact_id,
            sequence_step=body.sequence_step,
            account_id=body.account_id,
            force_variant=body.force_variant,
            created_by=body.created_by,
            queue_follow_ups=body.queue_follow_ups,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "attempt": result["attempt"].model_dump(mode="json"),
        "state": result["state"].model_dump(mode="json"),
        "tasks": [task.model_dump(mode="json") for task in result["tasks"]],
    }


@router.post("/outreach/queue-sequence")
async def queue_growth_sequence(body: QueueSequenceRequest, api_key: APIKeyDep = "", session: SessionDep = None):
    try:
        tasks = await queue_basic_sequence(
            session,
            business_context_key=body.business_context_key,
            lead_contact_id=body.lead_contact_id,
            created_by=body.created_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"tasks": [task.model_dump(mode="json") for task in tasks]}


@router.post("/outreach/attempts/{attempt_id}/events")
async def log_growth_email_event(
    attempt_id: str,
    body: EmailEventRequest,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    try:
        attempt = await record_email_event(
            session,
            attempt_id=attempt_id,
            event_type=body.event_type,
            metadata=body.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"attempt": attempt.model_dump(mode="json")}


@router.get("/outreach/performance")
async def get_growth_email_performance(
    business_context_key: str,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    return await summarize_email_performance(session, business_context_key=business_context_key)


@router.post("/content/generate")
async def generate_growth_content(
    body: ContentGenerationRequest,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    payload = await generate_daily_content_bundle(
        session,
        business_context_key=body.business_context_key,
        posts_per_day=body.posts_per_day,
        blog_count=body.blog_count,
        newsletter_count=body.newsletter_count,
        created_by=body.created_by,
    )
    return {
        "run_date": payload["run_date"],
        "counts": payload["counts"],
        "assets": [asset.model_dump(mode="json") for asset in payload["assets"]],
        "source": payload["source"],
    }
