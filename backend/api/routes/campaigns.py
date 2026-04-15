from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from api.routes._deps import APIKeyDep, SessionDep

router = APIRouter()


class CampaignCreate(BaseModel):
    name: str
    description: Optional[str] = None
    target_signal_status: Optional[str] = None
    target_route_queue: Optional[str] = None
    target_max_dom: Optional[int] = None
    target_min_heat_score: Optional[int] = 0
    action_type: Optional[str] = "call_cadence"


@router.get("/api/campaigns")
async def list_campaigns(api_key: APIKeyDep, session: SessionDep):
    from services.campaign_service import ensure_campaigns_table, list_campaigns

    await ensure_campaigns_table(session)
    return await list_campaigns(session)


@router.post("/api/campaigns")
async def create_campaign(body: CampaignCreate, api_key: APIKeyDep, session: SessionDep):
    from services.campaign_service import create_campaign, ensure_campaigns_table

    await ensure_campaigns_table(session)
    return await create_campaign(session, body.dict())


@router.post("/api/campaigns/{campaign_id}/activate")
async def activate_campaign(campaign_id: str, api_key: APIKeyDep, session: SessionDep):
    from services.campaign_service import activate_campaign, ensure_campaigns_table

    await ensure_campaigns_table(session)
    return await activate_campaign(session, campaign_id)


@router.post("/api/campaigns/{campaign_id}/pause")
async def pause_campaign(campaign_id: str, api_key: APIKeyDep, session: SessionDep):
    from services.campaign_service import pause_campaign

    return await pause_campaign(session, campaign_id)


@router.get("/api/campaigns/{campaign_id}/leads")
async def get_campaign_leads(campaign_id: str, api_key: APIKeyDep, session: SessionDep):
    from services.campaign_service import ensure_campaigns_table, get_campaign_leads

    await ensure_campaigns_table(session)
    return await get_campaign_leads(session, campaign_id)
