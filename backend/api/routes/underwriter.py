from fastapi import APIRouter, HTTPException

from api.routes._deps import APIKeyDep, SessionDep
from models.sql_models import Lead as SQLLead
from services.underwriter_service import generate_underwriter_snapshot

router = APIRouter()


@router.get("/api/underwriter/{lead_id}")
async def underwriter_snapshot(lead_id: str, session: SessionDep, api_key: APIKeyDep):
    lead = await session.get(SQLLead, lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    payload = lead.model_dump()
    return generate_underwriter_snapshot(payload)
