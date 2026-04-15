"""REACTOR Signal API - live intelligence feed."""

from fastapi import APIRouter

from api.routes._deps import APIKeyDep, SessionDep
from services.signal_engine import compute_lead_signals, compute_live_signals

router = APIRouter(prefix="/api", tags=["signals"])


@router.get("/signals/live")
async def get_live_signals(limit: int = 50, api_key: APIKeyDep = "", session: SessionDep = None):
    signals = await compute_live_signals(session, limit=limit)
    return {"signals": signals, "total": len(signals)}


@router.get("/signals/lead/{lead_id}")
async def get_lead_signals(lead_id: str, api_key: APIKeyDep = "", session: SessionDep = None):
    signals = await compute_lead_signals(session, lead_id)
    return {"signals": signals, "lead_id": lead_id}
