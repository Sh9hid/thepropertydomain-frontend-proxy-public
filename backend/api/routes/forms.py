from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any, Optional
from pydantic import BaseModel
import os
from pathlib import Path

from core.database import get_session
from core.security import get_api_key
from services.form_engine import generate_agency_agreement, generate_offer_form

router = APIRouter()

class AgreementRequest(BaseModel):
    lead_id: str
    campaign_type: str = "EXCLUSIVE" # or "AUCTION"

class OfferRequest(BaseModel):
    lead_id: str
    buyer_name: str
    buyer_phone: str
    price: int
    deposit: Optional[str] = "0.25% Initial / 10% Balance"
    settlement_days: int = 42
    finance: str = "NO"
    conditions: str = "Nil"
    solicitor: str = "TBA"

@router.post("/api/forms/agreement")
async def create_agreement(req: AgreementRequest, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    res = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": req.lead_id})
    lead = res.mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    path = await generate_agency_agreement(dict(lead), req.campaign_type)
    filename = Path(path).name
    
    return {
        "status": "success",
        "url": f"/api/forms/download/agreement/{filename}",
        "path": path
    }

@router.post("/api/forms/offer")
async def create_offer(req: OfferRequest, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    res = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": req.lead_id})
    lead = res.mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    offer_data = {
        "name": req.buyer_name,
        "phone": req.buyer_phone,
        "price": req.price,
        "deposit": req.deposit,
        "settlement": req.settlement_days,
        "finance": req.finance,
        "conditions": req.conditions,
        "solicitor": req.solicitor
    }
    
    path = await generate_offer_form(dict(lead), offer_data)
    filename = Path(path).name
    
    return {
        "status": "success",
        "url": f"/api/forms/download/offer/{filename}",
        "path": path
    }

# File serving logic would usually be handled via StaticFiles in main.py
# but we can add a helper or rely on the global static mount if configured.
