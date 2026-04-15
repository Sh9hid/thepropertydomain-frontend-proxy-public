"""REACTOR Admin — feature flags and system control."""
import os
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
from core.security import require_admin_access
from services.source_health_service import list_source_health

router = APIRouter(prefix="/api/admin", tags=["admin"])

# In-memory feature flags (production would use DB)
_FEATURE_FLAGS: Dict[str, bool] = {
    "accent_coach_v2": False,
    "ai_call_analysis": True,
    "auto_outreach_agents": False,
    "signal_engine_v2": False,
    "voice_transcription": True,
    "missed_deal_detector": True,
    "distress_intel": True,
    "orchestration_engine": False,
}

ADMIN_PASSWORD = os.environ.get("REACTOR_ADMIN_PASSWORD", "reactor_admin_2026")

class AdminAuth(BaseModel):
    password: str

class FeatureFlagUpdate(BaseModel):
    flag: str
    enabled: bool

@router.post("/verify", dependencies=[Depends(require_admin_access)])
async def verify_admin(auth: AdminAuth):
    if auth.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid admin password")
    return {"authenticated": True}

@router.get("/feature-flags", dependencies=[Depends(require_admin_access)])
async def get_feature_flags():
    return {"flags": _FEATURE_FLAGS}

@router.post("/feature-flags", dependencies=[Depends(require_admin_access)])
async def update_feature_flag(payload: FeatureFlagUpdate):
    if payload.flag not in _FEATURE_FLAGS:
        raise HTTPException(status_code=404, detail=f"Unknown flag: {payload.flag}")
    _FEATURE_FLAGS[payload.flag] = payload.enabled
    return {"flags": _FEATURE_FLAGS}

@router.get("/system-status", dependencies=[Depends(require_admin_access)])
async def get_system_status():
    return {
        "status": "operational",
        "version": "2.0.0",
        "codename": "REACTOR",
        "features": _FEATURE_FLAGS,
        "environment": os.environ.get("ENVIRONMENT", "development"),
        "source_health": await list_source_health(),
    }
