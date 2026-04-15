"""Voice Training Daily Plan API."""

from __future__ import annotations

from datetime import datetime
from typing import Dict

from fastapi import APIRouter, Query
from sqlmodel import select

from api.routes._deps import SessionDep
from core.config import SYDNEY_TZ
from models.org_models import VoiceTrainingPlan
from services.daily_voice_plan_service import generate_daily_plan, get_plan_history

router = APIRouter(prefix="/voice-training", tags=["voice_training"])


def _plan_dict(p: VoiceTrainingPlan) -> Dict:
    return {
        "id": p.id,
        "plan_date": p.plan_date,
        "rep_id": p.rep_id,
        "calls_analysed": p.calls_analysed,
        "source_call_ids": p.source_call_ids,
        "status": p.status,
        "key_focus": p.key_focus,
        "mistakes": p.mistakes,
        "drills": p.drills,
        "improved_phrases": p.improved_phrases,
        "session_structure": p.session_structure,
        "overall_score": p.overall_score,
        "provider_used": p.provider_used,
        "tokens_used": p.tokens_used,
        "created_at": p.created_at.isoformat(),
    }


@router.get("/today")
async def get_today_plan(rep_id: str = "Shahid", session: SessionDep = None):
    plan = await generate_daily_plan(session, rep_id=rep_id)
    if not plan:
        return {"status": "no_data", "message": "No voice data available to generate a plan."}
    return _plan_dict(plan)


@router.post("/generate")
async def post_generate_plan(rep_id: str = "Shahid", session: SessionDep = None):
    """Force-regenerate today's plan."""
    existing = (
        await session.execute(
            select(VoiceTrainingPlan).where(
                VoiceTrainingPlan.rep_id == rep_id,
                VoiceTrainingPlan.plan_date == datetime.now(SYDNEY_TZ).strftime("%Y-%m-%d"),
            )
        )
    ).scalars().first()
    if existing:
        await session.delete(existing)
        await session.commit()
    plan = await generate_daily_plan(session, rep_id=rep_id)
    if not plan:
        return {"status": "no_data"}
    return _plan_dict(plan)


@router.get("/history")
async def get_history(rep_id: str = "Shahid", limit: int = Query(default=14, le=60), session: SessionDep = None):
    plans = await get_plan_history(session, rep_id=rep_id, limit=limit)
    return [_plan_dict(p) for p in plans]
