"""Research API."""

from __future__ import annotations

from typing import Dict, Optional

from fastapi import APIRouter, Query

from api.routes._deps import SessionDep
from models.org_models import ResearchArea, ResearchNote, ResearchRun
from services.research_service import get_recent_notes, get_recent_runs, run_research

router = APIRouter(prefix="/research", tags=["research"])


def _note_dict(n: ResearchNote) -> Dict:
    return {
        "id": n.id,
        "title": n.title,
        "area": n.area,
        "thesis": n.thesis,
        "evidence": n.evidence,
        "recommendation": n.recommendation,
        "confidence": n.confidence,
        "produced_by_agent": n.produced_by_agent,
        "ticket_raised_id": n.ticket_raised_id,
        "run_id": n.run_id,
        "evidence_json": n.evidence_json,
        "created_at": n.created_at.isoformat(),
    }


def _run_dict(r: ResearchRun) -> Dict:
    return {
        "id": r.id,
        "area": r.area,
        "status": r.status,
        "started_at": r.started_at.isoformat(),
        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
        "notes_created": r.notes_created,
        "tickets_raised": r.tickets_raised,
        "provider_used": r.provider_used,
        "tokens_used": r.tokens_used,
        "cost_usd": r.cost_usd,
        "error": r.error,
    }


@router.post("/run/{area}")
async def post_run_research(area: str, session: SessionDep):
    if area not in (ResearchArea.SALES, ResearchArea.REAL_ESTATE, ResearchArea.APP_TECH):
        return {"error": f"Unknown area: {area}. Use: sales, real_estate, app_tech"}
    run = await run_research(session, area)
    return _run_dict(run)


@router.post("/run/all")
async def post_run_all(session: SessionDep):
    results = []
    for area in (ResearchArea.SALES, ResearchArea.REAL_ESTATE, ResearchArea.APP_TECH):
        run = await run_research(session, area)
        results.append(_run_dict(run))
    return results


@router.get("/notes")
async def get_notes(area: Optional[str] = None, limit: int = Query(default=30, le=200), session: SessionDep = None):
    notes = await get_recent_notes(session, area=area, limit=limit)
    return [_note_dict(n) for n in notes]


@router.get("/runs")
async def get_runs(limit: int = Query(default=10, le=50), session: SessionDep = None):
    runs = await get_recent_runs(session, limit=limit)
    return [_run_dict(r) for r in runs]
