from __future__ import annotations

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from api.routes._deps import APIKeyDep, SessionDep
from models.schemas import (
    CallAnalysisResponse,
    CoachingReportResponse,
    RepScoreSummaryResponse,
    SpeechUploadResponse,
)
from services.speech_pipeline_service import (
    get_call_analysis_payload,
    get_call_coaching_payload,
    get_rep_score_summary,
    ingest_uploaded_recorded_call,
)

router = APIRouter()


@router.post("/api/speech/uploads", response_model=SpeechUploadResponse)
async def upload_recorded_call(
    file: UploadFile = File(...),
    lead_id: str = Form(""),
    rep_id: str = Form(""),
    source: str = Form("upload"),
    call_type: str = Form("recorded_call"),
    outcome: str = Form("uploaded"),
    started_at: str = Form(""),
    duration_seconds: int = Form(0),
    transcript_hint: str = Form(""),
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    return await ingest_uploaded_recorded_call(
        session,
        upload=file,
        lead_id=lead_id,
        rep_id=rep_id,
        source=source,
        call_type=call_type,
        outcome=outcome,
        started_at=started_at or None,
        duration_seconds=duration_seconds,
        transcript_hint=transcript_hint,
    )


@router.get("/api/calls/{call_id}/analysis", response_model=CallAnalysisResponse)
async def get_call_analysis(call_id: str, api_key: APIKeyDep = "", session: SessionDep = None):
    try:
        return await get_call_analysis_payload(session, call_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/calls/{call_id}/coaching-report", response_model=CoachingReportResponse)
async def get_call_coaching_report(call_id: str, api_key: APIKeyDep = "", session: SessionDep = None):
    try:
        return await get_call_coaching_payload(session, call_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/reps/{rep_id}/scores", response_model=RepScoreSummaryResponse)
async def get_rep_scores(rep_id: str, api_key: APIKeyDep = "", session: SessionDep = None):
    return await get_rep_score_summary(session, rep_id)
