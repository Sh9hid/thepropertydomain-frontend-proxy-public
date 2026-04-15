from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.routes._deps import APIKeyDep, SessionDep
from models.distress_schemas import (
    DistressLandlordWatchResponse,
    DistressManualIngestRequest,
    DistressRunListResponse,
    DistressSignalListResponse,
    DistressSourceListResponse,
    DistressSourceRunRequest,
)
from services.distress_intel_service import (
    ensure_distress_sources,
    get_distress_landlord_watch,
    ingest_manual_distress_signals,
    list_distress_runs,
    list_distress_signals,
    list_distress_sources,
    run_all_enabled_distress_sources,
    run_distress_source,
)

router = APIRouter(tags=["Distress"])


@router.get("/api/distress/sources", response_model=DistressSourceListResponse)
async def get_distress_sources(api_key: APIKeyDep = "", session: SessionDep = None):
    await ensure_distress_sources(session)
    return await list_distress_sources(session)


@router.get("/api/distress/runs", response_model=DistressRunListResponse)
async def get_distress_runs(limit: int = Query(default=50, ge=1, le=200), api_key: APIKeyDep = "", session: SessionDep = None):
    await ensure_distress_sources(session)
    return await list_distress_runs(session, limit=limit)


@router.get("/api/distress/signals", response_model=DistressSignalListResponse)
async def get_distress_signals(
    signal_type: Optional[str] = Query(default=None),
    source_key: Optional[str] = Query(default=None),
    suburb: Optional[str] = Query(default=None),
    query: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=300),
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    await ensure_distress_sources(session)
    return await list_distress_signals(
        session,
        signal_type=signal_type,
        source_key=source_key,
        suburb=suburb,
        query=query,
        limit=limit,
    )


@router.post("/api/distress/sources/{source_key}/run")
async def run_distress_source_route(
    source_key: str,
    body: DistressSourceRunRequest,
    api_key: APIKeyDep,
    session: SessionDep,
):
    await ensure_distress_sources(session)
    result = await run_distress_source(session, source_key=source_key, requested_by=body.requested_by)
    if result.get("error") and "not found" in str(result["error"]).lower():
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/api/distress/run-all")
async def run_all_distress_sources_route(body: DistressSourceRunRequest, api_key: APIKeyDep, session: SessionDep):
    await ensure_distress_sources(session)
    return await run_all_enabled_distress_sources(session, requested_by=body.requested_by)


@router.post("/api/distress/manual-ingest")
async def ingest_distress_signals_route(body: DistressManualIngestRequest, api_key: APIKeyDep, session: SessionDep):
    try:
        return await ingest_manual_distress_signals(session, body)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/distress/landlord-watch", response_model=DistressLandlordWatchResponse)
async def get_distress_landlord_watch_route(
    min_properties: int = Query(default=5, ge=1, le=100),
    min_years_since_finance_event: int = Query(default=10, ge=1, le=50),
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    await ensure_distress_sources(session)
    return await get_distress_landlord_watch(
        session,
        min_properties=min_properties,
        min_years_since_finance_event=min_years_since_finance_event,
    )
