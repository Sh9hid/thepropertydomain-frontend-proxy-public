from typing import Optional

from fastapi import APIRouter, Depends, File, Form, Query, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session
from core.security import get_api_key
from models.archive_schemas import (
    AssetBatchUpsertRequest,
    AssetBatchUpsertResponse,
    LeadEvidenceLinkRequest,
    SyncRunCompleteRequest,
    SyncRunStartRequest,
    SyncStatusResponse,
)
from services.archive_mirror_service import (
    complete_sync_run,
    create_sync_run,
    get_asset_detail,
    get_asset_download_path,
    get_asset_or_404,
    link_asset_to_lead,
    list_assets,
    list_sync_runs,
    refresh_asset_derivatives,
    save_uploaded_asset,
    upsert_asset_manifests,
)

router = APIRouter(tags=["Evidence"])


@router.post("/api/system/sync/full-mirror/start")
async def start_full_mirror_sync(
    body: SyncRunStartRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    run = await create_sync_run(session, body)
    return run.model_dump()


@router.get("/api/system/sync/status", response_model=SyncStatusResponse)
async def get_sync_status(
    run_id: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=50),
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    runs = await list_sync_runs(session, run_id=run_id, limit=limit)
    return {"runs": [run.model_dump() for run in runs]}


@router.post("/api/system/sync/assets/batch", response_model=AssetBatchUpsertResponse)
async def batch_upsert_sync_assets(
    body: AssetBatchUpsertRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    items = await upsert_asset_manifests(session, body.sync_run_id, body.assets)
    upload_required_count = sum(1 for item in items if item.upload_required)
    return {
        "sync_run_id": body.sync_run_id,
        "accepted": len(items),
        "upload_required_count": upload_required_count,
        "assets": [item.model_dump() for item in items],
    }


@router.post("/api/system/sync/assets/{asset_id}/upload")
async def upload_sync_asset(
    asset_id: str,
    sync_run_id: str = Form(...),
    sha256: Optional[str] = Form(default=None),
    modified_at: Optional[str] = Form(default=None),
    file: UploadFile = File(...),
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    asset = await save_uploaded_asset(
        session=session,
        asset_id=asset_id,
        sync_run_id=sync_run_id,
        upload=file,
        sha256=sha256,
        modified_at=modified_at,
    )
    return await get_asset_detail(session, asset.id)


@router.post("/api/system/sync/runs/{run_id}/complete")
async def finalize_sync_run(
    run_id: str,
    body: SyncRunCompleteRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    run = await complete_sync_run(session, run_id, body)
    return run.model_dump()


@router.post("/api/system/sync/assets/{asset_id}/refresh-derivatives")
async def refresh_sync_asset_derivatives(
    asset_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    asset = await get_asset_or_404(session, asset_id)
    await refresh_asset_derivatives(session, asset)
    await session.commit()
    return await get_asset_detail(session, asset_id)


@router.get("/api/evidence/assets")
async def get_evidence_assets(
    search: Optional[str] = Query(default=None),
    lead_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    return await list_assets(session, search=search, lead_id=lead_id, limit=limit, offset=offset)


@router.get("/api/evidence/assets/{asset_id}")
async def get_evidence_asset_detail(
    asset_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    return await get_asset_detail(session, asset_id)


@router.get("/api/evidence/assets/{asset_id}/download")
async def download_evidence_asset(
    asset_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    asset, absolute_path = await get_asset_download_path(session, asset_id)
    return FileResponse(
        absolute_path,
        filename=asset.original_name,
        media_type=asset.mime_type or "application/octet-stream",
    )


@router.post("/api/evidence/assets/{asset_id}/link-lead")
async def attach_evidence_to_lead(
    asset_id: str,
    body: LeadEvidenceLinkRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    link = await link_asset_to_lead(
        session=session,
        asset_id=asset_id,
        lead_id=body.lead_id,
        link_type=body.link_type,
        confidence_score=body.confidence_score,
        rationale=body.rationale,
        include_on_lead=body.include_on_lead,
        linked_by=body.linked_by,
    )
    return link.model_dump()
