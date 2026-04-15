from fastapi import APIRouter, HTTPException, Query

from api.routes._deps import APIKeyDep, SessionDep
from models.control_schemas import (
    ControlArtifactApplyRequest,
    ControlDowngradeApproveRequest,
    ControlLiveSnapshot,
    ControlMissionApproveRequest,
    ControlMissionCommandRequest,
    ControlMissionDetail,
    ControlMissionListResponse,
    ControlMissionPreview,
    ControlMissionRestartRequest,
    ControlOrgRunDetail,
    ControlWorkItemDecisionRequest,
    ControlWorkItemListResponse,
    RunArtifactPayload,
    WorkItemPayload,
)
from services.control_service import (
    apply_patch_artifact,
    approve_control_downgrade,
    approve_control_mission,
    approve_work_item,
    create_control_mission,
    get_control_live_snapshot,
    get_control_mission_detail,
    get_control_org_run_detail,
    list_control_missions,
    list_control_work_items,
    preview_control_mission,
    reject_work_item,
    restart_control_mission,
)

router = APIRouter(tags=["Control"])


@router.post("/api/control/command", response_model=ControlMissionDetail)
async def create_control_command(body: ControlMissionCommandRequest, api_key: APIKeyDep, session: SessionDep):
    try:
        return await create_control_mission(session, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/control/command/preview", response_model=ControlMissionPreview)
async def preview_control_command(body: ControlMissionCommandRequest, api_key: APIKeyDep, session: SessionDep):
    try:
        return await preview_control_mission(session, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/api/control/missions", response_model=ControlMissionListResponse)
async def get_control_missions(limit: int = Query(default=20, ge=1, le=100), api_key: APIKeyDep = "", session: SessionDep = None):
    return await list_control_missions(session, limit=limit)


@router.get("/api/control/live", response_model=ControlLiveSnapshot)
async def get_control_live(limit: int = Query(default=8, ge=4, le=20), api_key: APIKeyDep = "", session: SessionDep = None):
    return await get_control_live_snapshot(session, limit=limit)


@router.get("/api/control/missions/{mission_id}", response_model=ControlMissionDetail)
async def get_control_mission(mission_id: str, api_key: APIKeyDep, session: SessionDep):
    try:
        return await get_control_mission_detail(session, mission_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/control/missions/{mission_id}/approve", response_model=ControlMissionDetail)
async def approve_control_mission_route(
    mission_id: str,
    body: ControlMissionApproveRequest,
    api_key: APIKeyDep,
    session: SessionDep,
):
    try:
        return await approve_control_mission(session, mission_id, approved_by=body.approved_by)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/control/missions/{mission_id}/restart", response_model=ControlMissionDetail)
async def restart_control_mission_route(
    mission_id: str,
    body: ControlMissionRestartRequest,
    api_key: APIKeyDep,
    session: SessionDep,
):
    try:
        return await restart_control_mission(session, mission_id, restarted_by=body.restarted_by)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/control/missions/{mission_id}/approve-downgrade", response_model=ControlMissionDetail)
async def approve_control_downgrade_route(
    mission_id: str,
    body: ControlDowngradeApproveRequest,
    api_key: APIKeyDep,
    session: SessionDep,
):
    try:
        return await approve_control_downgrade(
            session,
            mission_id,
            approved_by=body.approved_by,
            selected_model_alias=body.selected_model_alias,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/control/runs/{run_id}", response_model=ControlOrgRunDetail)
async def get_control_run(run_id: str, api_key: APIKeyDep, session: SessionDep):
    try:
        return await get_control_org_run_detail(session, run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/control/org/{run_id}", response_model=ControlOrgRunDetail)
async def get_control_org_alias(run_id: str, api_key: APIKeyDep, session: SessionDep):
    try:
        return await get_control_org_run_detail(session, run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/control/work-items", response_model=ControlWorkItemListResponse)
async def get_control_work_items(
    mission_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    return await list_control_work_items(session, mission_id=mission_id, status=status)


@router.post("/api/control/work-items/{work_item_id}/approve", response_model=WorkItemPayload)
async def approve_control_work_item_route(
    work_item_id: str,
    body: ControlWorkItemDecisionRequest,
    api_key: APIKeyDep,
    session: SessionDep,
):
    try:
        return await approve_work_item(session, work_item_id, approved_by=body.approved_by, rationale=body.rationale or "")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/control/work-items/{work_item_id}/reject", response_model=WorkItemPayload)
async def reject_control_work_item_route(
    work_item_id: str,
    body: ControlWorkItemDecisionRequest,
    api_key: APIKeyDep,
    session: SessionDep,
):
    try:
        return await reject_work_item(session, work_item_id, approved_by=body.approved_by, rationale=body.rationale or "")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/control/artifacts/{artifact_id}/apply", response_model=RunArtifactPayload)
async def apply_control_artifact_route(
    artifact_id: str,
    body: ControlArtifactApplyRequest,
    api_key: APIKeyDep,
    session: SessionDep,
):
    try:
        return await apply_patch_artifact(session, artifact_id, applied_by=body.applied_by)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
