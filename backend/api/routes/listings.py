import tempfile
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session
from core.security import get_api_key
from core.utils import parse_client_datetime
from models.schemas import (
    InspectionReportRequest,
    LawyerSignoffRequest,
    ListingSendRequest,
    MarketingStatusRequest,
    MarketReadyRequest,
    OfferEventRequest,
    PriceGuidanceDraftRequest,
    PriceGuidanceUpdateRequest,
)
from services.listing_workflow import (
    approve_price_guidance,
    build_listing_workflow_payload,
    create_inspection_report,
    draft_price_guidance,
    ensure_listing_workflow,
    generate_authority_pack,
    record_offer_event,
    save_uploaded_document,
    send_authority_pack,
    set_lawyer_signoff,
    set_marketing_status,
    set_market_ready,
    update_price_guidance,
)

router = APIRouter()


def _workflow_error(exc: Exception) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@router.get("/api/listings/{lead_id}")
async def get_listing_workflow(lead_id: str, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    await ensure_listing_workflow(session, lead_id)
    return await build_listing_workflow_payload(session, lead_id)


@router.post("/api/listings/{lead_id}/documents")
async def upload_listing_document(
    lead_id: str,
    kind: str = Form(...),
    uploaded_by: str = Form("operator"),
    file: UploadFile = File(...),
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    suffix = Path(file.filename or "upload").suffix
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
        contents = await file.read()
        handle.write(contents)
        temp_path = Path(handle.name)
    try:
        return await save_uploaded_document(session, lead_id, kind, temp_path, file.filename or "document", uploaded_by=uploaded_by)
    except ValueError as exc:
        raise _workflow_error(exc) from exc
    finally:
        temp_path.unlink(missing_ok=True)


@router.post("/api/listings/{lead_id}/inspection-report")
async def create_listing_inspection(
    lead_id: str,
    body: InspectionReportRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await create_inspection_report(
            session,
            lead_id,
            inspected_by=body.inspected_by,
            inspection_at=parse_client_datetime(body.inspection_at),
            occupancy=body.occupancy,
            condition_rating=body.condition_rating,
            summary=body.summary,
            notes=body.notes,
        )
    except ValueError as exc:
        raise _workflow_error(exc) from exc


@router.post("/api/listings/{lead_id}/price-guidance/draft")
async def create_price_guidance_draft(
    lead_id: str,
    body: PriceGuidanceDraftRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await draft_price_guidance(
            session,
            lead_id,
            override_low=body.low,
            override_high=body.high,
            override_rationale=body.rationale,
        )
    except ValueError as exc:
        raise _workflow_error(exc) from exc


@router.patch("/api/listings/{lead_id}/price-guidance/{guidance_id}")
async def patch_price_guidance_draft(
    lead_id: str,
    guidance_id: str,
    body: PriceGuidanceUpdateRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await update_price_guidance(
            session,
            lead_id,
            guidance_id,
            estimate_low=body.estimate_low,
            estimate_high=body.estimate_high,
            rationale=body.rationale,
            comparables=[item.model_dump() for item in body.comparables],
        )
    except ValueError as exc:
        raise _workflow_error(exc) from exc


@router.post("/api/listings/{lead_id}/price-guidance/{guidance_id}/approve")
async def approve_listing_price_guidance(
    lead_id: str,
    guidance_id: str,
    approved_by: str = Form("operator"),
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await approve_price_guidance(session, lead_id, guidance_id, approved_by=approved_by)
    except ValueError as exc:
        raise _workflow_error(exc) from exc


@router.post("/api/listings/{lead_id}/authority-pack/draft")
async def draft_authority_pack(
    lead_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await generate_authority_pack(session, lead_id)
    except ValueError as exc:
        raise _workflow_error(exc) from exc


@router.post("/api/listings/{lead_id}/authority-pack/send")
async def send_listing_authority_pack(
    lead_id: str,
    body: ListingSendRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await send_authority_pack(session, lead_id, recipient_email=body.recipient_email, recipient_name=body.recipient_name)
    except ValueError as exc:
        raise _workflow_error(exc) from exc


@router.post("/api/listings/{lead_id}/offers")
async def log_offer_event(
    lead_id: str,
    body: OfferEventRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await record_offer_event(
            session,
            lead_id,
            amount=body.amount,
            buyer_name=body.buyer_name,
            conditions=body.conditions,
            channel=body.channel,
            status=body.status,
            received_at=parse_client_datetime(body.received_at),
            notes=body.notes,
        )
    except ValueError as exc:
        raise _workflow_error(exc) from exc


@router.post("/api/listings/{lead_id}/marketing-status")
async def update_marketing_status(
    lead_id: str,
    body: MarketingStatusRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    return await set_marketing_status(session, lead_id, body.status, body.note)


@router.post("/api/listings/{lead_id}/lawyer-signoff")
async def update_lawyer_signoff(
    lead_id: str,
    body: LawyerSignoffRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    return await set_lawyer_signoff(session, lead_id, body.status)


@router.post("/api/listings/{lead_id}/market-ready")
async def update_market_ready(
    lead_id: str,
    body: MarketReadyRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await set_market_ready(session, lead_id, body.market_ready)
    except ValueError as exc:
        raise _workflow_error(exc) from exc
