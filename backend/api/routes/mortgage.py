from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from api.routes._deps import APIKeyDep, SessionDep
from services.lender_product_service import list_lender_products, list_recent_lender_deltas, sync_lender_products
from services.mortgage_intelligence_service import (
    ensure_bank_registry_seeded,
    get_lead_mortgage_opportunities,
    list_bank_data_holders,
    recompute_mortgage_opportunities,
    summarize_mortgage_coverage,
)
from services.mortgage_profile_service import (
    add_feedback,
    extract_mortgage_profile_from_calls,
    get_or_create_mortgage_profile,
    update_mortgage_profile,
)

router = APIRouter(prefix="/api/mortgage", tags=["mortgage"])


class MortgageProfileUpdateBody(BaseModel):
    current_lender: str | None = None
    current_rate: float | None = None
    repayment_type: str | None = None
    loan_balance_estimate: int | None = None
    loan_balance_band: str | None = None
    fixed_or_variable: str | None = None
    fixed_expiry: str | None = None
    offset_account: bool | None = None
    owner_occupancy_confirmed: str | None = None
    refinance_interest: str | None = None
    serviceability_notes: str | None = None


class MortgageFeedbackBody(BaseModel):
    feedback_type: str
    opportunity_type: str | None = None
    note: str | None = None
    created_by: str = "operator"


@router.get("/coverage")
async def get_mortgage_coverage(api_key: APIKeyDep = "", session: SessionDep = None):
    return await summarize_mortgage_coverage(session)


@router.get("/leads/{lead_id}/profile")
async def get_mortgage_profile(lead_id: str, api_key: APIKeyDep = "", session: SessionDep = None):
    profile = await get_or_create_mortgage_profile(session, lead_id)
    return profile.model_dump()


@router.patch("/leads/{lead_id}/profile")
async def patch_mortgage_profile(
    lead_id: str,
    body: MortgageProfileUpdateBody,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    profile = await update_mortgage_profile(session, lead_id, body.model_dump(exclude_none=True))
    return profile.model_dump()


@router.post("/leads/{lead_id}/profile/extract-from-calls")
async def extract_mortgage_profile(lead_id: str, api_key: APIKeyDep = "", session: SessionDep = None):
    profile = await extract_mortgage_profile_from_calls(session, lead_id)
    return profile.model_dump()


@router.post("/leads/{lead_id}/feedback")
async def create_mortgage_feedback(
    lead_id: str,
    body: MortgageFeedbackBody,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    row = await add_feedback(
        session,
        lead_id,
        body.feedback_type,
        opportunity_type=body.opportunity_type,
        note=body.note,
        created_by=body.created_by,
    )
    return row.model_dump()


@router.get("/lenders")
async def get_mortgage_lenders(api_key: APIKeyDep = "", session: SessionDep = None):
    await ensure_bank_registry_seeded(session)
    lenders = await list_bank_data_holders(session)
    return {"lenders": [item.model_dump() for item in lenders], "total": len(lenders)}


@router.post("/recompute")
async def recompute_all_mortgage_opportunities(api_key: APIKeyDep = "", session: SessionDep = None):
    return await recompute_mortgage_opportunities(session)


@router.post("/lenders/sync")
async def sync_all_lender_products(api_key: APIKeyDep = "", session: SessionDep = None):
    return await sync_lender_products(session)


@router.post("/lenders/{lender_id}/sync")
async def sync_single_lender_products(lender_id: str, api_key: APIKeyDep = "", session: SessionDep = None):
    return await sync_lender_products(session, lender_id=lender_id)


@router.get("/products")
async def get_mortgage_products(
    occupancy_target: str | None = None,
    rate_type: str | None = None,
    limit: int = 50,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    items = await list_lender_products(session, occupancy_target=occupancy_target, rate_type=rate_type, limit=limit)
    return {"products": [item.model_dump() for item in items], "total": len(items)}


@router.get("/deltas")
async def get_recent_lender_deltas(limit: int = 50, api_key: APIKeyDep = "", session: SessionDep = None):
    items = await list_recent_lender_deltas(session, limit=limit)
    return {"deltas": [item.model_dump() for item in items], "total": len(items)}


@router.post("/leads/{lead_id}/recompute")
async def recompute_lead_mortgage_opportunities(lead_id: str, api_key: APIKeyDep = "", session: SessionDep = None):
    return await recompute_mortgage_opportunities(session, lead_id=lead_id)


@router.get("/leads/{lead_id}/opportunities")
async def get_lead_opportunities(lead_id: str, api_key: APIKeyDep = "", session: SessionDep = None):
    items = await get_lead_mortgage_opportunities(session, lead_id)
    return {"lead_id": lead_id, "opportunities": [item.model_dump() for item in items], "total": len(items)}
