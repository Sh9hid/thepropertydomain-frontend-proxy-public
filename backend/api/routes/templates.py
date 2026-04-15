"""Template management API routes."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.routes._deps import APIKeyDep, SessionDep

router = APIRouter()


@router.get("/api/templates")
async def list_templates(
    channel: Optional[str] = None,
    stage: Optional[str] = None,
    active_only: bool = True,
    session: SessionDep = None,
    api_key: APIKeyDep = "",
):
    from services.template_library import list_templates
    return await list_templates(session, channel=channel, stage=stage, active_only=active_only)


@router.get("/api/templates/{template_id}")
async def get_template(template_id: str, session: SessionDep = None, api_key: APIKeyDep = ""):
    from services.template_library import get_template
    result = await get_template(session, template_id)
    if not result:
        raise HTTPException(status_code=404, detail="Template not found")
    return result


@router.post("/api/templates")
async def create_template(data: dict, session: SessionDep = None, api_key: APIKeyDep = ""):
    from services.template_library import create_template
    return await create_template(session, data)


@router.patch("/api/templates/{template_id}")
async def update_template(template_id: str, data: dict, session: SessionDep = None, api_key: APIKeyDep = ""):
    from services.template_library import update_template
    result = await update_template(session, template_id, data)
    if not result:
        raise HTTPException(status_code=404, detail="Template not found")
    return result


@router.post("/api/templates/{template_id}/clone")
async def clone_template(template_id: str, session: SessionDep = None, api_key: APIKeyDep = ""):
    from services.template_library import get_template, create_template
    original = await get_template(session, template_id)
    if not original:
        raise HTTPException(status_code=404, detail="Template not found")
    clone_data = {k: v for k, v in original.items() if k not in ("id", "created_at", "updated_at", "send_count", "open_count", "reply_count", "booking_count")}
    clone_data["name"] = f"{clone_data.get('name', '')} (copy)"
    clone_data["variant"] = "B"
    return await create_template(session, clone_data)


@router.post("/api/templates/{template_id}/preview")
async def preview_template(template_id: str, lead_data: dict = None, session: SessionDep = None, api_key: APIKeyDep = ""):
    from services.template_library import get_template, fill_template
    template = await get_template(session, template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    sample_lead = lead_data or {
        "owner_first_name": "John",
        "owner_name": "John Smith",
        "address": "42 Oak Street, Oakville",
        "suburb": "Oakville",
        "estimated_value_low": 850000,
        "estimated_value_high": 950000,
        "ownership_duration_years": 12,
        "suburb_median": "920,000",
        "suburb_growth": "4.2",
    }
    return fill_template(template["body"], sample_lead, subject=template.get("subject"))


@router.post("/api/templates/seed")
async def seed_templates(session: SessionDep = None, api_key: APIKeyDep = ""):
    from services.template_library import seed_default_templates
    return await seed_default_templates(session)
