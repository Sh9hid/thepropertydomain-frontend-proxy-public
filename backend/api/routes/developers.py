"""Developer/builder database CRUD routes."""
from __future__ import annotations

import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import select

from api.routes._deps import APIKeyDep, SessionDep
from core.utils import now_iso
from models.sql_models import Developer

router = APIRouter()


class DeveloperCreate(BaseModel):
    company_name: str
    contact_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    specialization: str = "residential"
    preferred_suburbs: list[str] = []
    budget_range: Optional[str] = None
    land_size_range: Optional[str] = None
    notes: Optional[str] = None


@router.get("/api/developers")
async def list_developers(
    status: Optional[str] = "active",
    session: SessionDep = None,
    api_key: APIKeyDep = "",
):
    stmt = select(Developer)
    if status:
        stmt = stmt.where(Developer.status == status)
    result = await session.execute(stmt)
    return [d.model_dump() for d in result.scalars().all()]


@router.post("/api/developers")
async def create_developer(data: DeveloperCreate, session: SessionDep = None, api_key: APIKeyDep = ""):
    dev = Developer(
        id=str(uuid.uuid4()),
        **data.model_dump(),
        created_at=now_iso(),
        updated_at=now_iso(),
    )
    session.add(dev)
    await session.commit()
    await session.refresh(dev)
    return dev.model_dump()


@router.patch("/api/developers/{dev_id}")
async def update_developer(dev_id: str, data: dict, session: SessionDep = None, api_key: APIKeyDep = ""):
    dev = await session.get(Developer, dev_id)
    if not dev:
        raise HTTPException(status_code=404, detail="Developer not found")
    for k, v in data.items():
        if hasattr(dev, k) and k not in ("id", "created_at"):
            setattr(dev, k, v)
    dev.updated_at = now_iso()
    await session.commit()
    await session.refresh(dev)
    return dev.model_dump()


@router.delete("/api/developers/{dev_id}")
async def delete_developer(dev_id: str, session: SessionDep = None, api_key: APIKeyDep = ""):
    dev = await session.get(Developer, dev_id)
    if not dev:
        raise HTTPException(status_code=404, detail="Developer not found")
    dev.status = "archived"
    dev.updated_at = now_iso()
    await session.commit()
    return {"status": "archived"}
