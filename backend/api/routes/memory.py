"""Memory API."""
from __future__ import annotations

from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session
from services.memory_service import (
    write_agent_memory, get_agent_memory, agent_memory_compact,
    write_org_memory, get_org_memory, org_memory_compact,
)

router = APIRouter(prefix="/memory", tags=["memory"])


class WriteAgentMemRequest(BaseModel):
    agent_id: str
    memory_type: str
    content: str
    source_type: str = "system"
    source_id: Optional[str] = None
    importance: int = 5


class WriteOrgMemRequest(BaseModel):
    topic: str
    memory_type: str
    content: str
    department: Optional[str] = None
    business_context_key: Optional[str] = None
    source_type: str = "system"
    source_id: Optional[str] = None
    importance: int = 5


def _amem(m) -> Dict:
    return {
        "id": m.id, "agent_id": m.agent_id, "memory_type": m.memory_type,
        "content": m.content, "source_type": m.source_type, "source_id": m.source_id,
        "importance": m.importance, "created_at": m.created_at.isoformat(),
    }


def _omem(m) -> Dict:
    return {
        "id": m.id, "topic": m.topic, "department": m.department,
        "business_context_key": m.business_context_key,
        "memory_type": m.memory_type, "content": m.content,
        "source_type": m.source_type, "source_id": m.source_id,
        "importance": m.importance, "created_at": m.created_at.isoformat(),
    }


@router.post("/agent")
async def post_agent_memory(body: WriteAgentMemRequest, session: AsyncSession = Depends(get_session)):
    m = await write_agent_memory(
        session, body.agent_id, body.memory_type, body.content,
        body.source_type, body.source_id, body.importance,
    )
    return _amem(m)


@router.get("/agent/{agent_id}")
async def get_agent_mem(
    agent_id: str,
    memory_type: Optional[str] = None,
    min_importance: int = 3,
    limit: int = Query(default=10, le=50),
    session: AsyncSession = Depends(get_session),
):
    mems = await get_agent_memory(session, agent_id, memory_type, min_importance, limit)
    return [_amem(m) for m in mems]


@router.get("/agent/{agent_id}/compact")
async def get_agent_compact(agent_id: str, session: AsyncSession = Depends(get_session)):
    return {"compact": await agent_memory_compact(session, agent_id)}


@router.post("/org")
async def post_org_memory(body: WriteOrgMemRequest, session: AsyncSession = Depends(get_session)):
    m = await write_org_memory(
        session, body.topic, body.memory_type, body.content,
        body.department, body.business_context_key, body.source_type, body.source_id, body.importance,
    )
    return _omem(m)


@router.get("/org")
async def get_org_mem(
    department: Optional[str] = None,
    topic: Optional[str] = None,
    business_context_key: Optional[str] = None,
    min_importance: int = 3,
    limit: int = Query(default=10, le=50),
    session: AsyncSession = Depends(get_session),
):
    mems = await get_org_memory(session, department, topic, business_context_key, min_importance, limit)
    return [_omem(m) for m in mems]


@router.get("/org/compact")
async def get_org_compact(
    department: Optional[str] = None,
    business_context_key: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
):
    return {"compact": await org_memory_compact(session, department, business_context_key)}
