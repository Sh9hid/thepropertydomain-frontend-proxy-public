"""
Memory service — bounded, selective, queryable memory for agents and the org.

Design:
- Stores concise summaries, lessons, and patterns — NOT raw chat transcripts
- Retrieval is selective: filter by agent, department, type, and importance
- Writes happen at ticket completion, conversation resolution, research findings
- TTL not enforced at DB level — stale entries are ranked lower by importance
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from models.org_models import AgentMemory, OrgMemory, MemType

log = logging.getLogger(__name__)

MAX_CONTENT_STORE = 2000
MAX_RETRIEVE = 10


# ─── Agent memory ─────────────────────────────────────────────────────────────

async def write_agent_memory(
    session: AsyncSession,
    agent_id: str,
    memory_type: str,
    content: str,
    source_type: str = "system",
    source_id: Optional[str] = None,
    importance: int = 5,
) -> AgentMemory:
    mem = AgentMemory(
        agent_id=agent_id,
        memory_type=memory_type,
        content=content[:MAX_CONTENT_STORE],
        source_type=source_type,
        source_id=source_id,
        importance=importance,
    )
    session.add(mem)
    await session.commit()
    await session.refresh(mem)
    return mem


async def get_agent_memory(
    session: AsyncSession,
    agent_id: str,
    memory_type: Optional[str] = None,
    min_importance: int = 3,
    limit: int = MAX_RETRIEVE,
) -> List[AgentMemory]:
    q = (
        select(AgentMemory)
        .where(AgentMemory.agent_id == agent_id, AgentMemory.importance >= min_importance)
        .order_by(AgentMemory.importance.desc(), AgentMemory.created_at.desc())
        .limit(limit)
    )
    if memory_type:
        q = q.where(AgentMemory.memory_type == memory_type)
    return (await session.execute(q)).scalars().all()


async def agent_memory_compact(
    session: AsyncSession,
    agent_id: str,
) -> str:
    """Return a compact string suitable for injection into a prompt."""
    memories = await get_agent_memory(session, agent_id, limit=8)
    if not memories:
        return ""
    lines = [f"[{m.memory_type}] {m.content[:200]}" for m in memories]
    return "Agent memory:\n" + "\n".join(lines)


# ─── Org memory ───────────────────────────────────────────────────────────────

async def write_org_memory(
    session: AsyncSession,
    topic: str,
    memory_type: str,
    content: str,
    department: Optional[str] = None,
    source_type: str = "system",
    source_id: Optional[str] = None,
    importance: int = 5,
) -> OrgMemory:
    mem = OrgMemory(
        topic=topic,
        department=department,
        memory_type=memory_type,
        content=content[:MAX_CONTENT_STORE],
        source_type=source_type,
        source_id=source_id,
        importance=importance,
    )
    session.add(mem)
    await session.commit()
    await session.refresh(mem)
    return mem


async def get_org_memory(
    session: AsyncSession,
    department: Optional[str] = None,
    topic: Optional[str] = None,
    min_importance: int = 3,
    limit: int = MAX_RETRIEVE,
) -> List[OrgMemory]:
    q = (
        select(OrgMemory)
        .where(OrgMemory.importance >= min_importance)
        .order_by(OrgMemory.importance.desc(), OrgMemory.created_at.desc())
        .limit(limit)
    )
    if department:
        q = q.where(OrgMemory.department == department)
    if topic:
        q = q.where(OrgMemory.topic.ilike(f"%{topic}%"))
    return (await session.execute(q)).scalars().all()


async def org_memory_compact(
    session: AsyncSession,
    department: Optional[str] = None,
    limit: int = 6,
) -> str:
    memories = await get_org_memory(session, department=department, limit=limit)
    if not memories:
        return ""
    lines = [f"[{m.topic}/{m.memory_type}] {m.content[:200]}" for m in memories]
    return "Org memory:\n" + "\n".join(lines)


# ─── Convenience: write on ticket completion ──────────────────────────────────

async def record_ticket_completion_memory(
    session: AsyncSession,
    ticket: Any,
    agent_id: str,
):
    """Called when a ticket is completed. Writes compact lessons."""
    lesson = (
        f"Ticket '{ticket.title}' ({ticket.department}/{ticket.kind}) "
        f"resolved by {agent_id}. "
        f"Resolution: {(ticket.resolution_notes or '')[:200]}"
    )
    await write_agent_memory(
        session, agent_id=agent_id,
        memory_type=MemType.LESSON,
        content=lesson,
        source_type="ticket",
        source_id=ticket.id,
        importance=6,
    )
    await write_org_memory(
        session,
        topic=ticket.kind,
        department=ticket.department,
        memory_type=MemType.PATTERN,
        content=lesson,
        source_type="ticket",
        source_id=ticket.id,
        importance=5,
    )


async def record_research_memory(
    session: AsyncSession,
    area: str,
    key_finding: str,
    confidence: str,
    note_id: str,
):
    """Write org memory when a research note is produced."""
    importance = {"high": 8, "medium": 6, "low": 4}.get(confidence, 5)
    await write_org_memory(
        session,
        topic=area,
        department="research",
        memory_type=MemType.SUMMARY,
        content=key_finding[:400],
        source_type="research_note",
        source_id=note_id,
        importance=importance,
    )
