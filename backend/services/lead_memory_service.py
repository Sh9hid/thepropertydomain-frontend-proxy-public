"""
Lead Memory Service — per-lead AI memory CRUD and keyword extraction.

Stores contextual fragments (objections, preferences, relationships, timing)
that persist across sessions and feed into call briefs and template selection.
No AI calls — extraction is deterministic regex/keyword based.
"""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso
from models.sql_models import Lead, LeadMemory

logger = logging.getLogger(__name__)

# ─── Memory types ─────────────────────────────────────────────────────────────
# call_insight | objection | preference | context | relationship | timing | behavioral

VALID_MEMORY_TYPES = {
    "call_insight",
    "objection",
    "preference",
    "context",
    "relationship",
    "timing",
    "behavioral",
}


# ─── CRUD ─────────────────────────────────────────────────────────────────────


async def add_memory(
    session: AsyncSession,
    lead_id: str,
    memory_type: str,
    content: str,
    source: str = "operator",
    importance: float = 0.5,
) -> dict:
    """Add a memory entry for a lead.

    Args:
        lead_id: The lead this memory belongs to.
        memory_type: One of VALID_MEMORY_TYPES.
        content: Free-text memory content.
        source: Where this came from (operator | ai_extraction | system).
        importance: 0.0 to 1.0 weight for prioritization.

    Returns:
        Dict representation of the created memory.
    """
    if memory_type not in VALID_MEMORY_TYPES:
        logger.warning(
            "add_memory: unknown memory_type '%s', defaulting to 'context'",
            memory_type,
        )
        memory_type = "context"

    now = now_iso()
    mem = LeadMemory(
        id=str(uuid.uuid4()),
        lead_id=lead_id,
        memory_type=memory_type,
        content=content.strip(),
        source=source,
        importance=max(0.0, min(1.0, importance)),
        created_at=now,
    )
    session.add(mem)
    await session.commit()
    await session.refresh(mem)
    logger.info("Added %s memory for lead %s", memory_type, lead_id)
    return _memory_to_dict(mem)


async def get_memories(
    session: AsyncSession,
    lead_id: str,
    memory_type: str = None,
) -> list[dict]:
    """Get all memories for a lead, optionally filtered by type."""
    q = (
        select(LeadMemory)
        .where(LeadMemory.lead_id == lead_id)
        .order_by(LeadMemory.importance.desc(), LeadMemory.created_at.desc())
    )
    if memory_type:
        q = q.where(LeadMemory.memory_type == memory_type)

    result = await session.execute(q)
    rows = result.scalars().all()
    return [_memory_to_dict(r) for r in rows]


async def delete_memory(session: AsyncSession, memory_id: str) -> bool:
    """Delete a single memory entry. Returns True if found and deleted."""
    result = await session.execute(
        select(LeadMemory).where(LeadMemory.id == memory_id)
    )
    mem = result.scalars().first()
    if not mem:
        return False

    await session.delete(mem)
    await session.commit()
    logger.info("Deleted memory %s", memory_id)
    return True


# ─── Keyword extraction ──────────────────────────────────────────────────────

# Patterns: (regex, memory_type, importance, content_template)
# The content_template can include {match} for the captured group.

_EXTRACTION_PATTERNS: List[tuple] = [
    # Relationship mentions
    (
        re.compile(
            r"\b(?:wife|husband|partner|spouse|son|daughter|mother|father|"
            r"brother|sister|family)\b[^.!?\n]{0,80}",
            re.IGNORECASE,
        ),
        "relationship",
        0.7,
        None,  # use full match as content
    ),
    # Timing / "not selling until"
    (
        re.compile(
            r"\b(?:not\s+(?:selling|moving|ready|interested)\s+(?:until|for|before|til))"
            r"[^.!?\n]{0,80}",
            re.IGNORECASE,
        ),
        "timing",
        0.8,
        None,
    ),
    # Explicit timeline mentions
    (
        re.compile(
            r"\b(?:in\s+(?:\d+)\s+(?:months?|years?|weeks?))"
            r"[^.!?\n]{0,60}",
            re.IGNORECASE,
        ),
        "timing",
        0.7,
        None,
    ),
    # Retirement / downsizing
    (
        re.compile(
            r"\b(?:retir(?:ing|ed|ement)|downsize|downsizing|moving\s+to\s+[A-Z])"
            r"[^.!?\n]{0,80}",
            re.IGNORECASE,
        ),
        "timing",
        0.8,
        None,
    ),
    # Objections: "doesn't want" / "won't" / "no interest"
    (
        re.compile(
            r"\b(?:doesn't\s+want|won't\s+\w+|not\s+interested|"
            r"no\s+interest|don't\s+call|never\s+sell|leave\s+(?:me|us)\s+alone)"
            r"[^.!?\n]{0,80}",
            re.IGNORECASE,
        ),
        "objection",
        0.9,
        None,
    ),
    # Price expectations
    (
        re.compile(
            r"\b(?:wants?\s+(?:\$[\d,.]+|at\s+least|over|minimum)|"
            r"won't\s+(?:sell|accept)\s+(?:under|below|less))"
            r"[^.!?\n]{0,80}",
            re.IGNORECASE,
        ),
        "objection",
        0.8,
        None,
    ),
    # Preference for contact method
    (
        re.compile(
            r"\b(?:prefer(?:s|red)?\s+(?:email|sms|text|call|phone|morning|afternoon|evening))"
            r"[^.!?\n]{0,60}",
            re.IGNORECASE,
        ),
        "preference",
        0.7,
        None,
    ),
    # Renovation / development mentions
    (
        re.compile(
            r"\b(?:renovat(?:ing|ed|ion)|develop(?:ing|ment|er)|granny\s+flat|"
            r"subdivision|building|extension|knockdown)"
            r"[^.!?\n]{0,80}",
            re.IGNORECASE,
        ),
        "context",
        0.6,
        None,
    ),
    # Financial stress signals
    (
        re.compile(
            r"\b(?:mortgage\s+stress|financial\s+(?:difficulty|hardship|trouble)|"
            r"arrears|behind\s+on\s+payments|struggling\s+with)"
            r"[^.!?\n]{0,80}",
            re.IGNORECASE,
        ),
        "context",
        0.9,
        None,
    ),
    # Existing agent relationship
    (
        re.compile(
            r"\b(?:already\s+(?:have|has|got|using)\s+(?:an?\s+)?agent|"
            r"listed\s+with|signed\s+(?:with|up))"
            r"[^.!?\n]{0,80}",
            re.IGNORECASE,
        ),
        "objection",
        0.8,
        None,
    ),
]


async def extract_memories_from_notes(
    session: AsyncSession,
    lead_id: str,
    notes: str,
) -> list[dict]:
    """Extract memories from free-text notes using keyword patterns.

    Scans the text for known patterns and creates LeadMemory entries.
    Returns list of extracted memories.  Does not use AI.
    """
    if not notes or not notes.strip():
        return []

    extracted: list[dict] = []
    seen_content: set[str] = set()  # dedup within this extraction

    for pattern, mem_type, importance, _tpl in _EXTRACTION_PATTERNS:
        for match in pattern.finditer(notes):
            content = match.group(0).strip()
            # Normalize for dedup
            norm = content.lower()
            if norm in seen_content:
                continue
            seen_content.add(norm)

            mem = await add_memory(
                session,
                lead_id=lead_id,
                memory_type=mem_type,
                content=content,
                source="ai_extraction",
                importance=importance,
            )
            extracted.append(mem)

    if extracted:
        logger.info(
            "Extracted %d memories from notes for lead %s", len(extracted), lead_id,
        )
    return extracted


# ─── AI context summary ──────────────────────────────────────────────────────


async def refresh_ai_context_summary(
    session: AsyncSession, lead_id: str
) -> str:
    """Build a one-sentence summary from lead memories and update
    lead.ai_context_summary.

    The summary is deterministic — it concatenates the top memories
    by importance into a brief string.  No LLM call.
    """
    memories = await get_memories(session, lead_id)
    if not memories:
        return ""

    # Take top 5 by importance
    top = sorted(memories, key=lambda m: m["importance"], reverse=True)[:5]

    parts: list[str] = []
    for m in top:
        label = m["memory_type"].replace("_", " ").title()
        content = m["content"]
        # Truncate long content
        if len(content) > 120:
            content = content[:117] + "..."
        parts.append(f"[{label}] {content}")

    summary = "; ".join(parts)
    # Cap total length
    if len(summary) > 500:
        summary = summary[:497] + "..."

    # Update lead
    lead_result = await session.execute(
        select(Lead).where(Lead.id == lead_id)
    )
    lead = lead_result.scalars().first()
    if lead:
        lead.ai_context_summary = summary
        lead.updated_at = now_iso()
        await session.commit()

    return summary


# ─── Internal helpers ─────────────────────────────────────────────────────────


def _memory_to_dict(mem: LeadMemory) -> dict:
    """Convert a LeadMemory ORM row to a plain dict."""
    return {
        "id": mem.id,
        "lead_id": mem.lead_id,
        "memory_type": mem.memory_type,
        "content": mem.content,
        "source": mem.source,
        "importance": mem.importance,
        "source_event_id": mem.source_event_id,
        "created_at": mem.created_at,
        "expires_at": mem.expires_at,
    }
