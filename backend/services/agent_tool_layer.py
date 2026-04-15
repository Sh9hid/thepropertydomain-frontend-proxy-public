"""
Agent tool layer — bounded, typed, event-logged safe tool execution.

Agents can ONLY call tools defined here.
No arbitrary file writes, no shell execution, no unsafe side effects.

Tool categories:
  - read_leads          — read lead data
  - read_call_log       — read call outcomes
  - read_metrics        — read deterministic call metrics
  - read_transcripts    — read transcript text
  - create_ticket       — raise a ticket
  - create_research_note — write a research finding
  - write_memory        — write agent or org memory
  - read_orch_events    — recent orchestration events
  - read_provider_health — current provider status
  - read_tickets        — list tickets
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.events import event_manager

log = logging.getLogger(__name__)

# ─── Tool registry ─────────────────────────────────────────────────────────────

TOOL_SCHEMAS: Dict[str, Dict] = {
    "read_leads": {
        "description": "Read top leads by heat_score or lifecycle_stage.",
        "params": {"limit": "int (default 10)", "status": "str optional", "suburb": "str optional"},
    },
    "read_call_log": {
        "description": "Read recent call log entries.",
        "params": {"limit": "int (default 20)", "days_back": "int (default 7)"},
    },
    "read_transcripts": {
        "description": "Read recent call transcript texts.",
        "params": {"limit": "int (default 5)", "days_back": "int (default 14)"},
    },
    "read_orch_events": {
        "description": "Read recent orchestration events (failures, completions).",
        "params": {"limit": "int (default 20)", "level": "str optional (info/warn/error)"},
    },
    "read_provider_health": {
        "description": "Get current provider health snapshot.",
        "params": {},
    },
    "read_tickets": {
        "description": "List open tickets.",
        "params": {"department": "str optional", "status": "str optional", "limit": "int (default 20)"},
    },
    "create_ticket": {
        "description": "Raise a new ticket.",
        "params": {
            "title": "str REQUIRED",
            "description": "str",
            "department": "str REQUIRED (research/revenue/engineering/qa/voice)",
            "kind": "str REQUIRED (bug/feature/research/followup/anomaly/training)",
            "priority": "int (1-10)",
            "severity": "str (low/medium/high/critical)",
            "evidence_json": "dict",
        },
    },
    "create_research_note": {
        "description": "Write a research finding to the research notes table.",
        "params": {
            "title": "str REQUIRED",
            "area": "str REQUIRED (sales/real_estate/app_tech)",
            "thesis": "str REQUIRED",
            "evidence": "str",
            "recommendation": "str",
            "confidence": "str (low/medium/high)",
        },
    },
    "write_agent_memory": {
        "description": "Store a memory for this agent.",
        "params": {
            "memory_type": "str (fact/preference/lesson/pattern/summary)",
            "content": "str REQUIRED",
            "importance": "int (1-10)",
        },
    },
}


# ─── Implementations ──────────────────────────────────────────────────────────

async def _tool_read_leads(session: AsyncSession, params: Dict) -> List[Dict]:
    limit = min(int(params.get("limit", 10)), 30)
    status_filter = params.get("status")
    suburb_filter = params.get("suburb")

    q = """
        SELECT id, address, suburb, heat_score, lifecycle_stage, status,
               contact_status, why_now, last_outcome, touches_30d
        FROM leads
        WHERE heat_score > 0
    """
    args: Dict = {"limit": limit}
    if status_filter:
        q += " AND status = :status"
        args["status"] = status_filter
    if suburb_filter:
        q += " AND suburb LIKE :suburb"
        args["suburb"] = f"%{suburb_filter}%"
    q += " ORDER BY heat_score DESC, call_today_score DESC LIMIT :limit"
    rows = (await session.execute(text(q), args)).mappings().all()
    return [dict(r) for r in rows]


async def _tool_read_call_log(session: AsyncSession, params: Dict) -> List[Dict]:
    limit = min(int(params.get("limit", 20)), 50)
    days_back = int(params.get("days_back", 7))
    cutoff = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
    rows = (await session.execute(
        text("""
            SELECT id, lead_address, outcome, connected, timestamp,
                   call_duration_seconds, note, operator
            FROM call_log
            WHERE logged_at >= :cutoff
            ORDER BY logged_at DESC
            LIMIT :limit
        """),
        {"cutoff": cutoff, "limit": limit},
    )).mappings().all()
    return [dict(r) for r in rows]


async def _tool_read_transcripts(session: AsyncSession, params: Dict) -> List[Dict]:
    limit = min(int(params.get("limit", 5)), 15)
    days_back = int(params.get("days_back", 14))
    cutoff = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
    rows = (await session.execute(
        text("""
            SELECT t.id, t.call_id, substr(t.full_text, 1, 800) as text_preview,
                   t.confidence, t.created_at
            FROM transcripts t
            WHERE t.created_at >= :cutoff
              AND t.status = 'complete'
            ORDER BY t.created_at DESC
            LIMIT :limit
        """),
        {"cutoff": cutoff, "limit": limit},
    )).mappings().all()
    return [dict(r) for r in rows]


async def _tool_read_orch_events(session: AsyncSession, params: Dict) -> List[Dict]:
    limit = min(int(params.get("limit", 20)), 50)
    level = params.get("level")
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    q = """
        SELECT event_type, agent_role, provider, message, level, ts
        FROM orch_events
        WHERE ts >= :cutoff
    """
    args: Dict = {"cutoff": cutoff, "limit": limit}
    if level:
        q += " AND level = :level"
        args["level"] = level
    q += " ORDER BY ts DESC LIMIT :limit"
    rows = (await session.execute(text(q), args)).mappings().all()
    return [dict(r) for r in rows]


async def _tool_read_provider_health(session: AsyncSession, params: Dict) -> List[Dict]:
    from services.orchestration_engine import get_provider_snapshot
    return get_provider_snapshot()


async def _tool_read_tickets(session: AsyncSession, params: Dict) -> List[Dict]:
    limit = min(int(params.get("limit", 20)), 50)
    dept = params.get("department")
    status = params.get("status", "open")
    q = "SELECT id, title, department, kind, status, priority, severity, created_at FROM org_tickets WHERE 1=1"
    args: Dict = {"limit": limit}
    if dept:
        q += " AND department = :dept"
        args["dept"] = dept
    if status:
        q += " AND status = :status"
        args["status"] = status
    q += " ORDER BY priority ASC, created_at DESC LIMIT :limit"
    rows = (await session.execute(text(q), args)).mappings().all()
    return [dict(r) for r in rows]


async def _tool_create_ticket(session: AsyncSession, params: Dict, agent_id: str) -> Dict:
    from services.ticket_service import create_ticket
    t = await create_ticket(
        session,
        title=str(params["title"])[:200],
        description=str(params.get("description", ""))[:1000],
        department=str(params["department"]),
        kind=str(params["kind"]),
        priority=int(params.get("priority", 5)),
        severity=str(params.get("severity", "medium")),
        created_by_type="agent",
        created_by_id=agent_id,
        evidence_json=params.get("evidence_json") or {},
    )
    return {"ticket_id": t.id, "title": t.title, "status": t.status}


async def _tool_create_research_note(session: AsyncSession, params: Dict, agent_id: str) -> Dict:
    from models.org_models import ResearchNote
    note = ResearchNote(
        title=str(params["title"])[:200],
        area=str(params["area"]),
        thesis=str(params.get("thesis", ""))[:1000],
        evidence=str(params.get("evidence", ""))[:1000],
        recommendation=str(params.get("recommendation", ""))[:500],
        confidence=str(params.get("confidence", "medium")),
        produced_by_agent=agent_id,
    )
    session.add(note)
    await session.commit()
    await session.refresh(note)
    return {"note_id": note.id, "title": note.title}


async def _tool_write_agent_memory(session: AsyncSession, params: Dict, agent_id: str) -> Dict:
    from services.memory_service import write_agent_memory
    mem = await write_agent_memory(
        session,
        agent_id=agent_id,
        memory_type=str(params.get("memory_type", "lesson")),
        content=str(params.get("content", ""))[:1000],
        importance=int(params.get("importance", 5)),
    )
    return {"memory_id": mem.id}


# ─── Dispatcher ───────────────────────────────────────────────────────────────

_TOOL_MAP = {
    "read_leads":            _tool_read_leads,
    "read_call_log":         _tool_read_call_log,
    "read_transcripts":      _tool_read_transcripts,
    "read_orch_events":      _tool_read_orch_events,
    "read_provider_health":  _tool_read_provider_health,
    "read_tickets":          _tool_read_tickets,
    "create_ticket":         _tool_create_ticket,
    "create_research_note":  _tool_create_research_note,
    "write_agent_memory":    _tool_write_agent_memory,
}


async def execute_tool(
    session: AsyncSession,
    tool_name: str,
    params: Dict[str, Any],
    agent_id: str = "unknown",
) -> Any:
    """
    Execute a named tool with typed params and event logging.
    Returns result or raises ValueError for unknown tools.
    """
    if tool_name not in _TOOL_MAP:
        raise ValueError(f"Unknown tool: {tool_name}. Allowed: {list(_TOOL_MAP.keys())}")

    await event_manager.broadcast({
        "type": "ORCH_TOOL_CALL",
        "data": {
            "tool": tool_name,
            "agent_id": agent_id,
            "ts": datetime.utcnow().isoformat(),
        }
    })

    fn = _TOOL_MAP[tool_name]
    # Functions with agent_id signature
    if tool_name in ("create_ticket", "create_research_note", "write_agent_memory"):
        result = await fn(session, params, agent_id)
    else:
        result = await fn(session, params)

    await event_manager.broadcast({
        "type": "ORCH_TOOL_DONE",
        "data": {
            "tool": tool_name,
            "agent_id": agent_id,
            "result_preview": str(result)[:100],
            "ts": datetime.utcnow().isoformat(),
        }
    })
    return result


def list_tools() -> List[Dict]:
    return [{"name": k, **v} for k, v in TOOL_SCHEMAS.items()]
