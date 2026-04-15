"""
MCP Tool Server: lead_db

Wraps agent_tool_layer.py + hermes memory into Claude API tool definitions.
Provides read/write access to leads, call logs, transcripts, findings,
case memory, and pipeline data.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = logging.getLogger(__name__)

# ─── Tool Definitions (Anthropic API format) ────────────────────────────────

TOOLS: list[dict[str, Any]] = [
    {
        "name": "read_leads",
        "description": (
            "Read top leads ordered by heat_score. "
            "Filter by status, suburb, or minimum heat score."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max results (default 10, max 30)", "default": 10},
                "status": {"type": "string", "description": "Filter by lead status"},
                "suburb": {"type": "string", "description": "Filter by suburb name (partial match)"},
                "min_heat": {"type": "integer", "description": "Minimum heat_score threshold"},
            },
        },
    },
    {
        "name": "read_lead_detail",
        "description": (
            "Get full detail for a single lead including all intelligence fields, "
            "contact info, enrichment data, and recent activity."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string", "description": "The lead UUID"},
            },
            "required": ["lead_id"],
        },
    },
    {
        "name": "search_leads",
        "description": (
            "Full-text search across lead address, suburb, owner name. "
            "Use this when looking for a specific property or person."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search term"},
                "limit": {"type": "integer", "description": "Max results (default 10)", "default": 10},
            },
            "required": ["query"],
        },
    },
    {
        "name": "read_call_log",
        "description": "Read recent call log entries. Optionally filter by lead_id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max results (default 20)", "default": 20},
                "days_back": {"type": "integer", "description": "How many days back (default 7)", "default": 7},
                "lead_id": {"type": "string", "description": "Filter to a specific lead"},
            },
        },
    },
    {
        "name": "read_transcripts",
        "description": "Read recent call transcript texts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max results (default 5)", "default": 5},
                "days_back": {"type": "integer", "description": "Days back (default 14)", "default": 14},
            },
        },
    },
    {
        "name": "read_pipeline_summary",
        "description": (
            "Get aggregate pipeline statistics: lead counts by status/signal, "
            "average heat scores, calls today, conversion metrics."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "read_hermes_findings",
        "description": "Read recent HERMES intelligence findings.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max results (default 10)", "default": 10},
                "signal_type": {"type": "string", "description": "Filter by signal_type"},
            },
        },
    },
    {
        "name": "read_hermes_memory",
        "description": "Read HERMES memory entries (learnings, patterns, preferences).",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max results (default 10)", "default": 10},
                "memory_type": {"type": "string", "description": "Filter by type"},
            },
        },
    },
    {
        "name": "read_case_memory",
        "description": "Read all case memory entries for a specific lead.",
        "input_schema": {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string", "description": "The lead UUID"},
            },
            "required": ["lead_id"],
        },
    },
    {
        "name": "write_hermes_finding",
        "description": "Store a new intelligence finding.",
        "input_schema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Finding topic/title"},
                "summary": {"type": "string", "description": "Finding summary"},
                "signal_type": {"type": "string", "description": "e.g. market_signal, competitor_move, price_change"},
                "suburb": {"type": "string", "description": "Relevant suburb"},
            },
            "required": ["topic", "summary", "signal_type"],
        },
    },
    {
        "name": "write_case_memory",
        "description": "Store a case memory note for a specific lead.",
        "input_schema": {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string", "description": "The lead UUID"},
                "memory_type": {"type": "string", "description": "e.g. operator_note, call_outcome, preference"},
                "content": {"type": "string", "description": "The memory content"},
            },
            "required": ["lead_id", "memory_type", "content"],
        },
    },
    {
        "name": "create_ticket",
        "description": "Create a task ticket for the operator or a department.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "department": {"type": "string"},
                "kind": {"type": "string", "description": "bug/feature/research/followup/anomaly"},
                "priority": {"type": "integer", "description": "1-10 (1=highest)", "default": 5},
            },
            "required": ["title", "department", "kind"],
        },
    },
]


# ─── Tool Executors ──────────────────────────────────────────────────────────


async def execute(
    tool_name: str,
    params: dict[str, Any],
    session: AsyncSession,
    agent_id: str = "sdk_agent",
) -> str:
    """Execute a lead_db tool and return JSON string result."""
    try:
        fn = _EXECUTORS.get(tool_name)
        if not fn:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})
        result = await fn(session, params, agent_id)
        return json.dumps(result, default=str)
    except Exception as exc:
        log.warning("[mcp_lead_db] Tool %s failed: %s", tool_name, exc)
        return json.dumps({"error": str(exc)})


async def _read_leads(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    limit = min(int(params.get("limit", 10)), 30)
    q = """
        SELECT id, address, suburb, heat_score, lifecycle_stage, status,
               signal_status, contact_status, why_now, last_outcome,
               touches_30d, last_contacted_at, days_on_market
        FROM leads WHERE heat_score > 0
    """
    args: dict = {"limit": limit}
    if params.get("status"):
        q += " AND status = :status"
        args["status"] = params["status"]
    if params.get("suburb"):
        q += " AND suburb LIKE :suburb"
        args["suburb"] = f"%{params['suburb']}%"
    if params.get("min_heat"):
        q += " AND heat_score >= :min_heat"
        args["min_heat"] = int(params["min_heat"])
    q += " ORDER BY heat_score DESC LIMIT :limit"
    rows = (await session.execute(text(q), args)).mappings().all()
    return [dict(r) for r in rows]


async def _read_lead_detail(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    lead_id = params["lead_id"]
    row = (await session.execute(
        text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id}
    )).mappings().first()
    if not row:
        return {"error": f"Lead {lead_id} not found"}
    return dict(row)


async def _search_leads(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    query = params["query"]
    limit = min(int(params.get("limit", 10)), 30)
    pattern = f"%{query}%"
    rows = (await session.execute(
        text("""
            SELECT id, address, suburb, heat_score, status, signal_status, owner_name
            FROM leads
            WHERE address LIKE :p OR suburb LIKE :p OR owner_name LIKE :p
            ORDER BY heat_score DESC LIMIT :limit
        """),
        {"p": pattern, "limit": limit},
    )).mappings().all()
    return [dict(r) for r in rows]


async def _read_call_log(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    limit = min(int(params.get("limit", 20)), 50)
    days_back = int(params.get("days_back", 7))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    q = """
        SELECT id, lead_address, outcome, connected, timestamp,
               call_duration_seconds, note, operator
        FROM call_log WHERE logged_at >= :cutoff
    """
    args: dict = {"cutoff": cutoff, "limit": limit}
    if params.get("lead_id"):
        q += " AND lead_id = :lead_id"
        args["lead_id"] = params["lead_id"]
    q += " ORDER BY logged_at DESC LIMIT :limit"
    rows = (await session.execute(text(q), args)).mappings().all()
    return [dict(r) for r in rows]


async def _read_transcripts(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    limit = min(int(params.get("limit", 5)), 15)
    days_back = int(params.get("days_back", 14))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    rows = (await session.execute(
        text("""
            SELECT t.id, t.call_id, substr(t.full_text, 1, 800) as text_preview,
                   t.confidence, t.created_at
            FROM transcripts t
            WHERE t.created_at >= :cutoff AND t.status = 'complete'
            ORDER BY t.created_at DESC LIMIT :limit
        """),
        {"cutoff": cutoff, "limit": limit},
    )).mappings().all()
    return [dict(r) for r in rows]


async def _read_pipeline_summary(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    parts: dict[str, Any] = {}

    try:
        rows = (await session.execute(text("""
            SELECT signal_status, COUNT(*) as cnt, AVG(heat_score) as avg_heat
            FROM leads WHERE status NOT IN ('converted', 'dropped')
            GROUP BY signal_status ORDER BY cnt DESC LIMIT 10
        """))).mappings().all()
        parts["by_signal"] = [dict(r) for r in rows]
    except Exception:
        parts["by_signal"] = []

    try:
        rows = (await session.execute(text("""
            SELECT lifecycle_stage, COUNT(*) as cnt
            FROM leads WHERE status NOT IN ('converted', 'dropped')
            GROUP BY lifecycle_stage ORDER BY cnt DESC
        """))).mappings().all()
        parts["by_stage"] = [dict(r) for r in rows]
    except Exception:
        parts["by_stage"] = []

    try:
        row = (await session.execute(text("""
            SELECT COUNT(*) as calls_today FROM call_log
            WHERE DATE(logged_at) = DATE('now')
        """))).mappings().first()
        parts["calls_today"] = row["calls_today"] if row else 0
    except Exception:
        parts["calls_today"] = 0

    try:
        row = (await session.execute(text("""
            SELECT COUNT(*) as total_active FROM leads
            WHERE status NOT IN ('converted', 'dropped')
        """))).mappings().first()
        parts["total_active_leads"] = row["total_active"] if row else 0
    except Exception:
        parts["total_active_leads"] = 0

    return parts


async def _read_hermes_findings(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    limit = min(int(params.get("limit", 10)), 30)
    q = """
        SELECT id, topic, signal_type, summary, why_it_matters,
               confidence_score, actionability_score, created_at
        FROM hermes_findings WHERE 1=1
    """
    args: dict = {"limit": limit}
    if params.get("signal_type"):
        q += " AND signal_type = :st"
        args["st"] = params["signal_type"]
    q += " ORDER BY created_at DESC LIMIT :limit"
    rows = (await session.execute(text(q), args)).mappings().all()
    return [dict(r) for r in rows]


async def _read_hermes_memory(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    limit = min(int(params.get("limit", 10)), 30)
    q = "SELECT id, memory_type, title, body, confidence_score, created_at FROM hermes_memory WHERE 1=1"
    args: dict = {"limit": limit}
    if params.get("memory_type"):
        q += " AND memory_type = :mt"
        args["mt"] = params["memory_type"]
    q += " ORDER BY created_at DESC LIMIT :limit"
    rows = (await session.execute(text(q), args)).mappings().all()
    return [dict(r) for r in rows]


async def _read_case_memory(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    lead_id = params["lead_id"]
    rows = (await session.execute(
        text("""
            SELECT id, memory_type, content, source, importance, created_at
            FROM hermes_case_memory WHERE lead_id = :lid
            ORDER BY importance DESC, created_at DESC LIMIT 20
        """),
        {"lid": lead_id},
    )).mappings().all()
    return [dict(r) for r in rows]


async def _write_hermes_finding(session: AsyncSession, params: dict, agent_id: str) -> Any:
    import hashlib
    now = datetime.now(timezone.utc).isoformat()
    dedupe_key = hashlib.md5(f"{agent_id}:{params['topic']}:{now[:13]}".encode()).hexdigest()
    finding_id = hashlib.md5(f"{agent_id}:{now}".encode()).hexdigest()

    await session.execute(text("""
        INSERT OR IGNORE INTO hermes_findings
            (id, source_id, source_type, source_name, source_url, dedupe_key,
             company_scope, topic, signal_type, summary, why_it_matters,
             confidence_score, actionability_score, novelty_score, created_at)
        VALUES
            (:id, :src, 'sdk_agent', :name, '', :dk,
             'shared', :topic, :st, :summary, '',
             0.8, 0.85, 0.7, :now)
    """), {
        "id": finding_id, "src": agent_id, "name": agent_id,
        "dk": dedupe_key, "topic": params["topic"],
        "st": params["signal_type"], "summary": params["summary"],
        "now": now,
    })
    await session.commit()
    return {"finding_id": finding_id, "stored": True}


async def _write_case_memory(session: AsyncSession, params: dict, agent_id: str) -> Any:
    import uuid as _uuid
    mem_id = str(_uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    await session.execute(text("""
        INSERT INTO hermes_case_memory (id, lead_id, memory_type, content, source, importance, created_at)
        VALUES (:id, :lid, :mt, :content, :source, 0.7, :now)
    """), {
        "id": mem_id, "lid": params["lead_id"],
        "mt": params["memory_type"], "content": params["content"],
        "source": f"sdk_agent:{agent_id}", "now": now,
    })
    await session.commit()
    return {"memory_id": mem_id, "stored": True}


async def _create_ticket(session: AsyncSession, params: dict, agent_id: str) -> Any:
    from services.agent_tool_layer import _tool_create_ticket
    return await _tool_create_ticket(session, params, agent_id)


# ─── Executor Registry ───────────────────────────────────────────────────────

_EXECUTORS = {
    "read_leads": _read_leads,
    "read_lead_detail": _read_lead_detail,
    "search_leads": _search_leads,
    "read_call_log": _read_call_log,
    "read_transcripts": _read_transcripts,
    "read_pipeline_summary": _read_pipeline_summary,
    "read_hermes_findings": _read_hermes_findings,
    "read_hermes_memory": _read_hermes_memory,
    "read_case_memory": _read_case_memory,
    "write_hermes_finding": _write_hermes_finding,
    "write_case_memory": _write_case_memory,
    "create_ticket": _create_ticket,
}
