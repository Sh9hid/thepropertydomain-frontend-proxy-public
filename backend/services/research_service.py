"""
Research service — grounded research department.

Three areas:
  sales       — call/follow-up patterns, objections, script gaps
  real_estate — suburb/listing signals from internal DB
  app_tech    — product bottlenecks, failures, gaps from orch events + metrics

Rules:
  - All inputs come from real DB rows — no fabrication
  - LLM synthesizes findings, does NOT invent source facts
  - Prompts are bounded (2000 char context cap per area)
  - High-signal findings may auto-raise tickets
  - Concise outputs only
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from models.org_models import (
    ResearchArea, ResearchNote, ResearchRun, TicketDept, TicketKind,
    TicketSeverity,
)
from services.orchestration_engine import route_completion

log = logging.getLogger(__name__)

# Caps
MAX_LEADS_CONTEXT   = 20
MAX_CALLS_CONTEXT   = 30
MAX_EVENTS_CONTEXT  = 20
MAX_TICKET_RAISE_PER_RUN = 2  # prevent ticket storms


# ─── Data loaders ─────────────────────────────────────────────────────────────

async def _load_sales_context(session: AsyncSession) -> Dict[str, Any]:
    """Load recent call stats and lead lifecycle distribution."""
    # Recent call log outcomes — last 30 days
    cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
    call_rows = (await session.execute(
        text("""
            SELECT outcome, count(*) as cnt,
                   avg(call_duration_seconds) as avg_dur
            FROM call_log
            WHERE logged_at >= :cutoff
            GROUP BY outcome
            ORDER BY cnt DESC
            LIMIT 15
        """),
        {"cutoff": cutoff},
    )).mappings().all()

    # Lead lifecycle distribution
    lifecycle_rows = (await session.execute(
        text("""
            SELECT lifecycle_stage, count(*) as cnt
            FROM leads
            WHERE lifecycle_stage IS NOT NULL
            GROUP BY lifecycle_stage
            ORDER BY cnt DESC
            LIMIT 10
        """)
    )).mappings().all()

    # Leads with many touches but no booking — frustration pattern
    stuck_leads = (await session.execute(
        text("""
            SELECT address, suburb, heat_score, touches_30d, last_outcome, objection_reason
            FROM leads
            WHERE touches_30d >= 4
              AND status NOT IN ('booked', 'won', 'dead')
              AND last_outcome NOT IN ('booked_appraisal', 'booked_mortgage', '')
            ORDER BY heat_score DESC, touches_30d DESC
            LIMIT 10
        """)
    )).mappings().all()

    # Top objection reasons
    objection_rows = (await session.execute(
        text("""
            SELECT objection_reason, count(*) as cnt
            FROM leads
            WHERE objection_reason != ''
            GROUP BY objection_reason
            ORDER BY cnt DESC
            LIMIT 8
        """)
    )).mappings().all()

    return {
        "call_outcome_distribution": [dict(r) for r in call_rows],
        "lifecycle_distribution": [dict(r) for r in lifecycle_rows],
        "stuck_leads_sample": [dict(r) for r in stuck_leads],
        "top_objections": [dict(r) for r in objection_rows],
    }


async def _load_re_context(session: AsyncSession) -> Dict[str, Any]:
    """Load suburb/listing signals."""
    # High-score leads by suburb
    suburb_dist = (await session.execute(
        text("""
            SELECT suburb, count(*) as cnt,
                   avg(heat_score) as avg_heat,
                   sum(case when trigger_type='withdrawn' then 1 else 0 end) as withdrawn,
                   sum(case when trigger_type='da_lodged' then 1 else 0 end) as da_lodged
            FROM leads
            WHERE suburb IS NOT NULL
            GROUP BY suburb
            ORDER BY avg_heat DESC, cnt DESC
            LIMIT 15
        """)
    )).mappings().all()

    # Stale active listings (on market > 60 days)
    stale = (await session.execute(
        text("""
            SELECT address, suburb, days_on_market, list_date, heat_score
            FROM leads
            WHERE days_on_market >= 60
              AND status = 'active'
            ORDER BY days_on_market DESC
            LIMIT 10
        """)
    )).mappings().all()

    # Price-dropped listings
    price_drops = (await session.execute(
        text("""
            SELECT address, suburb, price_drop_count, heat_score
            FROM leads
            WHERE price_drop_count >= 1
            ORDER BY price_drop_count DESC, heat_score DESC
            LIMIT 10
        """)
    )).mappings().all()

    return {
        "suburb_distribution": [dict(r) for r in suburb_dist],
        "stale_listings": [dict(r) for r in stale],
        "price_dropped": [dict(r) for r in price_drops],
    }


async def _load_tech_context(session: AsyncSession) -> Dict[str, Any]:
    """Load orch events, circuit patterns, and provider failures."""
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()

    # Recent orch failures
    event_rows = (await session.execute(
        text("""
            SELECT event_type, agent_role, provider, message, ts
            FROM orch_events
            WHERE level IN ('warn', 'error')
              AND ts >= :cutoff
            ORDER BY ts DESC
            LIMIT :limit
        """),
        {"cutoff": cutoff, "limit": MAX_EVENTS_CONTEXT},
    )).mappings().all()

    # Ticket backlog (open / blocked)
    backlog = (await session.execute(
        text("""
            SELECT department, kind, count(*) as cnt
            FROM org_tickets
            WHERE status IN ('open', 'accepted', 'blocked')
            GROUP BY department, kind
            ORDER BY cnt DESC
        """)
    )).mappings().all()

    return {
        "recent_failures": [dict(r) for r in event_rows],
        "ticket_backlog": [dict(r) for r in backlog],
    }


# ─── Prompt builders ──────────────────────────────────────────────────────────

_SYSTEM_RESEARCHER = (
    "You are a grounded research analyst. You receive real data from a real estate "
    "lead management system. Your job is to find patterns, surface actionable insights, "
    "and write concise structured findings. Never invent data. "
    "If evidence is weak, say so. Return valid JSON only."
)

_RESEARCH_SCHEMA = """{
  "title": "...",
  "thesis": "...",
  "evidence": "bullet summary of real data patterns",
  "recommendation": "one concrete action",
  "confidence": "low|medium|high",
  "raise_ticket": true|false,
  "ticket_title": "...",
  "ticket_kind": "bug|feature|research|followup|anomaly"
}"""


def _sales_prompt(ctx: Dict) -> str:
    return f"""Analyse this real call and lead data from an Australian real estate outbound operation.

DATA:
{json.dumps(ctx, indent=2)[:2000]}

Find: objection clusters, follow-up gaps, high-touch/no-booking leads, script improvement opportunities.
Produce ONE finding per area that has strong evidence.

Return a JSON array with 1-2 items matching this schema:
{_RESEARCH_SCHEMA}"""


def _re_prompt(ctx: Dict) -> str:
    return f"""Analyse this suburb/listing data from an Australian real estate CRM.

DATA:
{json.dumps(ctx, indent=2)[:2000]}

Find: suburb opportunity patterns, stale listing patterns, price reduction signals.
Produce 1-2 concrete grounded findings.

Return a JSON array with 1-2 items matching this schema:
{_RESEARCH_SCHEMA}"""


def _tech_prompt(ctx: Dict) -> str:
    return f"""Analyse this operational data from a real estate lead management app.

DATA:
{json.dumps(ctx, indent=2)[:2000]}

Find: system failures, provider issues, ticket backlogs, product gaps.
Produce 1-2 grounded findings with concrete recommendations.

Return a JSON array with 1-2 items matching this schema:
{_RESEARCH_SCHEMA}"""


# ─── Core runner ──────────────────────────────────────────────────────────────

async def _run_research_area(
    session: AsyncSession,
    area: str,
    run_id: str,
) -> Tuple[List[ResearchNote], int]:
    """Run research for one area. Returns (notes_created, tickets_raised)."""
    from services.ticket_service import create_ticket, ticket_exists_for_pattern

    if area == ResearchArea.SALES:
        ctx = await _load_sales_context(session)
        prompt = _sales_prompt(ctx)
    elif area == ResearchArea.REAL_ESTATE:
        ctx = await _load_re_context(session)
        prompt = _re_prompt(ctx)
    elif area == ResearchArea.APP_TECH:
        ctx = await _load_tech_context(session)
        prompt = _tech_prompt(ctx)
    else:
        return [], 0

    messages = [
        {"role": "system", "content": _SYSTEM_RESEARCHER},
        {"role": "user", "content": prompt},
    ]

    try:
        result = await route_completion(
            work_type="research",
            messages=messages,
            max_tokens=1024,
            job_id=None,
            task_id=run_id,
        )
        text_raw = result.text.strip()
        # Extract JSON array
        if "```" in text_raw:
            text_raw = text_raw.split("```")[1]
            if text_raw.startswith("json"):
                text_raw = text_raw[4:]
        findings: List[Dict] = json.loads(text_raw)
        if isinstance(findings, dict):
            findings = [findings]
    except Exception as exc:
        log.warning("[research] area=%s LLM/parse failed: %s", area, exc)
        return [], 0

    notes_created: List[ResearchNote] = []
    tickets_raised = 0

    for finding in findings[:3]:  # cap output
        if not isinstance(finding, dict):
            continue
        title = str(finding.get("title", "Research finding"))[:200]
        thesis = str(finding.get("thesis", ""))[:1000]
        evidence = str(finding.get("evidence", ""))[:1000]
        recommendation = str(finding.get("recommendation", ""))[:500]
        confidence = str(finding.get("confidence", "medium"))

        note = ResearchNote(
            title=title,
            area=area,
            thesis=thesis,
            evidence=evidence,
            recommendation=recommendation,
            confidence=confidence,
            run_id=run_id,
            evidence_json={"source_data_keys": list(ctx.keys())},
        )
        session.add(note)
        await session.flush()
        notes_created.append(note)

        # Auto-raise ticket if high confidence + flag set + under storm cap
        should_raise = (
            finding.get("raise_ticket") is True
            and confidence in ("high",)
            and tickets_raised < MAX_TICKET_RAISE_PER_RUN
        )
        if should_raise:
            ticket_title = str(finding.get("ticket_title", title))[:200]
            duplicate = await ticket_exists_for_pattern(session, ticket_title, TicketDept.RESEARCH)
            if not duplicate:
                t = await create_ticket(
                    session,
                    title=ticket_title,
                    description=f"{thesis}\n\nRecommendation: {recommendation}",
                    department=TicketDept.RESEARCH,
                    kind=str(finding.get("ticket_kind", TicketKind.RESEARCH)),
                    priority=3,
                    severity=TicketSeverity.MEDIUM,
                    created_by_type="agent",
                    created_by_id="research_agent",
                    evidence_json={"research_note_id": note.id, "area": area, "confidence": confidence},
                )
                note.ticket_raised_id = t.id
                tickets_raised += 1

        # Write org memory for high-confidence findings
        if confidence in ("high", "medium"):
            from services.memory_service import record_research_memory
            await record_research_memory(
                session, area=area,
                key_finding=f"{title}: {recommendation}",
                confidence=confidence,
                note_id=note.id,
            )

    await session.commit()
    return notes_created, tickets_raised


# ─── Public entry points ───────────────────────────────────────────────────────

async def run_research(
    session: AsyncSession,
    area: str,
) -> ResearchRun:
    """Run research for one area. Creates a run record and notes."""
    run = ResearchRun(area=area, status="running")
    session.add(run)
    await session.commit()
    await session.refresh(run)

    from services.orchestration_engine import get_provider_snapshot
    providers = [p for p in get_provider_snapshot() if p["available"] and not p["circuit_open"]]
    provider_used = providers[0]["key"] if providers else "none"

    try:
        notes, tickets = await _run_research_area(session, area, run.id)
        run.status = "done"
        run.notes_created = len(notes)
        run.tickets_raised = tickets
        run.provider_used = provider_used
        run.completed_at = datetime.utcnow()
    except Exception as exc:
        log.error("[research] run failed for area=%s: %s", area, exc)
        run.status = "failed"
        run.error = str(exc)[:300]
        run.completed_at = datetime.utcnow()

    await session.commit()
    await session.refresh(run)

    from core.events import event_manager
    await event_manager.broadcast({
        "type": "RESEARCH_RUN_DONE",
        "data": {
            "run_id": run.id,
            "area": area,
            "status": run.status,
            "notes_created": run.notes_created,
            "tickets_raised": run.tickets_raised,
            "ts": datetime.utcnow().isoformat(),
        }
    })
    return run


async def get_recent_notes(
    session: AsyncSession,
    area: Optional[str] = None,
    limit: int = 20,
) -> List[ResearchNote]:
    q = select(ResearchNote).order_by(ResearchNote.created_at.desc()).limit(limit)
    if area:
        q = q.where(ResearchNote.area == area)
    return (await session.execute(q)).scalars().all()


async def get_recent_runs(
    session: AsyncSession,
    limit: int = 10,
) -> List[ResearchRun]:
    return (await session.execute(
        select(ResearchRun).order_by(ResearchRun.started_at.desc()).limit(limit)
    )).scalars().all()
