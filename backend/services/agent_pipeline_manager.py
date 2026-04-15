"""
NYLA — Pipeline Manager Agent

Named agent that runs on a 2-hour cadence. Scans the lead pipeline,
identifies stalled deals, overdue follow-ups, and hot leads that need
immediate action. Creates tickets for each actionable finding.

Uses OpenAI via ai_router. Raises real tickets with evidence.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import USE_POSTGRES
from models.org_models import TicketDept, TicketKind, TicketSeverity
from services.ticket_service import create_ticket, ticket_exists_for_pattern

log = logging.getLogger("agent.nyla")

AGENT_ID = "nyla_pipeline_manager"
AGENT_NAME = "Nyla"
CADENCE_SECONDS = 2 * 3600  # every 2 hours

# Dialect helpers
_7d_ago = "NOW() - INTERVAL '7 days'" if USE_POSTGRES else "datetime('now', '-7 days')"
_2d_ago = "NOW() - INTERVAL '2 days'" if USE_POSTGRES else "datetime('now', '-2 days')"


async def run_cycle(session: AsyncSession) -> Dict[str, Any]:
    """One full cycle of the pipeline manager. Returns summary of actions taken."""
    raised: List[str] = []

    await _check_hot_leads_no_contact(session, raised)
    await _check_stalled_pipeline(session, raised)
    await _check_overdue_followups(session, raised)
    await _check_new_signals_unactioned(session, raised)
    await _check_outreach_needed(session, raised)

    log.info("[%s] Cycle complete — raised %d ticket(s)", AGENT_NAME, len(raised))
    return {"agent": AGENT_NAME, "tickets_raised": len(raised), "tickets": raised}


async def _check_hot_leads_no_contact(session: AsyncSession, raised: List) -> None:
    """High heat-score leads with zero contact history."""
    try:
        rows = (await session.execute(text("""
            SELECT id, address, suburb, heat_score, signal_status, created_at
            FROM leads
            WHERE heat_score >= 70
              AND (last_contacted_at IS NULL OR last_contacted_at = '')
              AND status NOT IN ('converted', 'dropped')
            ORDER BY heat_score DESC
            LIMIT 10
        """))).mappings().all()

        if not rows:
            return

        for row in rows:
            title = f"Hot lead untouched: {row['address']} (heat {row['heat_score']})"
            if await ticket_exists_for_pattern(session, f"Hot lead untouched: {row['address']}", TicketDept.REVENUE, hours_back=24):
                continue
            await create_ticket(
                session,
                title=title,
                description=(
                    f"{row['address']}, {row['suburb']} has heat score {row['heat_score']} "
                    f"({row['signal_status']}) but has never been contacted. "
                    f"Surfaced {str(row['created_at'])[:10]}. Call today."
                ),
                department=TicketDept.REVENUE,
                kind=TicketKind.FOLLOWUP,
                priority=2,
                severity=TicketSeverity.HIGH,
                created_by_type="agent",
                created_by_id=AGENT_ID,
                related_lead_id=row["id"],
                evidence_json={
                    "heat_score": row["heat_score"],
                    "signal_status": row["signal_status"],
                    "created_at": str(row["created_at"]),
                },
                tags=["nyla", "hot_lead", "no_contact"],
            )
            raised.append(title)
    except Exception as exc:
        log.warning("[%s] hot leads check failed: %s", AGENT_NAME, exc)


async def _check_stalled_pipeline(session: AsyncSession, raised: List) -> None:
    """Leads contacted but not progressed in 7+ days."""
    try:
        rows = (await session.execute(text(f"""
            SELECT id, address, suburb, heat_score, status, last_contacted_at, signal_status
            FROM leads
            WHERE last_contacted_at IS NOT NULL
              AND last_contacted_at != ''
              AND status NOT IN ('converted', 'dropped', 'not_interested')
              AND last_contacted_at < {_7d_ago}
            ORDER BY heat_score DESC
            LIMIT 8
        """))).mappings().all()

        if not rows:
            return

        title = f"Pipeline stall: {len(rows)} leads with no progress in 7+ days"
        if await ticket_exists_for_pattern(session, "Pipeline stall:", TicketDept.REVENUE, hours_back=24):
            return

        lead_summaries = []
        for row in rows:
            lead_summaries.append(
                f"  - {row['address']}, {row['suburb']} "
                f"(heat {row['heat_score']}, last contact: {str(row['last_contacted_at'])[:10]})"
            )

        await create_ticket(
            session,
            title=title,
            description=(
                f"{len(rows)} leads were contacted but haven't progressed in over a week.\n\n"
                + "\n".join(lead_summaries[:8])
                + "\n\nReview and either re-engage with a new angle or update status."
            ),
            department=TicketDept.REVENUE,
            kind=TicketKind.FOLLOWUP,
            priority=3,
            severity=TicketSeverity.MEDIUM,
            created_by_type="agent",
            created_by_id=AGENT_ID,
            evidence_json={"stalled_count": len(rows), "leads": [dict(r) for r in rows[:5]]},
            tags=["nyla", "pipeline_stall"],
        )
        raised.append(title)
    except Exception as exc:
        log.warning("[%s] stalled pipeline check failed: %s", AGENT_NAME, exc)


async def _check_overdue_followups(session: AsyncSession, raised: List) -> None:
    """Leads with follow_up_due_at in the past."""
    try:
        rows = (await session.execute(text("""
            SELECT id, address, suburb, follow_up_due_at, heat_score
            FROM leads
            WHERE follow_up_due_at IS NOT NULL
              AND follow_up_due_at != ''
              AND follow_up_due_at < :now
              AND status NOT IN ('converted', 'dropped')
            ORDER BY follow_up_due_at ASC
            LIMIT 15
        """), {"now": datetime.now(timezone.utc).isoformat()})).mappings().all()

        if len(rows) < 3:
            return

        title = f"Overdue follow-ups: {len(rows)} leads past due"
        if await ticket_exists_for_pattern(session, "Overdue follow-ups:", TicketDept.REVENUE, hours_back=12):
            return

        await create_ticket(
            session,
            title=title,
            description=(
                f"{len(rows)} leads have overdue follow-up dates. "
                f"Oldest: {rows[0]['address']} (due {str(rows[0]['follow_up_due_at'])[:10]}). "
                f"Clear the backlog before taking on new outreach."
            ),
            department=TicketDept.REVENUE,
            kind=TicketKind.FOLLOWUP,
            priority=2,
            severity=TicketSeverity.HIGH if len(rows) >= 10 else TicketSeverity.MEDIUM,
            created_by_type="agent",
            created_by_id=AGENT_ID,
            evidence_json={"overdue_count": len(rows)},
            tags=["nyla", "overdue_followup"],
        )
        raised.append(title)
    except Exception as exc:
        log.warning("[%s] overdue followups check failed: %s", AGENT_NAME, exc)


async def _check_new_signals_unactioned(session: AsyncSession, raised: List) -> None:
    """New high-value signals (withdrawn, expired, probate) from last 48h with no action."""
    try:
        rows = (await session.execute(text(f"""
            SELECT id, address, suburb, signal_status, heat_score, created_at
            FROM leads
            WHERE signal_status IN ('WITHDRAWN', 'EXPIRED', 'PROBATE')
              AND (last_contacted_at IS NULL OR last_contacted_at = '')
              AND status NOT IN ('converted', 'dropped')
              AND created_at >= {_2d_ago}
            ORDER BY heat_score DESC
            LIMIT 10
        """))).mappings().all()

        if not rows:
            return

        title = f"Fresh signals unactioned: {len(rows)} in last 48h"
        if await ticket_exists_for_pattern(session, "Fresh signals unactioned:", TicketDept.REVENUE, hours_back=24):
            return

        lead_lines = [
            f"  - {r['address']}, {r['suburb']} ({r['signal_status']}, heat {r['heat_score']})"
            for r in rows[:6]
        ]

        await create_ticket(
            session,
            title=title,
            description=(
                f"{len(rows)} high-value signals came in during the last 48 hours "
                f"and haven't been actioned yet.\n\n"
                + "\n".join(lead_lines)
                + "\n\nFirst agent to call a withdrawn listing usually wins."
            ),
            department=TicketDept.REVENUE,
            kind=TicketKind.FOLLOWUP,
            priority=1,
            severity=TicketSeverity.CRITICAL,
            created_by_type="agent",
            created_by_id=AGENT_ID,
            evidence_json={"signal_count": len(rows), "signals": [dict(r) for r in rows[:5]]},
            tags=["nyla", "fresh_signal", "urgent"],
        )
        raised.append(title)
    except Exception as exc:
        log.warning("[%s] new signals check failed: %s", AGENT_NAME, exc)


async def _check_outreach_needed(session: AsyncSession, raised: List) -> None:
    """Raise tickets for leads that need email or SMS outreach."""
    _3d = "NOW() - INTERVAL '3 days'" if USE_POSTGRES else "datetime('now', '-3 days')"

    # 1. REA enquiries not responded to → email/call ticket
    try:
        rows = (await session.execute(text(f"""
            SELECT id, address, suburb, rea_enquiries, rea_last_enquiry_at
            FROM leads
            WHERE COALESCE(rea_listing_id, '') <> ''
              AND COALESCE(rea_enquiries, 0) > 0
              AND rea_last_enquiry_at >= {_3d}
              AND (last_contacted_at IS NULL OR last_contacted_at < rea_last_enquiry_at)
              AND status NOT IN ('converted', 'dropped')
            ORDER BY rea_last_enquiry_at DESC
            LIMIT 5
        """))).mappings().all()

        for row in rows:
            title = f"Reply to REA enquiry: {row['address']}, {row['suburb']}"
            if await ticket_exists_for_pattern(session, f"Reply to REA enquiry: {row['address']}", TicketDept.REVENUE, hours_back=24):
                continue
            await create_ticket(
                session,
                title=title,
                description=(
                    f"{row['address']}, {row['suburb']} has {row['rea_enquiries']} REA enquiries. "
                    f"Latest enquiry: {str(row.get('rea_last_enquiry_at') or '')[:10]}. "
                    f"Call or email the enquirer within 24h for best conversion."
                ),
                department=TicketDept.REVENUE,
                kind=TicketKind.OUTREACH_EMAIL,
                priority=1,
                severity=TicketSeverity.HIGH,
                created_by_type="agent",
                created_by_id=AGENT_ID,
                related_lead_id=row["id"],
                evidence_json={"rea_enquiries": row["rea_enquiries"]},
                tags=["nyla", "outreach", "rea_enquiry"],
            )
            raised.append(title)
    except Exception as exc:
        log.debug("[%s] REA enquiry outreach check failed: %s", AGENT_NAME, exc)

    # 2. Leads contacted but no prospectus sent → send prospectus email
    try:
        rows = (await session.execute(text(f"""
            SELECT id, address, suburb, last_outcome, heat_score
            FROM leads
            WHERE last_contacted_at IS NOT NULL
              AND last_contacted_at >= {_3d}
              AND last_outcome IN ('connected', 'callback', 'interested')
              AND (prospectus_sent_at IS NULL OR prospectus_sent_at = '')
              AND status NOT IN ('converted', 'dropped', 'not_interested')
            ORDER BY heat_score DESC
            LIMIT 5
        """))).mappings().all()

        for row in rows:
            title = f"Send prospectus: {row['address']}, {row['suburb']}"
            if await ticket_exists_for_pattern(session, f"Send prospectus: {row['address']}", TicketDept.REVENUE, hours_back=48):
                continue
            await create_ticket(
                session,
                title=title,
                description=(
                    f"{row['address']}, {row['suburb']} — last call outcome was '{row['last_outcome']}' "
                    f"but no prospectus has been sent yet. Draft and send a prospectus email as Nitin Puri."
                ),
                department=TicketDept.REVENUE,
                kind=TicketKind.OUTREACH_EMAIL,
                priority=2,
                severity=TicketSeverity.MEDIUM,
                created_by_type="agent",
                created_by_id=AGENT_ID,
                related_lead_id=row["id"],
                evidence_json={"last_outcome": row["last_outcome"], "heat_score": row["heat_score"]},
                tags=["nyla", "outreach", "prospectus"],
            )
            raised.append(title)
    except Exception as exc:
        log.debug("[%s] prospectus outreach check failed: %s", AGENT_NAME, exc)
