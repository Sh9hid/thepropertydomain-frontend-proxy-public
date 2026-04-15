"""
Auto-ticket watcher — raises tickets from real evidence, not noise.

Triggers:
  1. Provider circuit breaker opened (orch_events in last hour)
  2. Repeated orchestration task failures (>= 3 in last 2 hours, same work_type)
  3. Anomalous call metric: connected_rate < 5% over 30 dials (today)
  4. No call activity logged today (by 4 PM AEST)
  5. Research run failures

Rules:
  - Deduplication: won't raise same pattern twice in 24h
  - Max 5 auto-tickets per watcher run
  - Every raised ticket includes evidence_json
  - Emits TICKET_CREATED via ticket_service (which broadcasts websocket)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import SYDNEY_TZ
from models.org_models import TicketDept, TicketKind, TicketSeverity
from services.ticket_service import create_ticket, ticket_exists_for_pattern

log = logging.getLogger(__name__)
MAX_TICKETS_PER_RUN = 5


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def _check_circuit_breakers(session: AsyncSession, raised: List) -> None:
    if len(raised) >= MAX_TICKETS_PER_RUN:
        return
    cutoff = (_utc_now() - timedelta(hours=1)).isoformat()
    rows = (await session.execute(
        text("""
            SELECT provider, count(*) as cnt
            FROM orch_events
            WHERE event_type = 'task_failed'
              AND ts >= :cutoff
              AND provider IS NOT NULL
            GROUP BY provider
            HAVING cnt >= 3
        """),
        {"cutoff": cutoff},
    )).mappings().all()

    for row in rows:
        provider = row["provider"]
        cnt = row["cnt"]
        title = f"Circuit breaker: {provider} failing ({cnt} failures in 1h)"
        if await ticket_exists_for_pattern(session, f"Circuit breaker: {provider}", TicketDept.ENGINEERING):
            continue
        await create_ticket(
            session,
            title=title,
            description=f"Provider '{provider}' had {cnt} task failures in the last hour. Circuit breaker may be open.",
            department=TicketDept.ENGINEERING,
            kind=TicketKind.ANOMALY,
            priority=2,
            severity=TicketSeverity.HIGH,
            created_by_type="system",
            evidence_json={"provider": provider, "failure_count": cnt, "window_hours": 1},
        )
        raised.append(title)
        if len(raised) >= MAX_TICKETS_PER_RUN:
            return


async def _check_repeated_orch_failures(session: AsyncSession, raised: List) -> None:
    if len(raised) >= MAX_TICKETS_PER_RUN:
        return
    cutoff = (_utc_now() - timedelta(hours=2)).isoformat()
    bind = await session.connection()
    dialect_name = bind.dialect.name if bind is not None else ""
    work_type_expr = "json_extract(data, '$.work_type')" if dialect_name == "sqlite" else "data->>'work_type'"
    rows = (
        await session.execute(
            text(
                f"""
                SELECT {work_type_expr} as work_type, count(*) as cnt
                FROM orch_events
                WHERE event_type = 'task_failed'
                  AND ts >= :cutoff
                GROUP BY work_type
                HAVING cnt >= 3
                """
            ),
            {"cutoff": cutoff},
        )
    ).mappings().all()

    for row in rows:
        wt = row["work_type"] or "unknown"
        cnt = row["cnt"]
        title = f"Repeated task failures: {wt} ({cnt} in 2h)"
        if await ticket_exists_for_pattern(session, f"Repeated task failures: {wt}", TicketDept.ENGINEERING):
            continue
        await create_ticket(
            session,
            title=title,
            description=f"Work type '{wt}' has failed {cnt} times in the last 2 hours. Investigate agent/provider routing.",
            department=TicketDept.ENGINEERING,
            kind=TicketKind.BUG,
            priority=3,
            severity=TicketSeverity.MEDIUM,
            created_by_type="system",
            evidence_json={"work_type": wt, "failure_count": cnt},
        )
        raised.append(title)
        if len(raised) >= MAX_TICKETS_PER_RUN:
            return


async def _check_call_connect_rate(session: AsyncSession, raised: List) -> None:
    if len(raised) >= MAX_TICKETS_PER_RUN:
        return
    today = datetime.now(SYDNEY_TZ).strftime("%Y-%m-%d")
    rows = (await session.execute(
        text("""
            SELECT count(*) as total,
                   sum(case when connected=1 then 1 else 0 end) as connected
            FROM call_log
            WHERE logged_date = :today
        """),
        {"today": today},
    )).mappings().first()

    if not rows:
        return
    total = rows["total"] or 0
    connected = rows["connected"] or 0
    if total < 20:
        return  # not enough data
    rate = connected / total
    if rate < 0.05:
        title = f"Low call connect rate today: {rate:.0%} ({connected}/{total})"
        if not await ticket_exists_for_pattern(session, "Low call connect rate", TicketDept.REVENUE):
            await create_ticket(
                session,
                title=title,
                description=f"Today's connect rate is {rate:.1%} ({connected}/{total} dials). Possible wrong number list or time-of-day issue.",
                department=TicketDept.REVENUE,
                kind=TicketKind.ANOMALY,
                priority=3,
                severity=TicketSeverity.MEDIUM,
                created_by_type="system",
                evidence_json={"total_dials": total, "connected": connected, "connect_rate": rate, "date": today},
            )
            raised.append(title)


async def _check_no_call_activity(session: AsyncSession, raised: List) -> None:
    """Flag if no calls logged today and it's past 4 PM AEST."""
    if len(raised) >= MAX_TICKETS_PER_RUN:
        return
    now_sydney = datetime.now(SYDNEY_TZ)
    if now_sydney.hour < 16:
        return
    today = now_sydney.strftime("%Y-%m-%d")
    row = (await session.execute(
        text("SELECT count(*) as cnt FROM call_log WHERE logged_date = :today"),
        {"today": today},
    )).mappings().first()
    if row and (row["cnt"] or 0) == 0:
        title = f"No call activity recorded today ({today})"
        if not await ticket_exists_for_pattern(session, "No call activity", TicketDept.REVENUE, hours_back=12):
            await create_ticket(
                session,
                title=title,
                description=f"No calls have been logged today ({today}) as of {now_sydney.strftime('%H:%M')} AEST.",
                department=TicketDept.REVENUE,
                kind=TicketKind.FOLLOWUP,
                priority=4,
                severity=TicketSeverity.LOW,
                created_by_type="system",
                evidence_json={"date": today, "check_time_aest": now_sydney.strftime("%H:%M")},
            )
            raised.append(title)


async def _check_research_failures(session: AsyncSession, raised: List) -> None:
    if len(raised) >= MAX_TICKETS_PER_RUN:
        return
    cutoff = (_utc_now() - timedelta(hours=12)).isoformat()
    rows = (await session.execute(
        text("""
            SELECT area, count(*) as cnt
            FROM org_research_runs
            WHERE status = 'failed'
              AND started_at >= :cutoff
            GROUP BY area
        """),
        {"cutoff": cutoff},
    )).mappings().all()

    for row in rows:
        area = row["area"]
        title = f"Research run failing: {area}"
        if await ticket_exists_for_pattern(session, title, TicketDept.ENGINEERING):
            continue
        await create_ticket(
            session,
            title=title,
            description=f"Research area '{area}' has been failing in the last 12 hours. Check provider health and research service.",
            department=TicketDept.ENGINEERING,
            kind=TicketKind.BUG,
            priority=4,
            severity=TicketSeverity.LOW,
            created_by_type="system",
            evidence_json={"area": area, "failures": row["cnt"]},
        )
        raised.append(title)


# ─── Main watcher entry ───────────────────────────────────────────────────────

async def run_auto_ticket_watcher(session: AsyncSession) -> List[str]:
    """
    Run all checks. Returns list of ticket titles raised.
    Safe to call on a schedule (e.g. every 30 minutes).
    """
    raised: List[str] = []
    try:
        await _check_circuit_breakers(session, raised)
        await _check_repeated_orch_failures(session, raised)
        await _check_call_connect_rate(session, raised)
        await _check_no_call_activity(session, raised)
        await _check_research_failures(session, raised)
        # Follow-up backlog now handled by Nyla (agent_pipeline_manager)
    except Exception as exc:
        log.error("[auto_watcher] error: %s", exc)
    if raised:
        log.info("[auto_watcher] raised %d ticket(s): %s", len(raised), raised)
    return raised


async def _check_followup_overdue_backlog(session: AsyncSession, raised: List) -> None:
    """Escalate when overdue follow-up volume is building without action."""
    if len(raised) >= MAX_TICKETS_PER_RUN:
        return
    rows = (
        await session.execute(
            text(
                """
                SELECT
                    COUNT(*) AS overdue_count,
                    SUM(CASE WHEN NULLIF(follow_up_due_at, '')::timestamptz < NOW() - INTERVAL '1 day' THEN 1 ELSE 0 END) AS overdue_gt_1d,
                    SUM(CASE WHEN NULLIF(follow_up_due_at, '')::timestamptz < NOW() - INTERVAL '3 day' THEN 1 ELSE 0 END) AS overdue_gt_3d
                FROM leads
                WHERE follow_up_due_at IS NOT NULL
                  AND follow_up_due_at != ''
                  AND NULLIF(follow_up_due_at, '')::timestamptz < NOW()
                  AND status NOT IN ('converted', 'dropped')
                """
            )
        )
    ).mappings().first()
    if not rows:
        return
    overdue_count = int(rows.get("overdue_count") or 0)
    overdue_gt_1d = int(rows.get("overdue_gt_1d") or 0)
    overdue_gt_3d = int(rows.get("overdue_gt_3d") or 0)

    # SLA tiers:
    #  - Tier 1: sustained backlog
    #  - Tier 2: materially stale (>24h)
    #  - Tier 3: severe staleness (>72h)
    if overdue_count < 15 and overdue_gt_1d < 6 and overdue_gt_3d < 3:
        return

    severity = TicketSeverity.MEDIUM
    priority = 3
    if overdue_gt_1d >= 10:
        severity = TicketSeverity.HIGH
        priority = 2
    if overdue_gt_3d >= 5:
        severity = TicketSeverity.CRITICAL
        priority = 1

    title = (
        f"Follow-up backlog overdue: {overdue_count} total "
        f"({overdue_gt_1d} >24h, {overdue_gt_3d} >72h)"
    )
    if await ticket_exists_for_pattern(session, "Follow-up backlog overdue", TicketDept.REVENUE, hours_back=12):
        return
    await create_ticket(
        session,
        title=title,
        description=(
            "Overdue follow-up volume crossed operational threshold. "
            "Prioritize callback/follow-up lane and clear stale queue before new outreach."
        ),
        department=TicketDept.REVENUE,
        kind=TicketKind.FOLLOWUP,
        priority=priority,
        severity=severity,
        created_by_type="system",
        evidence_json={
            "overdue_count": overdue_count,
            "overdue_gt_1d": overdue_gt_1d,
            "overdue_gt_3d": overdue_gt_3d,
            "evaluated_at_utc": _utc_now().isoformat(),
        },
    )
    raised.append(title)
