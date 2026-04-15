"""
REX — REA Listing Analyst Agent

Named agent that runs on a 3-hour cadence. Monitors REA listing
performance, identifies underperformers, flags stale listings needing
refresh, and tracks which title/description variants convert best.

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

log = logging.getLogger("agent.rex")

AGENT_ID = "rex_listing_analyst"
AGENT_NAME = "Rex"
CADENCE_SECONDS = 3 * 3600  # every 3 hours

# Dialect helpers
_7d_ago = "NOW() - INTERVAL '7 days'" if USE_POSTGRES else "datetime('now', '-7 days')"
_3d_ago = "NOW() - INTERVAL '3 days'" if USE_POSTGRES else "datetime('now', '-3 days')"
_14d_ago = "NOW() - INTERVAL '14 days'" if USE_POSTGRES else "datetime('now', '-14 days')"


async def run_cycle(session: AsyncSession) -> Dict[str, Any]:
    """One full cycle of the listing analyst. Returns summary of actions taken."""
    raised: List[str] = []

    await _check_underperforming_listings(session, raised)
    await _check_stale_listings(session, raised)
    await _check_variant_performance(session, raised)
    await _check_listings_no_photos(session, raised)
    await _check_new_enquiries(session, raised)

    log.info("[%s] Cycle complete — raised %d ticket(s)", AGENT_NAME, len(raised))
    return {"agent": AGENT_NAME, "tickets_raised": len(raised), "tickets": raised}


async def _check_underperforming_listings(session: AsyncSession, raised: List) -> None:
    """Listings with views but zero enquiries after 7+ days live."""
    try:
        rows = (await session.execute(text(f"""
            SELECT id, address, suburb, rea_listing_id, rea_views, rea_enquiries,
                   rea_title_variant, rea_last_edit_at
            FROM leads
            WHERE COALESCE(rea_listing_id, '') <> ''
              AND COALESCE(rea_views, 0) >= 20
              AND COALESCE(rea_enquiries, 0) = 0
              AND rea_last_edit_at < {_7d_ago}
            ORDER BY rea_views DESC
            LIMIT 10
        """))).mappings().all()

        if not rows:
            return

        title = f"Underperforming listings: {len(rows)} with views but zero enquiries"
        if await ticket_exists_for_pattern(session, "Underperforming listings:", TicketDept.REVENUE, hours_back=24):
            return

        listing_lines = [
            f"  - {r['address']}, {r['suburb']} — {r['rea_views']} views, 0 enquiries (variant: {r.get('rea_title_variant') or 'default'})"
            for r in rows[:6]
        ]

        await create_ticket(
            session,
            title=title,
            description=(
                f"{len(rows)} REA listings have had views but zero enquiries.\n\n"
                + "\n".join(listing_lines)
                + "\n\nConsider refreshing title/description or swapping template variant."
            ),
            department=TicketDept.REVENUE,
            kind=TicketKind.ANOMALY,
            priority=3,
            severity=TicketSeverity.MEDIUM,
            created_by_type="agent",
            created_by_id=AGENT_ID,
            evidence_json={
                "underperforming_count": len(rows),
                "listings": [dict(r) for r in rows[:5]],
            },
            tags=["rex", "rea", "underperforming"],
        )
        raised.append(title)
    except Exception as exc:
        log.warning("[%s] underperforming listings check failed: %s", AGENT_NAME, exc)


async def _check_stale_listings(session: AsyncSession, raised: List) -> None:
    """Listings not refreshed in 14+ days."""
    try:
        rows = (await session.execute(text(f"""
            SELECT id, address, suburb, rea_listing_id, rea_last_edit_at, rea_views
            FROM leads
            WHERE COALESCE(rea_listing_id, '') <> ''
              AND rea_last_edit_at < {_14d_ago}
            ORDER BY rea_last_edit_at ASC
            LIMIT 10
        """))).mappings().all()

        if not rows:
            return

        title = f"Stale REA listings: {len(rows)} not refreshed in 14+ days"
        if await ticket_exists_for_pattern(session, "Stale REA listings:", TicketDept.REVENUE, hours_back=24):
            return

        listing_lines = [
            f"  - {r['address']}, {r['suburb']} — last edit: {str(r.get('rea_last_edit_at') or '')[:10]}"
            for r in rows[:6]
        ]

        await create_ticket(
            session,
            title=title,
            description=(
                f"{len(rows)} REA listings haven't been refreshed in over 2 weeks.\n\n"
                + "\n".join(listing_lines)
                + "\n\nREA ranks recently-updated listings higher. Refresh these to maintain visibility."
            ),
            department=TicketDept.REVENUE,
            kind=TicketKind.FOLLOWUP,
            priority=3,
            severity=TicketSeverity.MEDIUM,
            created_by_type="agent",
            created_by_id=AGENT_ID,
            evidence_json={"stale_count": len(rows)},
            tags=["rex", "rea", "stale_listing"],
        )
        raised.append(title)
    except Exception as exc:
        log.warning("[%s] stale listings check failed: %s", AGENT_NAME, exc)


async def _check_variant_performance(session: AsyncSession, raised: List) -> None:
    """Compare title/description variants and flag clear winners/losers."""
    try:
        rows = (await session.execute(text("""
            SELECT rea_title_variant,
                   COUNT(*) as listing_count,
                   SUM(COALESCE(rea_views, 0)) as total_views,
                   SUM(COALESCE(rea_enquiries, 0)) as total_enquiries
            FROM leads
            WHERE COALESCE(rea_listing_id, '') <> ''
              AND rea_title_variant IS NOT NULL
            GROUP BY rea_title_variant
            HAVING COUNT(*) >= 3
        """))).mappings().all()

        if len(rows) < 2:
            return

        # Calculate CTR per variant
        variants = []
        for r in rows:
            views = int(r["total_views"] or 0)
            enq = int(r["total_enquiries"] or 0)
            ctr = round(enq / max(views, 1) * 100, 2)
            variants.append({
                "variant": r["rea_title_variant"],
                "listings": r["listing_count"],
                "views": views,
                "enquiries": enq,
                "ctr": ctr,
            })

        variants.sort(key=lambda v: v["ctr"], reverse=True)
        best = variants[0]
        worst = variants[-1]

        if best["ctr"] <= worst["ctr"] or best["views"] < 10:
            return

        title = f"REA variant insight: '{best['variant']}' outperforming at {best['ctr']}% CTR"
        if await ticket_exists_for_pattern(session, "REA variant insight:", TicketDept.REVENUE, hours_back=48):
            return

        variant_lines = [
            f"  - {v['variant']}: {v['ctr']}% CTR ({v['enquiries']}/{v['views']} across {v['listings']} listings)"
            for v in variants
        ]

        await create_ticket(
            session,
            title=title,
            description=(
                "Template variant performance comparison:\n\n"
                + "\n".join(variant_lines)
                + f"\n\nConsider migrating underperformers to the '{best['variant']}' variant."
            ),
            department=TicketDept.REVENUE,
            kind=TicketKind.INSIGHT,
            priority=4,
            severity=TicketSeverity.LOW,
            created_by_type="agent",
            created_by_id=AGENT_ID,
            evidence_json={"variants": variants},
            tags=["rex", "rea", "variant_analysis"],
        )
        raised.append(title)
    except Exception as exc:
        log.warning("[%s] variant performance check failed: %s", AGENT_NAME, exc)


async def _check_listings_no_photos(session: AsyncSession, raised: List) -> None:
    """Listings pushed to REA with no photos."""
    try:
        rows = (await session.execute(text("""
            SELECT id, address, suburb, rea_listing_id
            FROM leads
            WHERE COALESCE(rea_listing_id, '') <> ''
              AND (rea_photos IS NULL OR rea_photos = '[]' OR rea_photos = '')
        """))).mappings().all()

        if not rows:
            return

        title = f"Listings without photos: {len(rows)} on REA with no images"
        if await ticket_exists_for_pattern(session, "Listings without photos:", TicketDept.REVENUE, hours_back=48):
            return

        await create_ticket(
            session,
            title=title,
            description=(
                f"{len(rows)} listings are live on REA without photos. "
                f"Listings with photos get 3-5x more engagement.\n\n"
                + "\n".join(f"  - {r['address']}, {r['suburb']}" for r in rows[:8])
            ),
            department=TicketDept.REVENUE,
            kind=TicketKind.FOLLOWUP,
            priority=2,
            severity=TicketSeverity.HIGH,
            created_by_type="agent",
            created_by_id=AGENT_ID,
            evidence_json={"no_photo_count": len(rows)},
            tags=["rex", "rea", "no_photos"],
        )
        raised.append(title)
    except Exception as exc:
        log.warning("[%s] no-photos check failed: %s", AGENT_NAME, exc)


async def _check_new_enquiries(session: AsyncSession, raised: List) -> None:
    """New REA enquiries that haven't been responded to."""
    try:
        rows = (await session.execute(text(f"""
            SELECT id, address, suburb, rea_listing_id, rea_enquiries, rea_last_enquiry_at
            FROM leads
            WHERE COALESCE(rea_listing_id, '') <> ''
              AND COALESCE(rea_enquiries, 0) > 0
              AND rea_last_enquiry_at >= {_3d_ago}
              AND (last_contacted_at IS NULL OR last_contacted_at < rea_last_enquiry_at)
            ORDER BY rea_last_enquiry_at DESC
            LIMIT 10
        """))).mappings().all()

        if not rows:
            return

        title = f"Unresponded REA enquiries: {len(rows)} leads need callback"
        if await ticket_exists_for_pattern(session, "Unresponded REA enquiries:", TicketDept.REVENUE, hours_back=12):
            return

        lead_lines = [
            f"  - {r['address']}, {r['suburb']} — {r['rea_enquiries']} enquiries"
            for r in rows[:6]
        ]

        await create_ticket(
            session,
            title=title,
            description=(
                f"{len(rows)} leads have REA enquiries that haven't been followed up.\n\n"
                + "\n".join(lead_lines)
                + "\n\nREA enquiries are high-intent. Call within 24h for best conversion."
            ),
            department=TicketDept.REVENUE,
            kind=TicketKind.FOLLOWUP,
            priority=1,
            severity=TicketSeverity.CRITICAL,
            created_by_type="agent",
            created_by_id=AGENT_ID,
            evidence_json={"enquiry_count": len(rows), "leads": [dict(r) for r in rows[:5]]},
            tags=["rex", "rea", "enquiry", "urgent"],
        )
        raised.append(title)
    except Exception as exc:
        log.warning("[%s] new enquiries check failed: %s", AGENT_NAME, exc)
