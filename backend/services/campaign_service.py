"""
Campaign Service — manages campaign lifecycle and execution.

Campaigns are continuous execution loops, not static configs.
They target leads by criteria, run actions (SMS/email/call cadence),
and track outcomes.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def ensure_campaigns_table(session: AsyncSession) -> None:
    """Create campaigns and campaign_leads tables if not exists."""
    dialect = session.get_bind().dialect.name if session.get_bind() is not None else ""
    id_column = "INTEGER PRIMARY KEY AUTOINCREMENT" if dialect == "sqlite" else "BIGSERIAL PRIMARY KEY"
    await session.execute(text("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'draft',
            target_signal_status TEXT,
            target_route_queue TEXT,
            target_max_dom INTEGER,
            target_min_heat_score INTEGER DEFAULT 0,
            action_type TEXT NOT NULL DEFAULT 'call_cadence',
            sequence_json TEXT,
            leads_count INTEGER DEFAULT 0,
            enrolled_leads INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            opened_count INTEGER DEFAULT 0,
            replied_count INTEGER DEFAULT 0,
            converted_count INTEGER DEFAULT 0,
            created_at TEXT,
            activated_at TEXT,
            completed_at TEXT
        )
    """))
    await session.execute(text(f"""
        CREATE TABLE IF NOT EXISTS campaign_leads (
            id {id_column},
            campaign_id TEXT NOT NULL,
            lead_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'enrolled',
            enrolled_at TEXT,
            last_action_at TEXT,
            outcome TEXT,
            UNIQUE(campaign_id, lead_id)
        )
    """))
    await session.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_campaign_leads ON campaign_leads(campaign_id, status)"
    ))
    await session.commit()


async def list_campaigns(session: AsyncSession) -> List[Dict[str, Any]]:
    """Return all campaigns with real-time lead counts."""
    try:
        rows = (await session.execute(text("""
            SELECT c.*,
                COUNT(cl.id) as enrolled_leads
            FROM campaigns c
            LEFT JOIN campaign_leads cl ON cl.campaign_id = c.id
            GROUP BY c.id
            ORDER BY c.created_at DESC
        """))).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        # Table may not exist yet
        return []


async def create_campaign(session: AsyncSession, data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new campaign in DRAFT state."""
    import hashlib
    from core.utils import now_iso
    campaign_id = hashlib.md5(f"{data.get('name', '')}:{now_iso()}".encode()).hexdigest()[:12]
    now = datetime.now(timezone.utc).isoformat()

    await session.execute(text("""
        INSERT INTO campaigns
            (id, name, description, status, target_signal_status, target_route_queue,
             target_max_dom, target_min_heat_score, action_type, sequence_json, created_at)
        VALUES
            (:id, :name, :description, 'draft', :target_signal_status, :target_route_queue,
             :target_max_dom, :target_min_heat_score, :action_type, :sequence_json, :created_at)
    """), {
        "id": campaign_id,
        "name": data.get("name", "Untitled Campaign"),
        "description": data.get("description", ""),
        "target_signal_status": data.get("target_signal_status"),
        "target_route_queue": data.get("target_route_queue"),
        "target_max_dom": data.get("target_max_dom"),
        "target_min_heat_score": data.get("target_min_heat_score", 0),
        "action_type": data.get("action_type", "call_cadence"),
        "sequence_json": data.get("sequence_json"),
        "created_at": now,
    })
    await session.commit()
    return {"id": campaign_id, **data, "status": "draft", "created_at": now}


async def activate_campaign(session: AsyncSession, campaign_id: str) -> Dict[str, Any]:
    """
    Activate a campaign: find matching leads and enroll them.
    Returns enrollment summary.
    """
    # Get campaign
    row = (await session.execute(
        text("SELECT * FROM campaigns WHERE id = :id"),
        {"id": campaign_id}
    )).mappings().first()

    if not row:
        return {"error": "Campaign not found"}

    campaign = dict(row)

    # Build lead query from campaign targeting criteria
    conditions = ["status NOT IN ('converted', 'dropped')"]
    params: Dict[str, Any] = {}

    if campaign.get("target_signal_status"):
        conditions.append("signal_status = :signal_status")
        params["signal_status"] = campaign["target_signal_status"]

    if campaign.get("target_route_queue"):
        conditions.append("route_queue = :route_queue")
        params["route_queue"] = campaign["target_route_queue"]

    if campaign.get("target_max_dom"):
        conditions.append("(days_on_market IS NULL OR days_on_market <= :max_dom)")
        params["max_dom"] = campaign["target_max_dom"]

    if campaign.get("target_min_heat_score"):
        conditions.append("heat_score >= :min_heat")
        params["min_heat"] = campaign["target_min_heat_score"]

    where = " AND ".join(conditions)
    lead_rows = (await session.execute(
        text(f"SELECT id FROM leads WHERE {where} LIMIT 500"),
        params
    )).mappings().all()

    lead_ids = [r["id"] for r in lead_rows]
    now = datetime.now(timezone.utc).isoformat()

    dialect = session.get_bind().dialect.name if session.get_bind() is not None else ""
    enroll_sql = (
        """
        INSERT OR IGNORE INTO campaign_leads (campaign_id, lead_id, status, enrolled_at)
        VALUES (:campaign_id, :lead_id, 'enrolled', :now)
        """
        if dialect == "sqlite"
        else """
        INSERT INTO campaign_leads (campaign_id, lead_id, status, enrolled_at)
        VALUES (:campaign_id, :lead_id, 'enrolled', :now)
        ON CONFLICT (campaign_id, lead_id) DO NOTHING
        """
    )

    # Enroll leads
    enrolled = 0
    for lid in lead_ids:
        try:
            await session.execute(text(enroll_sql), {"campaign_id": campaign_id, "lead_id": lid, "now": now})
            enrolled += 1
        except Exception:
            pass

    # Update campaign status
    await session.execute(text("""
        UPDATE campaigns SET status = 'active', activated_at = :now, leads_count = :count
        WHERE id = :id
    """), {"now": now, "count": enrolled, "id": campaign_id})
    await session.commit()

    logger.info("[Campaign] Activated '%s' -> %d leads enrolled", campaign["name"], enrolled)
    return {"campaign_id": campaign_id, "enrolled": enrolled, "status": "active"}


async def pause_campaign(session: AsyncSession, campaign_id: str) -> Dict[str, Any]:
    await session.execute(
        text("UPDATE campaigns SET status = 'paused' WHERE id = :id"),
        {"id": campaign_id}
    )
    await session.commit()
    return {"status": "paused"}


async def get_campaign_leads(session: AsyncSession, campaign_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Get leads enrolled in a campaign with their lead data."""
    rows = (await session.execute(text("""
        SELECT cl.status as campaign_status, cl.enrolled_at, cl.outcome,
               l.id, l.address, l.suburb, l.owner_name, l.signal_status,
               l.heat_score, l.contact_phones, l.last_contacted_at
        FROM campaign_leads cl
        JOIN leads l ON l.id = cl.lead_id
        WHERE cl.campaign_id = :campaign_id
        ORDER BY l.heat_score DESC
        LIMIT :limit
    """), {"campaign_id": campaign_id, "limit": limit})).mappings().all()
    return [dict(r) for r in rows]
