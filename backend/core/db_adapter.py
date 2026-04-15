"""
Unified DB Adapter.
Centralises all SQLite / PostgreSQL branching into one place.
Route handlers call db_adapter.get_leads() without knowing which backend is active.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from sqlalchemy import String, cast, text
from sqlalchemy.ext.asyncio import AsyncSession
from core.config import USE_POSTGRES

class DBAdapter:
    """
    Thin abstraction over PostgreSQL. SQLite legacy methods are removed.
    """

    async def get_leads(
        self,
        limit: int = 2000,
        offset: int = 0,
        search: Optional[str] = None,
        db: Optional[AsyncSession] = None,
    ) -> List[Dict[str, Any]]:
        return await self._pg_get_leads(limit, offset, search, db)

    async def get_lead(self, lead_id: str, db: Optional[AsyncSession] = None) -> Optional[Dict[str, Any]]:
        from models.sql_models import Lead as SQLLead
        lead = await db.get(SQLLead, lead_id)
        return lead.model_dump() if lead else None

    async def upsert_lead(self, data: Dict[str, Any], db: Optional[AsyncSession] = None) -> None:
        from models.sql_models import Lead as SQLLead
        lead = SQLLead(**data)
        await db.merge(lead)
        await db.commit()

    async def get_today_queue(self, db: Optional[AsyncSession] = None) -> Dict[str, Any]:
        from core.logic import _hydrate_lead
        res = await db.execute(
            text("""
            SELECT * FROM leads
            WHERE COALESCE(queue_bucket, '') NOT IN ('suppressed', 'background')
                AND COALESCE(status, '') NOT IN ('converted', 'dropped')
            ORDER BY COALESCE(call_today_score, 0) DESC
            LIMIT 50
            """)
        )
        rows = res.mappings().all()
        leads = [_hydrate_lead(row) for row in rows]
        return {"leads": leads, "total": len(leads)}

    async def _pg_get_leads(
        self, limit: int, offset: int, search: Optional[str], db: AsyncSession
    ) -> List[Dict[str, Any]]:
        from sqlalchemy import case, desc, or_, func
        from sqlmodel import select
        from models.sql_models import Lead as SQLLead

        queue_order = case(
            (SQLLead.queue_bucket == "active", 0),
            (SQLLead.queue_bucket == "callback_due", 1),
            (SQLLead.queue_bucket == "booked", 2),
            (SQLLead.queue_bucket == "nurture", 3),
            (SQLLead.queue_bucket == "enrichment", 4),
            else_=9,
        )
        query = select(SQLLead)
        if search and search.strip():
            normalized = search.strip().lower()
            s = f"%{normalized}%"
            digit_token = "".join(ch for ch in normalized if ch.isdigit())
            search_clause = or_(
                SQLLead.address.ilike(s),
                SQLLead.owner_name.ilike(s),
                SQLLead.suburb.ilike(s),
                SQLLead.canonical_address.ilike(s),
                SQLLead.trigger_type.ilike(s),
                SQLLead.postcode.ilike(s),
                cast(SQLLead.contact_phones, String).ilike(s),
                cast(SQLLead.contact_emails, String).ilike(s),
            )
            if digit_token:
                digit_like = f"%{digit_token}%"
                digits_haystack = func.regexp_replace(
                    func.coalesce(SQLLead.address, "")
                    + " "
                    + func.coalesce(SQLLead.canonical_address, "")
                    + " "
                    + func.coalesce(SQLLead.postcode, "")
                    + " "
                    + func.coalesce(cast(SQLLead.contact_phones, String), "")
                    + " "
                    + func.coalesce(cast(SQLLead.contact_emails, String), ""),
                    r"[^0-9]",
                    "",
                    "g",
                )
                search_clause = or_(search_clause, digits_haystack.like(digit_like))
            query = query.where(search_clause)
        query = query.order_by(
            queue_order.asc(),
            case((SQLLead.next_action_at == None, 1), else_=0).asc(),
            SQLLead.next_action_at.asc(),
            desc(SQLLead.call_today_score),
        ).offset(offset).limit(limit)
        result = await db.execute(query)
        return [lead.model_dump() for lead in result.scalars().all()]


# Module-level singleton
db_adapter = DBAdapter()
