from __future__ import annotations

from typing import Any, Mapping

from fastapi import HTTPException
from sqlalchemy import String, cast, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.logic import _hydrate_lead
from core.utils import _decode_row
from models.sql_models import Lead as SQLLead
from services.daily_hit_list_service import enrich_leads_with_daily_hit_list
from services.timeline_service import get_lead_timeline

HEAVY_LEAD_FIELDS = {
    "activity_log",
    "stage_note_history",
    "description_deep",
    "exhaustive_summary",
    "summary_points",
    "key_details",
    "source_evidence",
    "source_provenance",
    "property_images",
    "sale_history",
    "listing_status_history",
    "nearby_sales",
    "seller_intent_signals",
    "refinance_signals",
    "timeline",
}


def _as_lead_dict(lead: Mapping[str, Any] | Any) -> dict[str, Any]:
    if lead is None:
        return {}
    if hasattr(lead, "model_dump"):
        return dict(lead.model_dump())
    if isinstance(lead, Mapping):
        return dict(lead)
    return dict(lead)


def build_lead_summary(lead: Mapping[str, Any] | Any) -> dict[str, Any]:
    hydrated = _hydrate_lead(_decode_row(_as_lead_dict(lead)))
    return {key: value for key, value in hydrated.items() if key not in HEAVY_LEAD_FIELDS}


def build_lead_detail(lead: Mapping[str, Any] | Any) -> dict[str, Any]:
    return _hydrate_lead(_decode_row(_as_lead_dict(lead)))


async def list_ranked_leads(
    session: AsyncSession,
    *,
    limit: int = 100,
    offset: int = 0,
    search: str | None = None,
    is_fresh: bool = False,
    signal_status: str | None = None,
    min_dom: int | None = None,
) -> dict[str, Any]:
    """Return leads ranked by call_today_score (descending)."""
    return await list_lead_summaries(
        session,
        limit=limit,
        offset=offset,
        search=search,
        is_fresh=is_fresh,
        signal_status=signal_status,
        min_dom=min_dom,
    )


async def list_lead_summaries(
    session: AsyncSession,
    *,
    limit: int = 100,
    offset: int = 0,
    search: str | None = None,
    is_fresh: bool = False,
    signal_status: str | None = None,
    min_dom: int | None = None,
) -> dict[str, Any]:
    recent_cutoff = None
    if is_fresh:
        from core.utils import now_sydney
        import datetime

        recent_cutoff = (now_sydney() - datetime.timedelta(hours=24)).isoformat()

    normalized_signal_status = str(signal_status or "").strip().upper()
    is_postgres = bool(session.bind and not str(session.bind.url).startswith("sqlite"))
    try:
        stmt = select(SQLLead)
        count_stmt = select(func.count()).select_from(SQLLead)
        if recent_cutoff:
            stmt = stmt.where(SQLLead.created_at >= recent_cutoff)
            count_stmt = count_stmt.where(SQLLead.created_at >= recent_cutoff)
        if min_dom is not None:
            stmt = stmt.where(SQLLead.days_on_market >= min_dom)
            count_stmt = count_stmt.where(SQLLead.days_on_market >= min_dom)
        if search and str(search).strip():
            normalized_search = str(search).strip().lower()
            q = f"%{normalized_search}%"
            digit_token = "".join(ch for ch in normalized_search if ch.isdigit())
            filter_clause = or_(
                func.lower(SQLLead.address).like(q),
                func.lower(SQLLead.owner_name).like(q),
                func.lower(SQLLead.suburb).like(q),
                func.lower(func.coalesce(SQLLead.canonical_address, "")).like(q),
                func.lower(func.coalesce(SQLLead.trigger_type, "")).like(q),
                func.lower(func.coalesce(SQLLead.postcode, "")).like(q),
                func.lower(cast(SQLLead.contact_phones, String)).like(q),
                func.lower(cast(SQLLead.contact_emails, String)).like(q),
                func.lower(func.coalesce(SQLLead.notes, "")).like(q),
                func.lower(func.coalesce(cast(SQLLead.source_tags, String), "")).like(q),
            )
            # On SQLite, also try digit-only match for phone/address number searches
            if not is_postgres and digit_token:
                digit_like = f"%{digit_token}%"
                filter_clause = or_(
                    filter_clause,
                    func.replace(func.replace(func.replace(
                        func.coalesce(cast(SQLLead.contact_phones, String), ""),
                        " ", ""), "(", ""), ")", "").like(digit_like),
                    func.replace(func.coalesce(SQLLead.address, ""), " ", "").like(digit_like),
                )
            if is_postgres and digit_token:
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
                filter_clause = or_(filter_clause, digits_haystack.like(digit_like))
            stmt = stmt.where(filter_clause)
            count_stmt = count_stmt.where(filter_clause)
        if normalized_signal_status:
            signal_clause = func.upper(SQLLead.signal_status) == normalized_signal_status
            stmt = stmt.where(signal_clause)
            count_stmt = count_stmt.where(signal_clause)

        total = int((await session.execute(count_stmt)).scalar_one())
        result = await session.execute(stmt.offset(offset).limit(limit))
        rows = result.scalars().all()
        summaries = [build_lead_summary(row) for row in rows]
        enriched = enrich_leads_with_daily_hit_list(summaries, limit=int(limit))
        return {"leads": enriched, "total": total}
    except Exception:
        # Schema-drift fallback for production DBs missing newer columns.
        if str(session.bind.url).startswith("sqlite"):
            col_rows = await session.execute(text("PRAGMA table_info(leads)"))
            columns = {str(row[1]).lower() for row in col_rows.fetchall()}
        else:
            col_rows = await session.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'leads'
                    """
                )
            )
            columns = {str(row[0]).lower() for row in col_rows.fetchall()}

        where_parts: list[str] = []
        params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}
        if recent_cutoff and "created_at" in columns:
            where_parts.append("created_at >= :recent_cutoff")
            params["recent_cutoff"] = recent_cutoff
        if min_dom is not None and "days_on_market" in columns:
            where_parts.append("days_on_market >= :min_dom")
            params["min_dom"] = int(min_dom)
        if search and str(search).strip():
            q = f"%{str(search).strip().lower()}%"
            search_fields = [
                c
                for c in (
                    "address",
                    "owner_name",
                    "suburb",
                    "canonical_address",
                    "trigger_type",
                    "postcode",
                    "contact_phones",
                    "contact_emails",
                    "notes",
                    "source_tags",
                )
                if c in columns
            ]
            if search_fields:
                ors = [f"LOWER(COALESCE({f}, '')) LIKE :search_q" for f in search_fields]
                where_parts.append(f"({' OR '.join(ors)})")
                params["search_q"] = q
                digit_token = "".join(ch for ch in str(search).strip().lower() if ch.isdigit())
                if digit_token and is_postgres:
                    where_parts.append(
                        "REGEXP_REPLACE("
                        + " || ' ' || ".join(
                            [f"COALESCE(CAST({f} AS TEXT), '')" for f in search_fields]
                        )
                        + ", '[^0-9]', '', 'g') LIKE :digits_q"
                    )
                    params["digits_q"] = f"%{digit_token}%"
        if normalized_signal_status and "signal_status" in columns:
            where_parts.append("UPPER(COALESCE(signal_status, '')) = :signal_status")
            params["signal_status"] = normalized_signal_status

        where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        count_sql = f"SELECT COUNT(*) AS cnt FROM leads{where_sql}"
        total = int((await session.execute(text(count_sql), params)).scalar_one())

        order_parts: list[str] = []
        if "updated_at" in columns:
            order_parts.append("updated_at DESC")
        if "created_at" in columns:
            order_parts.append("created_at DESC")
        if "id" in columns:
            order_parts.append("id ASC")
        order_sql = f" ORDER BY {', '.join(order_parts)}" if order_parts else ""

        list_sql = f"SELECT * FROM leads{where_sql}{order_sql} LIMIT :limit OFFSET :offset"
        rows = (await session.execute(text(list_sql), params)).mappings().all()
        summaries = [build_lead_summary(dict(row)) for row in rows]
        enriched = enrich_leads_with_daily_hit_list(summaries, limit=int(limit))
        return {"leads": enriched, "total": total}


async def get_lead_detail_payload(
    session: AsyncSession,
    lead_id: str,
    *,
    include_timeline: bool = False,
) -> dict[str, Any]:
    lead = await session.get(SQLLead, lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    detail = build_lead_detail(lead.model_dump())
    if include_timeline:
        detail["timeline"] = await get_lead_timeline(lead_id, session, limit=50)
    return detail
