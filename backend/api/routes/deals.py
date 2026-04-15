from typing import List, Optional
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime
from zoneinfo import ZoneInfo

from core.database import get_session
from core.security import get_api_key
from pydantic import BaseModel

router = APIRouter()

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

_ACTIVE_STATUSES = (
    "captured",
    "qualified",
    "outreach_ready",
    "contacted",
    "appt_booked",
    "mortgage_appt_booked",
)


class DealResponse(BaseModel):
    lead_id: str
    address: str
    suburb: Optional[str]
    owner_name: Optional[str]
    days_on_market: int
    price_drop_count: int
    status: str
    intent_score: int
    call_reason: str
    heat_score: int
    call_today_score: int
    domain_enriched_date: Optional[str]
    last_outcome: Optional[str]
    last_outcome_at: Optional[str]
    next_action_at: Optional[str]
    contact_phones: List[str]


def _calculate_days_on_market_from_row(row: dict) -> int:
    dom = row.get("days_on_market") or 0
    if dom and dom > 0:
        return int(dom)
    base_date_str = row.get("list_date") or row.get("created_at")
    if not base_date_str:
        return 0
    try:
        base_date = datetime.fromisoformat(str(base_date_str).replace("Z", "+00:00"))
        now = datetime.now(SYDNEY_TZ)
        if base_date.tzinfo is None:
            base_date = base_date.replace(tzinfo=SYDNEY_TZ)
        delta = now - base_date
        return max(0, delta.days)
    except Exception:
        return 0


@router.get("/api/deals/today", response_model=List[DealResponse])
async def get_deals_today(
    session: AsyncSession = Depends(get_session),
    api_key: str = Depends(get_api_key),
):
    res = await session.execute(
        text(
            """
            SELECT
                id, address, suburb, owner_name, status,
                heat_score, call_today_score,
                days_on_market, price_drop_count, relisted,
                list_date, created_at,
                domain_enriched_date, last_outcome, last_outcome_at,
                next_action_at, contact_phones
            FROM leads
            WHERE status NOT IN ('converted', 'dropped')
            ORDER BY call_today_score DESC, heat_score DESC
            LIMIT 30
            """
        )
    )
    rows = [dict(r) for r in res.mappings().all()]

    import json

    deals = []
    for row in rows:
        dom = _calculate_days_on_market_from_row(row)
        pdc = int(row.get("price_drop_count") or 0)
        relisted = bool(row.get("relisted"))
        normalized_status = (row.get("status") or "").lower()

        # Intent scoring
        score = 0
        if dom >= 60:
            score += 3
        if dom >= 75:
            score += 2
        if dom >= 90:
            score += 2
        if pdc >= 1:
            score += 2
        if pdc >= 2:
            score += 2
        if relisted:
            score += 3
        if normalized_status == "withdrawn":
            score += 5
        if normalized_status == "expired":
            score += 5

        # Call reason
        if dom >= 70 and pdc >= 1:
            call_reason = "On market for extended period with price drops — seller likely struggling."
        elif normalized_status == "withdrawn":
            call_reason = "Listing withdrawn — seller may still want to sell but lost confidence."
        elif relisted:
            call_reason = "Relisted property — previous campaign likely failed."
        elif dom >= 90:
            call_reason = "Very high days on market — strong motivation signal."
        elif dom >= 60:
            call_reason = "Stale listing with strong motivation signal."
        else:
            call_reason = "Active lead — review for outreach."

        # Parse contact_phones
        raw_phones = row.get("contact_phones") or "[]"
        if isinstance(raw_phones, list):
            phones = raw_phones
        else:
            try:
                phones = json.loads(raw_phones)
            except Exception:
                phones = []

        deals.append(
            DealResponse(
                lead_id=str(row.get("id") or ""),
                address=str(row.get("address") or ""),
                suburb=row.get("suburb"),
                owner_name=row.get("owner_name"),
                days_on_market=dom,
                price_drop_count=pdc,
                status=normalized_status,
                intent_score=score,
                call_reason=call_reason,
                heat_score=int(row.get("heat_score") or 0),
                call_today_score=int(row.get("call_today_score") or 0),
                domain_enriched_date=row.get("domain_enriched_date"),
                last_outcome=row.get("last_outcome") or None,
                last_outcome_at=row.get("last_outcome_at"),
                next_action_at=row.get("next_action_at"),
                contact_phones=phones if isinstance(phones, list) else [],
            )
        )

    deals.sort(key=lambda x: (-x.call_today_score, -x.heat_score, -x.intent_score))
    return deals
