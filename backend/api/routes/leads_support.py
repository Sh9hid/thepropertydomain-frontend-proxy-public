"""
Support models and helpers for lead routes.
"""

import hashlib
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from services.daily_hit_list_service import enrich_leads_with_daily_hit_list
from services.lead_intelligence_service import (
    attach_intelligence_to_leads,
    fetch_intelligence_by_property_ids,
)


async def attach_deterministic_intelligence(
    session: AsyncSession,
    leads_payload: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    property_ids = [
        hashlib.md5(
            str((lead.get("canonical_address") or lead.get("address") or ""))
            .strip()
            .upper()
            .encode("utf-8")
        ).hexdigest()
        for lead in leads_payload
        if (lead.get("canonical_address") or lead.get("address"))
    ]
    intelligence_map = await fetch_intelligence_by_property_ids(session, property_ids)
    return attach_intelligence_to_leads(leads_payload, intelligence_map)


def rank_leads_for_hit_list(leads_payload: List[Dict[str, Any]], limit: int = 200) -> List[Dict[str, Any]]:
    ranked = enrich_leads_with_daily_hit_list(leads_payload, limit=limit)
    ranked.sort(
        key=lambda lead: (
            -(float(lead.get("priority_rank") or 0)),
            0 if lead.get("daily_hit_list_rank") else 1,
            int(lead.get("daily_hit_list_rank") or 999999),
            -int(lead.get("call_today_score") or 0),
            -int(lead.get("heat_score") or 0),
            str(lead.get("address") or ""),
            str(lead.get("id") or ""),
        )
    )
    return ranked


class LeadPatchPayload(BaseModel):
    owner_name: Optional[str] = None
    contact_phones: Optional[list] = None
    contact_emails: Optional[list] = None
    status: Optional[str] = None
    notes: Optional[str] = None
    estimated_completion: Optional[str] = None


class LeadFollowupPayload(BaseModel):
    preferred_contact_method: Optional[str] = None
    followup_frequency: Optional[str] = None
    market_updates_opt_in: Optional[bool] = None
    next_followup_at: Optional[str] = None
    followup_status: Optional[str] = None
    followup_notes: Optional[str] = None


class BulkLeadUpdate(BaseModel):
    lead_ids: List[str]
    update: Dict[str, Any]


class DirectSendEmailRequest(BaseModel):
    recipient: str
    subject: str = ""
    body: str


class DirectSendSMSRequest(BaseModel):
    recipient: str
    message: str


class LogCallRequest(BaseModel):
    outcome: str
    note: str = ""
    duration_seconds: int = 0
    user_id: Optional[str] = None
    next_action_due: Optional[str] = None
    recording_url: Optional[str] = None


def delta_values(
    before: List[str],
    after: List[str],
    normalizer: Callable[[Any], str],
) -> tuple[List[str], List[str]]:
    before_keys = {normalizer(value): value for value in before if normalizer(value)}
    after_keys = {normalizer(value): value for value in after if normalizer(value)}
    added = [after_keys[key] for key in after_keys.keys() - before_keys.keys()]
    removed = [before_keys[key] for key in before_keys.keys() - after_keys.keys()]
    return added, removed
