"""Opportunity routes backed by the deterministic missed-deals revenue engine."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.config import SYDNEY_TZ
from core.database import get_session
from models.opportunity_models import OpportunityAction
from services.missed_deals_service import get_missed_deals, summarize_missed_deals

router = APIRouter(prefix="/opportunities", tags=["opportunities"])

_OPENER_BY_REASON = {
    "MISSED_BOOKING": "You showed real intent on the last call. Let's lock the next step in now.",
    "NO_FOLLOW_UP": "We already discussed the next step. I want to get that scheduled properly today.",
    "STALE_HIGH_INTENT": "You were engaged recently and I do not want that momentum to go cold.",
    "PRICE_DROP_OPPORTUNITY": "The price has moved and this is exactly when serious sellers re-open the conversation.",
}

_LEGACY_DETECTOR_MAP = {
    "WARM_THEN_ABANDONED": "STALE_HIGH_INTENT",
    "OVERDUE_CALLBACK": "NO_FOLLOW_UP",
    "LONG_TALK_NO_BOOKING": "MISSED_BOOKING",
    "STALE_HOT_LEAD": "STALE_HIGH_INTENT",
    "MARKET_SIGNAL_NO_ACTION": "PRICE_DROP_OPPORTUNITY",
}


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SYDNEY_TZ)
    return parsed.astimezone(SYDNEY_TZ)


def _urgency_level(card: Dict[str, object]) -> str:
    return {
        "NOW": "critical",
        "TODAY": "high",
        "THIS_WEEK": "medium",
    }.get(str(card.get("urgency_band") or ""), "medium")


def _card_dict(card: Dict[str, object]) -> dict:
    hours_since_contact = float(card.get("hours_since_contact") or 0.0)
    stale_days = int(hours_since_contact // 24) if hours_since_contact else 0
    objection_tags = list(card.get("last_objection_tags") or [])
    urgency_level = _urgency_level(card)
    return {
        "lead_id": card["lead_id"],
        "lead_name": card["lead_name"],
        "address": card["address"],
        "suburb": card.get("suburb") or "",
        "postcode": card.get("postcode") or "",
        "status": card.get("status") or "",
        "reason": card.get("reason") or "",
        "urgency_level": urgency_level,
        "detector_reasons": [card.get("reason_detail") or ""],
        "confidence_basis": (
            f"priority={float(card.get('priority_score') or 0.0):.1f};"
            f"urgency={card.get('urgency_band')};"
            f"recovery={card.get('estimated_recovery_class')}"
        ),
        "days_since_last_contact": stale_days or None,
        "talk_time_total": int(card.get("talk_time_total") or 0),
        "connected_calls": int(card.get("connected_calls") or 0),
        "total_attempts": int(card.get("total_attempts") or 0),
        "next_action_due": card.get("follow_up_due_at"),
        "missed_value_reason": card.get("revenue_signal") or "",
        "recommended_action_type": card.get("action_type") or "",
        "recommended_action_text": card.get("suggested_action") or "",
        "recommended_contact_window": card.get("urgency_band") or "",
        "evidence_summary": f"{card.get('reason_label')}: {card.get('reason_detail')}",
        "call_brief": f"{card.get('revenue_signal')} {card.get('ignore_risk')}".strip(),
        "suggested_opener": _OPENER_BY_REASON.get(str(card.get("reason") or ""), "Calling because this lead needs immediate recovery action."),
        "last_call_summary": card.get("last_call_summary") or None,
        "objection_summary": ", ".join(objection_tags) if objection_tags else None,
        "stale_days": stale_days or None,
        "overdue_days": stale_days or None,
        "heat_score": int(card.get("heat_score") or 0),
        "evidence_score": int(card.get("evidence_score") or 0),
        "est_value": int(card.get("est_value") or 0) or None,
        "score": int(round(float(card.get("priority_score") or 0.0))),
    }


def _apply_filters(cards: List[dict], urgency: Optional[str], detector: Optional[str], suburb: Optional[str]) -> List[dict]:
    filtered = cards
    if urgency:
        filtered = [card for card in filtered if card["urgency_level"] == urgency]
    if detector:
        detector_token = _LEGACY_DETECTOR_MAP.get(detector.strip().upper(), detector.strip().upper())
        filtered = [
            card for card in filtered
            if detector_token == str(card.get("reason") or "").upper()
        ]
    if suburb:
        suburb_token = suburb.strip().lower()
        filtered = [card for card in filtered if suburb_token in str(card.get("suburb") or "").lower()]
    return filtered


def _sort_cards(cards: List[dict], sort_by: str) -> List[dict]:
    key_map = {
        "score": lambda item: item["score"],
        "overdue": lambda item: item.get("overdue_days") or 0,
        "talk_time": lambda item: item.get("talk_time_total") or 0,
        "heat": lambda item: item.get("heat_score") or 0,
    }
    selected = key_map.get(sort_by, key_map["score"])
    return sorted(cards, key=selected, reverse=True)


async def _load_action_state(session: AsyncSession) -> Dict[str, OpportunityAction]:
    rows = (
        await session.execute(
            select(OpportunityAction).order_by(OpportunityAction.created_at.desc())
        )
    ).scalars().all()
    latest_by_lead: Dict[str, OpportunityAction] = {}
    for row in rows:
        if row.lead_id not in latest_by_lead:
            latest_by_lead[row.lead_id] = row
    return latest_by_lead


def _action_hides_card(action: OpportunityAction, now: datetime) -> bool:
    if action.action in {"dismiss", "complete"}:
        return True
    if action.action == "snooze":
        expires_at = _parse_iso(action.expires_at)
        return expires_at is None or expires_at > now
    return False


@router.get("/missed-deals")
async def list_missed_deals(
    urgency: Optional[str] = Query(default=None, description="critical|high|medium"),
    detector: Optional[str] = Query(default=None, description="reason / action token"),
    suburb: Optional[str] = Query(default=None),
    sort_by: str = Query(default="score", description="score|overdue|talk_time|heat"),
    limit: int = Query(default=50, le=200),
    session: AsyncSession = Depends(get_session),
):
    cards = await get_missed_deals(session, date_range="today")
    action_state = await _load_action_state(session)
    now = datetime.now(SYDNEY_TZ)
    adapted = [
        _card_dict(card)
        for card in cards
        if not (
            action_state.get(str(card["lead_id"]))
            and _action_hides_card(action_state[str(card["lead_id"])], now)
        )
    ]
    adapted = _apply_filters(adapted, urgency=urgency, detector=detector, suburb=suburb)
    return _sort_cards(adapted, sort_by=sort_by)[:limit]


@router.get("/missed-deals/summary")
async def missed_deals_summary(session: AsyncSession = Depends(get_session)):
    cards = await get_missed_deals(session, date_range="today")
    summary = summarize_missed_deals(cards)
    return {
        "total_opportunities": summary["total_missed_deals"],
        "critical_count": summary["high_urgency_count"],
        "high_count": sum(1 for card in cards if card.get("urgency_band") == "TODAY"),
        "overdue_callbacks": summary["no_follow_up_count"],
        "warm_gone_cold": summary["stale_high_intent_count"],
        "high_value_neglected": sum(1 for card in cards if card.get("estimated_recovery_class") == "HIGH"),
        "by_detector": {
            "MISSED_BOOKING": sum(1 for card in cards if card.get("reason") == "MISSED_BOOKING"),
            "NO_FOLLOW_UP": summary["no_follow_up_count"],
            "STALE_HIGH_INTENT": summary["stale_high_intent_count"],
            "PRICE_DROP_OPPORTUNITY": summary["price_drop_opportunity_count"],
        },
    }


class DismissBody(BaseModel):
    note: Optional[str] = None


class SnoozeBody(BaseModel):
    hours: int = 24
    note: Optional[str] = None


class CompleteBody(BaseModel):
    note: Optional[str] = None


@router.post("/{lead_id}/dismiss")
async def dismiss_opportunity(
    lead_id: str,
    body: DismissBody,
    session: AsyncSession = Depends(get_session),
):
    existing = (await session.execute(
        select(OpportunityAction).where(OpportunityAction.lead_id == lead_id)
    )).scalars().all()
    for row in existing:
        await session.delete(row)

    action = OpportunityAction(
        lead_id=lead_id,
        action="dismiss",
        note=body.note,
    )
    session.add(action)
    await session.commit()
    return {"status": "dismissed", "lead_id": lead_id}


@router.post("/{lead_id}/snooze")
async def snooze_opportunity(
    lead_id: str,
    body: SnoozeBody,
    session: AsyncSession = Depends(get_session),
):
    expires = (datetime.now(SYDNEY_TZ) + timedelta(hours=body.hours)).isoformat()

    existing = (await session.execute(
        select(OpportunityAction).where(OpportunityAction.lead_id == lead_id)
    )).scalars().all()
    for row in existing:
        await session.delete(row)

    action = OpportunityAction(
        lead_id=lead_id,
        action="snooze",
        expires_at=expires,
        note=body.note,
    )
    session.add(action)
    await session.commit()
    return {"status": "snoozed", "lead_id": lead_id, "expires_at": expires, "hours": body.hours}


@router.post("/{lead_id}/complete-action")
async def complete_opportunity_action(
    lead_id: str,
    body: CompleteBody,
    session: AsyncSession = Depends(get_session),
):
    existing = (await session.execute(
        select(OpportunityAction).where(OpportunityAction.lead_id == lead_id)
    )).scalars().all()
    for row in existing:
        await session.delete(row)

    action = OpportunityAction(
        lead_id=lead_id,
        action="complete",
        note=body.note,
    )
    session.add(action)
    await session.commit()
    return {"status": "completed", "lead_id": lead_id}


@router.delete("/{lead_id}/clear")
async def clear_opportunity_action(
    lead_id: str,
    session: AsyncSession = Depends(get_session),
):
    existing = (await session.execute(
        select(OpportunityAction).where(OpportunityAction.lead_id == lead_id)
    )).scalars().all()
    for row in existing:
        await session.delete(row)
    await session.commit()
    return {"status": "cleared", "lead_id": lead_id}
