from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import SYDNEY_TZ
from services.metrics_service import get_daily_metrics
from services.missed_deals_service import get_missed_deals
from services.signal_engine import compute_live_signals

BRAND_NAME = "REACTOR"


def _now_sydney() -> datetime:
    return datetime.now(SYDNEY_TZ).replace(microsecond=0)


def _pct(value: Any) -> int:
    try:
        return int(round(float(value or 0.0) * 100))
    except (TypeError, ValueError):
        return 0


def _minutes(value: Any) -> int:
    try:
        return int(round(float(value or 0.0) / 60.0))
    except (TypeError, ValueError):
        return 0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _headline(signal: Dict[str, Any]) -> str:
    return str(signal.get("headline") or signal.get("signal_label") or signal.get("type") or "Signal")


def _detail(signal: Dict[str, Any]) -> str:
    return str(signal.get("detail") or signal.get("reason_detail") or signal.get("source") or "")


def _zone_label(signal: Dict[str, Any]) -> str:
    return str(signal.get("suburb") or signal.get("address") or "the patch")


async def build_growth_digest(session: AsyncSession) -> Dict[str, Any]:
    now = _now_sydney()
    metrics = await get_daily_metrics(
        session,
        date=now.date().isoformat(),
        lead_id=None,
        user_id=None,
        outcome=None,
    )
    live_signals = await compute_live_signals(session, limit=18)
    missed_deals = await get_missed_deals(session, date_range="today")

    urgent_signals = [signal for signal in live_signals if signal.get("urgency_band") == "NOW"]
    price_drop_signals = [signal for signal in live_signals if signal.get("type") == "PRICE_DROP"]
    news_signals = [signal for signal in live_signals if signal.get("type") == "NEWS_DISTRESS"]
    zone_signals = [signal for signal in live_signals if signal.get("type") == "HIGH_ACTIVITY_ZONE"]

    top_signal = live_signals[0] if live_signals else {}
    top_missed = missed_deals[0] if missed_deals else {}
    top_zone = zone_signals[0] if zone_signals else top_signal
    talk_time_minutes = _minutes(metrics.get("total_talk_time_seconds"))
    connect_rate_pct = _pct(metrics.get("connect_rate"))
    conversation_rate_pct = _pct(metrics.get("conversation_rate"))
    conversion_rate_pct = _pct(metrics.get("conversion_rate"))
    booked_count = _safe_int(metrics.get("appointments_booked_count"))

    x_posts: List[Dict[str, Any]] = [
        {
            "id": "x-calling-report",
            "type": "CALLING_REPORT",
            "title": "Today's calling report",
            "text": (
                f"{BRAND_NAME} daily report: {len(live_signals)} live signals, {len(urgent_signals)} NOW-priority, "
                f"{talk_time_minutes}m talk time, {connect_rate_pct}% connect rate. "
                f"Top trigger: {_headline(top_signal)} in {_zone_label(top_signal)}."
            ).strip(),
        },
        {
            "id": "x-missed-recovery",
            "type": "MISSED_DEALS",
            "title": "Top missed opportunities",
            "text": (
                f"{len(missed_deals)} recoverable missed deals are live today. "
                f"Highest-value recovery: {top_missed.get('lead_name') or 'none'} | "
                f"{top_missed.get('reason_label') or 'no priority'} | "
                f"{top_missed.get('suggested_action') or 'review queue'}."
            ).strip(),
        },
        {
            "id": "x-operator-discipline",
            "type": "COACHING",
            "title": "What agents are doing wrong",
            "text": (
                f"Low talk time hides in plain sight. Today: {talk_time_minutes}m talk time, "
                f"{conversation_rate_pct}% conversation rate, {booked_count} bookings. "
                f"Missed follow-up count: {sum(1 for card in missed_deals if card.get('reason') == 'NO_FOLLOW_UP')}."
            ).strip(),
        },
    ]

    reports: List[Dict[str, Any]] = [
        {
            "id": "report-calling",
            "title": "Today's calling report",
            "headline": _headline(top_signal),
            "summary": (
                f"{len(live_signals)} ranked signals are active. "
                f"{len(urgent_signals)} should be worked now. "
                f"Primary patch: {_zone_label(top_zone)}."
            ),
            "cta": top_signal.get("suggested_action") or "Work the top signal stack",
        },
        {
            "id": "report-missed",
            "title": "Top missed opportunities",
            "headline": top_missed.get("lead_name") or "Queue under control",
            "summary": (
                f"{len(missed_deals)} recoverable deals are still open. "
                f"{sum(1 for card in missed_deals if card.get('urgency_band') == 'NOW')} require immediate action."
            ),
            "cta": top_missed.get("suggested_action") or "Review missed-deal queue",
        },
        {
            "id": "report-wrong",
            "title": "What agents are doing wrong",
            "headline": f"{connect_rate_pct}% connect rate | {conversation_rate_pct}% conversation rate",
            "summary": (
                f"Operator drag is deterministic: "
                f"{sum(1 for card in missed_deals if card.get('reason') == 'NO_FOLLOW_UP')} missing follow-ups, "
                f"{sum(1 for card in missed_deals if card.get('reason') == 'MISSED_BOOKING')} missed booking asks, "
                f"{sum(1 for card in missed_deals if card.get('reason') == 'STALE_HIGH_INTENT')} stale hot leads."
            ),
            "cta": "Correct follow-up discipline before the next session",
        },
    ]

    share_cards: List[Dict[str, Any]] = [
        {
            "id": "card-signal-stack",
            "title": "Signal stack",
            "headline": _headline(top_signal),
            "subheadline": _detail(top_signal),
            "metric_label": "Urgent signals",
            "metric_value": str(len(urgent_signals)),
        },
        {
            "id": "card-price-drop",
            "title": "Price-drop pressure",
            "headline": f"{len(price_drop_signals)} price-drop reachouts open",
            "subheadline": _headline(price_drop_signals[0]) if price_drop_signals else "No price-drop trigger live",
            "metric_label": "Price-drop signals",
            "metric_value": str(len(price_drop_signals)),
        },
        {
            "id": "card-market-pressure",
            "title": "Market pressure",
            "headline": f"{len(news_signals)} news-linked market signals",
            "subheadline": _headline(news_signals[0]) if news_signals else "No local news pressure signal live",
            "metric_label": "Bookings",
            "metric_value": str(booked_count),
        },
    ]

    return {
        "brand": BRAND_NAME,
        "generated_at": now.isoformat(),
        "summary": {
            "live_signal_count": len(live_signals),
            "urgent_signal_count": len(urgent_signals),
            "missed_deal_count": len(missed_deals),
            "talk_time_minutes": talk_time_minutes,
            "connect_rate_pct": connect_rate_pct,
            "conversion_rate_pct": conversion_rate_pct,
        },
        "x_posts": x_posts,
        "reports": reports,
        "share_cards": share_cards,
    }
