"""Deterministic live signal engine for the sales terminal."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from zoneinfo import ZoneInfo

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
CACHE_TTL_SECONDS = 45


@dataclass
class _CacheEntry:
    expires_at: float
    payload: List[Dict[str, Any]]


_CACHE: Dict[str, _CacheEntry] = {}
_CACHE_LOCK = asyncio.Lock()


def _now_sydney() -> datetime:
    return datetime.now(SYDNEY_TZ).replace(microsecond=0)


def _bind_namespace(session: AsyncSession) -> str:
    bind = getattr(session, "bind", None)
    url = getattr(bind, "url", None)
    return str(url or "default")


async def _get_cached(session: AsyncSession, key: str) -> Optional[List[Dict[str, Any]]]:
    cache_key = f"{_bind_namespace(session)}:{key}"
    async with _CACHE_LOCK:
        entry = _CACHE.get(cache_key)
        if not entry or entry.expires_at <= time.time():
            _CACHE.pop(cache_key, None)
            return None
        return [dict(item) for item in entry.payload]


async def _set_cached(session: AsyncSession, key: str, payload: List[Dict[str, Any]]) -> None:
    cache_key = f"{_bind_namespace(session)}:{key}"
    async with _CACHE_LOCK:
        _CACHE[cache_key] = _CacheEntry(
            expires_at=time.time() + CACHE_TTL_SECONDS,
            payload=[dict(item) for item in payload],
        )


def _parse_iso(value: Any) -> Optional[datetime]:
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


def _normalize_suburb(value: Any) -> str:
    return str(value or "").strip().lower()


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _safe_json_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item or "").strip()]


def _hours_since(value: Any, now: datetime) -> Optional[int]:
    parsed = _parse_iso(value)
    if not parsed:
        return None
    return max(0, int(round((now - parsed).total_seconds() / 3600)))


def _urgency_band(score: float) -> str:
    if score >= 80:
        return "NOW"
    if score >= 65:
        return "TODAY"
    return "THIS_WEEK"


def _urgency_level(score: float) -> str:
    if score >= 80:
        return "critical"
    if score >= 65:
        return "high"
    return "medium"


def _signal_color(score: float) -> str:
    if score >= 80:
        return "#ff453a"
    if score >= 65:
        return "#ff9f0a"
    return "#30d158"


def _headline(type_: str, context: Dict[str, Any]) -> str:
    if type_ == "STALE_LISTING":
        return f"Listing stale {context['days_on_market']} days"
    if type_ == "PRICE_DROP":
        return "Recent price drop"
    if type_ == "NEARBY_SOLD":
        return "Nearby listing sold"
    if type_ == "HIGH_ACTIVITY_ZONE":
        return "High activity zone"
    if type_ == "OWNER_LIKELY_TO_CHURN":
        return "Owner likely to churn"
    if type_ == "NEWS_DISTRESS":
        return context.get("title") or "Market pressure signal"
    return type_.replace("_", " ").title()


def _detail(type_: str, context: Dict[str, Any]) -> str:
    address = context.get("address") or ""
    suburb = context.get("suburb") or ""
    if type_ == "STALE_LISTING":
        return f"{address} | {suburb} | {context['days_on_market']} DOM | {context['price_drop_count']} price drops"
    if type_ == "PRICE_DROP":
        return f"{address} | {suburb} | {context['price_drop_count']} price drops | last contact {context['hours_since_contact'] or 'n/a'}h ago"
    if type_ == "NEARBY_SOLD":
        return f"{address} | {suburb} | {context['recent_sales_count']} suburb sales in 30d"
    if type_ == "HIGH_ACTIVITY_ZONE":
        return f"{suburb} | {context['zone_activity_count']} stacked signals across live, delta, withdrawn, sold, and news"
    if type_ == "OWNER_LIKELY_TO_CHURN":
        return f"{address} | {suburb} | id4me enriched | stale follow-through | switch-agent risk"
    if type_ == "NEWS_DISTRESS":
        return f"{address or suburb} | {context.get('source_name') or 'news'} | {context.get('description') or ''}".strip(" |")
    return address or suburb


def _suggested_action(type_: str) -> str:
    return {
        "STALE_LISTING": "Call stale listing before the next markdown",
        "PRICE_DROP": "Call on revised pricing signal",
        "NEARBY_SOLD": "Use nearby sale as re-pricing opener",
        "HIGH_ACTIVITY_ZONE": "Work this suburb cluster now",
        "OWNER_LIKELY_TO_CHURN": "Lead with switch-agent conversation",
        "NEWS_DISTRESS": "Use local pressure context on outreach",
    }.get(type_, "Review signal and call")


def _reason_detail(type_: str, context: Dict[str, Any]) -> str:
    if type_ == "STALE_LISTING":
        return f"dom={context['days_on_market']}|price_drops={context['price_drop_count']}|heat={context['heat_score']}"
    if type_ == "PRICE_DROP":
        return f"price_drops={context['price_drop_count']}|hours_since_contact={context['hours_since_contact'] or 'na'}|status={context['signal_status']}"
    if type_ == "NEARBY_SOLD":
        return f"suburb_sales_30d={context['recent_sales_count']}|suburb={_normalize_suburb(context['suburb'])}|est_value={context['est_value']}"
    if type_ == "HIGH_ACTIVITY_ZONE":
        return (
            f"zone_activity={context['zone_activity_count']}|delta_withdrawn={context['delta_withdrawn_count']}|"
            f"sales_30d={context['recent_sales_count']}|news={context['news_count']}"
        )
    if type_ == "OWNER_LIKELY_TO_CHURN":
        return (
            f"id4me={'yes' if context['id4me_enriched'] else 'no'}|dom={context['days_on_market']}|"
            f"connected_calls={context['connected_calls']}|hours_since_contact={context['hours_since_contact'] or 'na'}"
        )
    if type_ == "NEWS_DISTRESS":
        return f"source={context.get('source_key') or 'news'}|severity={context.get('severity_score') or 0}|confidence={context.get('confidence_score') or 0}"
    return ""


def _signal_label(type_: str) -> str:
    return type_.replace("_", " ").title()


def _build_signal(
    *,
    type_: str,
    lead: Dict[str, Any],
    score: float,
    detected_at: str,
    context: Dict[str, Any],
    source: str,
    icon: str,
) -> Dict[str, Any]:
    normalized_score = round(min(100.0, max(0.0, score)), 2)
    return {
        "id": f"{type_.lower()}-{lead['id']}",
        "lead_id": str(lead["id"]),
        "type": type_,
        "signal_label": _signal_label(type_),
        "headline": _headline(type_, context),
        "detail": _detail(type_, context),
        "address": str(lead.get("address") or ""),
        "suburb": str(lead.get("suburb") or ""),
        "postcode": str(lead.get("postcode") or ""),
        "urgency": _urgency_level(normalized_score),
        "urgency_band": _urgency_band(normalized_score),
        "score": normalized_score,
        "color": _signal_color(normalized_score),
        "icon": icon,
        "detected_at": detected_at,
        "suggested_action": _suggested_action(type_),
        "reason_detail": _reason_detail(type_, context),
        "source": source,
    }


async def _load_leads(session: AsyncSession, lead_id: Optional[str] = None) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            id,
            address,
            suburb,
            postcode,
            owner_name,
            status,
            signal_status,
            days_on_market,
            price_drop_count,
            heat_score,
            call_today_score,
            est_value,
            last_contacted_at,
            created_at,
            updated_at,
            id4me_enriched,
            last_activity_type
        FROM leads
        WHERE COALESCE(id, '') != ''
    """
    params: Dict[str, Any] = {}
    if lead_id:
        sql += " AND id = :lead_id"
        params["lead_id"] = lead_id
    rows = (await session.execute(text(sql), params)).mappings().all()
    return [dict(row) for row in rows]


async def _load_call_stats(session: AsyncSession) -> Dict[str, Dict[str, Any]]:
    rows = (
        await session.execute(
            text(
                """
                SELECT
                    lead_id,
                    COUNT(*) AS total_calls,
                    SUM(CASE WHEN connected = 1 THEN 1 ELSE 0 END) AS connected_calls,
                    MAX(COALESCE(timestamp, logged_at)) AS last_call_at,
                    MAX(COALESCE(intent_signal, 0)) AS max_intent_signal
                FROM call_log
                WHERE COALESCE(lead_id, '') != ''
                GROUP BY lead_id
                """
            )
        )
    ).mappings().all()
    return {str(row["lead_id"]): dict(row) for row in rows}


async def _load_recent_sales(session: AsyncSession, *, cutoff_date: str) -> Dict[str, Dict[str, Any]]:
    rows = (
        await session.execute(
            text(
                """
                SELECT
                    LOWER(TRIM(COALESCE(suburb, ''))) AS suburb_key,
                    COUNT(*) AS recent_sales_count,
                    MAX(sale_date) AS latest_sale_date
                FROM sold_events
                WHERE COALESCE(sale_date, '') >= :cutoff_date
                GROUP BY LOWER(TRIM(COALESCE(suburb, '')))
                """
            ),
            {"cutoff_date": cutoff_date},
        )
    ).mappings().all()
    return {str(row["suburb_key"]): dict(row) for row in rows}


async def _load_distress_matches(
    session: AsyncSession,
    *,
    cutoff_iso: str,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    rows = (
        await session.execute(
            text(
                """
                SELECT
                    id,
                    source_key,
                    signal_type,
                    title,
                    owner_name,
                    address,
                    suburb,
                    postcode,
                    description,
                    occurred_at,
                    source_name,
                    confidence_score,
                    severity_score,
                    lead_ids,
                    created_at
                FROM distress_signals
                WHERE COALESCE(created_at, occurred_at, '') >= :cutoff_iso
                ORDER BY COALESCE(severity_score, 0) DESC, COALESCE(created_at, occurred_at, '') DESC
                """
            ),
            {"cutoff_iso": cutoff_iso},
        )
    ).mappings().all()

    by_lead: Dict[str, List[Dict[str, Any]]] = {}
    by_suburb: Dict[str, List[Dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        for linked_lead_id in _safe_json_list(row.get("lead_ids")):
            by_lead.setdefault(linked_lead_id, []).append(row)
        suburb_key = _normalize_suburb(row.get("suburb"))
        if suburb_key:
            by_suburb.setdefault(suburb_key, []).append(row)
    return by_lead, by_suburb


def _suburb_activity(leads: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    activity: Dict[str, Dict[str, int]] = {}
    for lead in leads:
        suburb_key = _normalize_suburb(lead.get("suburb"))
        if not suburb_key:
            continue
        bucket = activity.setdefault(
            suburb_key,
            {
                "total_leads": 0,
                "delta_withdrawn_count": 0,
                "stale_live_count": 0,
            },
        )
        bucket["total_leads"] += 1
        signal_status = str(lead.get("signal_status") or "").upper()
        if signal_status in {"DELTA", "WITHDRAWN"}:
            bucket["delta_withdrawn_count"] += 1
        if signal_status == "LIVE" and _safe_int(lead.get("days_on_market")) >= 60:
            bucket["stale_live_count"] += 1
    return activity


def _compute_signals_for_lead(
    *,
    lead: Dict[str, Any],
    call_stats: Dict[str, Any],
    suburb_sales: Dict[str, Any],
    suburb_activity: Dict[str, int],
    distress_matches: List[Dict[str, Any]],
    now: datetime,
) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    lead_id = str(lead["id"])
    suburb_key = _normalize_suburb(lead.get("suburb"))
    hours_since_contact = _hours_since(lead.get("last_contacted_at"), now)
    days_on_market = _safe_int(lead.get("days_on_market"))
    price_drop_count = _safe_int(lead.get("price_drop_count"))
    heat_score = _safe_int(lead.get("heat_score"))
    call_today_score = _safe_int(lead.get("call_today_score"))
    est_value = _safe_int(lead.get("est_value"))
    connected_calls = _safe_int(call_stats.get("connected_calls"))
    max_intent_signal = _safe_float(call_stats.get("max_intent_signal"))
    recent_sales_count = _safe_int(suburb_sales.get("recent_sales_count"))
    distress_match = distress_matches[0] if distress_matches else None
    delta_withdrawn_count = _safe_int(suburb_activity.get("delta_withdrawn_count"))
    news_count = len(distress_matches)
    zone_activity_count = (
        delta_withdrawn_count
        + _safe_int(suburb_activity.get("stale_live_count"))
        + recent_sales_count
        + news_count
    )

    base_context = {
        "address": str(lead.get("address") or ""),
        "suburb": str(lead.get("suburb") or ""),
        "days_on_market": days_on_market,
        "price_drop_count": price_drop_count,
        "heat_score": heat_score,
        "signal_status": str(lead.get("signal_status") or ""),
        "hours_since_contact": hours_since_contact,
        "est_value": est_value,
        "recent_sales_count": recent_sales_count,
        "zone_activity_count": zone_activity_count,
        "delta_withdrawn_count": delta_withdrawn_count,
        "news_count": news_count,
        "id4me_enriched": _safe_bool(lead.get("id4me_enriched")),
        "connected_calls": connected_calls,
    }

    if str(lead.get("signal_status") or "").upper() == "LIVE" and days_on_market >= 60:
        score = 50 + min(days_on_market - 60, 24) + min(price_drop_count * 5, 15) + min(heat_score / 8.0, 10)
        signals.append(
            _build_signal(
                type_="STALE_LISTING",
                lead=lead,
                score=score,
                detected_at=now.isoformat(),
                context=base_context,
                source="leads",
                icon="STALE",
            )
        )

    if price_drop_count >= 1 or str(lead.get("signal_status") or "").upper() == "DELTA":
        freshness_bonus = 10 if hours_since_contact is None or hours_since_contact >= 72 else 4 if hours_since_contact >= 24 else 0
        score = 58 + min(price_drop_count * 10, 20) + freshness_bonus + min(heat_score / 10.0, 8)
        signals.append(
            _build_signal(
                type_="PRICE_DROP",
                lead=lead,
                score=score,
                detected_at=now.isoformat(),
                context=base_context,
                source="leads",
                icon="DROP",
            )
        )

    if recent_sales_count > 0 and str(lead.get("signal_status") or "").upper() != "SOLD":
        score = 46 + recent_sales_count * 7 + min(days_on_market / 15.0, 12)
        signals.append(
            _build_signal(
                type_="NEARBY_SOLD",
                lead=lead,
                score=score,
                detected_at=str(suburb_sales.get("latest_sale_date") or now.isoformat()),
                context=base_context,
                source="sold_events",
                icon="SALE",
            )
        )

    if zone_activity_count >= 4:
        score = 42 + zone_activity_count * 6 + min(call_today_score / 8.0, 10)
        signals.append(
            _build_signal(
                type_="HIGH_ACTIVITY_ZONE",
                lead=lead,
                score=score,
                detected_at=now.isoformat(),
                context=base_context,
                source="suburb_cluster",
                icon="ZONE",
            )
        )

    if (
        _safe_bool(lead.get("id4me_enriched"))
        and days_on_market >= 45
        and (price_drop_count >= 1 or connected_calls >= 1 or max_intent_signal >= 0.6)
        and (hours_since_contact is None or hours_since_contact >= 72)
    ):
        score = 64 + min(days_on_market - 45, 20) + min(price_drop_count * 6, 12) + (8 if connected_calls else 0) + (6 if max_intent_signal >= 0.6 else 0)
        signals.append(
            _build_signal(
                type_="OWNER_LIKELY_TO_CHURN",
                lead=lead,
                score=score,
                detected_at=now.isoformat(),
                context=base_context,
                source="lead_behavior",
                icon="CHURN",
            )
        )

    if distress_match:
        distress_context = {
            **base_context,
            "title": str(distress_match.get("title") or ""),
            "description": str(distress_match.get("description") or ""),
            "source_name": str(distress_match.get("source_name") or ""),
            "source_key": str(distress_match.get("source_key") or ""),
            "severity_score": _safe_int(distress_match.get("severity_score")),
            "confidence_score": _safe_int(distress_match.get("confidence_score")),
        }
        score = 52 + _safe_int(distress_match.get("severity_score")) / 2.0 + (10 if lead_id in _safe_json_list(distress_match.get("lead_ids")) else 0)
        signals.append(
            _build_signal(
                type_="NEWS_DISTRESS",
                lead=lead,
                score=score,
                detected_at=str(distress_match.get("created_at") or distress_match.get("occurred_at") or now.isoformat()),
                context=distress_context,
                source=str(distress_match.get("source_key") or "news"),
                icon="NEWS",
            )
        )

    return signals


async def compute_live_signals(session: AsyncSession, limit: int = 50) -> List[Dict[str, Any]]:
    cached = await _get_cached(session, f"live:{limit}")
    if cached is not None:
        return cached

    now = _now_sydney()
    leads = await _load_leads(session)
    call_stats = await _load_call_stats(session)
    suburb_sales = await _load_recent_sales(session, cutoff_date=(now - timedelta(days=30)).date().isoformat())
    distress_by_lead, distress_by_suburb = await _load_distress_matches(
        session,
        cutoff_iso=(now - timedelta(days=14)).isoformat(),
    )
    suburb_activity = _suburb_activity(leads)

    signals: List[Dict[str, Any]] = []
    for lead in leads:
        suburb_key = _normalize_suburb(lead.get("suburb"))
        lead_distress = list(distress_by_lead.get(str(lead["id"]), []))
        if suburb_key:
            lead_distress.extend(distress_by_suburb.get(suburb_key, []))
        signals.extend(
            _compute_signals_for_lead(
                lead=lead,
                call_stats=call_stats.get(str(lead["id"]), {}),
                suburb_sales=suburb_sales.get(suburb_key, {}),
                suburb_activity=suburb_activity.get(suburb_key, {}),
                distress_matches=lead_distress,
                now=now,
            )
        )

    ordered = sorted(
        signals,
        key=lambda item: (float(item.get("score") or 0.0), str(item.get("detected_at") or "")),
        reverse=True,
    )[:limit]
    await _set_cached(session, f"live:{limit}", ordered)
    return ordered


async def compute_lead_signals(session: AsyncSession, lead_id: str) -> List[Dict[str, Any]]:
    cached = await _get_cached(session, f"lead:{lead_id}")
    if cached is not None:
        return cached

    now = _now_sydney()
    leads = await _load_leads(session, lead_id=lead_id)
    if not leads:
        return []

    lead = leads[0]
    suburb_key = _normalize_suburb(lead.get("suburb"))
    call_stats = await _load_call_stats(session)
    suburb_sales = await _load_recent_sales(session, cutoff_date=(now - timedelta(days=30)).date().isoformat())
    distress_by_lead, distress_by_suburb = await _load_distress_matches(
        session,
        cutoff_iso=(now - timedelta(days=14)).isoformat(),
    )
    suburb_activity = _suburb_activity(await _load_leads(session))

    lead_distress = list(distress_by_lead.get(lead_id, []))
    if suburb_key:
        lead_distress.extend(distress_by_suburb.get(suburb_key, []))

    ordered = sorted(
        _compute_signals_for_lead(
            lead=lead,
            call_stats=call_stats.get(lead_id, {}),
            suburb_sales=suburb_sales.get(suburb_key, {}),
            suburb_activity=suburb_activity.get(suburb_key, {}),
            distress_matches=lead_distress,
            now=now,
        ),
        key=lambda item: (float(item.get("score") or 0.0), str(item.get("detected_at") or "")),
        reverse=True,
    )
    await _set_cached(session, f"lead:{lead_id}", ordered)
    return ordered
