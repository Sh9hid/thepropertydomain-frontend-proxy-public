from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import uuid
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

TAG_RULES = (
    ("call_back", re.compile(r"\b(call\s*back|callback)\b", re.I)),
    ("interstate_owner", re.compile(r"\b(interstate|out\s+of\s+state|overseas)\b", re.I)),
    ("investor", re.compile(r"\b(investor|investment\s+property|portfolio)\b", re.I)),
    ("tenant", re.compile(r"\b(tenant|tenanted|renter|rental)\b", re.I)),
    ("wrong_number", re.compile(r"\b(wrong\s+number|wrong\s+person|not\s+the\s+owner|doesn'?t\s+live\s+here)\b", re.I)),
)
COMPANY_OWNER_PATTERNS = (
    re.compile(r"\bPTY\b", re.I),
    re.compile(r"\bLTD\b", re.I),
    re.compile(r"\bLIMITED\b", re.I),
    re.compile(r"\bHOLDINGS?\b", re.I),
    re.compile(r"\bTRUST\b", re.I),
    re.compile(r"\bSUPERANNUATION\b", re.I),
    re.compile(r"\bNOMINEES\b", re.I),
)
GOVERNMENT_OWNER_PATTERNS = (
    re.compile(r"\bCOUNCIL\b", re.I),
    re.compile(r"\bDEPARTMENT\b", re.I),
    re.compile(r"\bGOVERNMENT\b", re.I),
    re.compile(r"\bMINISTER\b", re.I),
    re.compile(r"\bSTATE OF\b", re.I),
    re.compile(r"\bCOMMONWEALTH\b", re.I),
)
LISTING_FAILURE_PATTERNS = (
    re.compile(r"\bWITHDRAWN\b", re.I),
    re.compile(r"\bEXPIRED\b", re.I),
    re.compile(r"\bFAILED\b", re.I),
    re.compile(r"\bLAPSED\b", re.I),
    re.compile(r"\bTERMINATED\b", re.I),
)
_SCHEMA_READY_KEYS: set[str] = set()


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _upper_text(value: Any) -> str:
    return _clean_text(value).upper()


def _parse_date(value: Any) -> dt.date | None:
    text_value = _clean_text(value)
    if not text_value or text_value in {"-", "N/A", "NULL"}:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%d %b %Y", "%d %B %Y"):
        try:
            return dt.datetime.strptime(text_value[:10] if fmt == "%Y-%m-%d" else text_value, fmt).date()
        except ValueError:
            continue
    try:
        return dt.date.fromisoformat(text_value[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any) -> dt.datetime | None:
    text_value = _clean_text(value)
    if not text_value:
        return None
    try:
        return dt.datetime.fromisoformat(text_value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return dt.datetime.strptime(text_value, fmt)
        except ValueError:
            continue
    return None


def _parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text_value = re.sub(r"[^0-9.\-]", "", str(value))
    if not text_value or text_value in {"-", ".", "-."}:
        return None
    try:
        return float(text_value)
    except ValueError:
        return None


def _parse_json(value: Any, default: Any) -> Any:
    if value in (None, "", []):
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _to_property_id(lead: dict[str, Any]) -> str:
    return hashlib.md5(_upper_text(lead.get("canonical_address") or lead.get("address")).encode("utf-8")).hexdigest()


def _to_person_id(name: Any, phone: Any = None) -> str:
    return hashlib.md5(f"{_upper_text(name)}|{_upper_text(phone)}".encode("utf-8")).hexdigest()


def extract_intelligence_tags(*texts: str | None) -> list[str]:
    haystack = "\n".join(_clean_text(text_value) for text_value in texts if _clean_text(text_value))
    return [label for label, pattern in TAG_RULES if haystack and pattern.search(haystack)]


def compute_ownership_years(last_sale_date: Any, *, today: dt.date | dt.datetime | None = None) -> float | None:
    parsed = _parse_date(last_sale_date)
    if not parsed:
        return None
    today_date = today.date() if isinstance(today, dt.datetime) else (today or dt.date.today())
    return round((today_date - parsed).days / 365.25, 2)


def compute_equity_proxy(estimated_value: Any, last_sale_price: Any) -> int | None:
    estimate = _parse_number(estimated_value)
    purchase = _parse_number(last_sale_price)
    if estimate is None or purchase is None:
        return None
    return int(round(estimate - purchase))


def compute_company_owner_flag(name: Any) -> bool:
    owner = _clean_text(name)
    if not owner or any(pattern.search(owner) for pattern in GOVERNMENT_OWNER_PATTERNS):
        return False
    return any(pattern.search(owner) for pattern in COMPANY_OWNER_PATTERNS)


def compute_listing_failure_signal(current_status: Any, listing_history: Iterable[dict[str, Any]] | None) -> float:
    statuses = [_clean_text(current_status)]
    for item in listing_history or []:
        statuses.extend([_clean_text(item.get("status")), _clean_text(item.get("listing_status"))])
    return 1.0 if any(pattern.search(status) for status in statuses for pattern in LISTING_FAILURE_PATTERNS if status) else 0.0


def compute_contactability_score(*, call_attempts: int, connected_calls: int, last_contact_at: Any, last_outcome: Any, tags: Iterable[str] | None = None, today: dt.datetime | None = None) -> float:
    now = today or dt.datetime.utcnow()
    parsed_contact = _parse_datetime(last_contact_at)
    tag_set = {str(tag).strip() for tag in (tags or []) if str(tag).strip()}
    outcome = _upper_text(last_outcome)
    score = 45.0 + min(max(int(connected_calls or 0), 0) * 18.0, 36.0) - min(max(int(call_attempts or 0) - 2, 0) * 6.0, 24.0)
    if "CALL_BACK" in outcome or "call_back" in tag_set:
        score += 18.0
    if any(flag in outcome for flag in ("WRONG_NUMBER", "WRONG PERSON", "NOT_ME")) or "wrong_number" in tag_set:
        score -= 80.0
    if parsed_contact:
        age_days = max((now - parsed_contact).days, 0)
        score += 12.0 if age_days <= 3 else 6.0 if age_days <= 14 else 0.0 if age_days <= 45 else -10.0 if age_days <= 90 else -24.0
    else:
        score -= 10.0
    return round(_clamp(score, 0.0, 100.0), 2)


def identity_key_for_lead(lead: dict[str, Any]) -> str:
    canonical_address = _upper_text(lead.get("canonical_address") or lead.get("address"))
    owner_identity = _upper_text(lead.get("owner_name") or lead.get("name"))
    parcel_details = _upper_text(lead.get("parcel_details"))
    return f"{canonical_address}|{parcel_details}|{owner_identity}" if parcel_details else f"{canonical_address}|{owner_identity}"


def determine_freshness_winner(*, existing_value: Any, existing_seen_at: Any, incoming_value: Any, incoming_seen_at: Any) -> Any:
    if incoming_value in (None, "", []):
        return existing_value
    if existing_value in (None, "", []):
        return incoming_value
    existing_dt = _parse_datetime(existing_seen_at)
    incoming_dt = _parse_datetime(incoming_seen_at)
    return incoming_value if (incoming_dt and existing_dt and incoming_dt > existing_dt) or (incoming_dt and not existing_dt) else existing_value


def _staleness_decay_factor(updated_at: Any, today: dt.datetime) -> tuple[float, int | None]:
    parsed = _parse_datetime(updated_at)
    if not parsed:
        return 1.0, None
    age_days = max((today - parsed).days, 0)
    return (1.0, age_days) if age_days <= 30 else (0.9, age_days) if age_days <= 90 else (0.75, age_days) if age_days <= 180 else (0.55, age_days)


def _build_reason(code: str, label: str, value: Any, weight: float, effect: float) -> dict[str, Any]:
    return {"code": code, "label": label, "value": value, "weight": round(weight, 4), "effect": round(effect, 2)}


def build_lead_intelligence_snapshot(*, lead: dict[str, Any], property_profile: dict[str, Any] | None = None, call_profile: dict[str, Any] | None = None, today: dt.datetime | None = None) -> dict[str, Any]:
    property_profile = property_profile or {}
    call_profile = call_profile or {}
    now = today or dt.datetime.utcnow()
    last_sale_date = lead.get("sale_date") or lead.get("last_sale_date")
    last_sale_price = lead.get("sale_price") or lead.get("last_sale_price")
    estimated_value = lead.get("estimated_value") or lead.get("estimated_value_high") or lead.get("estimated_value_low") or lead.get("est_value")
    ownership_years = compute_ownership_years(last_sale_date, today=now)
    equity_proxy = compute_equity_proxy(estimated_value, last_sale_price)
    absentee_owner = bool(_clean_text(lead.get("mailing_address")) and _upper_text(lead.get("mailing_address")) != _upper_text(lead.get("address") or lead.get("canonical_address")))
    company_owner = compute_company_owner_flag(lead.get("owner_name"))
    note_tags = extract_intelligence_tags(lead.get("stage_note"), lead.get("notes"), lead.get("ownership_notes"), lead.get("feedback"), lead.get("callback_notes"))
    investor_flag = bool("investor" in note_tags or lead.get("likely_landlord") or str(lead.get("owner_type") or "").strip().lower() == "company" or int(property_profile.get("same_owner_property_count") or 0) > 1)
    listing_failure_signal = compute_listing_failure_signal(lead.get("last_listing_status"), _parse_json(lead.get("listing_status_history"), []))
    contactability_score = compute_contactability_score(call_attempts=int(call_profile.get("call_attempts") or 0), connected_calls=int(call_profile.get("connected_calls") or 0), last_contact_at=call_profile.get("last_contact_at") or lead.get("last_contacted_at"), last_outcome=call_profile.get("last_outcome") or lead.get("last_outcome"), tags=note_tags, today=now)
    reasons: list[dict[str, Any]] = []
    raw_intent_score = 0.0
    if ownership_years is not None:
        effect = _clamp(ownership_years * 2.8, 0.0, 35.0); raw_intent_score += effect; reasons.append(_build_reason("ownership_years", "Long ownership tenure", ownership_years, 2.8, effect))
    if equity_proxy is not None:
        effect = _clamp(equity_proxy / 50000.0, 0.0, 30.0); raw_intent_score += effect; reasons.append(_build_reason("equity_proxy", "High equity proxy", equity_proxy, 1 / 50000.0, effect))
    if listing_failure_signal:
        raw_intent_score += 18.0; reasons.append(_build_reason("listing_failure_signal", "Prior listing failed", listing_failure_signal, 18.0, 18.0))
    if absentee_owner:
        raw_intent_score += 10.0; reasons.append(_build_reason("absentee_owner", "Mailing address differs from property", True, 10.0, 10.0))
    if investor_flag:
        raw_intent_score += 12.0; reasons.append(_build_reason("investor_flag", "Investor-linked owner", True, 12.0, 12.0))
    if company_owner:
        raw_intent_score += 4.0; reasons.append(_build_reason("company_owner", "Company/trust ownership pattern", True, 4.0, 4.0))
    if int(property_profile.get("same_owner_property_count") or 0) > 1:
        effect = _clamp((int(property_profile.get("same_owner_property_count")) - 1) * 4.0, 0.0, 12.0); raw_intent_score += effect; reasons.append(_build_reason("same_owner_property_count", "Owner linked to multiple properties", int(property_profile.get("same_owner_property_count") or 0), 4.0, effect))
    decay_factor, stale_age_days = _staleness_decay_factor(lead.get("updated_at") or lead.get("created_at"), now)
    intent_score = round(_clamp(raw_intent_score * decay_factor, 0.0, 100.0), 2)
    if stale_age_days is not None and decay_factor < 1.0:
        reasons.append(_build_reason("stale_lead_decay", "Stale lead decay applied", {"days_since_update": stale_age_days, "factor": decay_factor}, 1.0, round(intent_score - raw_intent_score, 2)))
    tags = sorted(set(note_tags + (["absentee_owner"] if absentee_owner else []) + (["investor"] if investor_flag else [])))
    return {"property_id": _to_property_id(lead), "intent_score": intent_score, "contactability_score": contactability_score, "priority_rank": round(intent_score * contactability_score, 4), "tags": tags, "reasons": reasons, "ownership_years": ownership_years, "equity_proxy": equity_proxy, "absentee_owner": absentee_owner, "company_owner": company_owner, "investor_flag": investor_flag, "listing_failure_signal": listing_failure_signal, "same_owner_property_count": int(property_profile.get("same_owner_property_count") or 0), "nearby_sales_count": int(property_profile.get("nearby_sales_count") or 0), "agent_dominance_score": property_profile.get("agent_dominance_score"), "last_updated": now.isoformat()}


async def ensure_intelligence_schema(session: AsyncSession) -> None:
    bind_url = str(session.bind.url) if session.bind is not None else "unknown"
    if bind_url in _SCHEMA_READY_KEYS:
        return
    sqlite_mode = session.bind is not None and session.bind.dialect.name == "sqlite"
    if not sqlite_mode:
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS intelligence"))
    ddls = [
        "CREATE TABLE IF NOT EXISTS intelligence.property (id TEXT PRIMARY KEY, address TEXT NOT NULL UNIQUE, suburb TEXT, postcode TEXT, parcel_details TEXT, property_type TEXT, land_size_sqm REAL, last_sale_date TEXT, last_sale_price INTEGER, estimated_value INTEGER, zoning_type TEXT, trigger_type TEXT, status TEXT DEFAULT 'captured', route_queue TEXT DEFAULT '', heat_score INTEGER DEFAULT 0, evidence_score INTEGER DEFAULT 0, created_at TEXT, updated_at TEXT)",
        "CREATE TABLE IF NOT EXISTS intelligence.party (id TEXT PRIMARY KEY, full_name TEXT NOT NULL, phone TEXT, owner_type TEXT, absentee_flag INTEGER, investor_flag INTEGER, source TEXT DEFAULT '', created_at TEXT, updated_at TEXT)",
        "CREATE TABLE IF NOT EXISTS intelligence.property_party (id TEXT PRIMARY KEY, property_id TEXT NOT NULL, party_id TEXT NOT NULL, role TEXT DEFAULT '', created_at TEXT, updated_at TEXT)",
        "CREATE TABLE IF NOT EXISTS intelligence.agent_profile (id TEXT PRIMARY KEY, agent_name TEXT NOT NULL, agency_name TEXT, suburb TEXT, suburb_activity_count INTEGER DEFAULT 0, last_updated TEXT)",
        "CREATE TABLE IF NOT EXISTS intelligence.lead_intelligence (property_id TEXT PRIMARY KEY, intent_score REAL DEFAULT 0, contactability_score REAL DEFAULT 0, priority_rank REAL DEFAULT 0, tags_json TEXT DEFAULT '[]', reasons_json TEXT DEFAULT '[]', ownership_years REAL, equity_proxy INTEGER, absentee_owner INTEGER, company_owner INTEGER, investor_flag INTEGER, listing_failure_signal REAL, same_owner_property_count INTEGER DEFAULT 0, nearby_sales_count INTEGER DEFAULT 0, agent_dominance_score REAL, last_updated TEXT)",
    ]
    for ddl in ddls:
        await session.execute(text(ddl))
    index_ddls = (
        [
            "CREATE UNIQUE INDEX IF NOT EXISTS intelligence.idx_intelligence_property_party_unique ON property_party(property_id, party_id, role)",
            "CREATE UNIQUE INDEX IF NOT EXISTS intelligence.idx_intelligence_agent_unique ON agent_profile(agent_name, agency_name, suburb)",
            "CREATE INDEX IF NOT EXISTS intelligence.idx_intelligence_lead_priority ON lead_intelligence(priority_rank DESC)",
        ]
        if sqlite_mode
        else [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_intelligence_property_party_unique ON intelligence.property_party(property_id, party_id, role)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_intelligence_agent_unique ON intelligence.agent_profile(agent_name, agency_name, suburb)",
            "CREATE INDEX IF NOT EXISTS idx_intelligence_lead_priority ON intelligence.lead_intelligence(priority_rank DESC)",
        ]
    )
    for ddl in index_ddls:
        await session.execute(text(ddl))
    _SCHEMA_READY_KEYS.add(bind_url)


async def _load_lead_row(session: AsyncSession, lead_id: str) -> dict[str, Any] | None:
    row = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    return dict(row) if row else None


async def _call_profile_for_lead(session: AsyncSession, lead_id: str) -> dict[str, Any]:
    row = (
        await session.execute(
            text(
                "SELECT COUNT(*) AS call_attempts, SUM(CASE WHEN COALESCE(connected, 0) = 1 THEN 1 ELSE 0 END) AS connected_calls, MAX(COALESCE(timestamp, logged_at)) AS last_contact_at, (SELECT outcome FROM call_log WHERE lead_id = :lead_id ORDER BY COALESCE(timestamp, logged_at, '') DESC, id DESC LIMIT 1) AS last_outcome FROM call_log WHERE lead_id = :lead_id"
            ),
            {"lead_id": lead_id},
        )
    ).mappings().first()
    return dict(row or {})


async def _same_owner_property_count(session: AsyncSession, owner_name: str) -> int:
    normalized_owner = _upper_text(owner_name)
    if not normalized_owner:
        return 0
    row = (
        await session.execute(
            text("SELECT COUNT(DISTINCT COALESCE(canonical_address, address)) AS cnt FROM leads WHERE UPPER(TRIM(COALESCE(owner_name, ''))) = :owner_name"),
            {"owner_name": normalized_owner},
        )
    ).mappings().first()
    return int((row or {}).get("cnt") or 0)


async def _agent_rollup(session: AsyncSession, agent_name: str, agency_name: str | None, suburb: str | None) -> tuple[int, float | None]:
    if not _clean_text(agent_name) or not _clean_text(suburb):
        return 0, None
    activity_row = (
        await session.execute(
            text("SELECT COUNT(*) AS cnt FROM leads WHERE UPPER(TRIM(COALESCE(agent_name, ''))) = :agent_name AND UPPER(TRIM(COALESCE(suburb, ''))) = :suburb AND UPPER(TRIM(COALESCE(agency_name, ''))) = :agency_name"),
            {"agent_name": _upper_text(agent_name), "agency_name": _upper_text(agency_name), "suburb": _upper_text(suburb)},
        )
    ).mappings().first()
    total_row = (
        await session.execute(
            text("SELECT COUNT(*) AS cnt FROM leads WHERE UPPER(TRIM(COALESCE(suburb, ''))) = :suburb AND TRIM(COALESCE(agent_name, '')) != ''"),
            {"suburb": _upper_text(suburb)},
        )
    ).mappings().first()
    activity_count = int((activity_row or {}).get("cnt") or 0)
    total_count = int((total_row or {}).get("cnt") or 0)
    return activity_count, round(activity_count / total_count, 4) if total_count else None


async def sync_lead_intelligence_for_lead(session: AsyncSession, lead_id: str, *, as_of: str | None = None) -> dict[str, Any]:
    await ensure_intelligence_schema(session)
    lead = await _load_lead_row(session, lead_id)
    if not lead:
        raise ValueError("Lead not found")
    now = _parse_datetime(as_of) or dt.datetime.utcnow()
    property_id = _to_property_id(lead)
    phones = _parse_json(lead.get("contact_phones"), [])
    primary_phone = phones[0] if phones else None
    person_id = _to_person_id(lead.get("owner_name"), primary_phone)
    same_owner_property_count = await _same_owner_property_count(session, lead.get("owner_name"))
    suburb_activity_count, agent_dominance_score = await _agent_rollup(session, str(lead.get("agent_name") or ""), str(lead.get("agency_name") or ""), str(lead.get("suburb") or ""))
    property_profile = {"same_owner_property_count": same_owner_property_count, "nearby_sales_count": len(_parse_json(lead.get("nearby_sales"), [])), "agent_dominance_score": agent_dominance_score}
    call_profile = await _call_profile_for_lead(session, lead_id)
    snapshot = build_lead_intelligence_snapshot(lead=lead, property_profile=property_profile, call_profile=call_profile, today=now)
    estimated_value = int(_parse_number(lead.get("estimated_value_high") or lead.get("est_value")) or 0)
    await session.execute(
        text("INSERT INTO intelligence.property (id, address, suburb, postcode, parcel_details, property_type, land_size_sqm, last_sale_date, last_sale_price, estimated_value, cadid, h3index, lat, lng, est_value, zoning_type, trigger_type, status, route_queue, heat_score, evidence_score, created_at, updated_at) VALUES (:id, :address, :suburb, :postcode, :parcel_details, :property_type, :land_size_sqm, :last_sale_date, :last_sale_price, :estimated_value, :cadid, :h3index, :lat, :lng, :est_value, :zoning_type, :trigger_type, :status, :route_queue, :heat_score, :evidence_score, :created_at, :updated_at) ON CONFLICT(id) DO UPDATE SET address = excluded.address, suburb = excluded.suburb, postcode = excluded.postcode, parcel_details = excluded.parcel_details, property_type = excluded.property_type, land_size_sqm = excluded.land_size_sqm, last_sale_date = excluded.last_sale_date, last_sale_price = excluded.last_sale_price, estimated_value = excluded.estimated_value, cadid = excluded.cadid, h3index = excluded.h3index, lat = excluded.lat, lng = excluded.lng, est_value = excluded.est_value, zoning_type = excluded.zoning_type, trigger_type = excluded.trigger_type, status = excluded.status, route_queue = excluded.route_queue, heat_score = excluded.heat_score, evidence_score = excluded.evidence_score, updated_at = excluded.updated_at"),
        {"id": property_id, "address": lead.get("address"), "suburb": lead.get("suburb"), "postcode": lead.get("postcode"), "parcel_details": lead.get("parcel_details"), "property_type": lead.get("property_type"), "land_size_sqm": _parse_number(lead.get("land_size_sqm")), "last_sale_date": lead.get("sale_date"), "last_sale_price": int(_parse_number(lead.get("sale_price")) or 0) or None, "estimated_value": estimated_value or None, "cadid": lead.get("cadid"), "h3index": lead.get("h3index"), "lat": float(lead.get("lat") or 0), "lng": float(lead.get("lng") or 0), "est_value": estimated_value, "zoning_type": lead.get("development_zone") or lead.get("zoning_type"), "trigger_type": lead.get("trigger_type"), "status": lead.get("status") or "captured", "route_queue": lead.get("route_queue") or "", "heat_score": int(lead.get("heat_score") or 0), "evidence_score": int(lead.get("evidence_score") or 0), "created_at": lead.get("created_at") or now.isoformat(), "updated_at": now.isoformat()},
    )
    await session.execute(
        text("INSERT INTO intelligence.party (id, full_name, phone, owner_type, absentee_flag, investor_flag, source, created_at, updated_at) VALUES (:id, :full_name, :phone, :owner_type, :absentee_flag, :investor_flag, 'lead_projection', :created_at, :updated_at) ON CONFLICT(id) DO UPDATE SET full_name = excluded.full_name, phone = excluded.phone, owner_type = excluded.owner_type, absentee_flag = excluded.absentee_flag, investor_flag = excluded.investor_flag, updated_at = excluded.updated_at"),
        {"id": person_id, "full_name": lead.get("owner_name") or "Unknown", "phone": primary_phone, "owner_type": lead.get("owner_type"), "absentee_flag": 1 if snapshot["absentee_owner"] else 0, "investor_flag": 1 if snapshot["investor_flag"] else 0, "created_at": lead.get("created_at") or now.isoformat(), "updated_at": now.isoformat()},
    )
    await session.execute(
        text("INSERT INTO intelligence.property_party (id, property_id, party_id, role, created_at, updated_at) VALUES (:id, :property_id, :party_id, :role, :created_at, :updated_at) ON CONFLICT(property_id, party_id, role) DO UPDATE SET updated_at = excluded.updated_at"),
        {"id": str(uuid.uuid4()), "property_id": property_id, "party_id": person_id, "role": "owner", "created_at": lead.get("created_at") or now.isoformat(), "updated_at": now.isoformat()},
    )
    if _clean_text(lead.get("agent_name")):
        await session.execute(
            text("INSERT INTO intelligence.agent_profile (id, agent_name, agency_name, suburb, suburb_activity_count, last_updated) VALUES (:id, :agent_name, :agency_name, :suburb, :suburb_activity_count, :last_updated) ON CONFLICT(agent_name, agency_name, suburb) DO UPDATE SET suburb_activity_count = excluded.suburb_activity_count, last_updated = excluded.last_updated"),
            {"id": hashlib.md5(f'{_upper_text(lead.get("agent_name"))}|{_upper_text(lead.get("agency_name"))}|{_upper_text(lead.get("suburb"))}'.encode("utf-8")).hexdigest(), "agent_name": lead.get("agent_name"), "agency_name": lead.get("agency_name"), "suburb": lead.get("suburb"), "suburb_activity_count": suburb_activity_count, "last_updated": now.isoformat()},
        )
    await session.execute(
        text("INSERT INTO intelligence.lead_intelligence (property_id, intent_score, contactability_score, priority_rank, tags_json, reasons_json, ownership_years, equity_proxy, absentee_owner, company_owner, investor_flag, listing_failure_signal, same_owner_property_count, nearby_sales_count, agent_dominance_score, last_updated) VALUES (:property_id, :intent_score, :contactability_score, :priority_rank, :tags_json, :reasons_json, :ownership_years, :equity_proxy, :absentee_owner, :company_owner, :investor_flag, :listing_failure_signal, :same_owner_property_count, :nearby_sales_count, :agent_dominance_score, :last_updated) ON CONFLICT(property_id) DO UPDATE SET intent_score = excluded.intent_score, contactability_score = excluded.contactability_score, priority_rank = excluded.priority_rank, tags_json = excluded.tags_json, reasons_json = excluded.reasons_json, ownership_years = excluded.ownership_years, equity_proxy = excluded.equity_proxy, absentee_owner = excluded.absentee_owner, company_owner = excluded.company_owner, investor_flag = excluded.investor_flag, listing_failure_signal = excluded.listing_failure_signal, same_owner_property_count = excluded.same_owner_property_count, nearby_sales_count = excluded.nearby_sales_count, agent_dominance_score = excluded.agent_dominance_score, last_updated = excluded.last_updated"),
        {"property_id": property_id, "intent_score": snapshot["intent_score"], "contactability_score": snapshot["contactability_score"], "priority_rank": snapshot["priority_rank"], "tags_json": json.dumps(snapshot["tags"]), "reasons_json": json.dumps(snapshot["reasons"]), "ownership_years": snapshot["ownership_years"], "equity_proxy": snapshot["equity_proxy"], "absentee_owner": 1 if snapshot["absentee_owner"] else 0, "company_owner": 1 if snapshot["company_owner"] else 0, "investor_flag": 1 if snapshot["investor_flag"] else 0, "listing_failure_signal": snapshot["listing_failure_signal"], "same_owner_property_count": snapshot["same_owner_property_count"], "nearby_sales_count": snapshot["nearby_sales_count"], "agent_dominance_score": snapshot["agent_dominance_score"], "last_updated": now.isoformat()},
    )
    return snapshot


async def sync_all_lead_intelligence(session: AsyncSession, *, as_of: str | None = None) -> dict[str, Any]:
    await ensure_intelligence_schema(session)
    rows = (await session.execute(text("SELECT id FROM leads"))).mappings().all()
    processed = 0
    for row in rows:
        await sync_lead_intelligence_for_lead(session, str(row["id"]), as_of=as_of)
        processed += 1
    return {"processed": processed, "as_of": (_parse_datetime(as_of) or dt.datetime.utcnow()).isoformat()}


async def fetch_intelligence_by_property_ids(session: AsyncSession, property_ids: list[str]) -> dict[str, dict[str, Any]]:
    try:
        await ensure_intelligence_schema(session)
    except Exception:
        return {}
    if not property_ids:
        return {}
    placeholders = ", ".join(f":pid_{index}" for index, _ in enumerate(property_ids))
    params = {f"pid_{index}": property_id for index, property_id in enumerate(property_ids)}
    try:
        rows = (await session.execute(text(f"SELECT * FROM intelligence.lead_intelligence WHERE property_id IN ({placeholders})"), params)).mappings().all()
    except Exception:
        return {}
    mapped: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        item["tags_json"] = _parse_json(item.get("tags_json"), [])
        item["reasons_json"] = _parse_json(item.get("reasons_json"), [])
        mapped[str(item["property_id"])] = item
    return mapped


def attach_intelligence_to_leads(leads: list[dict[str, Any]], intelligence_map: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for lead in leads:
        property_id = _to_property_id(lead)
        intelligence = intelligence_map.get(property_id, {})
        next_lead = dict(lead)
        next_lead["property_id"] = property_id
        next_lead["intent_score"] = intelligence.get("intent_score", lead.get("intent_score", 0))
        next_lead["contactability_score"] = intelligence.get("contactability_score")
        next_lead["priority_rank"] = intelligence.get("priority_rank", 0)
        next_lead["intelligence_tags"] = intelligence.get("tags_json", [])
        next_lead["intelligence_reasons"] = intelligence.get("reasons_json", [])
        next_lead["ownership_years"] = intelligence.get("ownership_years", lead.get("ownership_duration_years"))
        next_lead["equity_proxy"] = intelligence.get("equity_proxy")
        next_lead["company_owner"] = bool(intelligence.get("company_owner")) if intelligence else False
        next_lead["investor_flag"] = bool(intelligence.get("investor_flag")) if intelligence else False
        next_lead["listing_failure_signal"] = intelligence.get("listing_failure_signal")
        next_lead["intelligence_last_updated"] = intelligence.get("last_updated")
        enriched.append(next_lead)
    return enriched


__all__ = [
    "attach_intelligence_to_leads",
    "build_lead_intelligence_snapshot",
    "compute_company_owner_flag",
    "compute_contactability_score",
    "compute_equity_proxy",
    "compute_listing_failure_signal",
    "compute_ownership_years",
    "determine_freshness_winner",
    "ensure_intelligence_schema",
    "extract_intelligence_tags",
    "fetch_intelligence_by_property_ids",
    "identity_key_for_lead",
    "sync_all_lead_intelligence",
    "sync_lead_intelligence_for_lead",
]
