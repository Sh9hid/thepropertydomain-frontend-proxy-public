import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import ALL_TARGET_SUBURBS
from core.events import event_manager
from core.utils import now_iso
from models.distress_schemas import (
    DistressLandlordCandidate,
    DistressLandlordWatchResponse,
    DistressManualIngestRequest,
    DistressRunListResponse,
    DistressRunPayload,
    DistressSignalListResponse,
    DistressSignalPayload,
    DistressSourceListResponse,
    DistressSourcePayload,
)
from services.probate_scraper import _extract_suburb_postcode, _fetch_probate_notices

_logger = logging.getLogger(__name__)

_FIRE_FEED_URL = "https://www.rfs.nsw.gov.au/feeds/majorIncidents.json"
_TAX_SIGNAL_TYPES = {"delinquent_tax", "tax_lien", "tax_default"}
_LIEN_SIGNAL_TYPES = {"lien", "tax_lien"}
_DEFAULT_ROUTE_QUEUE = "real_estate"
_DEFAULT_SIGNAL_STATUS = "OFF-MARKET"
_DEFAULT_TARGET_POSTCODES = {"2517", "2518", "2756", "2765", "2762", "2155", "2768"}

_DEFAULT_DISTRESS_SOURCES: List[Dict[str, Any]] = [
    {
        "source_key": "nsw_probate_gazette",
        "label": "NSW Probate Gazette",
        "signal_type": "probate",
        "enabled": True,
        "mode": "official_feed",
        "cadence_minutes": 1440,
        "source_url": "https://gazette.legislation.nsw.gov.au/api/v1/search?category=probate",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "Official NSW Government Gazette probate/deceased estate notices.",
    },
    {
        "source_key": "nsw_rfs_major_incidents",
        "label": "NSW RFS Major Incidents",
        "signal_type": "fire_report",
        "enabled": True,
        "mode": "official_feed",
        "cadence_minutes": 60,
        "source_url": _FIRE_FEED_URL,
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "Official NSW Rural Fire Service major incidents feed.",
    },
    {
        "source_key": "legacy_com_obituaries",
        "label": "Legacy.com Obituaries (NSW)",
        "signal_type": "obituary",
        "enabled": True,
        "mode": "api_feed",
        "cadence_minutes": 360,
        "source_url": "https://www.legacy.com/api/obituaries?affiliateid=2261&regionid=35",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "Legacy.com undocumented JSON API — NSW region obituaries. Free, no auth required.",
    },
    {
        "source_key": "asic_insolvency",
        "label": "ASIC Insolvency Notices",
        "signal_type": "insolvency",
        "enabled": True,
        "mode": "api_feed",
        "cadence_minutes": 1440,
        "source_url": "https://insolvencynotices.asic.gov.au",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "ASIC public insolvency notices — external administration, liquidation. No auth required.",
    },
    {
        "source_key": "newsapi_distress",
        "label": "NewsAPI Distress Signals",
        "signal_type": "news",
        "enabled": True,
        "mode": "api_feed",
        "cadence_minutes": 60,
        "source_url": "https://newsapi.org/v2/everything",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "NewsAPI keyword search for fire, eviction, mortgage, bankruptcy in target suburbs. Requires NEWSAPI_KEY env var.",
    },
    {
        "source_key": "obituaries_manual",
        "label": "Obituaries Import (Manual)",
        "signal_type": "obituary",
        "enabled": False,
        "mode": "manual_feed",
        "cadence_minutes": 1440,
        "source_url": "",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "Manual or licensed obituary feed import.",
    },
    {
        "source_key": "evictions_manual",
        "label": "Eviction Records Import",
        "signal_type": "eviction",
        "enabled": False,
        "mode": "manual_feed",
        "cadence_minutes": 1440,
        "source_url": "",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "Manual or licensed tribunal/court export import. No bulk court scraper enabled.",
    },
    {
        "source_key": "divorce_manual",
        "label": "Divorce Filings Import",
        "signal_type": "divorce",
        "enabled": False,
        "mode": "manual_feed",
        "cadence_minutes": 1440,
        "source_url": "",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "Manual or licensed court export import. No public bulk filing scraper enabled.",
    },
    {
        "source_key": "liens_manual",
        "label": "Lien Notices Import",
        "signal_type": "lien",
        "enabled": False,
        "mode": "manual_feed",
        "cadence_minutes": 1440,
        "source_url": "",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "Manual or licensed lien/tax-default export import.",
    },
    {
        "source_key": "code_violations_manual",
        "label": "Code Violations Import",
        "signal_type": "code_violation",
        "enabled": False,
        "mode": "manual_feed",
        "cadence_minutes": 1440,
        "source_url": "",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "Manual council/building-order import. Council source adapters can be added per jurisdiction.",
    },
    {
        "source_key": "social_distress_manual",
        "label": "Social Distress Import",
        "signal_type": "social_distress",
        "enabled": False,
        "mode": "manual_feed",
        "cadence_minutes": 720,
        "source_url": "",
        "coverage_suburbs": ALL_TARGET_SUBURBS,
        "notes": "Approved export/manual import only. No default Facebook/Craigslist scraper is enabled.",
    },
]


def _json_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True)


def _normalize_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _normalize_key(value: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "", _normalize_text(value).lower())


def _normalize_owner_name(value: Optional[str]) -> str:
    cleaned = _normalize_text(value)
    cleaned = re.sub(r"\b(mr|mrs|ms|dr|estate of|the estate of)\b", "", cleaned, flags=re.I)
    return re.sub(r"\s+", " ", cleaned).strip()


def _is_target_area(suburb: Optional[str], postcode: Optional[str], text_blob: str = "") -> bool:
    suburb_key = _normalize_key(suburb)
    target_suburb_keys = {_normalize_key(item) for item in ALL_TARGET_SUBURBS}
    if suburb_key and suburb_key in target_suburb_keys:
        return True
    if postcode and str(postcode).strip() in _DEFAULT_TARGET_POSTCODES:
        return True
    haystack = _normalize_key(text_blob)
    return any(item in haystack for item in target_suburb_keys)


def _extract_postcode(text_blob: str) -> Optional[str]:
    match = re.search(r"\b(2\d{3})\b", text_blob or "")
    return match.group(1) if match else None


def _extract_target_suburb(text_blob: str) -> Optional[str]:
    haystack = _normalize_key(text_blob)
    for suburb in ALL_TARGET_SUBURBS:
        if _normalize_key(suburb) in haystack:
            return suburb
    return None


def _extract_address_candidate(text_blob: str) -> Optional[str]:
    text_blob = _normalize_text(text_blob)
    if not text_blob:
        return None
    match = re.search(
        r"(\d{1,5}\s+[A-Za-z0-9' .-]+?\s(?:Street|St|Road|Rd|Avenue|Ave|Drive|Dr|Close|Place|Pl|Lane|Way|Court|Ct|Crescent|Cres))",
        text_blob,
        flags=re.I,
    )
    return _normalize_text(match.group(1)) if match else None


def _parse_isoish(value: Optional[str]) -> Optional[datetime]:
    text_value = _normalize_text(value)
    if not text_value:
        return None
    try:
        if text_value.endswith("Z"):
            return datetime.fromisoformat(text_value.replace("Z", "+00:00"))
        return datetime.fromisoformat(text_value)
    except Exception:
        return None


def _years_since(value: Optional[str]) -> Optional[float]:
    parsed = _parse_isoish(value)
    if not parsed:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta_days = (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).days
    return round(delta_days / 365.25, 1)


def _make_signal_ref(source_key: str, external_ref: str) -> str:
    return f"distress:{source_key}:{external_ref}"


def _row_to_source(row: Dict[str, Any]) -> DistressSourcePayload:
    return DistressSourcePayload(
        id=row["id"],
        source_key=row["source_key"],
        label=row["label"],
        signal_type=row["signal_type"],
        enabled=bool(row.get("enabled")),
        mode=row.get("mode") or "manual_feed",
        cadence_minutes=int(row.get("cadence_minutes") or 1440),
        source_url=row.get("source_url"),
        coverage_suburbs=_json_loads(row.get("coverage_suburbs"), []),
        notes=row.get("notes") or "",
        last_run_at=row.get("last_run_at"),
        last_success_at=row.get("last_success_at"),
        last_error=row.get("last_error"),
        metrics=_json_loads(row.get("metrics"), {}),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


def _row_to_run(row: Dict[str, Any]) -> DistressRunPayload:
    return DistressRunPayload(
        id=row["id"],
        source_id=row.get("source_id"),
        source_key=row["source_key"],
        requested_by=row.get("requested_by") or "scheduler",
        status=row.get("status") or "queued",
        records_scanned=int(row.get("records_scanned") or 0),
        records_created=int(row.get("records_created") or 0),
        records_linked=int(row.get("records_linked") or 0),
        records_created_as_leads=int(row.get("records_created_as_leads") or 0),
        error_summary=row.get("error_summary"),
        metrics=_json_loads(row.get("metrics"), {}),
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        updated_at=row.get("updated_at"),
    )


def _row_to_signal(row: Dict[str, Any]) -> DistressSignalPayload:
    return DistressSignalPayload(
        id=row["id"],
        source_key=row["source_key"],
        signal_type=row["signal_type"],
        external_ref=row["external_ref"],
        title=row["title"],
        owner_name=row.get("owner_name"),
        address=row.get("address"),
        suburb=row.get("suburb"),
        postcode=row.get("postcode"),
        description=row.get("description") or "",
        occurred_at=row.get("occurred_at"),
        source_name=row.get("source_name") or "",
        source_url=row.get("source_url"),
        confidence_score=float(row.get("confidence_score") or 0),
        severity_score=int(row.get("severity_score") or 0),
        status=row.get("status") or "captured",
        lead_ids=_json_loads(row.get("lead_ids"), []),
        inferred_owner_matches=_json_loads(row.get("inferred_owner_matches"), []),
        inferred_property_matches=_json_loads(row.get("inferred_property_matches"), []),
        payload=_json_loads(row.get("payload"), {}),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


def _dedupe_strings(values: List[Any]) -> List[str]:
    seen: set[str] = set()
    items: List[str] = []
    for raw_value in values:
        text_value = _normalize_text(str(raw_value or ""))
        if not text_value:
            continue
        marker = text_value.lower()
        if marker in seen:
            continue
        seen.add(marker)
        items.append(text_value)
    return items


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _parse_au_datetime(value: Optional[str]) -> Optional[str]:
    text_value = _normalize_text(value)
    if not text_value:
        return None
    for pattern in ("%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y %H:%M", "%d %b %Y %H:%M", "%d %B %Y %H:%M"):
        try:
            parsed = datetime.strptime(text_value, pattern)
            return parsed.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            continue
    return None


def _signal_evidence_note(signal: Dict[str, Any]) -> str:
    label = _normalize_text(signal.get("title") or signal.get("signal_type") or "Distress signal")
    source_name = _normalize_text(signal.get("source_name") or signal.get("source_key") or "Distress source")
    source_url = _normalize_text(signal.get("source_url"))
    if source_url:
        return f"{source_name}: {label} ({source_url})"
    return f"{source_name}: {label}"


async def _fetch_source_row(session: AsyncSession, source_key: str) -> Optional[Dict[str, Any]]:
    result = await session.execute(
        text("SELECT * FROM distress_sources WHERE source_key = :source_key"),
        {"source_key": source_key},
    )
    row = result.mappings().first()
    return dict(row) if row else None


async def _load_lead_row(session: AsyncSession, lead_id: str) -> Optional[Dict[str, Any]]:
    result = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    row = result.mappings().first()
    return dict(row) if row else None


async def ensure_distress_sources(session: AsyncSession) -> DistressSourceListResponse:
    now = now_iso()
    inserted = False
    for source in _DEFAULT_DISTRESS_SOURCES:
        existing = await session.execute(
            text("SELECT id FROM distress_sources WHERE source_key = :source_key"),
            {"source_key": source["source_key"]},
        )
        if existing.mappings().first():
            continue
        await session.execute(
            text(
                """
                INSERT INTO distress_sources (
                    id, source_key, label, signal_type, enabled, mode, cadence_minutes,
                    source_url, coverage_suburbs, notes, metrics, created_at, updated_at
                ) VALUES (
                    :id, :source_key, :label, :signal_type, :enabled, :mode, :cadence_minutes,
                    :source_url, :coverage_suburbs, :notes, :metrics, :created_at, :updated_at
                )
                """
            ),
            {
                "id": str(uuid.uuid4()),
                "source_key": source["source_key"],
                "label": source["label"],
                "signal_type": source["signal_type"],
                "enabled": bool(source.get("enabled")),
                "mode": source.get("mode") or "manual_feed",
                "cadence_minutes": int(source.get("cadence_minutes") or 1440),
                "source_url": source.get("source_url") or "",
                "coverage_suburbs": _json_dumps(source.get("coverage_suburbs") or []),
                "notes": source.get("notes") or "",
                "metrics": _json_dumps({}),
                "created_at": now,
                "updated_at": now,
            },
        )
        inserted = True
    if inserted:
        await session.commit()
    return await list_distress_sources(session)


async def list_distress_sources(session: AsyncSession) -> DistressSourceListResponse:
    result = await session.execute(
        text("SELECT * FROM distress_sources ORDER BY enabled DESC, label ASC")
    )
    return DistressSourceListResponse(
        sources=[_row_to_source(dict(row)) for row in result.mappings().all()]
    )


async def list_distress_runs(session: AsyncSession, limit: int = 50) -> DistressRunListResponse:
    result = await session.execute(
        text("SELECT * FROM distress_runs ORDER BY COALESCE(started_at, updated_at) DESC LIMIT :limit"),
        {"limit": max(1, min(int(limit or 50), 200))},
    )
    return DistressRunListResponse(
        runs=[_row_to_run(dict(row)) for row in result.mappings().all()]
    )


async def list_distress_signals(
    session: AsyncSession,
    signal_type: Optional[str] = None,
    source_key: Optional[str] = None,
    suburb: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 100,
) -> DistressSignalListResponse:
    clauses: List[str] = []
    params: Dict[str, Any] = {"limit": max(1, min(int(limit or 100), 300))}
    if signal_type:
        clauses.append("signal_type = :signal_type")
        params["signal_type"] = signal_type
    if source_key:
        clauses.append("source_key = :source_key")
        params["source_key"] = source_key
    if suburb:
        clauses.append("LOWER(COALESCE(suburb, '')) = :suburb")
        params["suburb"] = suburb.strip().lower()
    if query:
        clauses.append(
            "(LOWER(COALESCE(title, '')) LIKE :query OR LOWER(COALESCE(description, '')) LIKE :query OR LOWER(COALESCE(owner_name, '')) LIKE :query OR LOWER(COALESCE(address, '')) LIKE :query)"
        )
        params["query"] = f"%{query.strip().lower()}%"
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = await session.execute(
        text(
            f"""
            SELECT * FROM distress_signals
            {where_sql}
            ORDER BY COALESCE(occurred_at, created_at) DESC
            LIMIT :limit
            """
        ),
        params,
    )
    count_row = await session.execute(
        text(f"SELECT COUNT(*) AS total FROM distress_signals {where_sql}"),
        {key: value for key, value in params.items() if key != "limit"},
    )
    total = int((count_row.mappings().first() or {}).get("total") or 0)
    return DistressSignalListResponse(
        signals=[_row_to_signal(dict(row)) for row in rows.mappings().all()],
        total=total,
    )


def _normalize_probate_notice(notice: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    content = _normalize_text(
        notice.get("content")
        or notice.get("body")
        or notice.get("text")
        or json.dumps(notice, ensure_ascii=True)
    )
    title = _normalize_text(notice.get("title") or notice.get("deceased_name") or "Probate notice")
    owner_name = _normalize_owner_name(notice.get("deceased_name") or notice.get("title"))
    address = _normalize_text(notice.get("address") or notice.get("property_address") or "")
    if not address:
        address = _extract_address_candidate(content) or ""
    suburb, postcode = _extract_suburb_postcode(f"{address} {content}")
    suburb = suburb or _extract_target_suburb(f"{title} {address} {content}")
    postcode = postcode or _extract_postcode(f"{address} {content}")
    if not _is_target_area(suburb, postcode, f"{title} {address} {content}"):
        return None
    external_ref = _normalize_text(
        str(notice.get("gazette_id") or notice.get("id") or notice.get("slug") or "")
    )
    if not external_ref:
        external_ref = hashlib.md5(f"{title}|{address}|{content}".encode("utf-8")).hexdigest()
    occurred_at = (
        _parse_isoish(notice.get("published_at") or notice.get("publicationDate") or notice.get("pubDate"))
        or _parse_isoish(notice.get("published"))
    )
    occurred_iso = occurred_at.isoformat() if occurred_at else None
    return {
        "external_ref": external_ref,
        "signal_type": "probate",
        "title": title,
        "owner_name": owner_name or None,
        "address": address or None,
        "suburb": suburb or None,
        "postcode": postcode or None,
        "description": content[:1200],
        "occurred_at": occurred_iso,
        "source_name": "NSW Government Gazette",
        "source_url": notice.get("url") or notice.get("link") or "",
        "confidence_score": 78,
        "severity_score": 70,
        "payload": notice,
    }


async def _collect_probate_signals() -> List[Dict[str, Any]]:
    notices = _fetch_probate_notices()
    signals: List[Dict[str, Any]] = []
    for notice in notices:
        signal = _normalize_probate_notice(notice)
        if signal:
            signals.append(signal)
    return signals


def _fire_severity(category: str) -> int:
    label = _normalize_text(category).lower()
    if "emergency" in label:
        return 92
    if "watch and act" in label:
        return 82
    if "advice" in label:
        return 68
    return 58


def _normalize_fire_feature(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    properties = feature.get("properties") or {}
    category = _normalize_text(properties.get("category"))
    if "planned burn" in category.lower():
        return None
    title = _normalize_text(properties.get("title") or "Fire report")
    description = _normalize_text(properties.get("description") or "")
    location_blob = _normalize_text(" ".join(re.findall(r"LOCATION:\s*([^\n]+)", description, flags=re.I)))
    address = _extract_address_candidate(f"{title} {location_blob}")
    suburb, postcode = _extract_suburb_postcode(f"{location_blob} {title}")
    suburb = suburb or _extract_target_suburb(f"{location_blob} {title} {description}")
    postcode = postcode or _extract_postcode(f"{location_blob} {description}")
    if not _is_target_area(suburb, postcode, f"{title} {location_blob} {description}"):
        return None
    occurred_at = _parse_au_datetime(properties.get("pubDate")) or _parse_au_datetime(properties.get("updated"))
    external_ref = _normalize_text(properties.get("guid") or properties.get("link") or "")
    if not external_ref:
        external_ref = hashlib.md5(f"{title}|{location_blob}|{description}".encode("utf-8")).hexdigest()
    council_area_match = re.search(r"COUNCIL AREA:\s*([^\n]+)", description, flags=re.I)
    status_match = re.search(r"STATUS:\s*([^\n]+)", description, flags=re.I)
    incident_type_match = re.search(r"TYPE:\s*([^\n]+)", description, flags=re.I)
    return {
        "external_ref": external_ref,
        "signal_type": "fire_report",
        "title": title,
        "owner_name": None,
        "address": address or None,
        "suburb": suburb or None,
        "postcode": postcode or None,
        "description": description[:1200],
        "occurred_at": occurred_at,
        "source_name": "NSW Rural Fire Service",
        "source_url": properties.get("link") or _FIRE_FEED_URL,
        "confidence_score": 74,
        "severity_score": _fire_severity(category),
        "payload": {
            "category": category,
            "council_area": council_area_match.group(1).strip() if council_area_match else "",
            "status": status_match.group(1).strip() if status_match else "",
            "incident_type": incident_type_match.group(1).strip() if incident_type_match else "",
            "raw": properties,
        },
    }


async def _collect_fire_signals() -> List[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(_FIRE_FEED_URL, headers={"Accept": "application/json"})
        response.raise_for_status()
        payload = response.json()
    features = payload.get("features") or []
    signals: List[Dict[str, Any]] = []
    for feature in features:
        signal = _normalize_fire_feature(feature)
        if signal:
            signals.append(signal)
    return signals


async def _upsert_signal(
    session: AsyncSession,
    source_row: Dict[str, Any],
    signal: Dict[str, Any],
) -> tuple[Dict[str, Any], bool]:
    now = now_iso()
    external_ref = _normalize_text(signal.get("external_ref"))
    if not external_ref:
        external_ref = hashlib.md5(
            f"{source_row['source_key']}|{signal.get('title')}|{signal.get('address')}|{signal.get('occurred_at')}".encode("utf-8")
        ).hexdigest()
    existing_result = await session.execute(
        text("SELECT * FROM distress_signals WHERE source_key = :source_key AND external_ref = :external_ref"),
        {"source_key": source_row["source_key"], "external_ref": external_ref},
    )
    existing_row = existing_result.mappings().first()
    signal_payload = {
        "source_key": source_row["source_key"],
        "signal_type": _normalize_text(signal.get("signal_type") or source_row.get("signal_type") or "distress"),
        "external_ref": external_ref,
        "title": _normalize_text(signal.get("title") or signal.get("description") or "Distress signal"),
        "owner_name": _normalize_owner_name(signal.get("owner_name")) or None,
        "address": _normalize_text(signal.get("address")) or None,
        "suburb": _normalize_text(signal.get("suburb")) or None,
        "postcode": _normalize_text(signal.get("postcode")) or None,
        "description": _normalize_text(signal.get("description"))[:1200],
        "occurred_at": signal.get("occurred_at"),
        "source_name": _normalize_text(signal.get("source_name") or source_row.get("label") or source_row["source_key"]),
        "source_url": _normalize_text(signal.get("source_url") or source_row.get("source_url")) or None,
        "confidence_score": float(signal.get("confidence_score") or 70),
        "severity_score": int(signal.get("severity_score") or 50),
        "status": _normalize_text(signal.get("status") or "captured") or "captured",
        "lead_ids": signal.get("lead_ids") or [],
        "inferred_owner_matches": signal.get("inferred_owner_matches") or [],
        "inferred_property_matches": signal.get("inferred_property_matches") or [],
        "payload": signal.get("payload") or {},
        "updated_at": now,
    }
    if existing_row:
        signal_payload["id"] = existing_row["id"]
        await session.execute(
            text(
                """
                UPDATE distress_signals
                SET signal_type = :signal_type,
                    title = :title,
                    owner_name = :owner_name,
                    address = :address,
                    suburb = :suburb,
                    postcode = :postcode,
                    description = :description,
                    occurred_at = :occurred_at,
                    source_name = :source_name,
                    source_url = :source_url,
                    confidence_score = :confidence_score,
                    severity_score = :severity_score,
                    status = :status,
                    lead_ids = :lead_ids,
                    inferred_owner_matches = :inferred_owner_matches,
                    inferred_property_matches = :inferred_property_matches,
                    payload = :payload,
                    updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {
                **signal_payload,
                "lead_ids": _json_dumps(signal_payload["lead_ids"]),
                "inferred_owner_matches": _json_dumps(signal_payload["inferred_owner_matches"]),
                "inferred_property_matches": _json_dumps(signal_payload["inferred_property_matches"]),
                "payload": _json_dumps(signal_payload["payload"]),
            },
        )
        return signal_payload, False

    signal_payload["id"] = str(uuid.uuid4())
    signal_payload["created_at"] = now
    await session.execute(
        text(
            """
            INSERT INTO distress_signals (
                id, source_key, signal_type, external_ref, title, owner_name, address, suburb, postcode,
                description, occurred_at, source_name, source_url, confidence_score, severity_score, status,
                lead_ids, inferred_owner_matches, inferred_property_matches, payload, created_at, updated_at
            ) VALUES (
                :id, :source_key, :signal_type, :external_ref, :title, :owner_name, :address, :suburb, :postcode,
                :description, :occurred_at, :source_name, :source_url, :confidence_score, :severity_score, :status,
                :lead_ids, :inferred_owner_matches, :inferred_property_matches, :payload, :created_at, :updated_at
            )
            """
        ),
        {
            **signal_payload,
            "lead_ids": _json_dumps(signal_payload["lead_ids"]),
            "inferred_owner_matches": _json_dumps(signal_payload["inferred_owner_matches"]),
            "inferred_property_matches": _json_dumps(signal_payload["inferred_property_matches"]),
            "payload": _json_dumps(signal_payload["payload"]),
        },
    )
    return signal_payload, True


async def _find_matching_leads(session: AsyncSession, signal: Dict[str, Any]) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    address = _normalize_text(signal.get("address"))
    owner_name = _normalize_owner_name(signal.get("owner_name"))
    suburb = _normalize_text(signal.get("suburb"))
    postcode = _normalize_text(signal.get("postcode"))
    address_key = _normalize_key(address)
    owner_key = _normalize_key(owner_name)

    async def add_rows(rows: List[Dict[str, Any]]) -> None:
        for row in rows:
            lead_id = str(row.get("id") or "")
            if not lead_id or lead_id in seen_ids:
                continue
            match_type = ""
            confidence = 0.0
            row_address_key = _normalize_key(row.get("address"))
            row_owner_key = _normalize_key(row.get("owner_name"))
            if address_key and row_address_key == address_key:
                match_type = "address_match"
                confidence = 96.0
            elif owner_key and suburb and row_owner_key == owner_key and _normalize_key(row.get("suburb")) == _normalize_key(suburb):
                match_type = "owner_suburb_match"
                confidence = 82.0
            elif suburb and _normalize_key(row.get("suburb")) == _normalize_key(suburb):
                match_type = "suburb_match"
                confidence = 66.0
            if not match_type:
                continue
            seen_ids.add(lead_id)
            row_payload = dict(row)
            row_payload["match_type"] = match_type
            row_payload["match_confidence"] = confidence
            matches.append(row_payload)

    if address:
        address_rows = await session.execute(
            text("SELECT * FROM leads WHERE LOWER(COALESCE(address, '')) = :address LIMIT 12"),
            {"address": address.lower()},
        )
        await add_rows([dict(row) for row in address_rows.mappings().all()])

    if not matches and (suburb or postcode):
        clauses: List[str] = []
        params: Dict[str, Any] = {}
        if suburb:
            clauses.append("LOWER(COALESCE(suburb, '')) = :suburb")
            params["suburb"] = suburb.lower()
        if postcode:
            clauses.append("postcode = :postcode")
            params["postcode"] = postcode
        candidate_rows = await session.execute(
            text(
                f"""
                SELECT * FROM leads
                WHERE {' OR '.join(clauses)}
                ORDER BY COALESCE(call_today_score, 0) DESC, COALESCE(evidence_score, 0) DESC
                LIMIT 150
                """
            ),
            params,
        )
        await add_rows([dict(row) for row in candidate_rows.mappings().all()])

    matches.sort(
        key=lambda row: (
            -float(row.get("match_confidence") or 0),
            -int(row.get("call_today_score") or 0),
            str(row.get("address") or ""),
        )
    )
    return matches[:12]


async def _annotate_lead_with_signal(
    session: AsyncSession,
    lead_id: str,
    signal: Dict[str, Any],
    match_type: str,
    confidence: float,
) -> None:
    lead = await _load_lead_row(session, lead_id)
    if not lead:
        return
    now = now_iso()
    source_tags = _dedupe_strings(
        _json_loads(lead.get("source_tags"), []) + ["distress_intel", signal["source_key"], signal["signal_type"]]
    )
    risk_flags = _dedupe_strings(
        _json_loads(lead.get("risk_flags"), []) + [f"distress_{signal['signal_type']}"]
    )
    source_evidence = _dedupe_strings(
        _json_loads(lead.get("source_evidence"), []) + [_signal_evidence_note(signal)]
    )
    activity_log = _json_loads(lead.get("activity_log"), [])
    activity_log.append(
        {
            "ts": now,
            "action": "distress_signal_linked",
            "signal_type": signal["signal_type"],
            "title": signal["title"],
            "match_type": match_type,
            "confidence": confidence,
            "source_key": signal["source_key"],
        }
    )
    evidence_score = max(_safe_int(lead.get("evidence_score")), min(100, len(source_evidence) * 12))
    heat_score = max(_safe_int(lead.get("heat_score")), _safe_int(signal.get("severity_score"), 50))
    call_today_score = max(_safe_int(lead.get("call_today_score")), min(100, heat_score + 8))
    confidence_score = max(_safe_int(lead.get("confidence_score")), _safe_int(signal.get("confidence_score"), 70))
    await session.execute(
        text(
            """
            UPDATE leads
            SET owner_name = :owner_name,
                suburb = :suburb,
                postcode = :postcode,
                source_tags = :source_tags,
                risk_flags = :risk_flags,
                source_evidence = :source_evidence,
                activity_log = :activity_log,
                heat_score = :heat_score,
                confidence_score = :confidence_score,
                evidence_score = :evidence_score,
                call_today_score = :call_today_score,
                route_queue = CASE WHEN COALESCE(route_queue, '') = '' THEN :route_queue ELSE route_queue END,
                queue_bucket = CASE WHEN COALESCE(queue_bucket, '') = '' THEN :queue_bucket ELSE queue_bucket END,
                lead_archetype = CASE WHEN COALESCE(lead_archetype, '') = '' THEN :lead_archetype ELSE lead_archetype END,
                updated_at = :updated_at
            WHERE id = :id
            """
        ),
        {
            "id": lead_id,
            "owner_name": lead.get("owner_name") or signal.get("owner_name"),
            "suburb": lead.get("suburb") or signal.get("suburb"),
            "postcode": lead.get("postcode") or signal.get("postcode"),
            "source_tags": _json_dumps(source_tags),
            "risk_flags": _json_dumps(risk_flags),
            "source_evidence": _json_dumps(source_evidence),
            "activity_log": _json_dumps(activity_log[-80:]),
            "heat_score": heat_score,
            "confidence_score": confidence_score,
            "evidence_score": evidence_score,
            "call_today_score": call_today_score,
            "route_queue": _DEFAULT_ROUTE_QUEUE,
            "queue_bucket": "enrichment",
            "lead_archetype": "distress_signal",
            "updated_at": now,
        },
    )


async def _create_signal_links(
    session: AsyncSession,
    signal: Dict[str, Any],
    matches: List[Dict[str, Any]],
) -> int:
    existing_result = await session.execute(
        text("SELECT lead_id FROM distress_signal_links WHERE signal_id = :signal_id"),
        {"signal_id": signal["id"]},
    )
    existing_lead_ids = {str(row["lead_id"]) for row in existing_result.mappings().all()}
    inserted = 0
    now = now_iso()
    for match in matches:
        lead_id = str(match.get("id") or "")
        if not lead_id:
            continue
        await _annotate_lead_with_signal(
            session,
            lead_id=lead_id,
            signal=signal,
            match_type=str(match.get("match_type") or "match"),
            confidence=float(match.get("match_confidence") or 70),
        )
        if lead_id in existing_lead_ids:
            continue
        await session.execute(
            text(
                """
                INSERT INTO distress_signal_links (
                    id, signal_id, lead_id, link_type, confidence_score, rationale, created_at
                ) VALUES (
                    :id, :signal_id, :lead_id, :link_type, :confidence_score, :rationale, :created_at
                )
                """
            ),
            {
                "id": str(uuid.uuid4()),
                "signal_id": signal["id"],
                "lead_id": lead_id,
                "link_type": match.get("match_type") or "match",
                "confidence_score": float(match.get("match_confidence") or 70),
                "rationale": f"{match.get('match_type') or 'match'} via distress source {signal['source_key']}",
                "created_at": now,
            },
        )
        inserted += 1
        existing_lead_ids.add(lead_id)
    return inserted


async def _create_lead_from_signal(
    session: AsyncSession,
    signal: Dict[str, Any],
) -> Optional[str]:
    address = _normalize_text(signal.get("address"))
    suburb = _normalize_text(signal.get("suburb"))
    postcode = _normalize_text(signal.get("postcode"))
    if not address or not _is_target_area(suburb, postcode, f"{address} {suburb} {postcode}"):
        return None
    now = now_iso()
    lead_id = hashlib.md5(f"distress-lead|{signal['source_key']}|{signal['external_ref']}".encode("utf-8")).hexdigest()
    source_tags = _dedupe_strings(["distress_intel", signal["source_key"], signal["signal_type"]])
    risk_flags = _dedupe_strings([f"distress_{signal['signal_type']}"])
    source_evidence = [_signal_evidence_note(signal)]
    activity_log = [
        {
            "ts": now,
            "action": "distress_signal_ingested",
            "signal_type": signal["signal_type"],
            "title": signal["title"],
            "source_key": signal["source_key"],
        }
    ]
    try:
        await session.execute(
            text(
                """
                INSERT INTO leads (
                    id, address, suburb, postcode, owner_name, trigger_type, record_type, status,
                    route_queue, queue_bucket, lead_archetype, source_tags, risk_flags, source_evidence,
                    activity_log, date_found, created_at, updated_at, heat_score, confidence_score,
                    readiness_score, conversion_score, call_today_score, evidence_score
                ) VALUES (
                    :id, :address, :suburb, :postcode, :owner_name, :trigger_type, :record_type, :status,
                    :route_queue, :queue_bucket, :lead_archetype, :source_tags, :risk_flags, :source_evidence,
                    :activity_log, :date_found, :created_at, :updated_at, :heat_score, :confidence_score,
                    :readiness_score, :conversion_score, :call_today_score, :evidence_score
                )
                """
            ),
            {
                "id": lead_id,
                "address": address,
                "suburb": suburb or None,
                "postcode": postcode or None,
                "owner_name": signal.get("owner_name"),
                "trigger_type": signal["signal_type"],
                "record_type": "distress_signal",
                "status": "captured",
                "route_queue": _DEFAULT_ROUTE_QUEUE,
                "queue_bucket": "enrichment",
                "lead_archetype": "distress_signal",
                "source_tags": _json_dumps(source_tags),
                "risk_flags": _json_dumps(risk_flags),
                "source_evidence": _json_dumps(source_evidence),
                "activity_log": _json_dumps(activity_log),
                "date_found": signal.get("occurred_at") or now,
                "created_at": now,
                "updated_at": now,
                "heat_score": min(100, max(55, _safe_int(signal.get("severity_score"), 60))),
                "confidence_score": min(100, max(45, _safe_int(signal.get("confidence_score"), 70))),
                "readiness_score": 58,
                "conversion_score": 40,
                "call_today_score": min(100, max(60, _safe_int(signal.get("severity_score"), 60) + 6)),
                "evidence_score": 28,
            },
        )
        return lead_id
    except Exception:
        existing = await session.execute(
            text("SELECT id FROM leads WHERE LOWER(COALESCE(address, '')) = :address"),
            {"address": address.lower()},
        )
        row = existing.mappings().first()
        return str(row["id"]) if row else None


# ── New live source collectors ────────────────────────────────────────────────

_LEGACY_COM_AU_URL = "https://www.legacy.com/api/obituaries"
_LEGACY_AFFILIATE_ID = "2261"  # Sydney Morning Herald
_LEGACY_REGION_ID = "35"       # NSW


async def _collect_legacy_obituary_signals() -> List[Dict[str, Any]]:
    """
    Fetch NSW obituaries from Legacy.com undocumented JSON API.
    No auth, no rate-limit documented. Paginates up to 5 pages.
    """
    signals: List[Dict[str, Any]] = []
    try:
        async with httpx.AsyncClient(timeout=20, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }) as client:
            for page in range(1, 6):
                params = {
                    "affiliateid": _LEGACY_AFFILIATE_ID,
                    "regionid": _LEGACY_REGION_ID,
                    "page": str(page),
                }
                resp = await client.get(_LEGACY_COM_AU_URL, params=params)
                if resp.status_code != 200:
                    break
                data = resp.json()
                obits = data if isinstance(data, list) else data.get("obituaries", data.get("results", []))
                if not obits:
                    break
                for obit in obits:
                    full_name = _normalize_text(obit.get("fullName") or obit.get("name") or "")
                    city = _normalize_text(obit.get("city") or obit.get("location") or "")
                    state = _normalize_text(obit.get("state") or "")
                    if state and state.upper() not in ("NSW", "NEW SOUTH WALES"):
                        continue
                    # Check if city matches a target suburb
                    if not _is_target_area(city, None, f"{city} {full_name}"):
                        continue
                    obit_id = str(obit.get("id") or obit.get("obituaryId") or "")
                    external_ref = obit_id or hashlib.md5(f"legacy:{full_name}:{city}".encode()).hexdigest()
                    pub_date = _normalize_text(obit.get("publishDate") or obit.get("createdDate") or "")
                    signals.append({
                        "external_ref": external_ref,
                        "signal_type": "obituary",
                        "title": f"Obituary: {full_name}" if full_name else "Obituary notice",
                        "owner_name": full_name,
                        "address": None,
                        "suburb": city,
                        "postcode": None,
                        "description": f"Deceased: {full_name}. Location: {city}, NSW. Source: Legacy.com",
                        "occurred_at": pub_date or None,
                        "source_name": "Legacy.com",
                        "source_url": f"https://www.legacy.com/obituaries/{obit_id}" if obit_id else _LEGACY_COM_AU_URL,
                        "confidence_score": 72,
                        "severity_score": 62,
                        "payload": obit,
                    })
                if len(obits) < 10:
                    break
    except Exception as exc:
        _logger.warning("[Legacy.com] obituary fetch error: %s", exc)
    return signals


_ASIC_INSOLVENCY_URL = "https://insolvencynotices.asic.gov.au/api/v1/notices/search"


async def _collect_asic_insolvency_signals() -> List[Dict[str, Any]]:
    """
    Query ASIC insolvency notices API for NSW businesses.
    Filters for suburbs in our target area using address-level matching.
    """
    signals: List[Dict[str, Any]] = []
    try:
        params = {
            "state": "NSW",
            "noticeType": "Appointment of Administrator",
            "pageSize": "100",
        }
        async with httpx.AsyncClient(timeout=20, headers={
            "User-Agent": "Mozilla/5.0 (compatible; PropertyIntelBot/1.0)",
            "Accept": "application/json",
        }) as client:
            resp = await client.get(_ASIC_INSOLVENCY_URL, params=params)
            if resp.status_code != 200:
                # Fallback: scrape HTML search results
                html_params = {"q": "NSW", "s": "0"}
                resp = await client.get("https://insolvencynotices.asic.gov.au", params=html_params)
                return signals  # HTML scraping requires selectolax — skip for now
            notices = resp.json()
            notice_list = notices if isinstance(notices, list) else notices.get("notices", notices.get("results", []))
            for notice in notice_list:
                address_raw = _normalize_text(
                    notice.get("address") or notice.get("registeredAddress") or ""
                )
                company = _normalize_text(notice.get("companyName") or notice.get("name") or "")
                suburb, postcode = _extract_suburb_postcode(address_raw)
                if not _is_target_area(suburb, postcode, f"{address_raw} {company}"):
                    continue
                external_ref = _normalize_text(str(notice.get("noticeId") or notice.get("id") or ""))
                if not external_ref:
                    external_ref = hashlib.md5(f"asic:{company}:{address_raw}".encode()).hexdigest()
                notice_type = _normalize_text(notice.get("noticeType") or "Insolvency")
                signals.append({
                    "external_ref": external_ref,
                    "signal_type": "insolvency",
                    "title": f"{notice_type}: {company}" if company else notice_type,
                    "owner_name": company,
                    "address": address_raw or None,
                    "suburb": suburb,
                    "postcode": postcode,
                    "description": f"ASIC: {notice_type}. Company: {company}. Address: {address_raw}",
                    "occurred_at": _normalize_text(notice.get("datePublished") or notice.get("date") or ""),
                    "source_name": "ASIC Insolvency Notices",
                    "source_url": f"https://insolvencynotices.asic.gov.au/notice/{external_ref}",
                    "confidence_score": 85,
                    "severity_score": 70,
                    "payload": notice,
                })
    except Exception as exc:
        _logger.warning("[ASIC] insolvency fetch error: %s", exc)
    return signals


_NEWSAPI_BASE = "https://newsapi.org/v2/everything"
_NEWSAPI_DISTRESS_QUERIES = [
    "mortgage default NSW eviction",
    "bankruptcy \"Hills District\" OR Windsor OR Penrith OR Wollongong",
    "fire damage property NSW Hills",
    "divorce property settlement NSW",
]


async def _collect_newsapi_signals() -> List[Dict[str, Any]]:
    """
    Search NewsAPI for distress signals in target area.
    Requires NEWSAPI_KEY env var. Skips gracefully if missing.
    """
    import os
    api_key = os.environ.get("NEWSAPI_KEY", "")
    if not api_key:
        return []

    signals: List[Dict[str, Any]] = []
    from_date = (datetime.now(timezone.utc) - __import__("datetime").timedelta(days=7)).strftime("%Y-%m-%d")

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            for query in _NEWSAPI_DISTRESS_QUERIES:
                params = {
                    "q": query,
                    "apiKey": api_key,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "from": from_date,
                    "pageSize": "20",
                }
                resp = await client.get(_NEWSAPI_BASE, params=params)
                if resp.status_code != 200:
                    continue
                articles = resp.json().get("articles", [])
                for article in articles:
                    title = _normalize_text(article.get("title") or "")
                    description = _normalize_text(article.get("description") or "")
                    content = f"{title} {description}"
                    suburb = _extract_target_suburb(content)
                    postcode = _extract_postcode(content)
                    if not suburb and not _is_target_area(None, postcode, content):
                        continue
                    external_ref = hashlib.md5(f"newsapi:{article.get('url', '')}".encode()).hexdigest()
                    published_at = _normalize_text(article.get("publishedAt") or "")
                    signals.append({
                        "external_ref": external_ref,
                        "signal_type": "news",
                        "title": title[:200] if title else "News signal",
                        "owner_name": None,
                        "address": None,
                        "suburb": suburb,
                        "postcode": postcode,
                        "description": description[:800],
                        "occurred_at": published_at or None,
                        "source_name": article.get("source", {}).get("name") or "NewsAPI",
                        "source_url": article.get("url") or _NEWSAPI_BASE,
                        "confidence_score": 55,
                        "severity_score": 45,
                        "payload": {"query": query, "url": article.get("url")},
                    })
    except Exception as exc:
        _logger.warning("[NewsAPI] distress fetch error: %s", exc)
    return signals


# ── Orchestrator: run one source end-to-end ───────────────────────────────────

_SOURCE_COLLECTORS = {
    "nsw_probate_gazette": _collect_probate_signals,
    "nsw_rfs_major_incidents": _collect_fire_signals,
    "legacy_com_obituaries": _collect_legacy_obituary_signals,
    "asic_insolvency": _collect_asic_insolvency_signals,
    "newsapi_distress": _collect_newsapi_signals,
}

_SOURCE_TICKER_TYPES = {
    "nsw_probate_gazette": "PROBATE",
    "nsw_rfs_major_incidents": "FIRE",
    "legacy_com_obituaries": "OBITUARY",
    "asic_insolvency": "INSOLVENCY",
    "newsapi_distress": "NEWS",
}


async def run_distress_source(
    session: AsyncSession,
    source_key: str,
    requested_by: str = "scheduler",
) -> Dict[str, Any]:
    """
    Run one distress source end-to-end:
    1. Fetch signals via source-specific collector
    2. Upsert each signal to distress_signals table
    3. Find matching leads; annotate or create leads
    4. Push new signals to ticker_events (WebSocket broadcast)
    5. Update distress_sources run metadata

    Returns: {source_key, signals_fetched, signals_new, leads_linked, leads_created}
    """
    from services.ticker_push import push_ticker_event

    await ensure_distress_sources(session)
    source_row = await _fetch_source_row(session, source_key)
    if not source_row:
        return {"error": f"Source '{source_key}' not found in distress_sources"}
    if not source_row.get("enabled"):
        return {"source_key": source_key, "skipped": True, "reason": "disabled"}

    collector = _SOURCE_COLLECTORS.get(source_key)
    if not collector:
        return {"source_key": source_key, "skipped": True, "reason": "no_collector"}

    run_id = str(uuid.uuid4())
    now = now_iso()
    await session.execute(
        text("""
            INSERT INTO distress_runs (id, source_id, source_key, requested_by, status,
                records_scanned, records_created, records_linked, records_created_as_leads,
                started_at, updated_at)
            VALUES (:id, :source_id, :source_key, :requested_by, 'running',
                0, 0, 0, 0, :now, :now)
        """),
        {"id": run_id, "source_id": source_row["id"], "source_key": source_key,
         "requested_by": requested_by, "now": now},
    )
    await session.commit()

    signals_new = 0
    leads_linked = 0
    leads_created = 0
    error_summary = None
    ticker_type = _SOURCE_TICKER_TYPES.get(source_key, "MARKET_SIGNAL")
    raw_signals: List[Dict[str, Any]] = []

    try:
        raw_signals = await collector()
        for signal in raw_signals:
            upserted_signal, is_new = await _upsert_signal(session, source_row, signal)
            if is_new:
                signals_new += 1
                linked_lead_ids: List[str] = []
                matches = await _find_matching_leads(session, upserted_signal)
                if matches:
                    links = await _create_signal_links(session, upserted_signal, matches)
                    leads_linked += links
                    linked_lead_ids = [str(match.get("id") or "") for match in matches if match.get("id")]
                elif upserted_signal.get("address"):
                    new_lead_id = await _create_lead_from_signal(session, upserted_signal)
                    if new_lead_id:
                        leads_created += 1
                        upserted_signal["lead_id_created"] = new_lead_id
                        linked_lead_ids = [new_lead_id]
                await _persist_signal_relationships(session, upserted_signal, linked_lead_ids, matches if matches else [])

                # Push new signal to ticker bar
                await push_ticker_event(
                    session,
                    event_type=ticker_type,
                    source=source_key,
                    address=upserted_signal.get("address") or "",
                    suburb=upserted_signal.get("suburb") or "",
                    postcode=upserted_signal.get("postcode") or "",
                    owner_name=upserted_signal.get("owner_name") or "",
                    heat_score=int(upserted_signal.get("severity_score") or 0),
                    lead_id=upserted_signal.get("lead_id_created", ""),
                    headline=upserted_signal.get("title", "")[:120],
                    extra={"source_key": source_key, "confidence": upserted_signal.get("confidence_score")},
                )
        await session.commit()
    except Exception as exc:
        error_summary = str(exc)[:500]
        _logger.error("[distress] run_distress_source %s error: %s", source_key, exc)

    completed_at = now_iso()
    await session.execute(
        text("""
            UPDATE distress_runs
            SET status = :status, records_scanned = :scanned, records_created = :new,
                records_linked = :linked, records_created_as_leads = :leads_created,
                error_summary = :error_summary, completed_at = :completed_at, updated_at = :completed_at
            WHERE id = :id
        """),
        {
            "id": run_id,
            "status": "error" if error_summary else "completed",
            "scanned": len(raw_signals) if "raw_signals" in dir() else 0,
            "new": signals_new,
            "linked": leads_linked,
            "leads_created": leads_created,
            "error_summary": error_summary,
            "completed_at": completed_at,
        },
    )
    await session.execute(
        text("""
            UPDATE distress_sources
            SET last_run_at = :now, last_success_at = :success_at, last_error = :error_summary, updated_at = :now
            WHERE source_key = :source_key
        """),
        {
            "source_key": source_key,
            "now": completed_at,
            "success_at": completed_at if not error_summary else source_row.get("last_success_at"),
            "error_summary": error_summary,
        },
    )
    await session.commit()

    return {
        "source_key": source_key,
        "run_id": run_id,
        "signals_fetched": len(raw_signals),
        "signals_new": signals_new,
        "leads_linked": leads_linked,
        "leads_created": leads_created,
        "error": error_summary,
    }


async def _persist_signal_relationships(
    session: AsyncSession,
    signal: Dict[str, Any],
    lead_ids: List[str],
    matches: List[Dict[str, Any]],
) -> None:
    if not signal.get("id"):
        return
    linked_ids = _dedupe_strings(lead_ids)
    owner_matches = _dedupe_strings([match.get("owner_name") for match in matches if match.get("owner_name")])
    property_matches = _dedupe_strings(
        [match.get("address") for match in matches if match.get("address")]
        + ([signal.get("address")] if signal.get("address") and linked_ids else [])
    )
    await session.execute(
        text(
            """
            UPDATE distress_signals
            SET lead_ids = :lead_ids,
                inferred_owner_matches = :owner_matches,
                inferred_property_matches = :property_matches,
                updated_at = :updated_at
            WHERE id = :id
            """
        ),
        {
            "id": signal["id"],
            "lead_ids": _json_dumps(linked_ids),
            "owner_matches": _json_dumps(owner_matches),
            "property_matches": _json_dumps(property_matches),
            "updated_at": now_iso(),
        },
    )
    signal["lead_ids"] = linked_ids
    signal["inferred_owner_matches"] = owner_matches
    signal["inferred_property_matches"] = property_matches


async def run_all_enabled_distress_sources(
    session: AsyncSession,
    requested_by: str = "operator",
) -> Dict[str, Any]:
    sources = await ensure_distress_sources(session)
    results: List[Dict[str, Any]] = []
    for source in sources.sources:
        if not source.enabled or source.mode == "manual_feed":
            continue
        results.append(await run_distress_source(session, source.source_key, requested_by=requested_by))
    return {"results": results, "count": len(results)}


async def ingest_manual_distress_signals(
    session: AsyncSession,
    body: DistressManualIngestRequest,
) -> Dict[str, Any]:
    await ensure_distress_sources(session)
    source_row = await _fetch_source_row(session, body.source_key)
    if not source_row:
        raise ValueError(f"Source '{body.source_key}' not found")

    run_id = str(uuid.uuid4())
    now = now_iso()
    await session.execute(
        text(
            """
            INSERT INTO distress_runs (
                id, source_id, source_key, requested_by, status,
                records_scanned, records_created, records_linked, records_created_as_leads,
                started_at, updated_at
            ) VALUES (
                :id, :source_id, :source_key, :requested_by, 'running',
                0, 0, 0, 0, :now, :now
            )
            """
        ),
        {
            "id": run_id,
            "source_id": source_row["id"],
            "source_key": body.source_key,
            "requested_by": body.requested_by,
            "now": now,
        },
    )
    await session.commit()

    signals_new = 0
    leads_linked = 0
    leads_created = 0
    created_signal_ids: List[str] = []
    error_summary = None

    try:
        for item in body.signals:
            signal = {
                "external_ref": item.external_ref,
                "signal_type": item.signal_type or source_row.get("signal_type") or "distress",
                "title": item.title,
                "owner_name": item.owner_name,
                "address": item.address,
                "suburb": item.suburb,
                "postcode": item.postcode,
                "description": item.description,
                "occurred_at": item.occurred_at,
                "source_name": item.source_name or source_row.get("label") or body.source_key,
                "source_url": item.source_url or source_row.get("source_url"),
                "confidence_score": item.confidence_score,
                "severity_score": item.severity_score,
                "payload": item.payload,
            }
            upserted_signal, is_new = await _upsert_signal(session, source_row, signal)
            if is_new:
                signals_new += 1
                created_signal_ids.append(str(upserted_signal["id"]))
            linked_lead_ids: List[str] = []
            matches = await _find_matching_leads(session, upserted_signal)
            if matches:
                links = await _create_signal_links(session, upserted_signal, matches)
                leads_linked += links
                linked_lead_ids = [str(match.get("id") or "") for match in matches if match.get("id")]
            elif upserted_signal.get("address"):
                new_lead_id = await _create_lead_from_signal(session, upserted_signal)
                if new_lead_id:
                    leads_created += 1
                    linked_lead_ids = [new_lead_id]
            await _persist_signal_relationships(session, upserted_signal, linked_lead_ids, matches if matches else [])
        await session.commit()
    except Exception as exc:
        error_summary = str(exc)[:500]
        _logger.error("[distress] manual ingest %s error: %s", body.source_key, exc)

    completed_at = now_iso()
    await session.execute(
        text(
            """
            UPDATE distress_runs
            SET status = :status,
                records_scanned = :scanned,
                records_created = :created,
                records_linked = :linked,
                records_created_as_leads = :leads_created,
                error_summary = :error_summary,
                completed_at = :completed_at,
                updated_at = :completed_at
            WHERE id = :id
            """
        ),
        {
            "id": run_id,
            "status": "error" if error_summary else "completed",
            "scanned": len(body.signals),
            "created": signals_new,
            "linked": leads_linked,
            "leads_created": leads_created,
            "error_summary": error_summary,
            "completed_at": completed_at,
        },
    )
    await session.execute(
        text(
            """
            UPDATE distress_sources
            SET last_run_at = :now,
                last_success_at = :success_at,
                last_error = :error_summary,
                updated_at = :now
            WHERE source_key = :source_key
            """
        ),
        {
            "source_key": body.source_key,
            "now": completed_at,
            "success_at": completed_at if not error_summary else source_row.get("last_success_at"),
            "error_summary": error_summary,
        },
    )
    await session.commit()

    return {
        "source_key": body.source_key,
        "run_id": run_id,
        "signals_received": len(body.signals),
        "signals_created": signals_new,
        "signals": created_signal_ids,
        "leads_linked": leads_linked,
        "leads_created": leads_created,
        "error": error_summary,
    }


async def get_enabled_distress_scheduler_sources(
    session: AsyncSession,
) -> List[Dict[str, Any]]:
    sources = await ensure_distress_sources(session)
    scheduled: List[Dict[str, Any]] = []
    for source in sources.sources:
        if not source.enabled or source.mode == "manual_feed":
            continue
        scheduled.append(
            {
                "source_key": source.source_key,
                "cadence_minutes": source.cadence_minutes,
            }
        )
    return scheduled


def _payload_finance_event_date(payload: Any) -> Optional[str]:
    payload_obj = _json_loads(payload, {})
    if not isinstance(payload_obj, dict):
        return None
    for key in (
        "recorded_finance_event_date",
        "finance_event_date",
        "latest_mortgage_date",
        "mortgage_recorded_at",
        "financeRecordedAt",
    ):
        value = _normalize_text(payload_obj.get(key))
        if value:
            return value
    return None


async def get_distress_landlord_watch(
    session: AsyncSession,
    min_properties: int = 5,
    min_years_since_finance_event: int = 10,
) -> DistressLandlordWatchResponse:
    rows = await session.execute(
        text(
            """
            SELECT
                owner_name,
                COUNT(DISTINCT LOWER(COALESCE(address, ''))) AS property_count,
                SUM(CASE
                    WHEN LOWER(COALESCE(owner_type, '')) LIKE '%rent%'
                      OR LOWER(COALESCE(owner_type, '')) LIKE '%invest%'
                    THEN 1 ELSE 0
                END) AS investor_records,
                MAX(COALESCE(last_settlement_date, settlement_date, date_found, created_at)) AS newest_record_date,
                GROUP_CONCAT(DISTINCT address) AS addresses,
                GROUP_CONCAT(DISTINCT id) AS lead_ids
            FROM leads
            WHERE TRIM(COALESCE(owner_name, '')) != ''
              AND TRIM(COALESCE(owner_name, '')) NOT IN ('-', '--')
            GROUP BY owner_name
            HAVING COUNT(DISTINCT LOWER(COALESCE(address, ''))) >= :min_properties
            ORDER BY property_count DESC, owner_name ASC
            """
        ),
        {"min_properties": max(1, int(min_properties or 5))},
    )

    landlords: List[DistressLandlordCandidate] = []
    for row in rows.mappings().all():
        lead_ids = [item for item in _dedupe_strings(str(row.get("lead_ids") or "").split(",")) if item]
        if not lead_ids:
            continue
        # The SQLAlchemy text IN binding above is not portable on SQLite; use a simple dynamic query instead.
        placeholders = ", ".join(f":lead_id_{idx}" for idx, _ in enumerate(lead_ids))
        dynamic_rows = await session.execute(
            text(
                f"""
                SELECT s.signal_type, s.payload, s.occurred_at
                FROM distress_signals s
                JOIN distress_signal_links l ON l.signal_id = s.id
                WHERE l.lead_id IN ({placeholders})
                """
            ),
            {f"lead_id_{idx}": lead_id for idx, lead_id in enumerate(lead_ids)},
        )
        signal_rows = dynamic_rows.mappings().all()
        delinquent_tax_signal_count = sum(1 for signal in signal_rows if signal.get("signal_type") in _TAX_SIGNAL_TYPES)
        lien_signal_count = sum(1 for signal in signal_rows if signal.get("signal_type") in _LIEN_SIGNAL_TYPES)
        if delinquent_tax_signal_count + lien_signal_count == 0:
            continue

        finance_dates = [
            _payload_finance_event_date(signal.get("payload")) or _normalize_text(signal.get("occurred_at"))
            for signal in signal_rows
            if signal.get("signal_type") in (_TAX_SIGNAL_TYPES | _LIEN_SIGNAL_TYPES)
        ]
        parsed_finance_dates = [_parse_isoish(value) for value in finance_dates if _normalize_text(value)]
        parsed_finance_dates = [value for value in parsed_finance_dates if value]
        if not parsed_finance_dates:
            continue
        most_recent_finance_event = max(parsed_finance_dates)
        years_since = _years_since(most_recent_finance_event.isoformat())
        if years_since is None or years_since < float(min_years_since_finance_event):
            continue

        landlords.append(
            DistressLandlordCandidate(
                owner_name=str(row.get("owner_name") or ""),
                property_count=int(row.get("property_count") or 0),
                investor_records=int(row.get("investor_records") or 0),
                newest_record_date=row.get("newest_record_date"),
                years_since_recorded_finance_event=years_since,
                inferred_no_recent_refi=True,
                delinquent_tax_signal_count=delinquent_tax_signal_count,
                lien_signal_count=lien_signal_count,
                addresses=_dedupe_strings(str(row.get("addresses") or "").split(",")),
                lead_ids=lead_ids,
                inference_note="Strict-proof candidate based on linked distress payload finance-event dates.",
            )
        )

    landlords.sort(
        key=lambda item: (
            -int(item.property_count or 0),
            -(int(item.delinquent_tax_signal_count or 0) + int(item.lien_signal_count or 0)),
            item.owner_name.lower(),
        )
    )
    return DistressLandlordWatchResponse(landlords=landlords)
