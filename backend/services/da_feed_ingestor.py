"""
NSW Planning Portal DA Feed Ingestor
=====================================
Pulls fresh Development Application (DA) data from the NSW Planning Portal's
DAApplicationTracker API and inserts matching leads into the SQLite database.

Confirmed working endpoint (verified 2026-03-19):
  POST https://api.apps1.nsw.gov.au/eplanning/data/v0/DAApplicationTracker
  No authentication required. Content-Type: application/json.

Response schema (GeoJSON FeatureCollection):
  Top-level: TotalCount, TotalPages, PageSize, PageNumber
  features[].properties:
    PLANNING_PORTAL_APP_NUMBER  — e.g. "DA-123456" or "CDC-206995"
    COUNCIL_NAME                — e.g. "Hawkesbury City Council"
    STATUS                      — e.g. "Under Assessment", "Approved"
    TYPE_OF_DEVELOPMENT         — comma-separated list, e.g. "Dwelling house,Residential Accommodation"
    APPLICATION_TYPE            — e.g. "Development Application", "Complying Development Certificate Application"
    LODGEMENT_DATE              — ISO date string, e.g. "2026-03-16"
    DETERMINATION_DATE          — ISO date string or absent
    FULL_ADDRESS                — e.g. "131 STOCKWELL ROAD OAKVILLE 2765"
  features[].geometry.coordinates: [lng, lat]

Strategy:
  Query by council (postcodes don't filter server-side), then post-filter
  addresses that contain one of the target postcodes.

  Postcode-to-council mapping:
    2765 -> Hawkesbury City Council, The Hills Shire Council
    2517 -> Wollongong City Council
    2518 -> Wollongong City Council
"""

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from core.utils import now_iso
from services.cadastral_identity import build_storage_address, extract_lot_plan, is_subdivision_signal
from services.scrape_stealth import build_headers, get_rotating_proxy_url, jitter_sleep_async

logger = logging.getLogger(__name__)

_DA_API_URL = "https://api.apps1.nsw.gov.au/eplanning/data/v0/DAApplicationTracker"
_SYDNEY_TZ = ZoneInfo("Australia/Sydney")
_PAGE_SIZE = 100  # Tested maximum without exceeding transfer limits

# Maps each council display name to the set of postcodes it covers.
# Only councils relevant to the 2765 (Hills/Hawkesbury) and 2517/2518 (Woonona) loci.
_COUNCIL_POSTCODE_MAP: dict[str, set[str]] = {
    "Hawkesbury City Council": {
        "2753", "2754", "2756", "2757", "2765", "2775", "2777",
    },
    "The Hills Shire Council": {
        "2153", "2154", "2155", "2156", "2157", "2158", "2159",
        "2761", "2765", "2768",
    },
    "Wollongong City Council": {
        "2500", "2502", "2505", "2506", "2508", "2515", "2516",
        "2517", "2518", "2519", "2525", "2526", "2527", "2528",
        "2529", "2530",
    },
}

# Keywords in TYPE_OF_DEVELOPMENT that indicate subdivision / multi-dwelling.
# These route to "DEVELOPMENT" queue; everything else goes to "RE".
_DEVELOPMENT_KEYWORDS = frozenset({
    "subdivision",
    "multi dwelling",
    "multi-dwelling",
    "secondary dwelling",
    "dual occupancy",
    "townhouse",
    "villa",
    "apartment",
    "residential flat",
    "manor house",
    "boarding house",
    "mixed use",
    "seniors living",
    "land subdivision",
    "torrens title",
    "strata",
})


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _extract_postcode(full_address: str) -> str:
    """Return the 4-digit postcode from FULL_ADDRESS, or empty string."""
    match = re.search(r"\b(\d{4})\b", full_address or "")
    return match.group(1) if match else ""


def _extract_suburb(full_address: str, postcode: str) -> str:
    """
    Extract suburb from FULL_ADDRESS.

    FULL_ADDRESS examples:
      '131 STOCKWELL ROAD OAKVILLE 2765'  -> 'Oakville'
      '51 DORADO STREET BOX HILL 2765'    -> 'Box Hill'
      '1 BLUNDELL PARADE CORRIMAL 2518'   -> 'Corrimal'

    Strategy: strip the postcode, then strip the street number + road token,
    and take whatever remains as the suburb (title-cased).
    """
    addr = (full_address or "").strip()
    # Remove postcode
    if postcode:
        addr = re.sub(r"\s*\b" + re.escape(postcode) + r"\b\s*", " ", addr).strip()
    # Strip leading street number (digits, optional letter suffix)
    addr = re.sub(r"^\d+[A-Za-z]?\b\s*", "", addr).strip()
    # The remaining tokens are: STREET_NAME ROAD_TYPE SUBURB
    # Road type tokens to strip (common NSW abbreviations)
    road_types = (
        r"\b(?:ROAD|RD|STREET|ST|AVENUE|AVE|AV|DRIVE|DR|PLACE|PL|COURT|CT|CLOSE|CL"
        r"|LANE|LN|PARADE|PDE|CRESCENT|CR|CIRCUIT|CCT|BOULEVARD|BVD|WAY|HIGHWAY|HWY"
        r"|TERRACE|TCE|GROVE|GR|RISE|TRACK|WALK|PATH|LOOP|LINK)\b"
    )
    # Split on last road-type token and take everything after it as suburb
    parts = re.split(road_types, addr, flags=re.IGNORECASE)
    if len(parts) >= 2:
        suburb = parts[-1].strip()
    else:
        # Fallback: take last word
        words = addr.split()
        suburb = words[-1] if words else addr
    return suburb.title() if suburb else ""


def _route_queue(type_of_development: str) -> str:
    """Return 'DEVELOPMENT' for subdivision/multi-dwelling DAs, 'RE' for all others."""
    low = (type_of_development or "").lower()
    for kw in _DEVELOPMENT_KEYWORDS:
        if kw in low:
            return "DEVELOPMENT"
    return "RE"


def _lead_id(address: str) -> str:
    return hashlib.md5(address.strip().lower().encode()).hexdigest()


# ─── API Fetch ────────────────────────────────────────────────────────────────

async def _fetch_council_das(
    council_name: str,
    lodgement_date_from: str,
    lodgement_date_to: str,
) -> list[dict[str, Any]]:
    """
    Fetch all DA records for one council over the given date window.
    Handles pagination. Returns list of properties dicts with _lat/_lng added.
    """
    records: list[dict[str, Any]] = []
    page = 1

    while True:
        payload = {
            "PageNumber": page,
            "PageSize": _PAGE_SIZE,
            "ApplicationStatus": "ALL",
            "LodgementDateFrom": lodgement_date_from,
            "LodgementDateTo": lodgement_date_to,
            "CouncilDisplayName": council_name,
        }
        try:
            await jitter_sleep_async()
            proxy = get_rotating_proxy_url()
            headers = build_headers({"Accept": "application/json"})
            async with httpx.AsyncClient(timeout=30.0, headers=headers, proxy=proxy) as client:
                resp = await client.post(_DA_API_URL, json=payload)
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "[DA] HTTP %s fetching council=%s page=%d: %s",
                exc.response.status_code, council_name, page, exc.response.text[:200],
            )
            break
        except Exception as exc:
            logger.warning(
                "[DA] Request error for council=%s page=%d: %s", council_name, page, exc
            )
            break

        for feat in data.get("features") or []:
            props = dict(feat.get("properties") or {})
            coords = (feat.get("geometry") or {}).get("coordinates") or [0.0, 0.0]
            props["_lng"] = float(coords[0]) if len(coords) > 1 else 0.0
            props["_lat"] = float(coords[1]) if len(coords) > 1 else 0.0
            records.append(props)

        total_pages = int(data.get("TotalPages") or 1)
        if page >= total_pages:
            break
        page += 1

    return records


# ─── Public API ───────────────────────────────────────────────────────────────

async def fetch_recent_das(postcodes: list[str], days_back: int = 3) -> list[dict]:
    """
    Fetch DAs filed in the last `days_back` days for the given postcodes.

    Queries DAApplicationTracker by council name, then post-filters to records
    whose FULL_ADDRESS contains one of the requested postcodes.

    Returns list of raw DA properties dicts (with _lat, _lng added).
    """
    today = datetime.now(_SYDNEY_TZ).date()
    date_from = (today - timedelta(days=days_back)).isoformat()
    date_to = today.isoformat()

    postcode_set = set(postcodes)

    councils_to_query: set[str] = set()
    for council, covered in _COUNCIL_POSTCODE_MAP.items():
        if covered & postcode_set:
            councils_to_query.add(council)

    if not councils_to_query:
        logger.warning("[DA] No council mapping for postcodes=%s", postcodes)
        return []

    all_records: list[dict] = []

    for council in sorted(councils_to_query):
        logger.info(
            "[DA] Fetching council=%s from=%s to=%s", council, date_from, date_to
        )
        recs = await _fetch_council_das(council, date_from, date_to)
        logger.info("[DA] council=%s raw records=%d", council, len(recs))

        for rec in recs:
            rec_postcode = _extract_postcode(rec.get("FULL_ADDRESS", ""))
            if rec_postcode in postcode_set:
                all_records.append(rec)

    logger.info(
        "[DA] Total matching records for postcodes=%s: %d", postcodes, len(all_records)
    )
    return all_records


async def ingest_das_to_leads(
    session: AsyncSession,
    postcodes: list[str],
    days_back: int = 3,
) -> dict:
    """
    Fetch DAs and upsert matching ones into the leads table.

    Lead field mapping (against actual SQLite schema):
      address          <- FULL_ADDRESS
      suburb           <- parsed from FULL_ADDRESS
      postcode         <- parsed from FULL_ADDRESS
      trigger_type     <- "Development Application"
      status           <- "captured"
      lifecycle_stage  <- "LIVE" (signals this is a fresh lead)
      date_found       <- LODGEMENT_DATE
      agency_name      <- "NSW Planning Portal"
      parcel_details   <- PLANNING_PORTAL_APP_NUMBER
      route_queue      <- "DEVELOPMENT" or "RE"
      heat_score       <- 60
      call_today_score <- 50
      evidence_score   <- 40
      lat, lng         <- from geometry coordinates

    Returns {"fetched": int, "inserted": int, "skipped": int}.
    """
    raw_das = await fetch_recent_das(postcodes, days_back)

    fetched = len(raw_das)
    inserted = 0
    skipped = 0
    now = now_iso()

    for da in raw_das:
        full_address = (da.get("FULL_ADDRESS") or "").strip()
        if not full_address:
            skipped += 1
            continue

        postcode = _extract_postcode(full_address)
        suburb = _extract_suburb(full_address, postcode)
        lodgement_date = da.get("LODGEMENT_DATE") or now[:10]
        type_of_dev = da.get("TYPE_OF_DEVELOPMENT") or ""
        app_number = da.get("PLANNING_PORTAL_APP_NUMBER") or ""
        council = da.get("COUNCIL_NAME") or ""
        da_status = da.get("STATUS") or ""
        lat = da.get("_lat") or 0.0
        lng = da.get("_lng") or 0.0

        route_q = _route_queue(type_of_dev)
        parcel_lot, parcel_plan = extract_lot_plan(full_address, app_number, type_of_dev)
        subdivision = is_subdivision_signal(type_of_dev, "Development Application")
        storage_address = build_storage_address(full_address, parcel_lot, parcel_plan, subdivision=subdivision)
        lead_id = _lead_id(storage_address)

        description = (
            f"DA lodged {lodgement_date}. Application: {app_number}. "
            f"Type: {type_of_dev}. Council: {council}. Status: {da_status}."
        )

        source_tags_json = json.dumps([
            "NSW Planning Portal",
            "Development Application",
            app_number,
        ])

        # Construct a direct link to the Planning Portal if possible
        # PAN numbers can be linked directly; others go to the tracker search
        if app_number.startswith("PAN-"):
            external_url = f"https://www.planningportal.nsw.gov.au/da-view-and-track-details/{app_number}"
        else:
            # Generic tracker with search param (portal handles some query strings)
            external_url = f"https://www.planningportal.nsw.gov.au/da-exhibition?search={app_number}"

        source_evidence = json.dumps([f"NSW Planning Portal: {external_url}"])
        
        try:
            await session.execute(
                text("""
                    INSERT INTO leads (
                        id, address, canonical_address, suburb, postcode,
                        trigger_type, status, lifecycle_stage,
                        date_found, agency_name, parcel_details, parcel_lot, parcel_plan,
                        route_queue, heat_score, call_today_score, evidence_score,
                        lat, lng, description_deep, source_tags,
                        external_link, source_evidence, record_type,
                        potential_contacts, contact_emails, contact_phones,
                        key_details, property_images, features,
                        summary_points, risk_flags, next_actions,
                        linked_files, stage_note_history, activity_log,
                        queue_bucket, lead_archetype, contactability_status,
                        contact_role, cadence_name, cadence_step,
                        next_action_type, next_action_channel,
                        next_action_title, next_action_reason,
                        next_message_template, last_outcome,
                        objection_reason, preferred_channel, strike_zone,
                        touches_14d, touches_30d,
                        created_at, updated_at
                    ) VALUES (
                        :id, :address, :canonical_address, :suburb, :postcode,
                        :trigger_type, :status, :lifecycle_stage,
                        :date_found, :agency_name, :parcel_details, :parcel_lot, :parcel_plan,
                        :route_queue, :heat_score, :call_today_score, :evidence_score,
                        :lat, :lng, :description_deep, :source_tags,
                        :external_link, :source_evidence, 'property_record',
                        '[]', '[]', '[]',
                        '[]', '[]', '[]',
                        '[]', '[]', '[]',
                        '[]', '[]', '[]',
                        '', '', '',
                        '', '', 0,
                        '', '',
                        '', '',
                        '', '',
                        '', '', '',
                        0, 0,
                        :created_at, :updated_at
                    )
                    ON CONFLICT(id) DO UPDATE SET
                        updated_at = :updated_at,
                        description_deep = EXCLUDED.description_deep,
                        date_found = EXCLUDED.date_found,
                        external_link = EXCLUDED.external_link,
                        source_evidence = EXCLUDED.source_evidence,
                        source_tags = EXCLUDED.source_tags,
                        parcel_lot = COALESCE(EXCLUDED.parcel_lot, leads.parcel_lot),
                        parcel_plan = COALESCE(EXCLUDED.parcel_plan, leads.parcel_plan)
                """),
                {
                    "id": lead_id,
                    "address": storage_address,
                    "canonical_address": full_address,
                    "suburb": suburb,
                    "postcode": postcode,
                    "trigger_type": "Development Application",
                    "status": "captured",
                    "lifecycle_stage": "LIVE",
                    "date_found": lodgement_date,
                    "agency_name": "NSW Planning Portal",
                    "parcel_details": app_number,
                    "parcel_lot": parcel_lot,
                    "parcel_plan": parcel_plan,
                    "route_queue": route_q,
                    "heat_score": 60,
                    "call_today_score": 50,
                    "evidence_score": 40,
                    "lat": lat,
                    "lng": lng,
                    "description_deep": description,
                    "source_tags": source_tags_json,
                    "external_link": external_url,
                    "source_evidence": source_evidence,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            inserted += 1
        except Exception as exc:
            err = str(exc)
            if "UNIQUE" in err.upper() or "unique" in err.lower():
                skipped += 1
            else:
                logger.warning("[DA] Insert error for '%s': %s", full_address, exc)
                skipped += 1

    try:
        await session.commit()
    except Exception as exc:
        logger.error("[DA] Commit failed: %s", exc)
        raise

    summary = {"fetched": fetched, "inserted": inserted, "skipped": skipped}
    logger.info("[DA] Ingest complete: %s", summary)
    return summary
