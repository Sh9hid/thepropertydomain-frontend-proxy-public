"""
Domain.com.au API enrichment service.

Quota: 500 calls/day — we cap at 490 to leave headroom.
Uses client_credentials OAuth2 flow for listing search.
Stores results in the leads table: main_image, property_images, est_value, domain_listing_id.
"""

import json
import logging
import asyncio
import datetime
from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from core.config import (
    DOMAIN_CLIENT_ID,
    DOMAIN_CLIENT_SECRET,
    DOMAIN_API_KEY,
    DOMAIN_CALLS_PER_DAY,
)

logger = logging.getLogger(__name__)

DOMAIN_TOKEN_URL = "https://auth.domain.com.au/v1/connect/token"
DOMAIN_API_BASE = "https://api.domain.com.au/v1"
DOMAIN_SCORE_MIN_FOR_ENRICHMENT = 60
DOMAIN_403_COOLDOWN_HOURS = 24

# Module-level token cache
_access_token: Optional[str] = None
_token_expiry: Optional[datetime.datetime] = None
_domain_blocked_until_utc: Optional[datetime.datetime] = None


async def _get_access_token() -> Optional[str]:
    """Fetch or return cached OAuth2 client_credentials token."""
    global _access_token, _token_expiry

    if not DOMAIN_CLIENT_ID or not DOMAIN_CLIENT_SECRET:
        logger.warning("Domain API credentials not configured")
        return None

    now = datetime.datetime.utcnow()
    if _access_token and _token_expiry and now < _token_expiry:
        return _access_token

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                DOMAIN_TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "scope": "api_listings_read api_properties_read",
                    "client_id": DOMAIN_CLIENT_ID,
                    "client_secret": DOMAIN_CLIENT_SECRET,
                },
            )
        if resp.status_code != 200:
            logger.error("Domain token fetch failed: %s %s", resp.status_code, resp.text)
            return None
        payload = resp.json()
        _access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        _token_expiry = now + datetime.timedelta(seconds=expires_in - 60)
        return _access_token
    except Exception as exc:
        logger.error("Domain token request error: %s", exc)
        return None


async def enrich_lead_from_domain(address: str, suburb: str) -> dict:
    """
    Look up address at Domain API and return enrichment data.

    Returns dict with keys: main_image, property_images (JSON list),
    est_value, domain_listing_id, suburb_median.
    Returns empty dict on failure.
    """
    token = await _get_access_token()
    if not token:
        return {}

    headers = {
        "Authorization": f"Bearer {token}",
        "X-Api-Key": DOMAIN_API_KEY,
        "Content-Type": "application/json",
    }

    search_body = {
        "listingType": "Sale",
        "propertyTypes": ["House", "Land"],
        "locations": [{"state": "NSW", "suburb": suburb}],
        "keywords": [address],
        "pageSize": 3,
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{DOMAIN_API_BASE}/listings/residential/_search",
                headers=headers,
                json=search_body,
            )
    except Exception as exc:
        logger.error("Domain listing search error for %s: %s", address, exc)
        return {}

    if resp.status_code != 200:
        logger.warning("Domain listing search %s: HTTP %s", address, resp.status_code)
        return {"__http_status": resp.status_code}

    results = resp.json()
    if not results:
        return {}

    first = results[0]
    listing = first.get("listing", first)

    listing_id = str(listing.get("id") or listing.get("listingId") or "")
    price_details = listing.get("priceDetails") or {}
    est_value = price_details.get("displayPrice") or price_details.get("price") or None

    media = listing.get("media") or []
    images = [m.get("url") for m in media if m.get("category") == "Image" and m.get("url")]
    main_image = images[0] if images else None

    # Try to extract a numeric value from display price (e.g. "$1,200,000")
    est_value_int = None
    if est_value:
        import re as _re
        nums = _re.sub(r"[^\d]", "", str(est_value))
        if nums:
            try:
                est_value_int = int(nums)
            except ValueError:
                pass

    return {
        "domain_listing_id": listing_id,
        "main_image": main_image,
        "property_images": json.dumps(images[:10]),
        "est_value": est_value_int,
    }


async def get_suburb_comparables(suburb: str, postcode: str) -> list[dict]:
    """
    Fetch recently sold comparable properties in a suburb via Domain API.
    Uses POST /v1/listings/residential/_search with saleMode=sold, soldWithin=90 days.
    Costs 1 Domain API call per suburb (not per property).
    Returns list of up to 5 recent sales: address, sold_price, sold_date, bedrooms.
    Returns [] on any failure.
    """
    token = await _get_access_token()
    if not token:
        return []

    payload = {
        "listingType": "Sale",
        "propertyTypes": ["House", "ApartmentUnitFlat", "Townhouse", "Villa"],
        "locations": [{"suburb": suburb, "postcode": postcode, "state": "NSW"}],
        "saleMode": "sold",
        "soldWithin": 90,
        "pageSize": 5,
        "sort": {"sortKey": "dateSold", "direction": "Descending"},
    }
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.post(
                f"{DOMAIN_API_BASE}/listings/residential/_search",
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-Api-Key": DOMAIN_API_KEY or "",
                    "Content-Type": "application/json",
                },
            )
        if resp.status_code != 200:
            logger.warning("Domain comparables search: %s", resp.status_code)
            return []
        results = resp.json()
        comps = []
        for item in results:
            listing = item.get("listing", {})
            price_details = listing.get("priceDetails", {})
            sold_price = price_details.get("price")
            sold_date = (listing.get("dateSold") or "")[:10]
            addr = listing.get("propertyDetails", {}).get("displayableAddress", "")
            beds = listing.get("propertyDetails", {}).get("bedrooms")
            prop_type = listing.get("propertyDetails", {}).get("propertyType", "")
            if sold_price and sold_price > 50_000:
                comps.append({
                    "address": addr,
                    "sold_price": int(sold_price),
                    "sold_date": sold_date,
                    "bedrooms": beds,
                    "property_type": prop_type,
                })
        return comps
    except Exception as exc:
        logger.warning("Domain comparables error: %s", exc)
        return []


async def run_enrichment_batch(session: AsyncSession, max_calls: int = DOMAIN_CALLS_PER_DAY) -> dict:
    """
    Enrich leads that have no domain_listing_id yet.
    Respects the daily call quota via max_calls.
    Returns summary dict.
    """
    global _domain_blocked_until_utc
    now_utc = datetime.datetime.utcnow()
    if _domain_blocked_until_utc and now_utc < _domain_blocked_until_utc:
        logger.warning(
            "Domain enrichment is paused until %s after repeated 403 responses",
            _domain_blocked_until_utc.isoformat(),
        )
        return {"skipped": 0, "enriched": 0, "failed": 0, "blocked_until": _domain_blocked_until_utc.isoformat()}

    # Check how many Domain API calls we've used today
    today = datetime.date.today().isoformat()
    res = await session.execute(
        text("SELECT COUNT(*) FROM leads WHERE domain_enriched_date = :today"),
        {"today": today},
    )
    used_today = res.scalar_one()
    remaining = max_calls - used_today
    if remaining <= 0:
        logger.info("Domain API quota exhausted for today (%d/%d used)", used_today, max_calls)
        return {"skipped": 0, "enriched": 0, "failed": 0, "quota_exhausted": True}

    # Fetch leads missing enrichment
    res = await session.execute(
        text(
            "SELECT id, address, suburb, source_tags, call_today_score, confidence_score FROM leads "
            "WHERE (domain_listing_id IS NULL OR domain_listing_id = '') "
            "ORDER BY call_today_score DESC NULLS LAST, confidence_score DESC NULLS LAST "
            "LIMIT :lim"
        ),
        {"lim": remaining},
    )
    rows = res.mappings().all()

    enriched = 0
    failed = 0
    skipped = 0

    for row in rows:
        source_tags_raw = row.get("source_tags")
        source_tags: list[str] = []
        if isinstance(source_tags_raw, list):
            source_tags = [str(t).lower() for t in source_tags_raw]
        elif isinstance(source_tags_raw, str):
            try:
                loaded = json.loads(source_tags_raw)
                if isinstance(loaded, list):
                    source_tags = [str(t).lower() for t in loaded]
            except json.JSONDecodeError:
                source_tags = [source_tags_raw.lower()]
        if any("cotality" in tag for tag in source_tags):
            skipped += 1
            continue
        call_today_score = row.get("call_today_score")
        if call_today_score is None or float(call_today_score) < DOMAIN_SCORE_MIN_FOR_ENRICHMENT:
            skipped += 1
            continue

        lead_id = row["id"]
        address = row["address"] or ""
        suburb = row["suburb"] or ""

        data = await enrich_lead_from_domain(address, suburb)
        if not data:
            failed += 1
            continue
        if int(data.get("__http_status") or 0) == 403:
            _domain_blocked_until_utc = datetime.datetime.utcnow() + datetime.timedelta(hours=DOMAIN_403_COOLDOWN_HOURS)
            logger.error("Domain returned HTTP 403. Pausing enrichment until %s", _domain_blocked_until_utc.isoformat())
            break
        if data.get("__http_status"):
            failed += 1
            continue

        update_params: dict = {
            "id": lead_id,
            "domain_listing_id": data.get("domain_listing_id", ""),
            "today": today,
        }
        set_clauses = [
            "domain_listing_id = :domain_listing_id",
            "domain_enriched_date = :today",
        ]
        if data.get("main_image"):
            set_clauses.append("main_image = :main_image")
            update_params["main_image"] = data["main_image"]
        if data.get("property_images"):
            set_clauses.append("property_images = :property_images")
            update_params["property_images"] = data["property_images"]
        if data.get("est_value"):
            set_clauses.append("est_value = :est_value")
            update_params["est_value"] = data["est_value"]

        await session.execute(
            text(f"UPDATE leads SET {', '.join(set_clauses)} WHERE id = :id"),
            update_params,
        )
        enriched += 1

        # Small delay to avoid bursting the API
        await asyncio.sleep(0.15)

    await session.commit()
    logger.info("Domain enrichment batch: %d enriched, %d failed, %d skipped", enriched, failed, skipped)
    result = {"enriched": enriched, "failed": failed, "skipped": skipped, "quota_used": used_today + enriched}
    if _domain_blocked_until_utc:
        result["blocked_until"] = _domain_blocked_until_utc.isoformat()
    return result
