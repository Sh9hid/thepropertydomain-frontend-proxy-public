"""
Domain Withdrawn Lead Detection

Polls Domain API for recently withdrawn listings in target suburbs.
Withdrawn = was listed, pulled without a recorded sale.
These are the highest-value outreach targets: owners who tested the market,
didn't sell, and may now be open to a private deal or appraisal.

Quota: shares the 490 call/day cap with domain_enrichment.py.
Dedups against existing leads by address hash.
"""

import hashlib
import json
import logging
import datetime
import asyncio
from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from core.config import (
    DOMAIN_CLIENT_ID,
    DOMAIN_CLIENT_SECRET,
    DOMAIN_API_KEY,
    DOMAIN_CALLS_PER_DAY,
    DOMAIN_SOURCE_403_THRESHOLD,
    DOMAIN_SOURCE_COOLDOWN_SECONDS,
)
from services.ingest_retry import can_attempt, mark_failure, mark_success
from services.source_health_service import mark_source_success, record_source_failure, should_skip_source

logger = logging.getLogger(__name__)

DOMAIN_TOKEN_URL = "https://auth.domain.com.au/v1/connect/token"
DOMAIN_API_BASE = "https://api.domain.com.au/v1"
DOMAIN_WITHDRAWN_SOURCE_KEY = "domain_withdrawn"
DOMAIN_WITHDRAWN_SOURCE_NAME = "Domain Withdrawn"
DOMAIN_WITHDRAWN_SOURCE_URL = f"{DOMAIN_API_BASE}/listings/residential/_search"

_access_token: Optional[str] = None
_token_expiry: Optional[datetime.datetime] = None


async def _post_with_backoff(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict,
    json_body: dict,
    max_attempts: int = 3,
) -> httpx.Response:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await client.post(url, headers=headers, json=json_body)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise httpx.HTTPStatusError("Transient upstream error", request=resp.request, response=resp)
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            await asyncio.sleep((2 ** (attempt - 1)) + 0.25)
    if last_exc:
        raise last_exc
    raise RuntimeError("Request failed without explicit exception")


async def _get_token() -> Optional[str]:
    global _access_token, _token_expiry
    if not DOMAIN_CLIENT_ID or not DOMAIN_CLIENT_SECRET:
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
            logger.error("Domain token failed: %s", resp.status_code)
            return None
        payload = resp.json()
        _access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        _token_expiry = now + datetime.timedelta(seconds=expires_in - 60)
        return _access_token
    except Exception as exc:
        logger.error("Domain token error: %s", exc)
        return None


async def fetch_withdrawn_listings(suburbs: list[str], state: str = "NSW") -> list[dict]:
    """
    Search Domain for recently withdrawn residential listings in given suburbs.
    Paginates until all results fetched (Domain returns max 200/page).
    Returns list of raw listing dicts from the Domain API.
    """
    should_skip, _ = await should_skip_source(
        source_key=DOMAIN_WITHDRAWN_SOURCE_KEY,
        source_type="domain",
        source_name=DOMAIN_WITHDRAWN_SOURCE_NAME,
        source_url=DOMAIN_WITHDRAWN_SOURCE_URL,
    )
    if should_skip:
        logger.info("Domain withdrawn source currently blocked/misconfigured; skipping pull")
        return []

    token = await _get_token()
    if not token:
        logger.warning("No Domain token available; skipping withdrawn search")
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-Api-Key": DOMAIN_API_KEY,
        "Content-Type": "application/json",
    }

    date_min = (datetime.datetime.utcnow() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    results: list[dict] = []
    had_successful_response = False
    did_record_failure = False

    for suburb in suburbs:
        page = 1
        while True:
            body = {
                "listingType": "Sale",
                "listingStatus": ["Withdrawn"],
                "propertyTypes": ["House", "Land", "Townhouse", "Unit"],
                "locations": [{"state": state, "suburb": suburb}],
                "pageSize": 200,
                "page": page,
                "dateRange": {"min": date_min},
            }
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await _post_with_backoff(
                        client,
                        f"{DOMAIN_API_BASE}/listings/residential/_search",
                        headers=headers,
                        json_body=body,
                    )

                if resp.status_code == 200:
                    had_successful_response = True
                    batch = resp.json() or []
                    results.extend(batch)
                    logger.info("Withdrawn search %s p%d: %d results", suburb, page, len(batch))
                    if len(batch) < 200:
                        break
                    page += 1
                    continue

                logger.warning("Withdrawn search %s: HTTP %s", suburb, resp.status_code)
                await record_source_failure(
                    source_key=DOMAIN_WITHDRAWN_SOURCE_KEY,
                    source_type="domain",
                    source_name=DOMAIN_WITHDRAWN_SOURCE_NAME,
                    source_url=DOMAIN_WITHDRAWN_SOURCE_URL,
                    error_code=resp.status_code,
                    logger=logger,
                    block_threshold=DOMAIN_SOURCE_403_THRESHOLD,
                    cooldown_seconds=DOMAIN_SOURCE_COOLDOWN_SECONDS,
                )
                did_record_failure = True
                break
            except Exception as exc:
                logger.error("Withdrawn search error %s: %s", suburb, exc)
                await record_source_failure(
                    source_key=DOMAIN_WITHDRAWN_SOURCE_KEY,
                    source_type="domain",
                    source_name=DOMAIN_WITHDRAWN_SOURCE_NAME,
                    source_url=DOMAIN_WITHDRAWN_SOURCE_URL,
                    error_code=type(exc).__name__,
                    logger=logger,
                )
                return []

    if had_successful_response and not did_record_failure:
        await mark_source_success(
            source_key=DOMAIN_WITHDRAWN_SOURCE_KEY,
            source_type="domain",
            source_name=DOMAIN_WITHDRAWN_SOURCE_NAME,
            source_url=DOMAIN_WITHDRAWN_SOURCE_URL,
            logger=logger,
        )

    return results


def _extract_lead_fields(listing: dict) -> dict:
    """Map a Domain API listing response to our leads table fields."""
    inner = listing.get("listing", listing)

    address_parts = inner.get("propertyDetails", {})
    street = address_parts.get("displayableAddress") or address_parts.get("streetAddress") or ""
    suburb = address_parts.get("suburb") or ""
    postcode = address_parts.get("postcode") or ""
    state = address_parts.get("state") or "NSW"

    full_address = street if suburb.lower() in street.lower() else f"{street}, {suburb} {state}"

    price_details = inner.get("priceDetails") or {}
    display_price = price_details.get("displayPrice") or ""

    media = inner.get("media") or []
    images = [m.get("url") for m in media if m.get("category") == "Image" and m.get("url")]

    listing_id = str(inner.get("id") or inner.get("listingId") or "")
    agency_obj = inner.get("advertiser") or {}
    agency_name = agency_obj.get("name") or ""
    agent_list = inner.get("agents") or []
    agent_name = agent_list[0].get("name", "") if agent_list else ""

    date_updated = (
        inner.get("dateUpdated")
        or inner.get("dateListed")
        or datetime.datetime.utcnow().isoformat()
    )
    signal_date = date_updated[:10] if date_updated else datetime.date.today().isoformat()

    # Calculate days on market
    dom = 0
    try:
        listed_str = inner.get("dateListed")
        if listed_str:
            listed_dt = datetime.date.fromisoformat(listed_str[:10])
            updated_dt = datetime.date.fromisoformat(signal_date)
            dom = (updated_dt - listed_dt).days
    except:
        pass

    # Deal Engine fields
    price_drop_count = 0
    relisted = False
    list_date = inner.get("dateListed")

    # Deterministic ID from address
    lead_id = hashlib.md5(full_address.encode()).hexdigest()

    return {
        "id": lead_id,
        "address": full_address.strip(),
        "suburb": suburb,
        "postcode": postcode,
        "trigger_type": "domain_withdrawn",
        "status": "withdrawn",          # Correct status for filtering
        "route_queue": "real_estate",
        "heat_score": 75,          # Withdrawn = high urgency â€” tested market, didn't sell
        "call_today_score": 60,
        "evidence_score": 30,
        "signal_date": signal_date,
        "agency_name": agency_name,
        "agent_name": agent_name,
        "domain_listing_id": listing_id,
        "days_on_market": dom,
        "listing_headline": inner.get("headline", ""),
        "price_drop_count": price_drop_count,
        "relisted": relisted,
        "list_date": list_date,
        "main_image": images[0] if images else None,
        "property_images": json.dumps(images[:10]) if images else "[]",
        "est_value": _parse_price(display_price),
        "source_tags": json.dumps(["domain_withdrawn", f"dom_{dom}"]),
        "source_evidence": json.dumps([
            f"Domain listing {listing_id} withdrawn {signal_date} after {dom} days on market"
        ]),
    }


def _parse_price(display_price: str) -> int:
    """Extract a numeric value from a Domain display price string."""
    import re
    if not display_price:
        return 0
    nums = re.sub(r"[^\d]", "", display_price)
    try:
        return int(nums) if nums else 0
    except ValueError:
        return 0


async def ingest_withdrawn_to_leads(
    session: AsyncSession,
    suburbs: list[str],
    state: str = "NSW",
) -> dict:
    """
    Fetch withdrawn Domain listings and upsert them as leads.
    New leads get a ticker_events push for live ticker bar display.
    Returns: {fetched, inserted, skipped}
    """
    from services.ticker_push import push_ticker_event

    allowed, retry_state = await can_attempt(session, "domain_withdrawn_ingest")
    if not allowed:
        return {
            "fetched": 0,
            "inserted": 0,
            "skipped": 0,
            "retry_deferred": True,
            "retry_state": retry_state,
        }

    try:
        raw = await fetch_withdrawn_listings(suburbs, state)
    except Exception as exc:
        state_obj = await mark_failure(
            session,
            "domain_withdrawn_ingest",
            str(exc),
            max_retries=3,
            base_delay_seconds=180,
        )
        await session.commit()
        return {"fetched": 0, "inserted": 0, "skipped": 0, "error": str(exc), "retry_state": state_obj}
    if not raw:
        await mark_success(session, "domain_withdrawn_ingest")
        await session.commit()
        return {"fetched": 0, "inserted": 0, "skipped": 0}

    now = datetime.datetime.utcnow().isoformat()
    inserted = 0
    skipped = 0

    for item in raw:
        fields = _extract_lead_fields(item)
        address = fields["address"]
        if not address:
            skipped += 1
            continue

        # Check if address already exists
        existing = await session.execute(
            text("SELECT id FROM leads WHERE LOWER(address) = LOWER(:addr) LIMIT 1"),
            {"addr": address},
        )
        if existing.scalar_one_or_none():
            skipped += 1
            continue

        try:
            await session.execute(
                text("""
                    INSERT INTO leads (
                        id, address, suburb, postcode, trigger_type, status,
                        route_queue, heat_score, call_today_score, evidence_score,
                        signal_date, agency_name, agent_name,
                        domain_listing_id, days_on_market, listing_headline,
                        price_drop_count, relisted, list_date,
                        main_image, property_images,
                        est_value, source_tags, source_evidence,
                        created_at, updated_at
                    ) VALUES (
                        :id, :address, :suburb, :postcode, :trigger_type, :status,
                        :route_queue, :heat_score, :call_today_score, :evidence_score,
                        :signal_date, :agency_name, :agent_name,
                        :domain_listing_id, :days_on_market, :listing_headline,
                        :price_drop_count, :relisted, :list_date,
                        :main_image, :property_images,
                        :est_value, :source_tags, :source_evidence,
                        :now, :now
                    )
                """),
                {**fields, "now": now},
            )
            await push_ticker_event(
                session,
                event_type="WITHDRAWAL",
                source="domain_withdrawn",
                address=address,
                suburb=fields.get("suburb", ""),
                postcode=fields.get("postcode", ""),
                heat_score=fields.get("heat_score", 75),
                lead_id=fields["id"],
                headline=f"Withdrawn after listing Â· {fields.get('agency_name', 'Domain')}",
                extra={"listing_id": fields.get("domain_listing_id", ""), "est_value": fields.get("est_value", 0)},
            )
            inserted += 1
        except Exception as exc:
            logger.warning("Insert withdrawn lead failed for %s: %s", address, exc)
            skipped += 1

    await session.commit()
    await mark_success(session, "domain_withdrawn_ingest")
    await session.commit()
    logger.info("Withdrawn ingest: fetched=%d inserted=%d skipped=%d", len(raw), inserted, skipped)
    return {"fetched": len(raw), "inserted": inserted, "skipped": skipped}

