"""
Domain Stale Active Listing Detection

Polls Domain API for active listings that have been on market for 70+ days.
These represent 'warm' leads where the owner may be frustrated with lack of sale
and open to a pivot (refinance, appraisal update, or auction strategy shift).
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
)
from core.utils import now_sydney
from services.ingest_retry import can_attempt, mark_failure, mark_success

logger = logging.getLogger(__name__)

DOMAIN_TOKEN_URL = "https://auth.domain.com.au/v1/connect/token"
DOMAIN_API_BASE = "https://api.domain.com.au/v1"

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


async def fetch_stale_listings(suburbs: list[str], days_min: int = 70, state: str = "NSW") -> list[dict]:
    """
    Search Domain for active listings that have been on the market for at least days_min.
    """
    token = await _get_token()
    if not token:
        logger.warning("No Domain token — skipping stale listing search")
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-Api-Key": DOMAIN_API_KEY,
        "Content-Type": "application/json",
    }

    # date_max is today - 70 days. Anything listed BEFORE this is stale.
    date_max = (now_sydney().date() - datetime.timedelta(days=days_min)).isoformat()
    # date_min is a reasonable lookback (e.g. 2 years ago) to avoid ancient data
    date_min = (now_sydney().date() - datetime.timedelta(days=730)).isoformat()

    results = []

    for suburb in suburbs:
        page = 1
        while True:
            body = {
                "listingType": "Sale",
                "listingStatus": ["Live"],
                "propertyTypes": ["House", "Land", "Townhouse", "Unit"],
                "locations": [{"state": state, "suburb": suburb}],
                "pageSize": 200,
                "page": page,
                "dateRange": {"min": date_min, "max": date_max},
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
                    batch = resp.json() or []
                    results.extend(batch)
                    logger.info("Stale search %s p%d: %d results", suburb, page, len(batch))
                    if len(batch) < 200:
                        break
                    page += 1
                else:
                    logger.warning("Stale search %s: HTTP %s", suburb, resp.status_code)
                    if resp.status_code in (401, 403):
                        raise RuntimeError(f"Domain stale search unauthorized: HTTP {resp.status_code}")
                    break
            except Exception as exc:
                logger.error("Stale search error %s: %s", suburb, exc)
                raise

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

    date_listed = inner.get("dateListed") or now_sydney().isoformat()
    signal_date = date_listed[:10]

    # Calculate days on market
    dom = 0
    try:
        listed_dt = datetime.date.fromisoformat(signal_date)
        dom = (now_sydney().date() - listed_dt).days
    except:
        pass

    # Detect price drops (heuristic: if history contains price changes or if current price is lower than previous)
    # Domain API sometimes provides price history in 'priceDetails' or we can check 'source_tags'
    price_drop_count = 0
    # Relisted check
    relisted = False # Default

    # Deterministic ID
    lead_id = hashlib.md5(full_address.encode()).hexdigest()

    return {
        "id": lead_id,
        "address": full_address.strip(),
        "suburb": suburb,
        "postcode": postcode,
        "trigger_type": "stale_active",
        "status": "active", # Correct status for filtering
        "route_queue": "real_estate",
        "heat_score": 65 + min(dom // 10, 20), # Escalates with age
        "call_today_score": 70,
        "evidence_score": 40,
        "signal_date": signal_date,
        "agency_name": agency_name,
        "agent_name": agent_name,
        "domain_listing_id": listing_id,
        "main_image": images[0] if images else None,
        "property_images": json.dumps(images[:10]) if images else "[]",
        "est_value": _parse_price(display_price),
        "source_tags": json.dumps(["stale_active", f"dom_{dom}"]),
        "days_on_market": dom,
        "listing_headline": inner.get("headline", ""),
        "price_drop_count": price_drop_count,
        "relisted": relisted,
        "list_date": date_listed,
        "source_evidence": json.dumps([
            f"Active listing {listing_id} on market for {dom} days (since {signal_date})"
        ]),
    }


def _parse_price(display_price: str) -> int:
    import re
    if not display_price:
        return 0
    nums = re.sub(r"[^\d]", "", display_price)
    try:
        return int(nums) if nums else 0
    except ValueError:
        return 0


async def ingest_stale_active_to_leads(
    session: AsyncSession,
    suburbs: list[str],
    days_min: int = 70,
    state: str = "NSW",
) -> dict:
    from services.ticker_push import push_ticker_event

    allowed, retry_state = await can_attempt(session, "domain_stale_active_ingest")
    if not allowed:
        return {
            "fetched": 0,
            "inserted": 0,
            "skipped": 0,
            "retry_deferred": True,
            "retry_state": retry_state,
        }

    try:
        raw = await fetch_stale_listings(suburbs, days_min, state)
    except Exception as exc:
        state_obj = await mark_failure(
            session,
            "domain_stale_active_ingest",
            str(exc),
            max_retries=3,
            base_delay_seconds=180,
        )
        await session.commit()
        return {"fetched": 0, "inserted": 0, "skipped": 0, "error": str(exc), "retry_state": state_obj}
    if not raw:
        await mark_success(session, "domain_stale_active_ingest")
        await session.commit()
        return {"fetched": 0, "inserted": 0, "skipped": 0}

    now = now_sydney().isoformat()
    inserted = 0
    skipped = 0

    for item in raw:
        fields = _extract_lead_fields(item)
        address = fields["address"]
        if not address:
            skipped += 1
            continue

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
            
            import re
            dom_match = re.search(r"on market for (\d+) days", fields["source_evidence"])
            dom_val = dom_match.group(1) if dom_match else "70+"

            await push_ticker_event(
                session,
                event_type="STALE_LISTING",
                source="stale_active",
                address=address,
                suburb=fields.get("suburb", ""),
                postcode=fields.get("postcode", ""),
                heat_score=fields.get("heat_score", 75),
                lead_id=fields["id"],
                headline=f"Stale Listing: {dom_val} days on market · {fields.get('agency_name', 'Domain')}",
                extra={"listing_id": fields.get("domain_listing_id", ""), "est_value": fields.get("est_value", 0)},
            )
            inserted += 1
        except Exception as exc:
            logger.warning("Insert stale lead failed for %s: %s", address, exc)
            skipped += 1

    await session.commit()
    await mark_success(session, "domain_stale_active_ingest")
    await session.commit()
    logger.info("Stale active ingest: fetched=%d inserted=%d skipped=%d", len(raw), inserted, skipped)
    return {"fetched": len(raw), "inserted": inserted, "skipped": skipped}
