"""
REAXML / WP-JSON Ingestion Worker
Polls 23 boutique agency feeds across both 2765 and 2517 loci.
Triggers on withdrawn/off-market listings.
Uses Redis modTime cache to avoid reprocessing unchanged feeds.
"""

import asyncio
import hashlib
import logging
from typing import Optional

import httpx
from lxml import etree

from core.config import AGENCY_FEEDS_2765, AGENCY_FEEDS_2517
from services.source_health_service import (
    STATUS_MISCONFIGURED,
    mark_source_success,
    record_source_failure,
    should_skip_source,
)

logger = logging.getLogger(__name__)

# Cache TTL for modTime check (seconds)
_MOD_TIME_CACHE_TTL = 900  # 15 min


class REAXMLIngestor:
    """
    Polls WP-JSON endpoints and REAXML feed directories for 40+ boutique agencies.
    Triggers on: <residential status="withdrawn"> or JSON equivalent.
    Uses modTime delta caching to avoid reprocessing unchanged feeds.
    """

    def __init__(self):
        self._all_feeds = AGENCY_FEEDS_2765 + AGENCY_FEEDS_2517

    async def poll_all_feeds(self) -> list[dict]:
        """
        Async-gather over all agency feeds.
        Returns list of normalised withdrawn listing dicts.
        """
        redis = None
        try:
            from core.database import get_redis
            redis = await get_redis()
        except Exception:
            pass

        tasks = [
            self._poll_one_feed(feed, redis)
            for feed in self._all_feeds
            if feed.get("wp_json") or feed.get("reaxml")
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        withdrawn = []
        for r in results:
            if isinstance(r, list):
                withdrawn.extend(r)
        logger.info(f"[REAXML] Polled {len(tasks)} feeds — {len(withdrawn)} withdrawn found")
        return withdrawn

    async def _poll_one_feed(self, feed: dict, redis) -> list[dict]:
        name = feed.get("name", "unknown")
        source_url = feed.get("wp_json") or feed.get("reaxml") or ""
        source_key = f"reaxml:{name}"
        should_skip, _ = await should_skip_source(
            source_key=source_key,
            source_type="reaxml",
            source_name=name,
            source_url=source_url,
        )
        if should_skip:
            return []
        try:
            if feed.get("wp_json"):
                listings = await self._poll_wp_json(feed["wp_json"], name, redis)
            elif feed.get("reaxml"):
                listings = await self._poll_reaxml_url(feed["reaxml"], name, redis)
            else:
                listings = []
            await mark_source_success(
                source_key=source_key,
                source_type="reaxml",
                source_name=name,
                source_url=source_url,
                logger=logger,
            )
            return listings
        except httpx.HTTPStatusError as e:
            status_code = str(getattr(e.response, "status_code", "unknown"))
            await record_source_failure(
                source_key=source_key,
                source_type="reaxml",
                source_name=name,
                source_url=source_url,
                error_code=status_code,
                logger=logger,
                failure_status=STATUS_MISCONFIGURED if status_code == "404" else None,
            )
            logger.warning(f"[REAXML] Feed '{name}' error: {e}")
            return []
        except Exception as e:
            await record_source_failure(
                source_key=source_key,
                source_type="reaxml",
                source_name=name,
                source_url=source_url,
                error_code=type(e).__name__,
                logger=logger,
            )
            logger.warning(f"[REAXML] Feed '{name}' error: {e}")
            return []

    async def _poll_wp_json(self, base_url: str, agency_name: str, redis) -> list[dict]:
        """GET {base_url}/wp-json/realty/v1/listings?status=withdrawn (VaultRE/Agentbox)."""
        if not base_url:
            return []
        url = base_url.rstrip("/") + "/wp-json/realty/v1/listings"
        cache_key = f"reaxml:modtime:{hashlib.md5(url.encode()).hexdigest()}"

        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(url, params={"status": "withdrawn", "per_page": 100})
            resp.raise_for_status()

        # Cache check: use ETag or Last-Modified header
        etag = resp.headers.get("ETag") or resp.headers.get("Last-Modified") or ""
        if redis and etag:
            cached = await redis.get(cache_key)
            if cached == etag:
                return []
            await redis.set(cache_key, etag, ex=_MOD_TIME_CACHE_TTL)

        data = resp.json()
        listings = data if isinstance(data, list) else data.get("listings", [])
        return [
            self._normalise_wp_listing(item, agency_name)
            for item in listings
            if self._is_withdrawn_wp(item)
        ]

    async def _poll_reaxml_url(self, reaxml_url: str, agency_name: str, redis) -> list[dict]:
        """Fetch REAXML feed (XML/bytes) and parse for withdrawn listings."""
        if not reaxml_url:
            return []
        cache_key = f"reaxml:modtime:{hashlib.md5(reaxml_url.encode()).hexdigest()}"

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(reaxml_url)
            resp.raise_for_status()

        last_mod = resp.headers.get("Last-Modified", "")
        if redis and last_mod:
            cached = await redis.get(cache_key)
            if cached == last_mod:
                return []
            await redis.set(cache_key, last_mod, ex=_MOD_TIME_CACHE_TTL)

        return await self._parse_reaxml(resp.content, agency_name)

    async def _parse_reaxml(self, xml_content: bytes, agency_name: str = "") -> list[dict]:
        """lxml parse of REAXML. Returns only withdrawn residential listings."""
        try:
            root = etree.fromstring(xml_content)
        except etree.XMLSyntaxError as e:
            logger.error(f"[REAXML] XML parse error: {e}")
            return []

        results = []
        for res in root.findall(".//residential"):
            status = res.get("status", "").lower()
            if status not in ("withdrawn", "offmarket", "off market"):
                continue

            address_el = res.find("address")
            suburb = ""
            street = ""
            if address_el is not None:
                suburb_el = address_el.find("suburb")
                street_el = address_el.find("street")
                suburb = suburb_el.text.strip() if suburb_el is not None and suburb_el.text else ""
                street = street_el.text.strip() if street_el is not None and street_el.text else ""

            images = [img.get("url", "") for img in res.findall(".//img") if img.get("url")]
            mod_time = res.get("modTime", "")
            listing_id = res.get("uniqueID", "")

            results.append({
                "address": street,
                "suburb": suburb,
                "status": "withdrawn",
                "mod_time": mod_time,
                "listing_id": listing_id,
                "images": images,
                "agency_name": agency_name,
                "source": "reaxml",
            })
        return results

    def _is_withdrawn_wp(self, item: dict) -> bool:
        status = (
            item.get("status") or
            item.get("listing_status") or
            item.get("property_status") or ""
        ).lower()
        return status in ("withdrawn", "offmarket", "off market", "off-market")

    def _normalise_wp_listing(self, item: dict, agency_name: str) -> dict:
        address = (
            item.get("address") or
            item.get("street_address") or
            item.get("full_address") or ""
        )
        suburb = item.get("suburb") or item.get("city") or ""
        images = item.get("images") or item.get("photos") or []
        if isinstance(images, list):
            images = [i.get("url", i) if isinstance(i, dict) else i for i in images]
        return {
            "address": address,
            "suburb": suburb,
            "status": "withdrawn",
            "listing_id": str(item.get("id") or item.get("listing_id") or ""),
            "images": images,
            "agency_name": agency_name,
            "source": "wp_json",
        }


# ─── Background loop ─────────────────────────────────────────────────────────

_ingestor = REAXMLIngestor()


async def _reaxml_poll_loop():
    """Runs every 15 minutes. Enqueues withdrawn listings to the delta engine."""
    from core.events import event_manager

    logger.info("[REAXML] Worker started — polling every 15 min")
    while True:
        try:
            withdrawn = await _ingestor.poll_all_feeds()
            if withdrawn:
                await event_manager.broadcast_log(
                    f"[REAXML] {len(withdrawn)} withdrawn listings found",
                    level="INFO",
                    category="REAXML",
                )
                # Forward to delta engine
                try:
                    from services.delta_engine import _delta_engine
                    from core.database import get_session
                    async for db in get_session():
                        await _delta_engine.ingest_withdrawn_batch(db, withdrawn)
                except Exception as e:
                    logger.warning(f"[REAXML] Delta engine ingest error: {e}")
        except Exception as e:
            logger.error(f"[REAXML] Poll loop error: {e}")
        await asyncio.sleep(900)  # 15 min
