"""
Contact Enrichment Service.
Integrates with InfoTrack (primary) and Whitepages AU (fallback) to fill phone/email
gaps in the 13K lead pool. Never overwrites manually entered contact data.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from sqlalchemy import text
import httpx

logger = logging.getLogger(__name__)

INFOTRACK_BASE = os.getenv("INFOTRACK_BASE_URL", "https://api.infotrack.com.au/v1")
INFOTRACK_API_KEY = os.getenv("INFOTRACK_API_KEY", "")
WHITEPAGES_API_KEY = os.getenv("WHITEPAGES_API_KEY", "")

# Redis rate-limit key
RATE_LIMIT_KEY = "enrichment:hourly_count"
MAX_LOOKUPS_PER_HOUR = 100


@dataclass
class EnrichmentResult:
    lead_id: str
    phones: List[str] = field(default_factory=list)
    emails: List[str] = field(default_factory=list)
    ok: bool = False
    provider: str = ""
    error: Optional[str] = None


class ContactEnrichmentService:
    """
    Asynchronous contact enrichment with rate limiting via Redis.
    """

    def __init__(self) -> None:
        self._redis = None

    async def _get_redis(self):
        if self._redis is None:
            try:
                import redis.asyncio as aioredis
                from core.config import REDIS_URL
                self._redis = aioredis.from_url(REDIS_URL, decode_responses=True)
            except Exception:
                pass
        return self._redis

    async def _within_rate_limit(self) -> bool:
        r = await self._get_redis()
        if r is None:
            return True  # no Redis — allow
        try:
            count = await r.incr(RATE_LIMIT_KEY)
            if count == 1:
                await r.expire(RATE_LIMIT_KEY, 3600)
            return count <= MAX_LOOKUPS_PER_HOUR
        except Exception:
            return True

    async def enrich_lead(self, lead_id: str) -> EnrichmentResult:
        """Enrich a single lead; returns EnrichmentResult with found contacts."""
        from core.database import async_engine
        from core.logic import _hydrate_lead
        from sqlalchemy.ext.asyncio import AsyncSession
        from sqlalchemy.orm import sessionmaker

        if not await self._within_rate_limit():
            return EnrichmentResult(lead_id=lead_id, error="rate_limit_exceeded")

        async_session = sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with async_session() as session:
                res = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
                row = res.mappings().first()
                if not row:
                    return EnrichmentResult(lead_id=lead_id, error="lead_not_found")
                lead = _hydrate_lead(row)

                # Never overwrite existing manual contacts
                if lead.get("contact_phones") and lead.get("contact_emails"):
                    return EnrichmentResult(
                        lead_id=lead_id,
                        phones=lead["contact_phones"],
                        emails=lead["contact_emails"],
                        ok=True,
                        provider="existing",
                    )

                result = await self._infotrack_lookup(
                    lead.get("owner_name", ""),
                    lead.get("address", ""),
                    lead.get("suburb", ""),
                    lead.get("postcode", ""),
                )

                if not result["phones"] and not result["emails"] and WHITEPAGES_API_KEY:
                    result = await self._whitepages_lookup(
                        lead.get("owner_name", ""),
                        lead.get("address", ""),
                        lead.get("suburb", ""),
                    )
                    result["provider"] = "whitepages"

                phones_to_set = result["phones"] if not lead.get("contact_phones") else lead["contact_phones"]
                emails_to_set = result["emails"] if not lead.get("contact_emails") else lead["contact_emails"]

                if phones_to_set or emails_to_set:
                    import json
                    from core.utils import now_iso
                    await session.execute(
                        text("UPDATE leads SET contact_phones = :p, contact_emails = :e, updated_at = :u WHERE id = :id"),
                        {
                            "p": json.dumps(phones_to_set) if phones_to_set else lead.get("contact_phones_raw"),
                            "e": json.dumps(emails_to_set) if emails_to_set else lead.get("contact_emails_raw"),
                            "u": now_iso(),
                            "id": lead_id,
                        }
                    )
                    await session.commit()
                    await self._log_lookup(session, lead_id, result.get("provider", "infotrack"), bool(phones_to_set or emails_to_set))

                return EnrichmentResult(
                    lead_id=lead_id,
                    phones=phones_to_set,
                    emails=emails_to_set,
                    ok=True,
                    provider=result.get("provider", "infotrack"),
                )
        except Exception as exc:
            logger.error(f"[Enrichment] Lead {lead_id}: {exc}")
            return EnrichmentResult(lead_id=lead_id, error=str(exc))

    async def bulk_enrich_batch(self, lead_ids: List[str]) -> List[EnrichmentResult]:
        """Enrich multiple leads sequentially (respects rate limit)."""
        results = []
        for lead_id in lead_ids:
            results.append(await self.enrich_lead(lead_id))
            await asyncio.sleep(0.1)  # gentle throttle
        return results

    async def _infotrack_lookup(
        self, owner_name: str, address: str, suburb: str, postcode: str = ""
    ) -> Dict[str, Any]:
        if not INFOTRACK_API_KEY:
            return {"phones": [], "emails": [], "provider": "infotrack_unconfigured"}
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{INFOTRACK_BASE}/people/search",
                    headers={"Authorization": f"Bearer {INFOTRACK_API_KEY}"},
                    json={
                        "name": owner_name,
                        "address": address,
                        "suburb": suburb,
                        "postcode": postcode,
                        "country": "AU",
                    },
                )
            if resp.status_code != 200:
                return {"phones": [], "emails": [], "provider": "infotrack"}
            data = resp.json()
            phones = [p["number"] for p in data.get("phones", []) if p.get("number")]
            emails = [e["address"] for e in data.get("emails", []) if e.get("address")]
            return {"phones": phones[:3], "emails": emails[:2], "provider": "infotrack"}
        except Exception as exc:
            logger.debug(f"[InfoTrack] lookup error: {exc}")
            return {"phones": [], "emails": [], "provider": "infotrack"}

    async def _whitepages_lookup(
        self, owner_name: str, address: str, suburb: str
    ) -> Dict[str, Any]:
        if not WHITEPAGES_API_KEY:
            return {"phones": [], "emails": [], "provider": "whitepages_unconfigured"}
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    "https://api.whitepages.com.au/v2/find",
                    params={"name": owner_name, "address": f"{address}, {suburb}", "apikey": WHITEPAGES_API_KEY},
                )
            if resp.status_code != 200:
                return {"phones": [], "emails": [], "provider": "whitepages"}
            data = resp.json()
            phones = [r.get("phone") for r in data.get("results", []) if r.get("phone")]
            return {"phones": phones[:2], "emails": [], "provider": "whitepages"}
        except Exception as exc:
            logger.debug(f"[Whitepages] lookup error: {exc}")
            return {"phones": [], "emails": [], "provider": "whitepages"}

    async def _log_lookup(self, session, lead_id: str, provider: str, hit: bool) -> None:
        from core.utils import now_iso
        try:
            await session.execute(
                text("""
                INSERT INTO enrichment_log (lead_id, provider, hit, created_at)
                VALUES (:lead_id, :provider, :hit, :created_at)
                ON CONFLICT DO NOTHING
                """),
                {"lead_id": lead_id, "provider": provider, "hit": 1 if hit else 0, "created_at": now_iso()}
            )
            await session.commit()
        except Exception:
            pass  # table may not exist yet — non-fatal


# Module-level singleton
enrichment_service = ContactEnrichmentService()
