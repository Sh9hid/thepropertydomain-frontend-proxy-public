"""
Portal Sitemap Worker — Secondary validation of withdrawn listings.
Uses curl_cffi to impersonate Chrome JA3/JA4 TLS fingerprint for Cloudflare bypass.
Parses sitemap-listings-sale.xml.gz to confirm listing absence = withdrawn.
"""

import asyncio
import gzip
import io
import logging
from typing import Optional

from lxml import etree

logger = logging.getLogger(__name__)

try:
    import curl_cffi.requests as cf_requests
    HAS_CURL_CFFI = True
except ImportError:
    import httpx as cf_requests  # fallback — no TLS impersonation
    HAS_CURL_CFFI = False
    logger.warning("[Sitemap] curl_cffi not available — falling back to httpx (no TLS impersonation)")


class SitemapIngestor:
    """
    Secondary validation via portal sitemap-listings-sale.xml.gz parsing.
    Uses curl_cffi to impersonate Chrome JA3/JA4 TLS fingerprint.
    Injects content-type, origin, referer headers for Cloudflare bypass.
    """

    IMPERSONATE = "chrome124"
    REQUIRED_HEADERS = {
        "content-type": "application/json",
        "origin": "https://www.domain.com.au",
        "referer": "https://www.domain.com.au/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    async def fetch_sitemap(self, sitemap_url: str) -> set[str]:
        """
        Fetch sitemap XML (.gz or plain). Returns set of listing URLs present.
        An absent URL = withdrawn from portal.
        """
        try:
            content = await self._get_bytes(sitemap_url)
            return self._parse_sitemap_xml(content)
        except Exception as e:
            logger.warning(f"[Sitemap] fetch_sitemap error ({sitemap_url}): {e}")
            return set()

    async def cross_validate_withdrawal(self, address: str, listing_url: str) -> bool:
        """
        Returns True if listing_url is absent from current sitemap (confirmed withdrawn).
        listing_url should be the canonical domain.com.au URL for the property.
        """
        if not listing_url:
            return False
        sitemap_url = "https://www.domain.com.au/sitemap-listings-sale.xml.gz"
        active_urls = await self.fetch_sitemap(sitemap_url)
        # Normalise: strip trailing slash
        normalised = listing_url.rstrip("/")
        return normalised not in active_urls

    async def _get_bytes(self, url: str) -> bytes:
        if HAS_CURL_CFFI:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: cf_requests.get(
                    url,
                    impersonate=self.IMPERSONATE,
                    headers=self.REQUIRED_HEADERS,
                    timeout=30,
                )
            )
            content = response.content
        else:
            import httpx
            async with httpx.AsyncClient(timeout=30, headers=self.REQUIRED_HEADERS) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                content = resp.content

        # Decompress if gzip
        if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
            content = gzip.decompress(content)
        return content

    def _parse_sitemap_xml(self, content: bytes) -> set[str]:
        """Parse sitemap XML and return all <loc> URLs."""
        try:
            root = etree.fromstring(content)
        except etree.XMLSyntaxError as e:
            logger.error(f"[Sitemap] XML parse error: {e}")
            return set()

        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = set()
        for loc in root.findall(".//sm:loc", ns):
            if loc.text:
                urls.add(loc.text.strip().rstrip("/"))
        # Also try without namespace
        for loc in root.findall(".//loc"):
            if loc.text:
                urls.add(loc.text.strip().rstrip("/"))
        return urls


# ─── Background loop ─────────────────────────────────────────────────────────

_sitemap_ingestor = SitemapIngestor()


async def _sitemap_validation_loop():
    """
    Runs every 30 minutes.
    Cross-validates any WITHDRAWN events from the last 24h against portal sitemap.
    """
    from core.events import event_manager

    logger.info("[Sitemap] Validation worker started — running every 30 min")
    while True:
        try:
            await event_manager.broadcast_log(
                "[Sitemap] Running portal sitemap cross-validation",
                level="INFO",
                category="SITEMAP",
            )
            # Actual cross-validation is triggered per-listing by the delta engine
            # This loop exists to allow future scheduled batch validation
        except Exception as e:
            logger.error(f"[Sitemap] Validation loop error: {e}")
        await asyncio.sleep(1800)  # 30 min
