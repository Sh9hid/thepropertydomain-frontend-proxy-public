from __future__ import annotations

from typing import Dict, List

from hermes.integrations import rss as rss_integration
from hermes.integrations.browser import fetch_public_page


def _github_release_feed(url: str) -> str:
    normalized = url.rstrip("/")
    if normalized.endswith("/releases"):
        return f"{normalized}.atom"
    return f"{normalized}/releases.atom"


async def collect(source) -> List[Dict[str, str]]:
    feed_url = source.rss_url
    if not feed_url and "github.com" in (source.base_url or ""):
        feed_url = _github_release_feed(source.base_url)
    if feed_url:
        try:
            return await rss_integration.fetch_feed_entries(feed_url, limit=25)
        except Exception:
            pass
    page = await fetch_public_page(source.base_url)
    return [{"title": page["title"], "url": page["url"], "published_at": "", "summary": page["summary"]}]
