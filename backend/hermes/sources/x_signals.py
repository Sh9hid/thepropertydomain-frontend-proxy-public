from __future__ import annotations

from typing import Dict, List

from hermes.integrations import rss as rss_integration
from hermes.integrations.browser import fetch_public_page


async def collect(source) -> List[Dict[str, str]]:
    if source.rss_url:
        try:
            return await rss_integration.fetch_feed_entries(source.rss_url, limit=25)
        except Exception:
            pass
    page = await fetch_public_page(source.base_url)
    return [{"title": page["title"], "url": page["url"], "published_at": "", "summary": page["summary"]}]
