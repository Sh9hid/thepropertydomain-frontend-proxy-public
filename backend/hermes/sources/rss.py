from __future__ import annotations

from typing import Dict, List

from hermes.integrations import rss as rss_integration


async def collect(source) -> List[Dict[str, str]]:
    target = source.rss_url or source.base_url
    return await rss_integration.fetch_feed_entries(target, limit=25)
