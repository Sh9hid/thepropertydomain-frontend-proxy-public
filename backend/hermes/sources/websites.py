from __future__ import annotations

from typing import Dict, List

from hermes.integrations.browser import fetch_public_page


async def collect(source) -> List[Dict[str, str]]:
    page = await fetch_public_page(source.base_url)
    return [{"title": page["title"], "url": page["url"], "published_at": "", "summary": page["summary"]}]
