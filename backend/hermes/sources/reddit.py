from __future__ import annotations

from typing import Dict, List

import httpx


def _reddit_json_url(base_url: str) -> str:
    cleaned = base_url.rstrip("/")
    if cleaned.endswith(".json"):
        return cleaned
    return f"{cleaned}.json?limit=25"


async def collect(source) -> List[Dict[str, str]]:
    headers = {"User-Agent": "woonona-hermes/1.0 (+public-reddit-ingest)"}
    url = source.rss_url or _reddit_json_url(source.base_url)
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        payload = response.json()

    items = payload.get("data", {}).get("children", [])
    results: List[Dict[str, str]] = []
    for item in items[:25]:
        data = item.get("data", {})
        permalink = data.get("permalink") or ""
        results.append(
            {
                "title": str(data.get("title") or "").strip(),
                "url": f"https://www.reddit.com{permalink}" if permalink else str(data.get("url") or "").strip(),
                "published_at": "",
                "summary": str(data.get("selftext") or "").strip(),
            }
        )
    return [item for item in results if item["title"] and item["url"]]
