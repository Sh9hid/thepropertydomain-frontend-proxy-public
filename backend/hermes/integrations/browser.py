from __future__ import annotations

import asyncio
import re
import time
from typing import Dict

import httpx


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_META_RE = re.compile(
    r'<meta[^>]+(?:name|property)=["\'](?P<key>description|og:title|og:description)["\'][^>]+content=["\'](?P<value>.*?)["\']',
    re.IGNORECASE | re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_FETCH_LOCK = asyncio.Lock()
_LAST_FETCH_AT = 0.0


def _clean(value: str) -> str:
    return _WS_RE.sub(" ", _TAG_RE.sub(" ", value or "")).strip()


async def fetch_public_page(url: str) -> Dict[str, str]:
    global _LAST_FETCH_AT
    async with _FETCH_LOCK:
        elapsed = time.monotonic() - _LAST_FETCH_AT
        if elapsed < 1.0:
            await asyncio.sleep(1.0 - elapsed)
        _LAST_FETCH_AT = time.monotonic()

    headers = {
        "User-Agent": "woonona-hermes/1.0 (+public-browser)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()

    html_text = response.text
    title_match = _TITLE_RE.search(html_text)
    meta: Dict[str, str] = {}
    for match in _META_RE.finditer(html_text):
        meta[match.group("key").lower()] = _clean(match.group("value"))

    title = meta.get("og:title") or _clean(title_match.group(1) if title_match else "")
    summary = meta.get("og:description") or meta.get("description") or ""
    return {
        "title": title or url,
        "summary": summary,
        "url": str(response.url),
        "html": html_text[:5000],
    }
