from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from typing import Dict, List

import httpx


_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def _clean_text(value: str) -> str:
    cleaned = html.unescape(_TAG_RE.sub(" ", value or ""))
    return _WHITESPACE_RE.sub(" ", cleaned).strip()


def _node_text(node: ET.Element | None, *paths: str) -> str:
    if node is None:
        return ""
    for path in paths:
        child = node.find(path)
        if child is not None and (child.text or "").strip():
            return _clean_text(child.text or "")
    return ""


async def fetch_feed_entries(url: str, limit: int = 25) -> List[Dict[str, str]]:
    headers = {
        "User-Agent": "woonona-hermes/1.0 (+public-ingest)",
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5",
    }
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()

    root = ET.fromstring(response.text)
    entries: List[Dict[str, str]] = []

    channel_items = root.findall(".//channel/item")
    atom_entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")

    for item in channel_items[:limit]:
        entries.append(
            {
                "title": _node_text(item, "title"),
                "url": _node_text(item, "link"),
                "published_at": _node_text(item, "pubDate", "published", "updated"),
                "summary": _node_text(item, "description", "summary"),
            }
        )

    for entry in atom_entries[: max(0, limit - len(entries))]:
        link = ""
        for candidate in entry.findall("{http://www.w3.org/2005/Atom}link"):
            href = (candidate.attrib.get("href") or "").strip()
            rel = (candidate.attrib.get("rel") or "alternate").strip()
            if href and rel in {"alternate", ""}:
                link = href
                break
        entries.append(
            {
                "title": _node_text(entry, "{http://www.w3.org/2005/Atom}title"),
                "url": link,
                "published_at": _node_text(
                    entry,
                    "{http://www.w3.org/2005/Atom}published",
                    "{http://www.w3.org/2005/Atom}updated",
                ),
                "summary": _node_text(
                    entry,
                    "{http://www.w3.org/2005/Atom}summary",
                    "{http://www.w3.org/2005/Atom}content",
                ),
            }
        )

    return [entry for entry in entries if entry.get("title") and entry.get("url")]
