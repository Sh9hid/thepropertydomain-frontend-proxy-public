import json
import os
import re
from typing import Any, Dict, Iterable, List

import httpx

from main import API_KEY

API_BASE = "http://localhost:8001/api"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
}


def _extract_json_ld_blocks(html: str) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for match in re.finditer(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.IGNORECASE | re.DOTALL):
        raw = match.group(1).strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            blocks.append(parsed)
        elif isinstance(parsed, list):
            blocks.extend(item for item in parsed if isinstance(item, dict))
    return blocks


def _iter_json_ld_nodes(blocks: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for block in blocks:
        if "@graph" in block and isinstance(block["@graph"], list):
            for item in block["@graph"]:
                if isinstance(item, dict):
                    yield item
        yield block


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    return re.sub(r"\s+", " ", str(value)).strip()


def _parse_listing_candidate(node: Dict[str, Any], fallback_suburb: str, source_name: str, source_url: str) -> Dict[str, Any] | None:
    address = node.get("address")
    if isinstance(address, dict):
        street = _text(address.get("streetAddress"))
        suburb = _text(address.get("addressLocality")) or fallback_suburb
        postcode = _text(address.get("postalCode"))
        region = _text(address.get("addressRegion")) or "NSW"
        full_address = ", ".join(bit for bit in [street, suburb, region, postcode] if bit)
    else:
        full_address = _text(address)
        suburb = fallback_suburb
        postcode = ""
        region = "NSW"
    offers = node.get("offers") if isinstance(node.get("offers"), dict) else {}
    sale_price = _text(offers.get("price"))
    sale_date = _text(node.get("datePosted") or node.get("datePublished") or node.get("soldDate"))
    agent = node.get("seller") if isinstance(node.get("seller"), dict) else {}
    agency_name = _text(agent.get("name"))
    agent_name = _text(node.get("agent") or "")
    if not full_address:
        return None
    return {
        "address": full_address,
        "suburb": suburb,
        "postcode": postcode,
        "state": region,
        "sale_date": sale_date,
        "sale_price": sale_price,
        "agent_name": agent_name,
        "agency_name": agency_name,
        "source_name": source_name,
        "source_url": source_url,
        "source_confidence": 68,
        "event_type": "sold",
    }


def _parse_feed(html: str, suburb: str, source_name: str, source_url: str) -> List[Dict[str, Any]]:
    parsed: List[Dict[str, Any]] = []
    for node in _iter_json_ld_nodes(_extract_json_ld_blocks(html)):
        candidate = _parse_listing_candidate(node, suburb, source_name, source_url)
        if candidate:
            parsed.append(candidate)
    return parsed


async def scrape_configured_feeds() -> List[Dict[str, Any]]:
    raw_config = os.getenv("FREE_SOLD_FEEDS", "[]")
    try:
        feeds = json.loads(raw_config)
    except json.JSONDecodeError:
        feeds = []
    if not isinstance(feeds, list):
        return []

    ingested: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30, headers=DEFAULT_HEADERS) as client:
        for feed in feeds:
            if not isinstance(feed, dict) or not feed.get("url"):
                continue
            response = await client.get(feed["url"])
            response.raise_for_status()
            for event in _parse_feed(response.text, _text(feed.get("suburb")), _text(feed.get("name")) or "feed", feed["url"]):
                api_response = await client.post(
                    f"{API_BASE}/sold-events",
                    headers={"X-API-KEY": API_KEY, "Content-Type": "application/json"},
                    json=event,
                )
                api_response.raise_for_status()
                ingested.append(api_response.json()["event"])
    return ingested


if __name__ == "__main__":
    import asyncio

    results = asyncio.run(scrape_configured_feeds())
    print(json.dumps({"ingested": len(results)}, indent=2))
