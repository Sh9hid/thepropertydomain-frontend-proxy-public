"""Scrape stealth — anti-detection helpers for web scraping."""
import asyncio
import random
from typing import Dict, Optional

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]


def build_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    }
    if extra:
        headers.update(extra)
    return headers


def get_rotating_proxy_url() -> Optional[str]:
    return None


def jitter_sleep(base: float = 1.0, jitter: float = 2.0) -> None:
    import time
    time.sleep(base + random.random() * jitter)


async def jitter_sleep_async(base: float = 1.0, jitter: float = 2.0) -> None:
    await asyncio.sleep(base + random.random() * jitter)
