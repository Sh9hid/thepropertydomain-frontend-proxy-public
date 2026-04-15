from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, List


DEFAULT_RESEARCH_PRIORITIES: List[str] = [
    "what profitable real estate operators are doing",
    "what mortgage lead gen players are doing",
    "what proptech startups are shipping",
    "what open-source agent/orchestration repos are worth copying",
    "what content styles are working on X/LinkedIn/blogs",
    "what changes in market data, channels, or platforms matter to us",
    "what can be repurposed into app features, outreach copy, and public intelligence",
]

ACTION_BUCKETS: List[str] = [
    "build in app",
    "use in outreach",
    "use in content",
    "save for later",
    "ignore",
]

PENDING_APPROVAL = "pending_approval"
APPROVED = "approved"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def source_due(last_fetched_at: str | None, frequency_minutes: int) -> bool:
    if not last_fetched_at:
        return True
    last_seen = parse_timestamp(last_fetched_at)
    if last_seen is None:
        return True
    return utc_now() >= last_seen + timedelta(minutes=max(5, frequency_minutes))


def compact_list(values: Iterable[str], limit: int = 10) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(clean)
        if len(result) >= limit:
            break
    return result
