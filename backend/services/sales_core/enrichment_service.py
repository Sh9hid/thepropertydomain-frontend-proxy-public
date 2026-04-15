from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.sales_core_models import EnrichmentState


DEFAULT_RETRY_BASE_SECONDS = 180


def _resolved_now(now: Optional[datetime]) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    return now if now.tzinfo else now.replace(tzinfo=timezone.utc)


def _coerce_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _cooldown_for_priority(priority_score: int) -> timedelta:
    if priority_score >= 100:
        return timedelta(minutes=20)
    if priority_score >= 80:
        return timedelta(minutes=45)
    return timedelta(hours=2)


def _priority_score(reason: str) -> int:
    normalized = (reason or "").strip().lower()
    if normalized in {"missing_contactability", "callable_blocked_missing_contact"}:
        return 110
    if normalized in {"hot_lead", "high_value"}:
        return 90
    return 60


async def enqueue_enrichment_job(
    session: AsyncSession,
    *,
    business_context_key: str,
    lead_contact_id: Optional[str],
    source: str,
    reason: str,
    now: Optional[datetime] = None,
    target_type: str = "lead_contact",
    target_id: Optional[str] = None,
) -> Dict[str, Any]:
    resolved_now = _resolved_now(now)
    normalized_target_id = target_id or lead_contact_id
    if not normalized_target_id:
        raise ValueError("target_id or lead_contact_id is required")

    statement = (
        select(EnrichmentState)
        .where(EnrichmentState.business_context_key == business_context_key)
        .where(EnrichmentState.target_type == target_type)
        .where(EnrichmentState.target_id == normalized_target_id)
        .where(EnrichmentState.source == source)
        .order_by(EnrichmentState.updated_at.desc())
    )
    existing = (await session.execute(statement)).scalars().first()
    existing_cooldown_until = _coerce_datetime(existing.cooldown_until) if existing else None
    if existing and existing_cooldown_until and existing_cooldown_until > resolved_now:
        return {"enqueued": False, "reason": "cooldown_active", "job": existing}

    priority = _priority_score(reason)
    cooldown = _cooldown_for_priority(priority)
    jitter_seconds = random.randint(5, 30)
    freshness_expires_at = resolved_now + timedelta(days=7)
    checksum = hashlib.md5(f"{business_context_key}:{target_type}:{normalized_target_id}:{source}".encode("utf-8")).hexdigest()

    if existing:
        existing.status = "queued"
        existing.reason = reason
        existing.priority_score = priority
        existing.updated_at = resolved_now
        existing.next_retry_at = resolved_now + timedelta(seconds=DEFAULT_RETRY_BASE_SECONDS + jitter_seconds)
        existing.cooldown_until = resolved_now + cooldown
        existing.freshness_expires_at = freshness_expires_at
        existing.checksum = checksum
        existing.payload_json = {
            **(existing.payload_json or {}),
            "reason": reason,
            "jitter_seconds": jitter_seconds,
        }
        job = existing
    else:
        job = EnrichmentState(
            business_context_key=business_context_key,
            lead_contact_id=lead_contact_id,
            target_type=target_type,
            target_id=normalized_target_id,
            source=source,
            status="queued",
            next_retry_at=resolved_now + timedelta(seconds=DEFAULT_RETRY_BASE_SECONDS + jitter_seconds),
            cooldown_until=resolved_now + cooldown,
            freshness_expires_at=freshness_expires_at,
            checksum=checksum,
            priority_score=priority,
            reason=reason,
            payload_json={"reason": reason, "jitter_seconds": jitter_seconds},
            created_at=resolved_now,
            updated_at=resolved_now,
        )
        session.add(job)

    await session.commit()
    await session.refresh(job)
    return {"enqueued": True, "reason": "queued", "job": job}


def next_retry_window(attempt_count: int, *, now: Optional[datetime] = None) -> datetime:
    resolved_now = _resolved_now(now)
    bounded_attempts = max(0, attempt_count)
    seconds = DEFAULT_RETRY_BASE_SECONDS * (2 ** min(bounded_attempts, 5))
    return resolved_now + timedelta(seconds=seconds)


async def claim_next_enrichment_job(
    session: AsyncSession,
    *,
    business_context_key: Optional[str] = None,
    source: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Optional[EnrichmentState]:
    resolved_now = _resolved_now(now)
    statement = (
        select(EnrichmentState)
        .where(EnrichmentState.status == "queued")
        .where((EnrichmentState.next_retry_at.is_(None)) | (EnrichmentState.next_retry_at <= resolved_now))
        .order_by(EnrichmentState.priority_score.desc(), EnrichmentState.created_at.asc())
    )
    if business_context_key:
        statement = statement.where(EnrichmentState.business_context_key == business_context_key)
    if source:
        statement = statement.where(EnrichmentState.source == source)
    job = (await session.execute(statement)).scalars().first()
    if job is None:
        return None
    job.status = "running"
    job.attempt_count += 1
    job.last_attempt_at = resolved_now
    job.next_retry_at = next_retry_window(job.attempt_count, now=resolved_now)
    job.updated_at = resolved_now
    await session.commit()
    await session.refresh(job)
    return job
