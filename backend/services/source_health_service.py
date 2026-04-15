from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import core.database as db_module

log = logging.getLogger(__name__)

STATUS_ACTIVE = "active"
STATUS_BLOCKED = "blocked"
STATUS_MISCONFIGURED = "misconfigured"
STATUS_DISABLED = "disabled"


async def ensure_source_health_schema(session: Optional[AsyncSession] = None) -> None:
    create_table = text(
        """
        CREATE TABLE IF NOT EXISTS source_health (
            source_key TEXT PRIMARY KEY,
            source_type TEXT DEFAULT '',
            source_name TEXT DEFAULT '',
            source_url TEXT DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active',
            last_error_code TEXT,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            blocked_until TEXT,
            last_checked_at TEXT,
            last_success_at TEXT
        )
        """
    )
    create_index = text("CREATE INDEX IF NOT EXISTS idx_source_health_status ON source_health(status, blocked_until)")
    if session is not None:
        await session.execute(create_table)
        await session.execute(create_index)
        return
    async with db_module._async_session_factory() as owned:
        await owned.execute(create_table)
        await owned.execute(create_index)
        await owned.commit()


def validate_source_url(url: str) -> bool:
    parsed = urlparse((url or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


async def _get_row(session: AsyncSession, source_key: str) -> Optional[dict[str, Any]]:
    row = (
        await session.execute(
            text("SELECT * FROM source_health WHERE source_key = :source_key"),
            {"source_key": source_key},
        )
    ).mappings().first()
    return dict(row) if row else None


async def _upsert_row(
    session: AsyncSession,
    *,
    source_key: str,
    source_type: str,
    source_name: str,
    source_url: str,
    status: str,
    last_error_code: Optional[str],
    consecutive_failures: int,
    blocked_until: Optional[str],
    last_checked_at: Optional[str],
    last_success_at: Optional[str],
) -> None:
    await session.execute(
        text(
            """
            INSERT INTO source_health (
                source_key, source_type, source_name, source_url, status,
                last_error_code, consecutive_failures, blocked_until,
                last_checked_at, last_success_at
            ) VALUES (
                :source_key, :source_type, :source_name, :source_url, :status,
                :last_error_code, :consecutive_failures, :blocked_until,
                :last_checked_at, :last_success_at
            )
            ON CONFLICT(source_key) DO UPDATE SET
                source_type = excluded.source_type,
                source_name = excluded.source_name,
                source_url = excluded.source_url,
                status = excluded.status,
                last_error_code = excluded.last_error_code,
                consecutive_failures = excluded.consecutive_failures,
                blocked_until = excluded.blocked_until,
                last_checked_at = excluded.last_checked_at,
                last_success_at = excluded.last_success_at
            """
        ),
        {
            "source_key": source_key,
            "source_type": source_type,
            "source_name": source_name,
            "source_url": source_url,
            "status": status,
            "last_error_code": last_error_code,
            "consecutive_failures": consecutive_failures,
            "blocked_until": blocked_until,
            "last_checked_at": last_checked_at,
            "last_success_at": last_success_at,
        },
    )


def _log_state_change(
    *,
    logger: Optional[logging.Logger],
    source_name: str,
    source_url: str,
    status: str,
    error_code: Optional[str],
    blocked_until: Optional[str],
) -> None:
    (logger or log).warning(
        "[source_health] source=%s url=%s status=%s error_code=%s next_retry=%s",
        source_name,
        source_url,
        status,
        error_code or "-",
        blocked_until or "-",
    )


async def should_skip_source(
    *,
    source_key: str,
    source_type: str,
    source_name: str,
    source_url: str,
) -> tuple[bool, Optional[dict[str, Any]]]:
    await ensure_source_health_schema()
    async with db_module._async_session_factory() as session:
        row = await _get_row(session, source_key)
        if not row:
            return False, None
        now = _parse_iso(_utcnow_iso())
        blocked_until = _parse_iso(row.get("blocked_until"))
        should_skip = row.get("status") in {STATUS_DISABLED, STATUS_MISCONFIGURED}
        should_skip = should_skip or (
            row.get("status") == STATUS_BLOCKED and blocked_until is not None and blocked_until > now
        )
        if should_skip:
            await _upsert_row(
                session,
                source_key=source_key,
                source_type=source_type,
                source_name=source_name,
                source_url=source_url,
                status=row.get("status") or STATUS_ACTIVE,
                last_error_code=row.get("last_error_code"),
                consecutive_failures=int(row.get("consecutive_failures") or 0),
                blocked_until=row.get("blocked_until"),
                last_checked_at=_utcnow_iso(),
                last_success_at=row.get("last_success_at"),
            )
            await session.commit()
        return should_skip, row


async def mark_source_success(
    *,
    source_key: str,
    source_type: str,
    source_name: str,
    source_url: str,
    logger: Optional[logging.Logger] = None,
) -> None:
    await ensure_source_health_schema()
    async with db_module._async_session_factory() as session:
        previous = await _get_row(session, source_key)
        now = _utcnow_iso()
        await _upsert_row(
            session,
            source_key=source_key,
            source_type=source_type,
            source_name=source_name,
            source_url=source_url,
            status=STATUS_ACTIVE,
            last_error_code=None,
            consecutive_failures=0,
            blocked_until=None,
            last_checked_at=now,
            last_success_at=now,
        )
        await session.commit()
        if previous and previous.get("status") != STATUS_ACTIVE:
            _log_state_change(
                logger=logger,
                source_name=source_name,
                source_url=source_url,
                status=STATUS_ACTIVE,
                error_code=None,
                blocked_until=None,
            )


async def record_source_failure(
    *,
    source_key: str,
    source_type: str,
    source_name: str,
    source_url: str,
    error_code: Any,
    logger: Optional[logging.Logger] = None,
    failure_status: Optional[str] = None,
    block_threshold: Optional[int] = None,
    cooldown_seconds: Optional[int] = None,
) -> dict[str, Any]:
    await ensure_source_health_schema()
    async with db_module._async_session_factory() as session:
        previous = await _get_row(session, source_key) or {}
        last_error_code = str(error_code)
        previous_error_code = str(previous.get("last_error_code")) if previous.get("last_error_code") is not None else None
        consecutive_failures = (
            int(previous.get("consecutive_failures") or 0) + 1
            if previous_error_code == last_error_code
            else 1
        )
        status = previous.get("status") or STATUS_ACTIVE
        blocked_until = previous.get("blocked_until")
        if failure_status == STATUS_MISCONFIGURED:
            status = STATUS_MISCONFIGURED
            blocked_until = None
        elif last_error_code == "403" and block_threshold and cooldown_seconds and consecutive_failures >= block_threshold:
            status = STATUS_BLOCKED
            blocked_until = (
                datetime.now(timezone.utc) + timedelta(seconds=cooldown_seconds)
            ).replace(microsecond=0).isoformat()
        now = _utcnow_iso()
        await _upsert_row(
            session,
            source_key=source_key,
            source_type=source_type,
            source_name=source_name,
            source_url=source_url,
            status=status,
            last_error_code=last_error_code,
            consecutive_failures=consecutive_failures,
            blocked_until=blocked_until,
            last_checked_at=now,
            last_success_at=previous.get("last_success_at"),
        )
        await session.commit()
        if previous.get("status") != status or previous.get("blocked_until") != blocked_until:
            _log_state_change(
                logger=logger,
                source_name=source_name,
                source_url=source_url,
                status=status,
                error_code=last_error_code,
                blocked_until=blocked_until,
            )
        return {
            "status": status,
            "last_error_code": last_error_code,
            "consecutive_failures": consecutive_failures,
            "blocked_until": blocked_until,
        }


async def list_source_health(session: Optional[AsyncSession] = None) -> list[dict[str, Any]]:
    await ensure_source_health_schema(session)
    if session is not None:
        rows = (
            await session.execute(
                text(
                    """
                    SELECT source_key, source_type, source_name, source_url, status,
                           last_error_code, consecutive_failures, blocked_until,
                           last_checked_at, last_success_at
                    FROM source_health
                    ORDER BY source_name, source_key
                    """
                )
            )
        ).mappings().all()
        return [dict(row) for row in rows]
    async with db_module._async_session_factory() as owned:
        return await list_source_health(owned)
