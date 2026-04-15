"""Audit log service — records system events for accountability."""
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso


async def write_lead_audit_log(
    session: AsyncSession,
    *,
    lead_id: str,
    action: str,
    actor: str = "system",
    detail: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> dict[str, Any]:
    """Write an audit log entry for a lead action."""
    entry = {
        "lead_id": lead_id,
        "action": action,
        "actor": actor,
        "detail": detail or "",
        "metadata": metadata or {},
        "created_at": now_iso(),
    }
    return entry
