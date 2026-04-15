from __future__ import annotations

import json
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso


def _norm_address(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", str(value or "").strip().lower())).strip()


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else [parsed]
    except Exception:
        return []


async def apply_precall_hygiene(session: AsyncSession, *, limit: int = 800) -> dict[str, int]:
    """
    GAP 22 guardrail:
    - If a lead address has a sold_event -> drop from call queue.
    - If listing signals indicate actively listed by competitor -> mark as competitor_listed and drop.
    """
    now = now_iso()
    changed_sold = 0
    changed_listed = 0

    rows = (
        await session.execute(
            text(
                """
                SELECT id, address, status, source_tags, risk_flags, contact_status,
                       last_listing_status, listing_status_history, signal_status, trigger_type
                FROM leads
                WHERE status NOT IN ('converted', 'dropped')
                ORDER BY COALESCE(updated_at, created_at, '') DESC
                LIMIT :limit
                """
            ),
            {"limit": max(50, min(int(limit or 800), 5000))},
        )
    ).mappings().all()

    sold_rows = (await session.execute(text("SELECT address FROM sold_events WHERE address IS NOT NULL"))).mappings().all()
    sold_norm = {_norm_address(str(row.get("address") or "")) for row in sold_rows if str(row.get("address") or "").strip()}

    for row in rows:
        lead_id = str(row.get("id") or "")
        if not lead_id:
            continue
        address = _norm_address(str(row.get("address") or ""))
        if not address:
            continue

        tags = [str(t) for t in _json_list(row.get("source_tags")) if str(t).strip()]
        risks = [str(t) for t in _json_list(row.get("risk_flags")) if str(t).strip()]

        # Sold wash
        if address in sold_norm:
            if "sold_wash" not in tags:
                tags.append("sold_wash")
            if "sold_recently" not in risks:
                risks.append("sold_recently")
            await session.execute(
                text(
                    """
                    UPDATE leads
                    SET status = 'dropped',
                        contact_status = 'sold_wash',
                        stage_note = :note,
                        source_tags = :tags,
                        risk_flags = :risks,
                        updated_at = :now
                    WHERE id = :id
                    """
                ),
                {
                    "id": lead_id,
                    "note": "Auto-wash: matched sold record, removed from outbound call queue.",
                    "tags": json.dumps(tags),
                    "risks": json.dumps(risks),
                    "now": now,
                },
            )
            changed_sold += 1
            continue

        # Active listing / competitor listing wash
        last_listing_status = str(row.get("last_listing_status") or "").lower()
        signal_status = str(row.get("signal_status") or "").lower()
        trigger_type = str(row.get("trigger_type") or "").lower()
        listing_history = json.dumps(_json_list(row.get("listing_status_history"))).lower()

        listed_now = any(
            marker in (last_listing_status + " " + signal_status + " " + trigger_type + " " + listing_history)
            for marker in ("for sale", "listed", "listing live", "active listing", "on market", "stale_active")
        )
        if listed_now:
            if "competitor_listed" not in tags:
                tags.append("competitor_listed")
            if "competitor_listed" not in risks:
                risks.append("competitor_listed")
            await session.execute(
                text(
                    """
                    UPDATE leads
                    SET status = 'dropped',
                        contact_status = 'competitor_listed',
                        stage_note = :note,
                        source_tags = :tags,
                        risk_flags = :risks,
                        updated_at = :now
                    WHERE id = :id
                    """
                ),
                {
                    "id": lead_id,
                    "note": "Auto-wash: actively listed/competitor-listed signal found. Suppressed from call queue.",
                    "tags": json.dumps(tags),
                    "risks": json.dumps(risks),
                    "now": now,
                },
            )
            changed_listed += 1

    if changed_sold or changed_listed:
        await session.commit()
    return {"sold_washed": changed_sold, "competitor_listed_washed": changed_listed}

