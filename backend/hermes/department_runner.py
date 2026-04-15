"""
HERMES Department Runner

Executes a department cycle using NIM/Gemini.
Each department head runs its cycle_prompt against live DB context,
produces structured output, and stores findings.
"""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hermes.departments import get_department, get_workspace_departments

logger = logging.getLogger(__name__)


async def run_department_cycle(
    dept_id: str,
    session: AsyncSession,
    extra_context: str = "",
) -> Dict[str, Any]:
    """
    Run one cycle of a department head.

    1. Load department config
    2. Build live context from DB
    3. Call NIM/Gemini with department persona + cycle_prompt
    4. Store output as hermes finding
    5. Return summary
    """
    dept = get_department(dept_id)
    if not dept:
        return {"error": f"Department '{dept_id}' not found"}

    # ── SDK Bridge: route Tier 1 departments through autonomous agent loop ──
    try:
        from agents.bridge import should_use_sdk, run_department_via_sdk
        if should_use_sdk(dept_id):
            logger.info(f"[Dept:{dept_id}] Routing to SDK agent loop")
            try:
                return await run_department_via_sdk(dept_id, session, extra_context)
            except Exception as sdk_exc:
                logger.warning(f"[Dept:{dept_id}] SDK agent failed, falling back to ai_ask: {sdk_exc}")
    except ImportError:
        pass  # agents package not available — continue with ai_ask

    workspace = dept["workspace"]
    persona = dept["persona"]
    cycle_prompt = dept["cycle_prompt"]
    name = dept["name"]

    context = await _build_live_context(dept_id, workspace, session)
    if extra_context:
        context += f"\n\nAdditional context:\n{extra_context}"

    full_prompt = f"{cycle_prompt}\n\nLIVE DATA:\n{context}"

    logger.info(f"[Dept:{dept_id}] Running cycle for '{name}'")

    try:
        from services.ai_router import ask as ai_ask
        raw_output = await ai_ask(
            task="operator_brief",
            prompt=full_prompt,
            system_override=persona,
        )
    except Exception as exc:
        logger.warning(f"[Dept:{dept_id}] AI call failed: {exc}")
        raw_output = ""

    if not raw_output:
        return {
            "dept_id": dept_id,
            "name": name,
            "status": "no_output",
            "findings": [],
        }

    finding = await _store_department_finding(dept_id, dept, raw_output, session)

    logger.info(f"[Dept:{dept_id}] Cycle complete")

    return {
        "dept_id": dept_id,
        "name": name,
        "workspace": workspace,
        "status": "complete",
        "output_preview": raw_output[:400],
        "finding_id": finding.get("id") if finding else None,
        "run_at": datetime.now(timezone.utc).isoformat(),
    }


async def run_all_workspace_departments(
    workspace: str,
    session: AsyncSession,
) -> List[Dict[str, Any]]:
    """Run all departments for a given workspace concurrently."""
    import asyncio

    depts = get_workspace_departments(workspace)
    if not depts:
        return []

    tasks = [run_department_cycle(dept["id"], session) for dept in depts]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    return [
        {"error": str(r)} if isinstance(r, Exception) else r
        for r in results
    ]


async def _build_live_context(dept_id: str, workspace: str, session: AsyncSession) -> str:
    """Pull relevant live data from DB to give the department agent context."""
    parts: List[str] = []

    try:
        rows = (await session.execute(text("""
            SELECT signal_status, COUNT(*) as cnt, AVG(heat_score) as avg_heat
            FROM leads
            WHERE status NOT IN ('converted', 'dropped')
            GROUP BY signal_status
            ORDER BY cnt DESC
            LIMIT 8
        """))).mappings().all()

        if rows:
            parts.append("LEAD PIPELINE:")
            for row in rows:
                parts.append(
                    f"  {row['signal_status'] or 'UNKNOWN'}: {row['cnt']} leads, "
                    f"avg heat {int(row['avg_heat'] or 0)}"
                )
    except Exception as exc:
        logger.debug(f"[Dept:{dept_id}] Pipeline query failed: {exc}")

    try:
        row = (await session.execute(text("""
            SELECT COUNT(*) as calls_today
            FROM call_log
            WHERE DATE(logged_at) = DATE('now')
        """))).mappings().first()
        if row:
            parts.append(f"CALLS TODAY: {row['calls_today']}")
    except Exception:
        pass

    if "lead_ops" in dept_id or "sales" in dept_id:
        try:
            urgent_rows = (await session.execute(text("""
                SELECT address, suburb, signal_status, heat_score, days_on_market, last_contacted_at
                FROM leads
                WHERE signal_status IN ('WITHDRAWN', 'EXPIRED', 'PROBATE')
                AND status NOT IN ('converted', 'dropped')
                ORDER BY heat_score DESC, created_at DESC
                LIMIT 10
            """))).mappings().all()

            if urgent_rows:
                parts.append("\nURGENT LEADS:")
                for r in urgent_rows:
                    contact = (
                        "never contacted"
                        if not r.get("last_contacted_at")
                        else f"last: {str(r['last_contacted_at'])[:10]}"
                    )
                    dom = f" ({r['days_on_market']}d DOM)" if r.get("days_on_market") else ""
                    parts.append(
                        f"  {r['address']}, {r['suburb']} — "
                        f"{r['signal_status']}{dom} — heat {r['heat_score']} — {contact}"
                    )
        except Exception as exc:
            logger.debug(f"[Dept:{dept_id}] Urgent leads query failed: {exc}")

    return "\n".join(parts) if parts else "No live data available."


async def _store_department_finding(
    dept_id: str,
    dept: Dict[str, Any],
    content: str,
    session: AsyncSession,
) -> Dict[str, Any] | None:
    """Store department output as a HERMES finding."""
    try:
        from core.utils import now_iso

        now = now_iso()
        dedupe_key = hashlib.md5(f"{dept_id}:{now[:13]}".encode()).hexdigest()  # hourly dedup
        finding_id = str(hashlib.md5(f"{dept_id}:{now}".encode()).hexdigest())

        await session.execute(text("""
            INSERT OR IGNORE INTO hermes_findings
                (id, source_id, source_type, source_name, source_url, dedupe_key,
                 company_scope, topic, signal_type, summary, why_it_matters,
                 confidence_score, actionability_score, novelty_score, created_at)
            VALUES
                (:id, :source_id, 'department_cycle', :source_name, '', :dedupe_key,
                 'shared', :topic, 'department_cycle', :summary, :why_it_matters,
                 0.8, 0.9, 0.75, :now)
        """), {
            "id": finding_id,
            "source_id": dept_id,
            "source_name": dept["name"],
            "dedupe_key": dedupe_key,
            "topic": f"[{dept['name']}] Cycle Output",
            "summary": content[:1000],
            "why_it_matters": dept["goal"],
            "now": now,
        })
        await session.commit()
        return {"id": finding_id}
    except Exception as exc:
        logger.warning(f"[Dept:{dept_id}] Finding store failed: {exc}")
        return None
