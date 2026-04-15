"""
MCP Tool Server: rea

Wraps rea_listing_worker.py functions for the REA ATLAS agent.
All mutating operations (execute_push, execute_refresh) require operator approval.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

log = logging.getLogger(__name__)

TOOLS: list[dict[str, Any]] = [
    {
        "name": "rea_analyze_portfolio",
        "description": (
            "Analyze current REA land listing portfolio performance. "
            "Returns views, enquiries, CTR by variant/suburb/size with top/bottom performers."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "rea_generate_push_plan",
        "description": (
            "Generate a plan to push unpublished land lots to REA. "
            "Returns list of lots to push with suggested titles/descriptions. "
            "Does NOT execute — requires operator approval."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "daily_limit": {
                    "type": "integer",
                    "description": "Max lots to push per day (default 15)",
                    "default": 15,
                },
            },
        },
    },
    {
        "name": "rea_generate_refresh_plan",
        "description": (
            "Generate a refresh plan for underperforming listings. "
            "Suggests title/description/price changes for low-CTR listings. "
            "Does NOT execute — requires operator approval."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Max listings to refresh (default 10)",
                    "default": 10,
                },
            },
        },
    },
    {
        "name": "rea_execute_push",
        "description": (
            "Execute a previously approved push plan. Publishes selected lots to REA. "
            "IMPORTANT: Only call this after operator has approved the push plan."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "lead_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of lead IDs to push to REA",
                },
            },
            "required": ["lead_ids"],
        },
    },
    {
        "name": "rea_execute_refresh",
        "description": (
            "Execute a previously approved refresh plan. Updates selected listings on REA. "
            "IMPORTANT: Only call this after operator has approved the refresh plan."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "lead_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of lead IDs to refresh on REA",
                },
            },
            "required": ["lead_ids"],
        },
    },
    {
        "name": "rea_pull_performance",
        "description": "Pull latest performance metrics from REA for all live listings.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "rea_self_improve",
        "description": (
            "Analyze what's working and what isn't across the portfolio. "
            "Suggests copy rotation strategies based on performance data."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "rea_list_listings",
        "description": "List current land listings with their REA status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Filter: 'live', 'unpushed', 'all' (default 'all')",
                    "default": "all",
                },
            },
        },
    },
]


async def execute(
    tool_name: str,
    params: dict[str, Any],
    session: AsyncSession,
    agent_id: str = "rea_atlas",
) -> str:
    """Execute a REA tool and return JSON string result."""
    try:
        fn = _EXECUTORS.get(tool_name)
        if not fn:
            return json.dumps({"error": f"Unknown REA tool: {tool_name}"})
        result = await fn(session, params)
        return json.dumps(result, default=str)
    except Exception as exc:
        log.warning("[mcp_rea] Tool %s failed: %s", tool_name, exc)
        return json.dumps({"error": str(exc)})


async def _analyze_portfolio(session: AsyncSession, params: dict) -> Any:
    from hermes.workers.rea_listing_worker import analyze_portfolio
    return await analyze_portfolio(session)


async def _generate_push_plan(session: AsyncSession, params: dict) -> Any:
    from hermes.workers.rea_listing_worker import generate_push_plan
    daily_limit = int(params.get("daily_limit", 15))
    return await generate_push_plan(session, daily_limit=daily_limit)


async def _generate_refresh_plan(session: AsyncSession, params: dict) -> Any:
    from hermes.workers.rea_listing_worker import generate_refresh_plan
    limit = int(params.get("limit", 10))
    return await generate_refresh_plan(session, limit=limit)


async def _execute_push(session: AsyncSession, params: dict) -> Any:
    from hermes.workers.rea_listing_worker import execute_push
    lead_ids = params.get("lead_ids", [])
    if not lead_ids:
        return {"error": "No lead_ids provided"}
    return await execute_push(session, lead_ids)


async def _execute_refresh(session: AsyncSession, params: dict) -> Any:
    from hermes.workers.rea_listing_worker import execute_refresh
    lead_ids = params.get("lead_ids", [])
    if not lead_ids:
        return {"error": "No lead_ids provided"}
    return await execute_refresh(session, lead_ids)


async def _pull_performance(session: AsyncSession, params: dict) -> Any:
    from hermes.workers.rea_listing_worker import pull_performance
    return await pull_performance(session)


async def _self_improve(session: AsyncSession, params: dict) -> Any:
    from hermes.workers.rea_listing_worker import self_improve
    return await self_improve(session)


async def _list_listings(session: AsyncSession, params: dict) -> Any:
    from sqlalchemy import text as sql_text
    status_filter = params.get("status", "all")
    if status_filter == "live":
        q = """
            SELECT id, address, suburb, land_size_sqm, estimated_value_mid,
                   rea_listing_id, rea_views, rea_enquiries, rea_last_edit_at
            FROM leads
            WHERE COALESCE(rea_listing_id, '') <> ''
              AND (LOWER(COALESCE(property_type, '')) = 'land'
                   OR LOWER(COALESCE(trigger_type, '')) = 'bathla_land')
            ORDER BY rea_enquiries DESC
        """
    elif status_filter == "unpushed":
        q = """
            SELECT id, address, suburb, land_size_sqm, estimated_value_mid
            FROM leads
            WHERE COALESCE(rea_listing_id, '') = ''
              AND (LOWER(COALESCE(property_type, '')) = 'land'
                   OR LOWER(COALESCE(trigger_type, '')) = 'bathla_land')
            ORDER BY estimated_value_mid DESC
        """
    else:
        q = """
            SELECT id, address, suburb, land_size_sqm, estimated_value_mid,
                   rea_listing_id, rea_views, rea_enquiries
            FROM leads
            WHERE LOWER(COALESCE(property_type, '')) = 'land'
               OR LOWER(COALESCE(trigger_type, '')) = 'bathla_land'
            ORDER BY COALESCE(rea_listing_id, '') DESC, estimated_value_mid DESC
        """
    rows = (await session.execute(sql_text(q))).mappings().all()
    return [dict(r) for r in rows]


_EXECUTORS = {
    "rea_analyze_portfolio": _analyze_portfolio,
    "rea_generate_push_plan": _generate_push_plan,
    "rea_generate_refresh_plan": _generate_refresh_plan,
    "rea_execute_push": _execute_push,
    "rea_execute_refresh": _execute_refresh,
    "rea_pull_performance": _pull_performance,
    "rea_self_improve": _self_improve,
    "rea_list_listings": _list_listings,
}
