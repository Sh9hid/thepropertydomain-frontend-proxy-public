"""
Nurture Sequence Service — Auto-create follow-up cycles based on response.

When a lead says "not interested now" or "call me in 6 months", this service
creates a structured nurture sequence with appropriate content and timing.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from models.sales_core_models import TaskQueue


# ─── Nurture Templates ────────────────────────────────────────────────────────

NURTURE_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "not_interested_timing": {
        "name": "Timing Objection Nurture",
        "frequency": "quarterly",
        "duration_days": 365,
        "content_types": ["market_update", "comparable_sales", "area_news"],
        "description": "Quarterly market updates for leads who aren't ready to sell now",
    },
    "price_issue": {
        "name": "Price Sensitivity Nurture",
        "frequency": "monthly",
        "duration_days": 180,
        "content_types": ["price_trends", "comparable_sales", "market_timing"],
        "description": "Monthly price trend updates for price-sensitive leads",
    },
    "has_agent": {
        "name": "Has Agent Nurture",
        "frequency": "biannually",
        "duration_days": 365,
        "content_types": ["market_update"],
        "description": "Biannual market updates for leads with existing agents",
        "trigger": "listing_expiration",
    },
    "long_term_prospect": {
        "name": "Long-Term Prospect Nurture",
        "frequency": "quarterly",
        "duration_days": 730,
        "content_types": ["market_update", "area_development", "community_news"],
        "description": "Quarterly updates for long-term prospects (2+ year timeline)",
    },
    "probate_sensitive": {
        "name": "Probate Sensitivity Nurture",
        "frequency": "monthly",
        "duration_days": 90,
        "content_types": ["probate_guidance", "estate_support", "market_update"],
        "description": "Monthly supportive content for probate leads",
    },
    "mortgage_cliff": {
        "name": "Mortgage Cliff Nurture",
        "frequency": "monthly",
        "duration_days": 180,
        "content_types": ["rate_alert", "refinance_options", "lender_comparison"],
        "description": "Monthly rate alerts for mortgage cliff leads",
    },
    "general_nurture": {
        "name": "General Nurture",
        "frequency": "monthly",
        "duration_days": 180,
        "content_types": ["market_update", "tips"],
        "description": "General monthly nurture for warm leads",
    },
}


def select_nurture_template(
    objections: List[str],
    timeline_markers: List[Dict[str, Any]],
    outcome: str,
) -> str:
    """Select the appropriate nurture template based on objections and timeline."""
    # Long-term timeline markers
    for marker in timeline_markers:
        if marker.get("type") == "years":
            try:
                years = int(marker.get("detail", 0))
                if years >= 2:
                    return "long_term_prospect"
            except (ValueError, TypeError):
                pass

    # Objection-based templates
    if "probate_sensitive" in objections:
        return "probate_sensitive"
    if "price" in objections:
        return "price_issue"
    if "has_agent" in objections:
        return "has_agent"
    if "timing" in objections:
        return "not_interested_timing"

    # Outcome-based templates
    outcome_lower = outcome.lower()
    if "mortgage" in outcome_lower or "refinance" in outcome_lower:
        return "mortgage_cliff"

    return "general_nurture"


def calculate_task_dates(
    frequency: str,
    duration_days: int,
    start_date: Optional[datetime] = None,
) -> List[datetime]:
    """Calculate task dates based on frequency and duration."""
    if start_date is None:
        start_date = datetime.utcnow()

    dates = []
    current = start_date

    if frequency == "monthly":
        interval = timedelta(days=30)
    elif frequency == "quarterly":
        interval = timedelta(days=90)
    elif frequency == "biannually":
        interval = timedelta(days=180)
    else:
        interval = timedelta(days=30)

    end_date = start_date + timedelta(days=duration_days)

    while current < end_date:
        dates.append(current)
        current += interval

    return dates


async def create_nurture_sequence(
    session: AsyncSession,
    workspace_key: str,
    lead_contact_id: str,
    lead_id: str,
    template_key: str,
    start_date: Optional[datetime] = None,
    reason: str = "",
    created_by: str = "hermes",
) -> List[TaskQueue]:
    """Create a nurture sequence for a lead."""
    template = NURTURE_TEMPLATES.get(template_key, NURTURE_TEMPLATES["general_nurture"])

    # Calculate task dates
    task_dates = calculate_task_dates(
        template["frequency"],
        template["duration_days"],
        start_date,
    )

    # Create tasks
    tasks = []
    for i, due_date in enumerate(task_dates):
        content_type = template["content_types"][i % len(template["content_types"])]
        task = TaskQueue(
            id=str(uuid.uuid4()),
            business_context_key=workspace_key,
            lead_contact_id=lead_contact_id,
            task_type=f"nurture_{content_type}",
            due_at=due_date,
            status="pending",
            priority=50 - (i * 5),
            reason=f"Nurture sequence: {template['name']} - {reason}",
            payload_json={
                "nurture_template": template_key,
                "content_type": content_type,
                "sequence_step": i + 1,
                "total_steps": len(task_dates),
                "template_name": template["name"],
            },
            created_by=created_by,
        )
        session.add(task)
        tasks.append(task)

    await session.commit()
    for task in tasks:
        await session.refresh(task)

    return tasks


async def create_nurture_from_outcome(
    session: AsyncSession,
    workspace_key: str,
    lead_contact_id: str,
    lead_id: str,
    outcome: str,
    note: str,
    objections: List[str],
    timeline_markers: List[Dict[str, Any]],
    created_by: str = "hermes",
) -> Dict[str, Any]:
    """Create nurture sequence based on call outcome."""
    # Select template
    template_key = select_nurture_template(objections, timeline_markers, outcome)
    template = NURTURE_TEMPLATES[template_key]

    # Calculate start date (now + cooldown)
    start_date = datetime.utcnow() + timedelta(days=30)

    # Create tasks
    tasks = await create_nurture_sequence(
        session,
        workspace_key=workspace_key,
        lead_contact_id=lead_contact_id,
        lead_id=lead_id,
        template_key=template_key,
        start_date=start_date,
        reason=f"Auto-created from outcome: {outcome}",
        created_by=created_by,
    )

    return {
        "template_key": template_key,
        "template_name": template["name"],
        "tasks_created": len(tasks),
        "start_date": start_date.isoformat(),
        "duration_days": template["duration_days"],
        "frequency": template["frequency"],
    }
