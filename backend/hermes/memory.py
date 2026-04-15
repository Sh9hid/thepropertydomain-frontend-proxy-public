from __future__ import annotations

from typing import Dict, List, Optional

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import HermesCampaign, HermesContent, HermesFinding, HermesMemoryEntry


async def write_memory_entry(
    session: AsyncSession,
    *,
    memory_type: str,
    title: str,
    body: str,
    tags: Optional[List[str]] = None,
    source_refs: Optional[List[str]] = None,
    confidence_score: float = 0.5,
    expires_at: Optional[str] = None,
) -> HermesMemoryEntry:
    existing = (
        await session.execute(
            select(HermesMemoryEntry).where(
                HermesMemoryEntry.memory_type == memory_type,
                HermesMemoryEntry.title == title,
                HermesMemoryEntry.body == body,
            )
        )
    ).scalars().first()
    if existing:
        return existing

    entry = HermesMemoryEntry(
        memory_type=memory_type,
        title=title,
        body=body,
        tags_json=list(tags or []),
        source_refs_json=list(source_refs or []),
        confidence_score=confidence_score,
        expires_at=expires_at,
    )
    session.add(entry)
    await session.flush()
    return entry


async def store_finding_memory(session: AsyncSession, finding: HermesFinding) -> HermesMemoryEntry:
    return await write_memory_entry(
        session,
        memory_type="market_memory",
        title=finding.topic,
        body=f"{finding.summary}\n\nWhy it matters: {finding.why_it_matters}",
        tags=[finding.company_scope, finding.signal_type, finding.source_type],
        source_refs=[finding.source_url],
        confidence_score=finding.confidence_score,
    )


async def store_content_approval_memory(
    session: AsyncSession,
    content: HermesContent,
    approved_by: str,
    note: str,
) -> HermesMemoryEntry:
    hypothesis = note.strip() or "Approved because the draft is grounded, native, and reusable."
    return await write_memory_entry(
        session,
        memory_type="content_memory",
        title=f"{content.content_type} approved for {content.audience}",
        body=f"{content.hook}\n\nHypothesis: {hypothesis}\nApproved by: {approved_by}",
        tags=[content.content_type, content.audience, "approved"],
        source_refs=content.source_refs_json,
        confidence_score=0.8,
    )


async def store_campaign_approval_memory(
    session: AsyncSession,
    campaign: HermesCampaign,
    approved_by: str,
    note: str,
) -> HermesMemoryEntry:
    rationale = note.strip() or "Approved as a reusable audience/channel pattern."
    return await write_memory_entry(
        session,
        memory_type="channel_memory",
        title=f"{campaign.campaign_type} {campaign.channel} sequence approved",
        body=f"{campaign.subject}\n\nGoal: {campaign.goal}\nRationale: {rationale}\nApproved by: {approved_by}",
        tags=[campaign.campaign_type, campaign.channel, campaign.stage, "approved"],
        confidence_score=0.82,
    )


def _ranking_rows(rows) -> List[Dict]:
    return [
        {
            "label": row[0],
            "count": int(row[1] or 0),
            "avg_score": round(float(row[2] or 0.0), 4),
        }
        for row in rows
        if row[0]
    ]


async def build_learning_loops(session: AsyncSession) -> Dict[str, List[Dict]]:
    source_type_rows = (
        await session.execute(
            select(
                HermesFinding.source_type,
                func.count(HermesFinding.id),
                func.avg(HermesFinding.actionability_score),
            )
            .group_by(HermesFinding.source_type)
            .order_by(func.avg(HermesFinding.actionability_score).desc(), func.count(HermesFinding.id).desc())
        )
    ).all()

    content_rows = (
        await session.execute(
            select(
                HermesContent.content_type,
                func.count(HermesContent.id),
                func.avg(case((HermesContent.status == "approved", 1.0), else_=0.0)),
            )
            .group_by(HermesContent.content_type)
            .order_by(func.count(HermesContent.id).desc())
        )
    ).all()

    campaign_rows = (
        await session.execute(
            select(
                HermesCampaign.channel,
                func.count(HermesCampaign.id),
                func.avg(case((HermesCampaign.status == "approved", 1.0), else_=0.0)),
            )
            .group_by(HermesCampaign.channel)
            .order_by(func.count(HermesCampaign.id).desc())
        )
    ).all()

    repo_rows = (
        await session.execute(
            select(
                HermesFinding.topic,
                func.count(HermesFinding.id),
                func.avg(HermesFinding.actionability_score),
            )
            .where(HermesFinding.source_type == "repo")
            .group_by(HermesFinding.topic)
            .order_by(func.avg(HermesFinding.actionability_score).desc())
        )
    ).all()

    return {
        "source_type_rankings": _ranking_rows(source_type_rows),
        "content_type_rankings": _ranking_rows(content_rows),
        "campaign_approval_rankings": _ranking_rows(campaign_rows),
        "repo_pattern_rankings": _ranking_rows(repo_rows),
    }


async def get_memory_snapshot(session: AsyncSession, limit: int = 50) -> Dict:
    entries = (
        await session.execute(
            select(HermesMemoryEntry).order_by(HermesMemoryEntry.created_at.desc()).limit(limit)
        )
    ).scalars().all()
    return {
        "entries": entries,
        "learning_loops": await build_learning_loops(session),
    }
