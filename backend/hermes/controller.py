from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso
from hermes.integrations.nvidia_nim import build_embed_provider, build_llm_provider
from hermes.jobs import APPROVED, COMPLETED, FAILED, PENDING_APPROVAL, RUNNING, source_due
from hermes.memory import (
    get_memory_snapshot,
    store_campaign_approval_memory,
    store_content_approval_memory,
    store_finding_memory,
)
from hermes.models import (
    HermesCampaign,
    HermesContent,
    HermesFinding,
    HermesRun,
    HermesSource,
    ensure_hermes_schema,
    reset_hermes_schema_state,
)
from hermes.scoring import build_dedupe_key
from hermes.workers.content_worker import generate_content_drafts
from hermes.workers.digest_worker import build_digest
from hermes.workers.outreach_worker import build_campaign_drafts
from hermes.workers.repurpose_worker import repurpose_item
from hermes.workers.research_worker import collect_findings_for_source
from hermes.workers import lead_signal_worker as _lead_signal_worker


log = logging.getLogger(__name__)


def _source_to_dict(source: HermesSource) -> Dict:
    return {
        "id": source.id,
        "name": source.name,
        "source_type": source.source_type,
        "base_url": source.base_url,
        "rss_url": source.rss_url,
        "enabled": source.enabled,
        "fetch_frequency_minutes": source.fetch_frequency_minutes,
        "tags": source.tags_json,
        "company_scope": source.company_scope,
        "credibility_score": source.credibility_score,
        "last_fetched_at": source.last_fetched_at,
        "created_at": source.created_at,
        "updated_at": source.updated_at,
    }


def _run_to_dict(run: HermesRun) -> Dict:
    output = run.output_json or {}
    return {
        "id": run.id,
        "job_type": run.job_type,
        "status": run.status,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "input_json": run.input_json,
        "output_json": output,
        "summary": output.get("summary", {}),
        "trace": output.get("trace", []),
        "error_text": run.error_text,
    }


def _finding_to_dict(finding: HermesFinding) -> Dict:
    return {
        "id": finding.id,
        "source_id": finding.source_id,
        "source_type": finding.source_type,
        "source_name": finding.source_name,
        "source_url": finding.source_url,
        "company_scope": finding.company_scope,
        "topic": finding.topic,
        "signal_type": finding.signal_type,
        "summary": finding.summary,
        "why_it_matters": finding.why_it_matters,
        "novelty_score": finding.novelty_score,
        "confidence_score": finding.confidence_score,
        "actionability_score": finding.actionability_score,
        "proposed_actions": finding.proposed_actions_json,
        "published_at": finding.published_at,
        "created_at": finding.created_at,
    }


def _content_to_dict(content: HermesContent) -> Dict:
    return {
        "id": content.id,
        "content_type": content.content_type,
        "audience": content.audience,
        "hook": content.hook,
        "body": content.body,
        "cta": content.cta,
        "status": content.status,
        "source_refs": content.source_refs_json,
        "repurposable": content.repurposable,
        "scheduled_for": content.scheduled_for,
        "published_at": content.published_at,
        "created_at": content.created_at,
        "updated_at": content.updated_at,
    }


def _campaign_to_dict(campaign: HermesCampaign) -> Dict:
    return {
        "id": campaign.id,
        "campaign_type": campaign.campaign_type,
        "audience": campaign.audience,
        "channel": campaign.channel,
        "stage": campaign.stage,
        "subject": campaign.subject,
        "message": campaign.message,
        "goal": campaign.goal,
        "status": campaign.status,
        "related_lead_id": campaign.related_lead_id,
        "created_at": campaign.created_at,
        "sent_at": campaign.sent_at,
    }


class HermesController:
    def __init__(self) -> None:
        self.llm_provider = build_llm_provider()
        self.embed_provider = build_embed_provider()
        self._scheduler_task: Optional[asyncio.Task] = None

    async def ensure_ready(self) -> None:
        await ensure_hermes_schema()

    async def create_source(self, session: AsyncSession, payload) -> Dict:
        await self.ensure_ready()
        source = HermesSource(
            name=payload.name,
            source_type=payload.source_type,
            base_url=payload.base_url,
            rss_url=payload.rss_url,
            enabled=payload.enabled,
            fetch_frequency_minutes=payload.fetch_frequency_minutes,
            tags_json=list(payload.tags or []),
            company_scope=payload.company_scope,
            credibility_score=payload.credibility_score,
            created_at=now_iso(),
            updated_at=now_iso(),
        )
        session.add(source)
        await session.commit()
        await session.refresh(source)
        return {"source": _source_to_dict(source)}

    async def list_sources(self, session: AsyncSession) -> Dict:
        await self.ensure_ready()
        sources = (await session.execute(select(HermesSource).order_by(HermesSource.updated_at.desc()))).scalars().all()
        return {"sources": [_source_to_dict(source) for source in sources]}

    async def patch_source(self, session: AsyncSession, source_id: str, payload) -> Dict:
        await self.ensure_ready()
        source = (await session.execute(select(HermesSource).where(HermesSource.id == source_id))).scalars().first()
        if not source:
            raise ValueError("Unknown Hermes source")
        for field_name in [
            "name",
            "source_type",
            "base_url",
            "rss_url",
            "enabled",
            "fetch_frequency_minutes",
            "company_scope",
            "credibility_score",
        ]:
            value = getattr(payload, field_name)
            if value is not None:
                setattr(source, field_name, value)
        if payload.tags is not None:
            source.tags_json = list(payload.tags)
        source.updated_at = now_iso()
        session.add(source)
        await session.commit()
        await session.refresh(source)
        return {"source": _source_to_dict(source)}

    async def _create_run(self, session: AsyncSession, *, job_type: str, input_json: Dict) -> HermesRun:
        run = HermesRun(job_type=job_type, status=RUNNING, input_json=input_json, output_json={})
        session.add(run)
        await session.flush()
        return run

    async def _finish_run(
        self,
        session: AsyncSession,
        run: HermesRun,
        *,
        status: str,
        trace: List[Dict],
        summary: Dict,
        result: Optional[Dict] = None,
        error_text: str = "",
    ) -> None:
        run.status = status
        run.completed_at = now_iso()
        run.error_text = error_text or None
        run.output_json = {"summary": summary, "trace": trace, "result": result or {}}
        session.add(run)
        await session.commit()

    async def sync_sources(self, session: AsyncSession, source_ids: Optional[List[str]] = None, force: bool = False) -> Dict:
        await self.ensure_ready()
        source_ids = list(source_ids or [])
        trace: List[Dict] = []
        run = await self._create_run(session, job_type="SYNC_SOURCES", input_json={"source_ids": source_ids, "force": force})

        query = select(HermesSource)
        if source_ids:
            query = query.where(HermesSource.id.in_(source_ids))
        else:
            query = query.where(HermesSource.enabled == True)  # noqa: E712
        sources = (await session.execute(query.order_by(HermesSource.updated_at.desc()))).scalars().all()

        sources_processed = 0
        new_findings = 0
        deduped = 0

        try:
            for source in sources:
                if not source.enabled:
                    continue
                if not force and not source_due(source.last_fetched_at, source.fetch_frequency_minutes):
                    trace.append({"stage": "source_skip", "source_id": source.id, "reason": "not_due"})
                    continue

                payloads = await collect_findings_for_source(source, self.llm_provider, trace)
                sources_processed += 1
                for payload in payloads:
                    dedupe_key = build_dedupe_key(payload.url, payload.topic, None)
                    existing = await session.execute(select(HermesFinding.id).where(HermesFinding.dedupe_key == dedupe_key))
                    if existing.first():
                        deduped += 1
                        continue
                    finding = HermesFinding(
                        source_id=source.id,
                        source_type=payload.source_type,
                        source_name=payload.source_name,
                        source_url=payload.url,
                        dedupe_key=dedupe_key,
                        company_scope=payload.company_scope,
                        topic=payload.topic,
                        signal_type=payload.signal_type,
                        summary=payload.summary,
                        why_it_matters=payload.why_it_matters,
                        novelty_score=payload.novelty_score,
                        confidence_score=payload.confidence_score,
                        actionability_score=payload.actionability_score,
                        proposed_actions_json=list(payload.proposed_actions),
                        created_at=now_iso(),
                    )
                    session.add(finding)
                    await session.flush()
                    await store_finding_memory(session, finding)
                    new_findings += 1
                source.last_fetched_at = now_iso()
                source.updated_at = now_iso()
                session.add(source)

            await session.commit()
            feed = await self.get_feed(session)
            summary = {
                "sources_processed": sources_processed,
                "new_findings": new_findings,
                "deduped_findings": deduped,
            }
            await self._finish_run(session, run, status=COMPLETED, trace=trace, summary=summary, result={"digest": feed["digest"]})
            return {"run": _run_to_dict(run), "summary": summary, "trace": trace}
        except Exception as exc:
            await session.rollback()
            summary = {"sources_processed": sources_processed, "new_findings": new_findings, "deduped_findings": deduped}
            await self._finish_run(session, run, status=FAILED, trace=trace, summary=summary, error_text=str(exc))
            raise

    async def _load_findings(self, session: AsyncSession, finding_ids: List[str]) -> List[HermesFinding]:
        query = select(HermesFinding)
        if finding_ids:
            query = query.where(HermesFinding.id.in_(finding_ids))
        findings = (await session.execute(query.order_by(HermesFinding.created_at.desc()))).scalars().all()
        return findings if finding_ids else findings[:5]

    async def run_command(self, session: AsyncSession, payload) -> Dict:
        await self.ensure_ready()
        if payload.command_type == "SYNC_SOURCES":
            return await self.sync_sources(
                session,
                source_ids=payload.source_ids,
                force=bool(payload.options.get("force", False)),
            )

        trace: List[Dict] = [{"stage": "command_received", "command_type": payload.command_type, "prompt": payload.prompt}]
        run = await self._create_run(
            session,
            job_type=payload.command_type,
            input_json={
                "prompt": payload.prompt,
                "finding_ids": payload.finding_ids,
                "source_ids": payload.source_ids,
                "campaign_type": payload.campaign_type,
                "channel": payload.channel,
                "options": payload.options,
            },
        )

        result: Dict = {}
        summary: Dict = {}
        try:
            if payload.command_type in {"GENERATE_CONTENT", "REPURPOSE_ITEM"}:
                findings = await self._load_findings(session, payload.finding_ids)
                drafts = (
                    await generate_content_drafts(findings, self.llm_provider, trace)
                    if payload.command_type == "GENERATE_CONTENT"
                    else await repurpose_item(findings, self.llm_provider, trace)
                )
                content_ids: List[str] = []
                for draft in drafts:
                    content = HermesContent(
                        content_type=draft.content_type,
                        audience=draft.target_audience,
                        hook=draft.hook,
                        body=draft.body,
                        cta=draft.cta,
                        status=PENDING_APPROVAL,
                        source_refs_json=list(draft.source_refs),
                        repurposable=draft.repurposable,
                        created_at=now_iso(),
                        updated_at=now_iso(),
                    )
                    session.add(content)
                    await session.flush()
                    content_ids.append(content.id)
                summary = {"content_created": len(content_ids)}
                result = {"content_ids": content_ids}
            elif payload.command_type == "BUILD_CAMPAIGN":
                drafts = await build_campaign_drafts(
                    campaign_type=str(payload.campaign_type or "seller"),
                    prompt=payload.prompt,
                    provider=self.llm_provider,
                    trace=trace,
                    channel=str(payload.channel or "email"),
                )
                campaign_ids: List[str] = []
                for draft in drafts:
                    campaign = HermesCampaign(
                        campaign_type=draft.campaign_type,
                        audience=str(payload.campaign_type or "seller"),
                        channel=draft.channel,
                        stage=draft.stage,
                        subject=draft.subject,
                        message=draft.message,
                        goal=draft.goal,
                        status=PENDING_APPROVAL,
                        created_at=now_iso(),
                    )
                    session.add(campaign)
                    await session.flush()
                    campaign_ids.append(campaign.id)
                summary = {"campaigns_created": len(campaign_ids)}
                result = {"campaign_ids": campaign_ids}
            elif payload.command_type in {"SUMMARIZE_WEEK", "FIND_COMPETITOR_MOVES", "FIND_OPEN_SOURCE_PATTERNS", "RESEARCH_TOPIC"}:
                findings = await self._load_findings(session, payload.finding_ids)
                if payload.command_type == "FIND_COMPETITOR_MOVES":
                    findings = [
                        item
                        for item in findings
                        if any(token in f"{item.topic} {item.summary}".lower() for token in ("competitor", "release", "changelog", "changed"))
                    ]
                elif payload.command_type == "FIND_OPEN_SOURCE_PATTERNS":
                    findings = [item for item in findings if item.source_type == "repo"]

                # For RESEARCH_TOPIC: use AI to actually research the prompt topic
                ai_finding_id = None
                if payload.command_type == "RESEARCH_TOPIC" and payload.prompt:
                    try:
                        from services.ai_router import ask as ai_ask
                        research_prompt = (
                            f"Research this topic for a real estate operator at Laing+Simmons Oakville | Windsor:\n\n"
                            f"TOPIC: {payload.prompt}\n\n"
                            f"Provide:\n"
                            f"1. A concise summary of current market signals relevant to this topic (3-5 paragraphs)\n"
                            f"2. Why it matters specifically to real estate sellers in NSW Western Sydney suburbs\n"
                            f"3. 3 specific action items the operator can take based on this information\n\n"
                            f"Be specific, use real data where possible, avoid generic statements."
                        )
                        ai_response = await ai_ask(task="suburb_analysis", prompt=research_prompt)
                        if ai_response:
                            from hermes.scoring import build_dedupe_key as _bdk
                            import hashlib
                            dedupe = hashlib.md5(f"research:{payload.prompt}:{now_iso()[:13]}".encode()).hexdigest()
                            new_finding = HermesFinding(
                                source_id="hermes_research",
                                source_type="ai_research",
                                source_name="HERMES Research Engine",
                                source_url="",
                                dedupe_key=dedupe,
                                company_scope="real_estate",
                                topic=f"Research: {payload.prompt[:120]}",
                                signal_type="research",
                                summary=ai_response[:2000],
                                why_it_matters=f"Direct research output for operator prompt: {payload.prompt[:200]}",
                                confidence_score=0.85,
                                actionability_score=0.90,
                                novelty_score=0.80,
                                proposed_actions_json=["Review findings", "Update call angles", "Share with team"],
                                created_at=now_iso(),
                            )
                            session.add(new_finding)
                            await session.flush()
                            ai_finding_id = new_finding.id
                            findings = [new_finding] + list(findings)
                            trace.append({"stage": "ai_research", "finding_id": ai_finding_id, "chars": len(ai_response)})
                    except Exception as _exc:
                        logger.warning(f"[HERMES] AI research failed: {_exc}")

                digest = build_digest(findings, [], [])
                summary = {"findings_returned": len(findings), "ai_finding_id": ai_finding_id}
                result = {"digest": digest, "finding_ids": [item.id for item in findings], "ai_finding_id": ai_finding_id}
                trace.append({"stage": "digest_build", "findings": len(findings)})
            elif payload.command_type == "REFRESH_LEAD_BRIEFS":
                finding_dicts = [_finding_to_dict(f) for f in await self._load_findings(session, payload.finding_ids)]
                refreshed_total = 0
                affected_suburbs: List[str] = []
                for fd in finding_dicts:
                    worker_result = await _lead_signal_worker.run(fd, session)
                    refreshed_total += worker_result.get("affected_leads", 0)
                    if worker_result.get("suburb"):
                        affected_suburbs.append(worker_result["suburb"])
                    trace.append({
                        "stage": "lead_signal_refresh",
                        "finding_id": fd.get("id"),
                        "suburb": worker_result.get("suburb"),
                        "affected_leads": worker_result.get("affected_leads", 0),
                        "reason": worker_result.get("reason"),
                    })
                summary = {"leads_refreshed": refreshed_total, "suburbs": list(set(affected_suburbs))}
                result = {"leads_refreshed": refreshed_total, "suburbs": list(set(affected_suburbs))}
            else:
                raise ValueError(f"Unsupported Hermes command: {payload.command_type}")

            await session.commit()
            await self._finish_run(session, run, status=COMPLETED, trace=trace, summary=summary, result=result)
            return {"run": _run_to_dict(run), "trace": trace, "result": result}
        except Exception as exc:
            await session.rollback()
            await self._finish_run(session, run, status=FAILED, trace=trace, summary=summary, error_text=str(exc))
            raise

    async def approve_content(self, session: AsyncSession, content_id: str, approved_by: str, note: str) -> Dict:
        await self.ensure_ready()
        content = (await session.execute(select(HermesContent).where(HermesContent.id == content_id))).scalars().first()
        if not content:
            raise ValueError("Unknown Hermes content")
        content.status = APPROVED
        content.updated_at = now_iso()
        session.add(content)
        await store_content_approval_memory(session, content, approved_by, note)
        await session.commit()
        await session.refresh(content)
        return {"content": _content_to_dict(content)}

    async def approve_campaign(self, session: AsyncSession, campaign_id: str, approved_by: str, note: str) -> Dict:
        await self.ensure_ready()
        campaign = (await session.execute(select(HermesCampaign).where(HermesCampaign.id == campaign_id))).scalars().first()
        if not campaign:
            raise ValueError("Unknown Hermes campaign")
        campaign.status = APPROVED
        session.add(campaign)
        await store_campaign_approval_memory(session, campaign, approved_by, note)
        await session.commit()
        await session.refresh(campaign)
        return {"campaign": _campaign_to_dict(campaign)}

    async def get_feed(self, session: AsyncSession) -> Dict:
        await self.ensure_ready()
        findings = (await session.execute(select(HermesFinding).order_by(HermesFinding.created_at.desc()).limit(25))).scalars().all()
        content = (await session.execute(select(HermesContent).order_by(HermesContent.created_at.desc()).limit(25))).scalars().all()
        campaigns = (await session.execute(select(HermesCampaign).order_by(HermesCampaign.created_at.desc()).limit(25))).scalars().all()
        runs = (await session.execute(select(HermesRun).order_by(HermesRun.started_at.desc()).limit(20))).scalars().all()
        pending_content = [item for item in content if item.status == PENDING_APPROVAL]
        pending_campaigns = [item for item in campaigns if item.status == PENDING_APPROVAL]
        return {
            "findings": [_finding_to_dict(item) for item in findings],
            "content": [_content_to_dict(item) for item in content],
            "campaigns": [_campaign_to_dict(item) for item in campaigns],
            "approvals": {
                "pending_content": [_content_to_dict(item) for item in pending_content],
                "pending_campaigns": [_campaign_to_dict(item) for item in pending_campaigns],
            },
            "jobs": {
                "running": [_run_to_dict(item) for item in runs if item.status == RUNNING],
                "queued": [],
                "completed": [_run_to_dict(item) for item in runs if item.status == COMPLETED][:10],
            },
            "digest": build_digest(findings, content, campaigns),
            "priorities": ["build in app", "use in outreach", "use in content", "save for later", "ignore"],
        }

    async def get_activity(self, session: AsyncSession) -> Dict:
        await self.ensure_ready()
        runs = (await session.execute(select(HermesRun).order_by(HermesRun.started_at.desc()).limit(50))).scalars().all()
        return {"runs": [_run_to_dict(run) for run in runs]}

    async def get_memory(self, session: AsyncSession) -> Dict:
        await self.ensure_ready()
        snapshot = await get_memory_snapshot(session, limit=50)
        entries = snapshot["entries"]
        return {
            "entries": [
                {
                    "id": entry.id,
                    "memory_type": entry.memory_type,
                    "title": entry.title,
                    "body": entry.body,
                    "tags": entry.tags_json,
                    "source_refs": entry.source_refs_json,
                    "confidence_score": entry.confidence_score,
                    "created_at": entry.created_at,
                    "expires_at": entry.expires_at,
                }
                for entry in entries
            ],
            "learning_loops": snapshot["learning_loops"],
        }

    async def start_scheduler(self, interval_seconds: int = 300) -> None:
        if self._scheduler_task and not self._scheduler_task.done():
            return
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(interval_seconds))

    async def stop_scheduler(self) -> None:
        if self._scheduler_task:
            self._scheduler_task.cancel()
            self._scheduler_task = None

    async def _scheduler_loop(self, interval_seconds: int) -> None:
        await self.ensure_ready()
        import core.database as db_module

        while True:
            try:
                async with db_module._async_session_factory() as session:
                    sources = (await session.execute(select(HermesSource).where(HermesSource.enabled == True))).scalars().all()  # noqa: E712
                    due_ids = [source.id for source in sources if source_due(source.last_fetched_at, source.fetch_frequency_minutes)]
                    if due_ids:
                        await self.sync_sources(session, source_ids=due_ids, force=False)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Hermes scheduler loop error: %s", exc)
            await asyncio.sleep(interval_seconds)


_CONTROLLER: Optional[HermesController] = None


def get_controller() -> HermesController:
    global _CONTROLLER
    if _CONTROLLER is None:
        _CONTROLLER = HermesController()
    return _CONTROLLER


def reset_controller_for_tests() -> None:
    global _CONTROLLER
    if _CONTROLLER and _CONTROLLER._scheduler_task:
        _CONTROLLER._scheduler_task.cancel()
    _CONTROLLER = None
    reset_hermes_schema_state()
