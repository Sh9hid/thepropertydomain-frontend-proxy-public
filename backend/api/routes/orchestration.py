"""
Orchestration API — jobs, tasks, agents, events, provider state.

Endpoints:
  POST   /orch/jobs                    create a job (and its tasks)
  GET    /orch/jobs                    list jobs (paginated, filterable)
  GET    /orch/jobs/{job_id}           job detail + tasks
  DELETE /orch/jobs/{job_id}           cancel a job
  GET    /orch/tasks/{task_id}         task detail
  GET    /orch/agents                  list agent role state
  GET    /orch/events                  event log (filterable)
  GET    /orch/providers               provider health snapshot
  GET    /orch/loop/status             autonomous loop health
  POST   /orch/loop/trigger            manually trigger loop poll
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.database import get_session
from models.orchestration_models import (
    JobStatus, OrchAgent, OrchEvent, OrchJob, OrchTask, TaskStatus,
)
from services.orchestration_engine import get_provider_snapshot, init_providers
from services.orchestration_loop import _LOOP_RUNNING, _orchestration_loop

router = APIRouter(prefix="/orch", tags=["orchestration"])


# ─── Request / Response schemas ───────────────────────────────────────────────

class TaskInput(BaseModel):
    title: str
    work_type: str
    input_data: Dict[str, Any] = {}
    parent_task_id: Optional[str] = None


class CreateJobRequest(BaseModel):
    title: str
    work_type: str = "implementation"
    description: Optional[str] = None
    priority: int = 5
    budget_tokens: Optional[int] = None
    context: Dict[str, Any] = {}
    tags: List[str] = []
    tasks: List[TaskInput] = []


class JobSummary(BaseModel):
    id: str
    title: str
    work_type: str
    status: str
    priority: int
    created_at: str
    started_at: Optional[str]
    completed_at: Optional[str]
    tokens_used: int
    cost_usd: float
    result_summary: Optional[str]
    tags: List[str]


class TaskSummary(BaseModel):
    id: str
    job_id: str
    title: str
    work_type: str
    status: str
    assigned_agent: Optional[str]
    provider: Optional[str]
    model: Optional[str]
    created_at: str
    started_at: Optional[str]
    completed_at: Optional[str]
    latency_ms: Optional[int]
    input_tokens: int
    output_tokens: int
    cost_usd: float
    retries: int
    failure_reason: Optional[str]


def _job_to_summary(j: OrchJob) -> JobSummary:
    return JobSummary(
        id=j.id,
        title=j.title,
        work_type=j.work_type,
        status=j.status,
        priority=j.priority,
        created_at=j.created_at.isoformat(),
        started_at=j.started_at.isoformat() if j.started_at else None,
        completed_at=j.completed_at.isoformat() if j.completed_at else None,
        tokens_used=j.tokens_used or 0,
        cost_usd=j.cost_usd or 0.0,
        result_summary=j.result_summary,
        tags=j.tags or [],
    )


def _task_to_summary(t: OrchTask) -> TaskSummary:
    return TaskSummary(
        id=t.id,
        job_id=t.job_id,
        title=t.title,
        work_type=t.work_type,
        status=t.status,
        assigned_agent=t.assigned_agent,
        provider=t.provider,
        model=t.model,
        created_at=t.created_at.isoformat(),
        started_at=t.started_at.isoformat() if t.started_at else None,
        completed_at=t.completed_at.isoformat() if t.completed_at else None,
        latency_ms=t.latency_ms,
        input_tokens=t.input_tokens or 0,
        output_tokens=t.output_tokens or 0,
        cost_usd=t.cost_usd or 0.0,
        retries=t.retries or 0,
        failure_reason=t.failure_reason,
    )


# ─── Jobs ─────────────────────────────────────────────────────────────────────

@router.post("/jobs", response_model=JobSummary)
async def create_job(body: CreateJobRequest, session: AsyncSession = Depends(get_session)):
    job = OrchJob(
        title=body.title,
        work_type=body.work_type,
        description=body.description,
        priority=body.priority,
        budget_tokens=body.budget_tokens,
        context=body.context,
        tags=body.tags,
        status=JobStatus.QUEUED,
    )
    session.add(job)
    await session.flush()  # get job.id

    for t in body.tasks:
        task = OrchTask(
            job_id=job.id,
            title=t.title,
            work_type=t.work_type,
            input_data=t.input_data,
            parent_task_id=t.parent_task_id,
            status=TaskStatus.PENDING,
        )
        session.add(task)

    # If no tasks provided, create a single default task
    if not body.tasks:
        task = OrchTask(
            job_id=job.id,
            title=body.title,
            work_type=body.work_type,
            input_data={"description": body.description or body.title, **body.context},
            status=TaskStatus.PENDING,
        )
        session.add(task)

    await session.commit()
    await session.refresh(job)

    from core.events import event_manager
    await event_manager.broadcast({
        "type": "ORCH_JOB_CREATED",
        "data": {
            "job_id": job.id,
            "title": job.title,
            "work_type": job.work_type,
            "ts": datetime.utcnow().isoformat(),
        }
    })
    return _job_to_summary(job)


@router.get("/jobs", response_model=List[JobSummary])
async def list_jobs(
    status: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    offset: int = 0,
    session: AsyncSession = Depends(get_session),
):
    q = select(OrchJob).order_by(OrchJob.created_at.desc()).limit(limit).offset(offset)
    if status:
        q = q.where(OrchJob.status == status)
    jobs = (await session.execute(q)).scalars().all()
    return [_job_to_summary(j) for j in jobs]


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(OrchJob).where(OrchJob.id == job_id))
    job = result.scalars().first()
    if not job:
        raise HTTPException(404, "Job not found")

    tasks_q = await session.execute(
        select(OrchTask).where(OrchTask.job_id == job_id).order_by(OrchTask.created_at)
    )
    tasks = tasks_q.scalars().all()

    return {
        **_job_to_summary(job).model_dump(),
        "description": job.description,
        "context": job.context,
        "failure_reason": job.failure_reason,
        "tasks": [_task_to_summary(t).model_dump() for t in tasks],
    }


@router.delete("/jobs/{job_id}")
async def cancel_job(job_id: str, session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(OrchJob).where(OrchJob.id == job_id))
    job = result.scalars().first()
    if not job:
        raise HTTPException(404, "Job not found")
    job.status = JobStatus.CANCELLED
    await session.commit()
    return {"ok": True, "job_id": job_id, "status": JobStatus.CANCELLED}


# ─── Tasks ────────────────────────────────────────────────────────────────────

@router.get("/tasks/{task_id}")
async def get_task(task_id: str, session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(OrchTask).where(OrchTask.id == task_id))
    task = result.scalars().first()
    if not task:
        raise HTTPException(404, "Task not found")
    return {
        **_task_to_summary(task).model_dump(),
        "input_data": task.input_data,
        "output_data": task.output_data,
        "file_changes": task.file_changes,
        "verification_result": task.verification_result,
    }


# ─── Agents ───────────────────────────────────────────────────────────────────

@router.get("/agents")
async def list_agents(session: AsyncSession = Depends(get_session)):
    from services.orchestration_agents import AGENT_DEFS

    # DB state
    db_agents = (await session.execute(select(OrchAgent))).scalars().all()
    db_by_role = {a.role: a for a in db_agents}

    agents = []
    for role, defn in AGENT_DEFS.items():
        db = db_by_role.get(role)
        agents.append({
            "role": role,
            "display_name": defn.display_name,
            "icon": defn.icon,
            "description": defn.description,
            "work_types": defn.work_types,
            "preferred_provider": defn.preferred_provider,
            "status": db.status if db else "idle",
            "current_task_id": db.current_task_id if db else None,
            "current_job_id": db.current_job_id if db else None,
            "tasks_completed": db.tasks_completed if db else 0,
            "tasks_failed": db.tasks_failed if db else 0,
            "total_tokens": db.total_tokens if db else 0,
            "total_cost_usd": db.total_cost_usd if db else 0.0,
            "last_active": db.last_active.isoformat() if db else None,
            "current_reasoning": db.current_reasoning if db else None,
        })
    return agents


# ─── Events ───────────────────────────────────────────────────────────────────

@router.get("/events")
async def list_events(
    job_id: Optional[str] = None,
    task_id: Optional[str] = None,
    event_type: Optional[str] = None,
    level: Optional[str] = None,
    limit: int = Query(default=100, le=500),
    session: AsyncSession = Depends(get_session),
):
    q = select(OrchEvent).order_by(OrchEvent.ts.desc()).limit(limit)
    if job_id:
        q = q.where(OrchEvent.job_id == job_id)
    if task_id:
        q = q.where(OrchEvent.task_id == task_id)
    if event_type:
        q = q.where(OrchEvent.event_type == event_type)
    if level:
        q = q.where(OrchEvent.level == level)
    events = (await session.execute(q)).scalars().all()
    return [
        {
            "id": e.id,
            "job_id": e.job_id,
            "task_id": e.task_id,
            "agent_role": e.agent_role,
            "provider": e.provider,
            "event_type": e.event_type,
            "level": e.level,
            "message": e.message,
            "ts": e.ts.isoformat(),
            "data": e.data,
        }
        for e in events
    ]


# ─── Providers ────────────────────────────────────────────────────────────────

@router.get("/providers")
async def provider_health():
    if not get_provider_snapshot():
        init_providers()
    return get_provider_snapshot()


# ─── Loop control ─────────────────────────────────────────────────────────────

@router.get("/loop/status")
async def loop_status():
    from services.orchestration_loop import _LOOP_RUNNING
    return {
        "running": _LOOP_RUNNING,
        "ts": datetime.utcnow().isoformat(),
    }


@router.post("/loop/trigger")
async def trigger_loop():
    """Manually wake the loop (for testing without background task)."""
    import asyncio
    from services.orchestration_loop import _orchestration_loop
    asyncio.create_task(_orchestration_loop())
    return {"ok": True, "message": "Loop iteration triggered"}
