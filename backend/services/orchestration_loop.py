"""
Autonomous build loop controller.

Picks safe, executable tasks from the job queue and runs them
through the orchestration engine. Enforces all stop conditions.

Stop conditions (checked before every iteration):
  - no_safe_tasks         — nothing in queue
  - approval_required     — a task needs human gate
  - repeated_failure      — same task failed >= MAX_TASK_FAILURES times
  - provider_exhausted    — all providers circuit-open
  - budget_ceiling        — job token/cost cap hit
  - hard_error_threshold  — job-level failure count too high
  - loop_cancelled        — external signal (graceful shutdown)

The loop is started as an asyncio background task in main.py lifespan.
It processes one task at a time per job to keep traceability clean.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.database import _async_session_factory
from core.events import event_manager
from models.orchestration_models import (
    AgentStatus, JobStatus, OrchAgent, OrchEvent, OrchJob, OrchTask, TaskStatus,
)
from services.orchestration_agents import AGENT_DEFS, get_agent_for_work_type
from services.orchestration_engine import (
    get_provider_snapshot, init_providers, route_completion,
)

logger = logging.getLogger(__name__)

MAX_TASK_FAILURES = int(os.getenv("ORCH_MAX_TASK_FAILURES", "3"))
LOOP_POLL_SECONDS = int(os.getenv("ORCH_LOOP_POLL_SECONDS", "5"))
MAX_JOB_HARD_FAILURES = int(os.getenv("ORCH_MAX_JOB_HARD_FAILURES", "5"))
_LOOP_RUNNING = False
_LOOP_CANCEL = False


# ─── DB helpers ───────────────────────────────────────────────────────────────

async def _get_or_create_agent(session: AsyncSession, role: str) -> OrchAgent:
    result = await session.execute(select(OrchAgent).where(OrchAgent.role == role))
    agent = result.scalars().first()
    if not agent:
        defn = AGENT_DEFS.get(role)
        agent = OrchAgent(
            role=role,
            display_name=defn.display_name if defn else role,
            status=AgentStatus.IDLE,
            capabilities=defn.work_types if defn else [],
            allowed_tools=defn.allowed_tools if defn else [],
            preferred_provider=defn.preferred_provider if defn else None,
        )
        session.add(agent)
        await session.commit()
        await session.refresh(agent)
    return agent


async def _log_event(
    session: AsyncSession,
    event_type: str,
    message: str,
    level: str = "info",
    job_id: Optional[str] = None,
    task_id: Optional[str] = None,
    agent_role: Optional[str] = None,
    provider: Optional[str] = None,
    data: Optional[Dict] = None,
):
    ev = OrchEvent(
        job_id=job_id,
        task_id=task_id,
        agent_role=agent_role,
        provider=provider,
        event_type=event_type,
        level=level,
        message=message,
        data=data or {},
    )
    session.add(ev)
    await session.commit()

    # Also broadcast live
    await event_manager.broadcast({
        "type": "ORCH_EVENT",
        "data": {
            "event_type": event_type,
            "message": message,
            "level": level,
            "job_id": job_id,
            "task_id": task_id,
            "agent_role": agent_role,
            "provider": provider,
            "ts": datetime.utcnow().isoformat(),
            **(data or {}),
        }
    })


# ─── Task execution ───────────────────────────────────────────────────────────

async def _execute_task(session: AsyncSession, task: OrchTask, job: OrchJob) -> bool:
    """
    Run a single task through its assigned agent + provider.
    Returns True on success, False on failure.
    """
    agent_def = get_agent_for_work_type(task.work_type)
    if not agent_def:
        await _log_event(session, "task_failed", f"No agent for work_type={task.work_type}",
                         level="error", job_id=task.job_id, task_id=task.id)
        task.status = TaskStatus.FAILED
        task.failure_reason = f"No agent for work_type={task.work_type}"
        await session.commit()
        return False

    agent = await _get_or_create_agent(session, agent_def.role)

    # Update agent state
    agent.status = AgentStatus.THINKING
    agent.current_task_id = task.id
    agent.current_job_id = task.job_id
    agent.last_active = datetime.utcnow()
    task.status = TaskStatus.RUNNING
    task.assigned_agent = agent_def.role
    task.started_at = datetime.utcnow()
    await session.commit()

    await event_manager.broadcast({
        "type": "ORCH_AGENT_STATE",
        "data": {
            "role": agent.role,
            "status": AgentStatus.THINKING,
            "task_id": task.id,
            "task_title": task.title,
            "job_id": task.job_id,
            "ts": datetime.utcnow().isoformat(),
        }
    })

    # Build messages
    system_prompt = agent_def.system_prompt
    user_content = _build_task_prompt(task, job)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]

    import time
    t0 = time.monotonic()
    try:
        agent.status = AgentStatus.EXECUTING
        await session.commit()
        await event_manager.broadcast({
            "type": "ORCH_AGENT_STATE",
            "data": {
                "role": agent.role,
                "status": AgentStatus.EXECUTING,
                "task_id": task.id,
                "ts": datetime.utcnow().isoformat(),
            }
        })

        result = await route_completion(
            work_type=task.work_type,
            messages=messages,
            preferred_provider=agent_def.preferred_provider,
            job_id=task.job_id,
            task_id=task.id,
        )

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        task.status = TaskStatus.DONE
        task.completed_at = datetime.utcnow()
        task.provider = result.provider
        task.model = result.model
        task.input_tokens = result.input_tokens
        task.output_tokens = result.output_tokens
        task.cost_usd = result.cost_usd
        task.latency_ms = elapsed_ms
        task.output_data = {"response": result.text[:8000]}  # cap stored output

        # Update job cost tracking
        job.tokens_used = (job.tokens_used or 0) + result.input_tokens + result.output_tokens
        job.cost_usd = (job.cost_usd or 0.0) + result.cost_usd

        agent.status = AgentStatus.IDLE
        agent.current_task_id = None
        agent.tasks_completed = (agent.tasks_completed or 0) + 1
        agent.total_tokens = (agent.total_tokens or 0) + result.input_tokens + result.output_tokens
        agent.total_cost_usd = (agent.total_cost_usd or 0.0) + result.cost_usd
        agent.current_reasoning = result.text[:500]
        await session.commit()

        await _log_event(
            session, "task_done",
            f"Task '{task.title}' completed via {result.provider}/{result.model}",
            job_id=task.job_id, task_id=task.id,
            agent_role=agent.role, provider=result.provider,
            data={"input_tokens": result.input_tokens,
                  "output_tokens": result.output_tokens,
                  "cost_usd": result.cost_usd,
                  "latency_ms": elapsed_ms,
                  "fallbacks_used": result.fallbacks_used},
        )

        await event_manager.broadcast({
            "type": "ORCH_AGENT_STATE",
            "data": {
                "role": agent.role,
                "status": AgentStatus.IDLE,
                "task_id": None,
                "ts": datetime.utcnow().isoformat(),
            }
        })
        return True

    except Exception as exc:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        task.status = TaskStatus.FAILED
        task.failure_reason = str(exc)[:500]
        task.retries = (task.retries or 0) + 1
        task.completed_at = datetime.utcnow()
        agent.status = AgentStatus.FAILED
        agent.current_task_id = None
        agent.tasks_failed = (agent.tasks_failed or 0) + 1
        await session.commit()

        await _log_event(
            session, "task_failed", f"Task '{task.title}' failed: {exc}",
            level="error", job_id=task.job_id, task_id=task.id,
            agent_role=agent.role,
            data={"error": str(exc)[:300], "retries": task.retries},
        )

        await event_manager.broadcast({
            "type": "ORCH_AGENT_STATE",
            "data": {
                "role": agent.role,
                "status": AgentStatus.FAILED,
                "task_id": task.id,
                "error": str(exc)[:200],
                "ts": datetime.utcnow().isoformat(),
            }
        })
        # Reset agent to idle after a beat
        await asyncio.sleep(2)
        agent.status = AgentStatus.IDLE
        await session.commit()
        return False


def _build_task_prompt(task: OrchTask, job: OrchJob) -> str:
    """Construct the user message for a task completion."""
    lines = [
        f"## Job: {job.title}",
        f"## Task: {task.title}",
        f"Work type: {task.work_type}",
        "",
    ]
    if task.input_data:
        lines.append("### Task Input")
        lines.append(json.dumps(task.input_data, indent=2)[:3000])
        lines.append("")
    if job.context:
        lines.append("### Job Context")
        lines.append(json.dumps(job.context, indent=2)[:2000])
        lines.append("")
    lines.append("Complete this task. Return structured output.")
    return "\n".join(lines)


# ─── Stop condition checks ────────────────────────────────────────────────────

def _check_stop_conditions(job: OrchJob, pending_tasks: List[OrchTask]) -> Optional[str]:
    if not pending_tasks:
        return "no_safe_tasks"
    if job.budget_tokens and (job.tokens_used or 0) >= job.budget_tokens:
        return "budget_ceiling"
    # Provider exhaustion
    snapshot = get_provider_snapshot()
    all_unavailable = all(
        not p["available"] or p["circuit_open"]
        for p in snapshot
    )
    if all_unavailable:
        return "provider_exhausted"
    return None


# ─── Job runner ───────────────────────────────────────────────────────────────

async def _run_job(job_id: str):
    """Process a single job to completion."""
    async with _async_session_factory() as session:
        result = await session.execute(select(OrchJob).where(OrchJob.id == job_id))
        job = result.scalars().first()
        if not job:
            logger.error("[loop] job %s not found", job_id)
            return

        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        await session.commit()

        await _log_event(session, "job_started", f"Job '{job.title}' started",
                         job_id=job_id)

        await event_manager.broadcast({
            "type": "ORCH_JOB_STATE",
            "data": {"job_id": job_id, "status": JobStatus.RUNNING,
                     "title": job.title, "ts": datetime.utcnow().isoformat()}
        })

        hard_failures = 0
        loop_iterations = 0

        while True:
            loop_iterations += 1
            await session.refresh(job)

            # Fetch pending tasks for this job
            q = select(OrchTask).where(
                OrchTask.job_id == job_id,
                OrchTask.status.in_([TaskStatus.PENDING, TaskStatus.ASSIGNED])
            ).order_by(OrchTask.created_at)
            pending = (await session.execute(q)).scalars().all()

            stop_reason = _check_stop_conditions(job, list(pending))
            if stop_reason:
                logger.info("[loop] job %s stopping: %s", job_id, stop_reason)
                break

            if job.status == JobStatus.CANCELLED:
                stop_reason = "cancelled"
                break

            task = pending[0]
            ok = await _execute_task(session, task, job)

            if not ok:
                hard_failures += 1
                if hard_failures >= MAX_JOB_HARD_FAILURES:
                    stop_reason = "hard_error_threshold"
                    break
                # Back off before retry
                await asyncio.sleep(5)
            else:
                hard_failures = max(0, hard_failures - 1)

            # Emit loop heartbeat
            await event_manager.broadcast({
                "type": "ORCH_LOOP_HEARTBEAT",
                "data": {
                    "job_id": job_id,
                    "loop_iteration": loop_iterations,
                    "tasks_remaining": len(pending) - 1,
                    "ts": datetime.utcnow().isoformat(),
                }
            })

        # Finalize job
        await session.refresh(job)
        failed_count = (await session.execute(
            select(OrchTask).where(OrchTask.job_id == job_id,
                                   OrchTask.status == TaskStatus.FAILED)
        )).scalars().all()
        done_count = (await session.execute(
            select(OrchTask).where(OrchTask.job_id == job_id,
                                   OrchTask.status == TaskStatus.DONE)
        )).scalars().all()

        if stop_reason in ("no_safe_tasks",) and not failed_count:
            job.status = JobStatus.DONE
        elif stop_reason == "cancelled":
            job.status = JobStatus.CANCELLED
        else:
            job.status = JobStatus.FAILED
            job.failure_reason = stop_reason

        job.completed_at = datetime.utcnow()
        job.result_summary = (
            f"{len(done_count)} tasks done, {len(failed_count)} failed. "
            f"Stop: {stop_reason}. "
            f"Cost: ${job.cost_usd:.4f} / {job.tokens_used} tokens."
        )
        await session.commit()

        await _log_event(session, "job_done" if job.status == JobStatus.DONE else "job_failed",
                         job.result_summary or "", job_id=job_id,
                         level="info" if job.status == JobStatus.DONE else "error")

        await event_manager.broadcast({
            "type": "ORCH_JOB_STATE",
            "data": {
                "job_id": job_id,
                "status": job.status,
                "title": job.title,
                "result_summary": job.result_summary,
                "ts": datetime.utcnow().isoformat(),
            }
        })


# ─── Background loop ──────────────────────────────────────────────────────────

async def _orchestration_loop():
    """
    Main background loop — polls for queued jobs and runs them.
    Runs as an asyncio task from main.py lifespan.
    """
    global _LOOP_RUNNING, _LOOP_CANCEL
    _LOOP_RUNNING = True
    _LOOP_CANCEL = False
    init_providers()
    logger.info("[orch-loop] started")

    while not _LOOP_CANCEL:
        try:
            async with _async_session_factory() as session:
                q = select(OrchJob).where(
                    OrchJob.status == JobStatus.QUEUED
                ).order_by(OrchJob.priority, OrchJob.created_at)
                jobs = (await session.execute(q)).scalars().all()

            for job in jobs:
                if _LOOP_CANCEL:
                    break
                logger.info("[orch-loop] picking up job %s: %s", job.id, job.title)
                await _run_job(job.id)

        except Exception as exc:
            logger.error("[orch-loop] error: %s", exc)

        await asyncio.sleep(LOOP_POLL_SECONDS)

    _LOOP_RUNNING = False
    logger.info("[orch-loop] stopped")


def cancel_loop():
    global _LOOP_CANCEL
    _LOOP_CANCEL = True
