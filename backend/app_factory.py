from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Iterable

from fastapi import FastAPI

import core.database as db
from hermes.controller import get_controller as get_hermes_controller
from runtime import loops as runtime_loops
from services.distress_intel_service import (
    ensure_distress_sources as _ensure_distress_sources,
    get_enabled_distress_scheduler_sources as _get_enabled_distress_scheduler_sources,
)
from services.speech_pipeline_service import ensure_speech_schema as _ensure_speech_schema
from services.voice_trainer_service import ensure_voice_trainer_schema as _ensure_voice_trainer_schema
from services.zoom_recording_sync_service import log_zoom_runtime_status, validate_zoom_runtime_config
try:
    from tools.migrate_coach_tables import create_coach_tables as _ensure_coach_schema
except ImportError:
    async def _ensure_coach_schema():
        pass  # tools/ excluded from Docker image
from workers.job_registry import VALID_RUNTIME_ROLES, get_runtime_task_names


async def _cancel_runtime_tasks(tasks: Iterable[asyncio.Task]) -> None:
    pending = [task for task in tasks if task and not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def _runtime_role_heartbeat_loop(runtime_role: str) -> None:
    heartbeat_name = "followup_scheduler" if runtime_role == "scheduler" else "followup_worker"
    while True:
        runtime_loops.record_loop_heartbeat(heartbeat_name, status="healthy", detail=f"runtime role={runtime_role}")
        await asyncio.sleep(30)


async def _start_role_runtime(runtime_role: str, scheduler_sources: list[dict] | None = None) -> list[asyncio.Task]:
    tasks: list[asyncio.Task] = []

    if runtime_role in {"worker", "scheduler"}:
        tasks.append(asyncio.create_task(_runtime_role_heartbeat_loop(runtime_role)))

    for task_name in get_runtime_task_names(runtime_role):
        tasks.append(asyncio.create_task(getattr(runtime_loops, task_name)()))

    if runtime_role == "scheduler":
        for source in scheduler_sources or []:
            cadence = int(source.get("cadence_minutes") or 1440)
            tasks.append(
                asyncio.create_task(
                    runtime_loops._distress_source_loop(source["source_key"], cadence)
                )
            )
        await get_hermes_controller().start_scheduler()

    return tasks


def create_app(
    runtime_role: str = "web",
    *,
    title: str = "Property Intelligence Core",
    docs_url: str | None = None,
    redoc_url: str | None = None,
) -> FastAPI:
    normalized_role = (runtime_role or "web").strip().lower()
    if normalized_role not in VALID_RUNTIME_ROLES:
        raise ValueError(f"Unsupported runtime role: {runtime_role!r}")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        import logging
        _log = logging.getLogger("boot")
        app.state.runtime_role = normalized_role
        app.state.runtime_tasks = []
        scheduler_sources: list[dict] = []

        # Boot-safe: catch all DB init errors so the app still serves /health
        try:
            validate_zoom_runtime_config()
        except Exception as exc:
            _log.warning("Zoom config check failed (non-fatal): %s", exc)

        # Retry DB init up to 5 times with backoff — handles transient DNS failures
        db_ready = False
        import core.config as _cfg
        is_sqlite = _cfg.DATABASE_URL.startswith("sqlite")
        for _attempt in range(5):
            try:
                # Run SQLite migrations first so SQLModel.metadata.create_all
                # (including distress_sources, etc.) fires on the SQLite path.
                if is_sqlite:
                    await db.init_sqlite_migrations()
                await db.init_postgres()
                await db.init_intelligence_schema()

                async with db._async_session_factory() as session:
                    await _ensure_speech_schema(session)
                    await session.commit()

                async with db._async_session_factory() as session:
                    await _ensure_voice_trainer_schema(session)
                    await session.commit()

                await _ensure_coach_schema()

                async with db._async_session_factory() as session:
                    await _ensure_distress_sources(session)
                    await session.commit()

                async with db._async_session_factory() as session:
                    scheduler_sources = await _get_enabled_distress_scheduler_sources(session)

                _log.info("DB init complete (attempt %d)", _attempt + 1)
                db_ready = True
                break
            except Exception as exc:
                wait = 2 ** _attempt  # 1, 2, 4, 8, 16 seconds
                _log.warning("DB init attempt %d/5 failed (%s), retrying in %ds…", _attempt + 1, exc, wait)
                await asyncio.sleep(wait)
        if not db_ready:
            _log.error("DB init failed after 5 attempts — app will boot without DB")

        try:
            log_zoom_runtime_status()
        except Exception:
            pass

        try:
            await db.get_redis()
        except Exception as exc:
            _log.warning("Redis init failed (non-fatal): %s", exc)

        try:
            await get_hermes_controller().ensure_ready()
        except Exception as exc:
            _log.warning("Hermes init failed (non-fatal): %s", exc)

        if normalized_role != "web":
            try:
                app.state.runtime_tasks = await _start_role_runtime(
                    normalized_role,
                    scheduler_sources if normalized_role == "scheduler" else None,
                )
            except Exception as exc:
                _log.warning("Runtime tasks failed (non-fatal): %s", exc)

        try:
            yield
        finally:
            await _cancel_runtime_tasks(app.state.runtime_tasks)
            try:
                await get_hermes_controller().stop_scheduler()
            except Exception:
                pass

    app = FastAPI(title=title, lifespan=lifespan, docs_url=docs_url, redoc_url=redoc_url)
    app.state.runtime_role = normalized_role
    app.state.runtime_tasks = []
    return app
