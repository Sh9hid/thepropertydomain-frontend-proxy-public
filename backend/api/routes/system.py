import datetime
import html
import asyncio
import hmac
import hashlib
import json
import os
import re
import smtplib
from base64 import b64encode
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Security, Request, BackgroundTasks, File, UploadFile, Form
from pydantic import BaseModel
from zoneinfo import ZoneInfo

from api.routes._deps import APIKeyDep, SessionDep
from core.config import (
    API_KEY, api_key_header, APP_TITLE, SYDNEY_TZ, STOCK_ROOT, 
    PROJECT_ROOT, PROJECT_LOG_PATH, BRAND_NAME, BRAND_AREA, BRAND_LOGO_URL, 
    PRINCIPAL_NAME, PRINCIPAL_EMAIL, PRINCIPAL_PHONE, PROJECT_MEMORY_RULE, 
    BACKGROUND_SEND_POLL_SECONDS, PRIMARY_STRIKE_SUBURB, SECONDARY_STRIKE_SUBURBS,
    USE_POSTGRES
)
from core.utils import (
    now_sydney, now_iso, format_sydney, parse_client_datetime, 
    _first_non_empty, _safe_int, _format_moneyish, _parse_json_list, 
    _encode_value, _decode_row, _dedupe_text_list, _normalize_phone, 
    _dedupe_by_phone, _parse_iso_datetime, _parse_calendar_date, 
    _month_range_from_date, _bool_db
)
from services.scoring import _trigger_bonus, _status_penalty, _score_lead
from models.schemas import *
from core.logic import *

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from core.database import _fetch_joined_task, _get_lead_or_404, get_session
from services.automations import _schedule_task, _refresh_lead_next_action
from services.integrations import send_email_service, _zoom_request, _send_http_text
from services.opencode_go_service import benchmark_opencode_go_glm
from runtime.loops import get_loop_heartbeats
from core.security import get_api_key
router = APIRouter()


class OpencodeGLMBenchmarkRequest(BaseModel):
    rounds: int = 3
    task: str = "operator_brief"
    prompt: str = "Return a short health acknowledgement for autonomous follow-up operations."

@router.post("/api/system/update_agent")
async def update_agent(update: AgentUpdate, api_key: APIKeyDep, session: SessionDep):
    parts: List[str] = []
    params: dict = {"id": update.id, "last_run": now_sydney().strftime("%I:%M %p")}
    if update.status is not None:
        parts.append("status = :status")
        params["status"] = update.status
    if update.activity is not None:
        parts.append("activity = :activity")
        params["activity"] = update.activity
    if update.health is not None:
        parts.append("health = :health")
        params["health"] = update.health
    parts.append("last_run = :last_run")
    if parts:
        await session.execute(
            text(f"UPDATE agents SET {', '.join(parts)} WHERE id = :id"),
            params,
        )
        await session.commit()
    return {"status": "success"}


@router.post("/api/queue/rebuild")
async def rebuild_queue(
    body: QueueRebuildRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """
    Recompute queue_bucket and auto-detect route_queue for all leads.
    horizon_days: only process leads updated in the last N days (ignored when force=True).
    force=True: process every lead regardless of age.
    """
    from core.logic import _queue_bucket_for_lead
    from core.utils import _decode_row

    if body.force:
        res = await session.execute(text("SELECT * FROM leads"))
    else:
        cutoff = (now_sydney() - datetime.timedelta(days=body.horizon_days)).isoformat()
        res = await session.execute(
            text(
                "SELECT * FROM leads WHERE updated_at >= :cutoff"
                " OR queue_bucket IS NULL OR queue_bucket = ''"
            ),
            {"cutoff": cutoff},
        )

    rows = res.mappings().all()
    updated = 0
    for row in rows:
        lead = _decode_row(dict(row))
        bucket = _queue_bucket_for_lead(lead)

        rq = (lead.get("route_queue") or "").strip()
        if not rq:
            _tt = (lead.get("trigger_type") or "").lower()
            if "mortgage" in _tt or "refinanc" in _tt or "cliff" in _tt:
                rq = "mortgage_ownit1st"
            elif "development" in _tt or "subdivision" in _tt or "da_" in _tt:
                rq = "development_acquisition"
            else:
                rq = "real_estate"

        await session.execute(
            text(
                "UPDATE leads SET queue_bucket = :bucket, route_queue = :rq"
                " WHERE id = :id"
            ),
            {"bucket": bucket, "rq": rq, "id": lead["id"]},
        )
        updated += 1

    await session.commit()
    return {"status": "ok", "updated": updated}


@router.get("/api/project/brand-profile")
async def get_brand_profile(api_key: str = Depends(get_api_key)):
    return {
        "brand_name": BRAND_NAME,
        "brand_area": BRAND_AREA,
        "logo_url": BRAND_LOGO_URL,
        "principal_name": PRINCIPAL_NAME,
        "principal_email": PRINCIPAL_EMAIL,
        "principal_phone": PRINCIPAL_PHONE,
        "memory_rule": PROJECT_MEMORY_RULE,
        "project_log_path": str(PROJECT_LOG_PATH),
    }


@router.get("/api/project/memory")
async def get_project_memory(api_key: str = Depends(get_api_key)):
    ensure_project_memory_file()
    return {
        "path": str(PROJECT_LOG_PATH),
        "content": PROJECT_LOG_PATH.read_text(encoding="utf-8"),
    }


@router.post("/api/project/memory")
async def save_project_memory(body: ProjectMemoryEntry, api_key: str = Depends(get_api_key)):
    ensure_project_memory_file()
    append_project_memory(body.prompt, body.intent, body.source)
    return {"status": "ok", "path": str(PROJECT_LOG_PATH)}


@router.get("/api/system/pulse")
async def get_pulse(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    try:
        res_agents = await session.execute(text("SELECT * FROM agents ORDER BY id"))
        agents = [dict(row) for row in res_agents.mappings().all()]
        res_last_hunt = await session.execute(text("SELECT MAX(last_run) FROM agents"))
        last_hunt = res_last_hunt.scalar_one_or_none() or "Never"
        res_counts = await session.execute(text("SELECT COUNT(*) FROM leads"))
        counts = res_counts.scalar_one()
        res_images = await session.execute(text("SELECT COUNT(*) FROM leads WHERE main_image IS NOT NULL AND main_image != ''"))
        with_images = res_images.scalar_one()
        res_phone = await session.execute(text("SELECT COUNT(*) FROM leads WHERE contact_phones IS NOT NULL AND contact_phones != '[]'"))
        with_phone = res_phone.scalar_one()
        res_email = await session.execute(text("SELECT COUNT(*) FROM leads WHERE contact_emails IS NOT NULL AND contact_emails != '[]'"))
        with_email = res_email.scalar_one()
        res_evidence = await session.execute(text("SELECT COUNT(*) FROM leads WHERE linked_files != '[]' OR source_evidence != '[]'"))
        with_evidence = res_evidence.scalar_one()
        res_geo = await session.execute(text("SELECT COUNT(*) FROM leads WHERE lat = 0 OR lng = 0"))
        without_geo = res_geo.scalar_one()
        cutoff = (now_sydney() + datetime.timedelta(days=7)).isoformat()
        res_calls = await session.execute(
            text("""
            SELECT COUNT(*) FROM tasks
            WHERE status = 'pending' AND channel = 'call' AND COALESCE(superseded_by, '') = ''
              AND due_at <= :cutoff
            """),
            {"cutoff": cutoff},
        )
        ready_to_call = res_calls.scalar_one()
        res_ingest = await session.execute(text("SELECT * FROM ingest_runs ORDER BY created_at DESC LIMIT 1"))
        last_ingest_row = res_ingest.mappings().first()
        res_text_ch = await session.execute(text("SELECT COUNT(*) FROM communication_accounts"))
        text_channels = res_text_ch.scalar_one()
        res_mail_ch = await session.execute(text("SELECT COUNT(*) FROM email_accounts"))
        mail_channels = res_mail_ch.scalar_one()
        today_start = now_sydney().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        res_today = await session.execute(
            text("SELECT COUNT(*) FROM leads WHERE created_at >= :today_start"),
            {"today_start": today_start},
        )
        intercepts_today = res_today.scalar_one()
        res_feed = await session.execute(text("SELECT MAX(created_at) FROM ingest_runs"))
        last_scan_raw = res_feed.scalar_one_or_none()
        last_scan = last_scan_raw or "Never"
        archive_assets = 0
        archive_uploaded_assets = 0
        last_archive_sync = "Never"
        try:
            archive_assets = (await session.execute(text("SELECT COUNT(*) FROM mirrored_assets"))).scalar_one() or 0
            archive_uploaded_assets = (
                await session.execute(text("SELECT COUNT(*) FROM mirrored_assets WHERE upload_status = 'completed'"))
            ).scalar_one() or 0
            last_archive_sync_raw = (await session.execute(text("SELECT MAX(completed_at) FROM sync_runs"))).scalar_one_or_none()
            last_archive_sync = last_archive_sync_raw or "Never"
        except Exception:
            pass
        feed_health = "ok" if last_scan_raw else "no_data"
        metrics = {
            "lead_count": counts,
            "with_images": with_images,
            "with_phone": with_phone,
            "with_email": with_email,
            "with_evidence": with_evidence,
            "without_geo": without_geo,
            "ready_to_call": ready_to_call,
            "text_channels": text_channels,
            "mail_channels": mail_channels,
            "last_ingest": dict(last_ingest_row) if last_ingest_row else None,
            "feed_health": feed_health,
            "last_scan": last_scan,
            "intercepts_today": intercepts_today,
            "archive_assets": archive_assets,
            "archive_uploaded_assets": archive_uploaded_assets,
            "last_archive_sync": last_archive_sync,
        }
        return {"last_hunt": last_hunt, "agents": agents, "metrics": metrics}
    except Exception as exc:
        return {
            "last_hunt": "Unavailable",
            "agents": [],
            "metrics": {"lead_count": 0, "feed_health": "degraded", "error": str(exc)},
            "degraded": True,
        }


@router.get("/api/system/runtime-health")
async def get_runtime_health(api_key: str = Depends(get_api_key)):
    role = (os.getenv("RUNTIME_ROLE") or "web").strip().lower()
    heartbeats = get_loop_heartbeats()
    now_dt = now_sydney()
    warnings: List[str] = []

    expected_loops: List[tuple[str, int]] = []
    if role == "scheduler":
        expected_loops = [("followup_scheduler", 5)]
    elif role == "worker":
        expected_loops = [("followup_worker", 2)]

    for loop_name, stale_minutes in expected_loops:
        last_seen_raw = heartbeats.get(loop_name)
        if not last_seen_raw:
            warnings.append(f"{loop_name} heartbeat not found.")
            continue
        last_seen = _parse_iso_datetime(last_seen_raw)
        if not last_seen:
            warnings.append(f"{loop_name} heartbeat is malformed.")
            continue
        if (now_dt - last_seen.astimezone(SYDNEY_TZ)).total_seconds() > stale_minutes * 60:
            warnings.append(f"{loop_name} heartbeat is stale (> {stale_minutes} min).")

    return {
        "runtime_role": role,
        "now": now_dt.isoformat(),
        "heartbeats": heartbeats,
        "warnings": warnings,
        "ok": len(warnings) == 0,
    }


@router.get("/api/system/enrichment-auth-debug")
async def get_enrichment_auth_debug(api_key: str = Depends(get_api_key)):
    """Safe runtime introspection for enrichment machine-token configuration."""
    machine_token = (os.getenv("ENRICHMENT_MACHINE_TOKEN") or "").strip()
    machine_id = (os.getenv("ENRICHMENT_MACHINE_ID") or "").strip()

    def _hash_prefix(value: str) -> Optional[str]:
        if not value:
            return None
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]

    return {
        "instance": {
            "hostname": os.getenv("HOSTNAME") or "unknown",
            "render_service_id": os.getenv("RENDER_SERVICE_ID") or None,
            "render_instance_id": os.getenv("RENDER_INSTANCE_ID") or None,
            "pid": os.getpid(),
        },
        "enrichment_machine_token": {
            "configured": bool(machine_token),
            "length": len(machine_token),
            "sha256_prefix": _hash_prefix(machine_token),
        },
        "enrichment_machine_id": machine_id or None,
    }


@router.get("/api/system/quota")
async def get_quota_status(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    try:
        res = await session.execute(text("SELECT endpoint_type, SUM(usage_count) as total FROM quota_logs GROUP BY endpoint_type"))
        logs = res.mappings().all()
        return {log['endpoint_type']: log['total'] for log in logs}
    except Exception:
        return {}


@router.post("/api/system/bench/opencode-glm")
async def bench_opencode_go_glm(
    body: OpencodeGLMBenchmarkRequest,
    api_key: str = Depends(get_api_key),
):
    result = await benchmark_opencode_go_glm(
        prompt=body.prompt,
        rounds=body.rounds,
        task=body.task,
    )
    return result
