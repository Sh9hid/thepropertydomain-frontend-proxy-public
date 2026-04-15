"""
Contact enrichment endpoints.
Phase 2: wire the existing enrichment queue to InfoTrack / Whitepages.
"""

import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import core.config as config
from core.database import get_session
from core.events import event_manager
from core.security import get_api_key, get_optional_api_key
from core.logic import _hydrate_lead
from models.schemas import CotalityPropertyIntelligenceResult
from models.sql_models import (
    COTALITY_PROPERTY_INTELLIGENCE_ALLOWED_FIELDS,
    COTALITY_PROPERTY_INTELLIGENCE_FIELD_TO_LEAD_COLUMN,
    SuburbMarketStat,
)
from services.enrichment_service import enrichment_service
from services.geocoding_service import run_geocoding_batch as _run_geocoding_batch
from services.hermes_lead_ops_service import refresh_hermes_for_lead
from services.lead_intelligence_service import sync_lead_intelligence_for_lead
from services.lead_read_cache import invalidate_lead_read_models
from scripts.workflow_replay import parse_property_payload

router = APIRouter()


class BatchEnrichRequest(BaseModel):
    lead_ids: List[str]


class ManualEnrichRequest(BaseModel):
    owner_name: Optional[str] = None
    phones: Optional[List[str]] = None
    emails: Optional[List[str]] = None
    date_of_birth: Optional[str] = None  # "YYYY-MM-DD"
    last_seen: Optional[str] = None      # raw text from id4me


class Id4MeQueueRequest(BaseModel):
    pass


class Id4MeJobResultRequest(BaseModel):
    status: str = "completed"
    matched_address: Optional[str] = None
    payload: ManualEnrichRequest = ManualEnrichRequest()
    raw_result: dict = {}
    csv_path: Optional[str] = None
    error_message: Optional[str] = None


ALLOWED_COTALITY_FIELDS = list(COTALITY_PROPERTY_INTELLIGENCE_ALLOWED_FIELDS)
JOB_STATUSES = {"queued", "running", "login_required", "review_required", "completed", "failed", "workflow_not_taught", "replay_failed", "no_results"}
PROPOSED_TO_LEAD_FIELD = dict(COTALITY_PROPERTY_INTELLIGENCE_FIELD_TO_LEAD_COLUMN)
JSON_LEAD_COLUMNS = {
    "sale_history",
    "listing_status_history",
    "nearby_sales",
    "source_evidence",
    "summary_points",
    "key_details",
    "seller_intent_signals",
    "refinance_signals",
    "source_provenance",
}
COTALITY_PROFILE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "cotality_profile.json"
WORKFLOW_ROOT = Path(__file__).resolve().parents[2] / "scripts" / "workflows"
FULL_ENRICH_WORKFLOW_PATH = WORKFLOW_ROOT / "cotality_full_enrich.json"
WORKER_STATE_PATH = Path(config.COTALITY_PROFILE_DIR) / "worker_state.json"
COTALITY_AUTO_APPLY_FIELDS = {
    "property_type",
    "bedrooms",
    "bathrooms",
    "car_spaces",
    "land_size_sqm",
    "building_size_sqm",
    "year_built",
    "owner_occupancy_status",
    "absentee_owner",
    "likely_landlord",
    "likely_owner_occupier",
    "owner_type",
    "estimated_value_low",
    "estimated_value_mid",
    "estimated_value_high",
    "valuation_confidence",
    "valuation_date",
    "rental_estimate_low",
    "rental_estimate_high",
    "yield_estimate",
    "last_sale_price",
    "last_sale_date",
    "last_listing_status",
    "last_listing_date",
    "listing_status_history",
    "nearby_sales",
    "ownership_notes",
    "source_evidence",
    "summary_points",
    "key_details",
    "seller_intent_signals",
    "refinance_signals",
}


class CotalityTeachProfileResponse(BaseModel):
    exists: bool
    path: str
    updated_at: Optional[str] = None
    steps: List[str] = []


class CotalityEnrichRequest(BaseModel):
    requested_fields: Optional[List[str]] = None


class EnrichmentHeartbeatRequest(BaseModel):
    note: Optional[str] = None
    machine_id: Optional[str] = None


class EnrichmentStatusUpdateRequest(BaseModel):
    status: str
    error_message: Optional[str] = None
    matched_address: Optional[str] = None
    note: Optional[str] = None
    machine_id: Optional[str] = None


class EnrichmentResultRequest(CotalityPropertyIntelligenceResult):
    pass


class ApplyCotalityUpdatesRequest(BaseModel):
    fields: List[str]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _cotality_job_is_stale(job: Optional[dict], *, now: Optional[datetime] = None) -> bool:
    if not job:
        return False
    if str(job.get("provider") or "").strip().lower() != "cotality":
        return False
    status = str(job.get("status") or "").strip().lower()
    if status not in {"running", "login_required"}:
        return False
    updated_at = _parse_iso_datetime(job.get("updated_at"))
    if not updated_at:
        return True
    baseline = now or datetime.now(timezone.utc)
    stale_after = timedelta(seconds=max(300, int(config.COTALITY_LOGIN_WAIT_SECONDS)))
    return baseline - updated_at >= stale_after


async def _requeue_stale_cotality_jobs(session: AsyncSession, *, lead_id: Optional[str] = None) -> int:
    now_dt = datetime.now(timezone.utc)
    stale_cutoff = (now_dt - timedelta(seconds=max(300, int(config.COTALITY_LOGIN_WAIT_SECONDS)))).isoformat()
    params = {"updated_before": stale_cutoff, "updated_at": now_dt.isoformat(), "lead_id": lead_id}
    query = """
        UPDATE enrichment_jobs
        SET status = 'queued',
            machine_id = NULL,
            error_message = 'Stale Cotality job recycled for local worker reclaim',
            updated_at = :updated_at,
            completed_at = NULL
        WHERE provider = 'cotality'
          AND status IN ('running', 'login_required')
          AND (updated_at IS NULL OR updated_at < :updated_before)
    """
    if lead_id:
        query += " AND lead_id = :lead_id"
    result = await session.execute(text(query), params)
    return int(result.rowcount or 0)


def _launch_cotality_runner_for_job(job_id: str) -> dict:
    return {"started": True, "job_id": job_id, "mode": "worker"}


def _normalize_requested_fields(fields: Optional[List[str]]) -> List[str]:
    requested = fields or ALLOWED_COTALITY_FIELDS
    normalized: List[str] = []
    for field in requested:
        if field in ALLOWED_COTALITY_FIELDS and field not in normalized:
            normalized.append(field)
    if not normalized:
        raise HTTPException(status_code=400, detail="No valid enrichment fields requested")
    return normalized


def _require_machine_token(request: Request) -> str:
    expected = (config.ENRICHMENT_MACHINE_TOKEN or "").strip()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="ENRICHMENT_MACHINE_TOKEN is not configured on the backend; set backend env ENRICHMENT_MACHINE_TOKEN and restart the API",
        )
    provided = (
        request.headers.get("X-Enrichment-Machine-Token")
        or request.headers.get("X-Machine-Token")
        or ""
    ).strip()
    if provided != expected:
        raise HTTPException(
            status_code=401,
            detail="Invalid machine token; runner must send X-Enrichment-Machine-Token matching backend ENRICHMENT_MACHINE_TOKEN",
        )
    return (
        request.headers.get("X-Enrichment-Machine-Id")
        or request.headers.get("X-Machine-Id")
        or config.ENRICHMENT_MACHINE_ID
    ).strip()


async def _resolve_enrichment_runner_identity(
    request: Request,
    provider: str,
    _api_key: Optional[str],
) -> str:
    normalized_provider = (provider or "").strip().lower()
    if normalized_provider == "cotality":
        return _require_machine_token(request)
    return _require_machine_token(request)


@router.get("/api/enrichment-jobs/auth-check")
async def enrichment_job_auth_check(request: Request):
    machine_id = _require_machine_token(request)
    return {
        "status": "ok",
        "machine_id": machine_id,
        "token_header": "X-Enrichment-Machine-Token",
        "env_var": "ENRICHMENT_MACHINE_TOKEN",
    }


def _lead_has_cotality_address(lead: dict) -> bool:
    address = str(lead.get("address") or "").strip()
    suburb = str(lead.get("suburb") or "").strip()
    postcode = str(lead.get("postcode") or "").strip()
    return bool(address and (suburb or postcode or "," in address))


def _safe_json_loads(value: Optional[str], fallback):
    try:
        loaded = json.loads(value) if value else fallback
    except Exception:
        return fallback
    return loaded


def _read_cotality_worker_state() -> dict:
    fallback = {
        "auth_state": "unknown",
        "updated_at": None,
        "machine_id": None,
        "profile_dir": str(Path(config.COTALITY_PROFILE_DIR)),
        "last_auth_check_at": None,
        "last_login_attempt_at": None,
        "last_login_url": None,
        "last_login_variant": None,
        "login_attempt_count": 0,
        "last_login_result": None,
        "attention_reason": "Worker state file not found",
    }
    if not WORKER_STATE_PATH.exists():
        return fallback
    try:
        loaded = _safe_json_loads(WORKER_STATE_PATH.read_text(encoding="utf-8"), fallback)
    except Exception:
        return {
            **fallback,
            "attention_reason": "Worker state file could not be read",
        }
    if not isinstance(loaded, dict):
        return fallback
    merged = {**fallback, **loaded}
    merged["profile_dir"] = str(merged.get("profile_dir") or Path(config.COTALITY_PROFILE_DIR))
    return merged


def _parse_number(value):
    if value in (None, "", []):
        return None
    if isinstance(value, (int, float)):
        return value
    cleaned = re.sub(r"[^\d.\-]", "", str(value))
    if not cleaned:
        return None
    if "." in cleaned:
        try:
            return float(cleaned)
        except Exception:
            return None
    try:
        return int(cleaned)
    except Exception:
        return None


def _coerce_lead_value(field: str, value):
    if field in {"sale_history", "listing_status_history", "nearby_sales", "seller_intent_signals", "refinance_signals"}:
        if isinstance(value, list):
            return value
        return None
    if field in {"source_evidence", "summary_points", "key_details"}:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return None
    if field in {"absentee_owner", "likely_landlord", "likely_owner_occupier"}:
        if isinstance(value, bool):
            return value
        lowered = str(value or "").strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
        return None
    if field in {"property_type", "last_sale_date", "valuation_date", "last_listing_status", "last_listing_date", "ownership_notes", "owner_occupancy_status", "tenure_bucket", "year_built", "owner_type", "valuation_confidence"}:
        return str(value).strip() if value not in (None, "") else None
    if field == "last_sale_price":
        numeric = _parse_number(value)
        return str(int(numeric)) if numeric is not None else None
    numeric = _parse_number(value)
    if numeric is None:
        return None
    if field in {"bedrooms", "bathrooms", "car_spaces", "land_size_sqm", "building_size_sqm", "ownership_duration_years", "yield_estimate"}:
        return float(numeric)
    return int(numeric)


def _current_lead_field_value(lead: dict, proposed_field: str):
    actual_field = PROPOSED_TO_LEAD_FIELD[proposed_field]
    return lead.get(actual_field)


async def _get_latest_cotality_job(session: AsyncSession, lead_id: str):
    return (
        await session.execute(
            text(
                """
                SELECT *
                FROM enrichment_jobs
                WHERE lead_id = :lead_id AND provider = 'cotality'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().first()


async def _get_latest_cotality_result(session: AsyncSession, lead_id: str):
    return (
        await session.execute(
            text(
                """
                SELECT r.*, j.lead_id
                FROM enrichment_results r
                JOIN enrichment_jobs j ON j.id = r.enrichment_job_id
                WHERE j.lead_id = :lead_id AND j.provider = 'cotality'
                ORDER BY r.created_at DESC
                LIMIT 1
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().first()


async def _get_latest_enrichment_job(session: AsyncSession, lead_id: str, provider: str):
    return (
        await session.execute(
            text(
                """
                SELECT *
                FROM enrichment_jobs
                WHERE lead_id = :lead_id AND provider = :provider
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"lead_id": lead_id, "provider": provider},
        )
    ).mappings().first()


async def _get_latest_enrichment_result(session: AsyncSession, lead_id: str, provider: str):
    return (
        await session.execute(
            text(
                """
                SELECT r.*, j.lead_id
                FROM enrichment_results r
                JOIN enrichment_jobs j ON j.id = r.enrichment_job_id
                WHERE j.lead_id = :lead_id AND j.provider = :provider
                ORDER BY r.created_at DESC
                LIMIT 1
                """
            ),
            {"lead_id": lead_id, "provider": provider},
        )
    ).mappings().first()


def _serialize_job(row) -> Optional[dict]:
    if not row:
        return None
    payload = dict(row)
    payload["requested_fields_json"] = _safe_json_loads(payload.get("requested_fields_json"), [])
    return payload


def _serialize_result(row) -> Optional[dict]:
    if not row:
        return None
    payload = dict(row)
    payload["raw_payload_json"] = _safe_json_loads(payload.get("raw_payload_json"), {})
    payload["proposed_updates_json"] = _safe_json_loads(payload.get("proposed_updates_json"), {})
    return payload


def _normalize_suburb_market_stats(stats: dict, lead_row: dict, now: str) -> Optional[dict]:
    if not isinstance(stats, dict) or not stats:
        return None
    suburb = str(stats.get("suburb") or lead_row.get("suburb") or "").strip()
    segment = str(stats.get("segment") or "houses").strip().lower()
    if not suburb:
        return None
    refresh_after = (datetime.fromisoformat(now) + timedelta(days=7)).isoformat()
    payload = {
        "id": f"{suburb.lower()}::{str(lead_row.get('state') or '').lower()}::{str(lead_row.get('postcode') or '').lower()}::{segment}",
        "suburb": suburb,
        "state": str(lead_row.get("state") or stats.get("state") or "").strip() or None,
        "postcode": str(lead_row.get("postcode") or stats.get("postcode") or "").strip() or None,
        "segment": segment,
        "source": "cotality",
        "median_value": _parse_number(stats.get("median_value")),
        "properties_sold": _parse_number(stats.get("properties_sold")),
        "median_asking_rent": _parse_number(stats.get("median_asking_rent")),
        "median_value_change_12m_pct": _parse_number(stats.get("median_value_change_12m_pct")),
        "days_on_market": _parse_number(stats.get("days_on_market")),
        "average_tenure_years": _parse_number(stats.get("average_tenure_years")),
        "median_value_change_5y_pct": _parse_number(stats.get("median_value_change_5y_pct")),
        "new_listings_12m": _parse_number(stats.get("new_listings_12m")),
        "rental_rate_observations": _parse_number(stats.get("rental_rate_observations")),
        "stats_json": stats,
        "refreshed_at": now,
        "refresh_after": refresh_after,
        "updated_at": now,
    }
    return payload


async def _upsert_suburb_market_stats(session: AsyncSession, lead_row: dict, raw_payload: dict, now: str) -> Optional[dict]:
    normalized = _normalize_suburb_market_stats(raw_payload.get("suburb_market_stats") or {}, lead_row, now)
    if not normalized:
        return None
    existing = (
        await session.execute(text("SELECT * FROM suburb_market_stats WHERE id = :id"), {"id": normalized["id"]})
    ).mappings().first()
    if existing and existing.get("refresh_after"):
        refresh_after = _parse_iso_datetime(existing.get("refresh_after"))
        if refresh_after and refresh_after > datetime.now(timezone.utc):
            return dict(existing)
    params = {**normalized, "stats_json": json.dumps(normalized["stats_json"]), "created_at": existing.get("created_at") if existing else now}
    await session.execute(
        text(
            """
            INSERT INTO suburb_market_stats (
                id, suburb, state, postcode, segment, source, median_value, properties_sold,
                median_asking_rent, median_value_change_12m_pct, days_on_market, average_tenure_years,
                median_value_change_5y_pct, new_listings_12m, rental_rate_observations, stats_json,
                refreshed_at, refresh_after, created_at, updated_at
            ) VALUES (
                :id, :suburb, :state, :postcode, :segment, :source, :median_value, :properties_sold,
                :median_asking_rent, :median_value_change_12m_pct, :days_on_market, :average_tenure_years,
                :median_value_change_5y_pct, :new_listings_12m, :rental_rate_observations, :stats_json,
                :refreshed_at, :refresh_after, :created_at, :updated_at
            )
            ON CONFLICT (id) DO UPDATE SET
                median_value = excluded.median_value,
                properties_sold = excluded.properties_sold,
                median_asking_rent = excluded.median_asking_rent,
                median_value_change_12m_pct = excluded.median_value_change_12m_pct,
                days_on_market = excluded.days_on_market,
                average_tenure_years = excluded.average_tenure_years,
                median_value_change_5y_pct = excluded.median_value_change_5y_pct,
                new_listings_12m = excluded.new_listings_12m,
                rental_rate_observations = excluded.rental_rate_observations,
                stats_json = excluded.stats_json,
                refreshed_at = excluded.refreshed_at,
                refresh_after = excluded.refresh_after,
                updated_at = excluded.updated_at
            """
        ),
        params,
    )
    return normalized


async def _get_latest_suburb_market_stats(session: AsyncSession, lead_row: dict, override_suburb: Optional[str] = None) -> Optional[dict]:
    suburb = str(override_suburb or lead_row.get("suburb") or "").strip()
    if not suburb:
        return None
    row = (
        await session.execute(
            text(
                """
                SELECT *
                FROM suburb_market_stats
                WHERE suburb = :suburb
                ORDER BY refreshed_at DESC, updated_at DESC
                LIMIT 1
                """
            ),
            {"suburb": suburb},
        )
    ).mappings().first()
    if not row:
        return None
    payload = dict(row)
    payload["stats_json"] = _safe_json_loads(payload.get("stats_json"), {})
    return payload


def _section_text_blob(raw_payload: dict) -> str:
    sections = raw_payload.get("sections") or {}
    if not isinstance(sections, dict):
        return ""
    parts: list[str] = []
    for section in sections.values():
        if not isinstance(section, dict):
            continue
        text_value = str(section.get("text") or "").strip()
        if text_value:
            parts.append(text_value)
    return "\n".join(parts)


def _normalize_lead_value_for_diff(actual_field: str, lead: dict):
    value = lead.get(actual_field)
    if actual_field in JSON_LEAD_COLUMNS and isinstance(value, str):
        return _safe_json_loads(value, [])
    return value


async def _apply_cotality_updates(
    session: AsyncSession,
    *,
    lead_row: dict,
    job: dict,
    result: dict,
    selected_fields: List[str],
    actor: str,
    auto_apply: bool,
) -> dict:
    requested_fields = _safe_json_loads(job.get("requested_fields_json"), ALLOWED_COTALITY_FIELDS)
    selected_fields = [field for field in selected_fields if field in requested_fields and field in ALLOWED_COTALITY_FIELDS]
    if not selected_fields:
        raise HTTPException(status_code=400, detail="No approved fields selected")

    proposed_updates = _safe_json_loads(result.get("proposed_updates_json"), {})
    lead = dict(lead_row)
    updates = {}
    provenance = _safe_json_loads(lead.get("source_provenance"), [])
    now = _utc_now_iso()
    applied = {}
    applied_changes = []

    for field in selected_fields:
        if field not in proposed_updates:
            continue
        actual_field = PROPOSED_TO_LEAD_FIELD[field]
        coerced = _coerce_lead_value(field, proposed_updates[field])
        if coerced is None:
            continue
        previous_value = _normalize_lead_value_for_diff(actual_field, lead)
        stored_value = json.dumps(coerced) if actual_field in JSON_LEAD_COLUMNS else coerced
        updates[actual_field] = stored_value
        lead[actual_field] = stored_value
        applied[field] = proposed_updates[field]
        applied_changes.append(
            {
                "field": field,
                "lead_field": actual_field,
                "old_value": previous_value,
                "new_value": coerced,
            }
        )
        provenance.append(
            {
                "field": actual_field,
                "source_name": "cotality_enrichment",
                "source_type": "browser_runner",
                "fetched_at": now,
                "verified_at": now,
                "verification_status": "auto_applied" if auto_apply else "approved_by_operator",
                "parse_method": "manual_teach_profile_runner",
                "confidence": "high",
                "usage_eligibility": "allowed",
                "job_id": job["id"],
                "old_value": previous_value,
                "new_value": coerced,
            }
        )

    if not updates:
        raise HTTPException(status_code=400, detail="Selected fields were not available to apply")

    updates["source_provenance"] = json.dumps(provenance)
    updates["enrichment_status"] = "completed"
    updates["enrichment_last_synced_at"] = now
    updates["updated_at"] = now
    set_clause = ", ".join(f"{key} = :{key}" for key in updates)
    await session.execute(
        text(f"UPDATE leads SET {set_clause} WHERE id = :lead_id"),
        {**updates, "lead_id": job["lead_id"]},
    )
    await session.execute(
        text(
            """
            UPDATE enrichment_jobs
            SET status = 'completed', updated_at = :now, completed_at = :now
            WHERE id = :job_id
            """
        ),
        {"job_id": job["id"], "now": now},
    )
    raw_payload = _safe_json_loads(result.get("raw_payload_json"), {})
    raw_payload["auto_applied"] = auto_apply
    raw_payload["applied_at"] = now
    raw_payload["applied_by"] = actor
    raw_payload["applied_fields"] = sorted(applied.keys())
    raw_payload["applied_changes"] = applied_changes
    await session.execute(
        text(
            """
            UPDATE enrichment_results
            SET raw_payload_json = :raw_payload_json
            WHERE id = :result_id
            """
        ),
        {"result_id": result["id"], "raw_payload_json": json.dumps(raw_payload)},
    )
    await session.commit()
    warnings: List[str] = []

    async def _record_warning(message: str) -> None:
        warnings.append(message)
        try:
            await session.rollback()
        except Exception:
            pass

    try:
        await sync_lead_intelligence_for_lead(session, job["lead_id"])
    except Exception as exc:
        await _record_warning(f"lead_intelligence_sync_failed:{exc}")
    try:
        invalidate_lead_read_models([job["lead_id"]])
    except Exception as exc:
        await _record_warning(f"lead_cache_invalidation_failed:{exc}")
    try:
        await refresh_hermes_for_lead(session, job["lead_id"], actor=actor)
    except Exception as exc:
        await _record_warning(f"hermes_refresh_failed:{exc}")
    updated_row = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": job["lead_id"]})
    ).mappings().first()
    return {
        "status": "completed",
        "applied_fields": sorted(applied.keys()),
        "applied_changes": applied_changes,
        "lead": _hydrate_lead(updated_row),
        "warnings": warnings,
    }


@router.get("/api/cotality/teach-profile", response_model=CotalityTeachProfileResponse)
async def get_cotality_teach_profile(
    api_key: str = Depends(get_api_key),
):
    workflow_files = sorted(WORKFLOW_ROOT.glob("*.json"))
    if workflow_files:
        updated_at = max(datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat() for path in workflow_files)
        return CotalityTeachProfileResponse(
            exists=True,
            path=str(WORKFLOW_ROOT),
            updated_at=updated_at,
            steps=[path.stem for path in workflow_files],
        )
    if not COTALITY_PROFILE_PATH.exists():
        return CotalityTeachProfileResponse(exists=False, path=str(WORKFLOW_ROOT))
    data = _safe_json_loads(COTALITY_PROFILE_PATH.read_text(encoding="utf-8"), {})
    steps = list((data.get("steps") or {}).keys()) if isinstance(data, dict) else []
    return CotalityTeachProfileResponse(
        exists=True,
        path=str(COTALITY_PROFILE_PATH),
        updated_at=data.get("updated_at") if isinstance(data, dict) else None,
        steps=steps,
    )


@router.get("/api/cotality/worker-status")
async def get_cotality_worker_status(
    api_key: str = Depends(get_api_key),
):
    state = _read_cotality_worker_state()
    return {
        "auth_state": state.get("auth_state") or "unknown",
        "updated_at": state.get("updated_at"),
        "machine_id": state.get("machine_id"),
        "profile_dir": state.get("profile_dir"),
        "last_auth_check_at": state.get("last_auth_check_at"),
        "last_login_attempt_at": state.get("last_login_attempt_at"),
        "last_login_url": state.get("last_login_url"),
        "last_login_variant": state.get("last_login_variant"),
        "login_attempt_count": state.get("login_attempt_count") or 0,
        "last_login_result": state.get("last_login_result"),
        "attention_reason": state.get("attention_reason"),
    }


def _normalize_phone(p: str) -> str:
    return re.sub(r"\D", "", p)


def _merge_json_list(existing_json: Optional[str], new_items: Optional[List[str]], key_fn=lambda x: x.lower().strip()) -> str:
    try:
        existing = json.loads(existing_json) if existing_json else []
    except Exception:
        existing = []
    if not isinstance(existing, list):
        existing = []
    seen = {key_fn(v) for v in existing}
    merged = list(existing)
    for item in (new_items or []):
        k = key_fn(item)
        if k and k not in seen:
            seen.add(k)
            merged.append(item)
    return json.dumps(merged)


def _split_name(full_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    parts = [part for part in str(full_name or "").strip().split() if part]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[-1]


async def _persist_manual_enrichment(
    lead_id: str,
    body: ManualEnrichRequest,
    session: AsyncSession,
):
    row = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    lead = row.mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    owner_age = None
    if body.date_of_birth:
        try:
            birth_year = int(body.date_of_birth.split("-")[0])
            owner_age = datetime.now(timezone.utc).year - birth_year
        except Exception:
            pass

    merged_phones = _merge_json_list(
        lead.get("contact_phones"), body.phones, key_fn=_normalize_phone
    )
    merged_emails = _merge_json_list(
        lead.get("contact_emails"), body.emails, key_fn=lambda x: x.lower().strip()
    )

    now_iso = datetime.now(timezone.utc).isoformat()

    updates: dict = {
        "contact_phones": merged_phones,
        "contact_emails": merged_emails,
        "id4me_enriched": 1,
        "id4me_enriched_at": now_iso,
        "enrichment_status": "ready",
        "enrichment_last_synced_at": now_iso,
        "updated_at": now_iso,
    }
    if body.owner_name:
        updates["owner_name"] = body.owner_name
        first_name, last_name = _split_name(body.owner_name)
        updates["owner_first_name"] = first_name
        updates["owner_last_name"] = last_name
    if body.date_of_birth:
        updates["date_of_birth"] = body.date_of_birth
    if owner_age is not None:
        updates["owner_age"] = owner_age
    if body.last_seen:
        updates["id4me_last_seen"] = body.last_seen
    if body.phones:
        updates["contactability_tier"] = "high"
        updates["contactability_reasons"] = json.dumps(["Manual enrichment supplied direct phone"])
    elif body.emails:
        updates["contactability_tier"] = "medium"
        updates["contactability_reasons"] = json.dumps(["Manual enrichment supplied email"])

    existing_provenance = []
    try:
        existing_provenance = json.loads(lead.get("source_provenance") or "[]")
        if not isinstance(existing_provenance, list):
            existing_provenance = []
    except Exception:
        existing_provenance = []
    provenance_entries = [
        {
            "field": "contact_phones",
            "source_name": "manual_enrichment",
            "source_type": "operator",
            "fetched_at": now_iso,
            "verified_at": now_iso,
            "verification_status": "operator_confirmed",
            "parse_method": "manual_capture",
            "confidence": "high",
            "usage_eligibility": "allowed",
        }
    ]
    if body.owner_name:
        provenance_entries.append({**provenance_entries[0], "field": "owner_name"})
    updates["source_provenance"] = json.dumps([*existing_provenance, *provenance_entries])

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    await session.execute(
        text(f"UPDATE leads SET {set_clause} WHERE id = :lead_id"),
        {**updates, "lead_id": lead_id},
    )
    await session.commit()

    await event_manager.broadcast({
        "type": "LEAD_ENRICHED",
        "lead_id": lead_id,
        "owner_name": updates.get("owner_name") or lead.get("owner_name"),
        "owner_age": owner_age,
        "id4me_enriched": True,
    })

    updated_row = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    return _hydrate_lead(updated_row.mappings().first() or {})


@router.post("/api/leads/{lead_id}/enrich-manual")
async def enrich_manual(
    lead_id: str,
    body: ManualEnrichRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """Receive enrichment payload from the Chrome extension and persist it."""
    enriched_lead = await _persist_manual_enrichment(lead_id, body, session)
    return {"status": "ok", **enriched_lead}


@router.post("/api/enrichment/batch")
async def enrich_batch(body: BatchEnrichRequest, api_key: str = Depends(get_api_key)):
    """Queue up to 50 leads for background enrichment."""
    if len(body.lead_ids) > 50:
        raise HTTPException(status_code=400, detail="Max 50 leads per batch")
    results = await enrichment_service.bulk_enrich_batch(body.lead_ids)
    hits = sum(1 for r in results if r.ok and (r.phones or r.emails))
    return {
        "status": "ok",
        "queued": len(results),
        "hits": hits,
        "results": [
            {
                "lead_id": r.lead_id,
                "ok": r.ok,
                "phones": len(r.phones),
                "emails": len(r.emails),
                "provider": r.provider,
                "error": r.error,
            }
            for r in results
        ],
    }


@router.get("/api/enrichment/status")
async def enrichment_status(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """Return enrichment queue depth, cost today, and hit rate."""
    res_no_phone = await session.execute(
        text("SELECT COUNT(*) as c FROM leads WHERE COALESCE(contact_phones, '[]') = '[]' OR COALESCE(contact_phones, '') = ''")
    )
    no_phone = res_no_phone.scalar_one()
    try:
        res_log = await session.execute(
            text("SELECT COUNT(*) as c, SUM(hit) as hits FROM enrichment_log WHERE DATE(created_at) = CURRENT_DATE")
        )
        log_today = res_log.mappings().first()
        total = log_today["c"] or 0
        hits = log_today["hits"] or 0
    except Exception:
        total, hits = 0, 0
    return {
        "leads_without_phone": no_phone,
        "lookups_today": total,
        "hits_today": hits,
        "hit_rate": round(hits / total * 100, 1) if total else 0,
    }


@router.post("/api/enrichment/{lead_id}")
async def enrich_single(lead_id: str, api_key: str = Depends(get_api_key)):
    """Immediately enrich a single lead."""
    result = await enrichment_service.enrich_lead(lead_id)
    if not result.ok:
        raise HTTPException(status_code=400, detail=result.error or "Enrichment failed")
    return {
        "status": "ok",
        "lead_id": result.lead_id,
        "phones_found": len(result.phones),
        "emails_found": len(result.emails),
        "provider": result.provider,
    }


@router.post("/api/enrichment/domain/run-batch")
async def run_domain_enrichment_batch(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """Manually trigger a Domain API enrichment batch (photos, listing data, est_value)."""
    from services.domain_enrichment import run_enrichment_batch

    result = await run_enrichment_batch(session)
    return {"status": "ok", **result}


@router.get("/api/enrichment/domain/status")
async def domain_enrichment_status(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    """How many leads have been enriched from Domain vs. still pending."""
    res_done = await session.execute(
        text("SELECT COUNT(*) FROM leads WHERE domain_listing_id IS NOT NULL AND domain_listing_id != ''")
    )
    res_pending = await session.execute(
        text("SELECT COUNT(*) FROM leads WHERE domain_listing_id IS NULL OR domain_listing_id = ''")
    )
    res_photos = await session.execute(
        text("SELECT COUNT(*) FROM leads WHERE main_image IS NOT NULL AND main_image != ''")
    )
    return {
        "domain_enriched": res_done.scalar_one(),
        "pending_enrichment": res_pending.scalar_one(),
        "with_photos": res_photos.scalar_one(),
    }


@router.get("/api/enrichment/queue/stats")
async def get_enrichment_queue_stats(
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """Return current Domain enrichment priority queue statistics."""
    from services.enrichment_queue_service import get_queue_stats, ensure_queue_table
    await ensure_queue_table(session)
    return await get_queue_stats(session)


@router.post("/api/enrichment/queue/rebuild")
async def rebuild_enrichment_queue(
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """Rebuild the Domain enrichment priority queue from current leads."""
    from services.enrichment_queue_service import rebuild_queue, ensure_queue_table
    await ensure_queue_table(session)
    count = await rebuild_queue(session)
    return {"queued": count, "message": f"Enrichment queue rebuilt with {count} leads"}


@router.post("/api/geocoding/run-batch")
async def run_geocoding_batch(
    limit: int = 100,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """
    Bulk geocode leads that have no lat/lng.
    Uses OSM Nominatim (free, rate-limited to 1 req/sec).
    Run this as a background job - keep limit <= 100 to avoid timeouts.
    """
    return await _run_geocoding_batch(session, limit=limit)


@router.post("/api/leads/{lead_id}/enrich/cotality")
async def queue_cotality_enrichment(
    lead_id: str,
    body: CotalityEnrichRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    lead = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    ).mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    if not _lead_has_cotality_address(dict(lead)):
        raise HTTPException(status_code=400, detail="Lead address is incomplete for Cotality search")
    recycled = await _requeue_stale_cotality_jobs(session, lead_id=lead_id)
    if recycled:
        await session.commit()
    existing_job = await _get_latest_cotality_job(session, lead_id)
    if existing_job and existing_job.get("status") in {"queued", "running", "login_required"}:
        return {
            "job_id": existing_job["id"],
            "status": existing_job["status"],
            "launch_status": {"started": True, "job_id": existing_job["id"], "mode": "worker"},
        }

    requested_fields = _normalize_requested_fields(body.requested_fields)
    now = _utc_now_iso()
    job_id = str(uuid.uuid4())

    await session.execute(
        text(
            """
            INSERT INTO enrichment_jobs (
                id, lead_id, provider, status, requested_fields_json, matched_address,
                machine_id, attempt_count, error_message, created_at, updated_at, completed_at
            ) VALUES (
                :id, :lead_id, 'cotality', 'queued', :requested_fields_json, NULL,
                NULL, 0, NULL, :created_at, :updated_at, NULL
            )
            """
        ),
        {
            "id": job_id,
            "lead_id": lead_id,
            "requested_fields_json": json.dumps(requested_fields),
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.execute(
        text(
            """
            UPDATE leads
            SET enrichment_status = 'queued', enrichment_last_synced_at = :now, updated_at = :now
            WHERE id = :lead_id
            """
        ),
        {"lead_id": lead_id, "now": now},
    )
    await session.commit()
    launch_status = _launch_cotality_runner_for_job(job_id)
    print(
        f"[cotality] queued job id={job_id} lead_id={lead_id} provider=cotality status=queued requested_fields={requested_fields}",
        flush=True,
    )
    return {"job_id": job_id, "status": "queued", "launch_status": launch_status}


@router.post("/api/leads/{lead_id}/enrich/id4me")
async def queue_id4me_enrichment(
    lead_id: str,
    body: Id4MeQueueRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    lead = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    ).mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    now = _utc_now_iso()
    existing_job = await _get_latest_enrichment_job(session, lead_id, "id4me")
    if existing_job and existing_job.get("status") in {"queued", "running", "login_required"}:
        return {"job_id": existing_job["id"], "status": existing_job["status"]}

    job_id = str(uuid.uuid4())
    await session.execute(
        text(
            """
            INSERT INTO enrichment_jobs (
                id, lead_id, provider, status, requested_fields_json, matched_address,
                machine_id, attempt_count, error_message, created_at, updated_at, completed_at
            ) VALUES (
                :id, :lead_id, 'id4me', 'queued', :requested_fields_json, NULL,
                NULL, 0, NULL, :created_at, :updated_at, NULL
            )
            """
        ),
        {
            "id": job_id,
            "lead_id": lead_id,
            "requested_fields_json": json.dumps([]),
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.execute(
        text(
            """
            UPDATE leads
            SET enrichment_status = 'queued', enrichment_last_synced_at = :now, updated_at = :now
            WHERE id = :lead_id
            """
        ),
        {"lead_id": lead_id, "now": now},
    )
    await session.commit()
    return {"job_id": job_id, "status": "queued"}


@router.get("/api/enrichment-jobs/next")
async def claim_next_enrichment_job(
    request: Request,
    provider: str = "cotality",
    job_id: Optional[str] = None,
    api_key: Optional[str] = Depends(get_optional_api_key),
    session: AsyncSession = Depends(get_session),
):
    machine_id = await _resolve_enrichment_runner_identity(request, provider, api_key)
    if (provider or "").strip().lower() == "cotality":
        recycled = await _requeue_stale_cotality_jobs(session)
        if recycled:
            await session.commit()
    now = _utc_now_iso()
    query_params = {"machine_id": machine_id, "updated_at": now, "provider": provider, "job_id": job_id}
    if job_id:
        job = (
            await session.execute(
                text(
                    """
                    UPDATE enrichment_jobs
                    SET status = 'running',
                        machine_id = :machine_id,
                        attempt_count = COALESCE(attempt_count, 0) + 1,
                        updated_at = :updated_at
                    WHERE id = :job_id AND provider = :provider AND status = 'queued'
                    RETURNING *
                    """
                ),
                query_params,
            )
        ).mappings().first()
    else:
        job = (
            await session.execute(
                text(
                    """
                    UPDATE enrichment_jobs
                    SET status = 'running',
                        machine_id = :machine_id,
                        attempt_count = COALESCE(attempt_count, 0) + 1,
                        updated_at = :updated_at
                    WHERE id = (
                        SELECT id
                        FROM enrichment_jobs
                        WHERE provider = :provider AND status = 'queued'
                        ORDER BY created_at ASC
                        LIMIT 1
                    )
                    RETURNING *
                    """
                ),
                query_params,
            )
        ).mappings().first()
    if not job:
        return {"job": None}
    lead = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": job["lead_id"]})
    ).mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead for queued job was not found")
    await session.execute(
        text("UPDATE leads SET enrichment_status = 'running', enrichment_last_synced_at = :now WHERE id = :id"),
        {"id": job["lead_id"], "now": now},
    )
    await session.commit()
    print(
        f"[{provider}] claimed job id={job['id']} lead_id={job['lead_id']} machine_id={machine_id} provider={provider} status=running",
        flush=True,
    )
    if provider == "id4me":
        return {
            "job": {
                "id": job["id"],
                "lead_id": job["lead_id"],
                "provider": "id4me",
                "status": "running",
                "attempt_count": int(job.get("attempt_count") or 0),
            },
            "lead": {
                "id": lead["id"],
                "address": lead.get("address"),
                "suburb": lead.get("suburb"),
                "state": lead.get("state"),
                "postcode": lead.get("postcode"),
                "owner_name": lead.get("owner_name"),
            },
        }
    return {
        "job": {
            "id": job["id"],
            "lead_id": job["lead_id"],
            "provider": "cotality",
            "status": "running",
            "requested_fields": _safe_json_loads(job.get("requested_fields_json"), ALLOWED_COTALITY_FIELDS),
            "attempt_count": int(job.get("attempt_count") or 0) + 1,
        },
        "lead": {
            "id": lead["id"],
            "address": lead.get("address"),
            "suburb": lead.get("suburb"),
            "state": lead.get("state"),
            "postcode": lead.get("postcode"),
            "property_type": lead.get("property_type"),
            "bedrooms": lead.get("bedrooms"),
            "bathrooms": lead.get("bathrooms"),
            "car_spaces": lead.get("car_spaces"),
            "land_size_sqm": lead.get("land_size_sqm"),
            "building_size_sqm": lead.get("floor_size_sqm"),
            "last_sale_price": lead.get("sale_price"),
            "last_sale_date": lead.get("sale_date"),
            "estimated_value_low": lead.get("estimated_value_low"),
            "estimated_value_high": lead.get("estimated_value_high"),
        },
        "teach_profile": {
            "exists": FULL_ENRICH_WORKFLOW_PATH.exists() or COTALITY_PROFILE_PATH.exists(),
            "path": str(FULL_ENRICH_WORKFLOW_PATH if FULL_ENRICH_WORKFLOW_PATH.exists() else COTALITY_PROFILE_PATH),
        },
    }


@router.post("/api/enrichment-jobs/{job_id}/heartbeat")
async def heartbeat_enrichment_job(
    job_id: str,
    body: EnrichmentHeartbeatRequest,
    request: Request,
    api_key: Optional[str] = Depends(get_optional_api_key),
    session: AsyncSession = Depends(get_session),
):
    job = (
        await session.execute(text("SELECT provider FROM enrichment_jobs WHERE id = :id"), {"id": job_id})
    ).mappings().first()
    if not job:
        raise HTTPException(status_code=404, detail="Enrichment job not found")
    machine_id = body.machine_id or await _resolve_enrichment_runner_identity(request, job.get("provider") or "", api_key)
    now = _utc_now_iso()
    await session.execute(
        text(
            """
            UPDATE enrichment_jobs
            SET machine_id = :machine_id,
                updated_at = :updated_at,
                error_message = CASE WHEN :note IS NULL OR :note = '' THEN error_message ELSE :note END
            WHERE id = :id
            """
        ),
        {"id": job_id, "machine_id": machine_id, "updated_at": now, "note": body.note},
    )
    await session.commit()
    return {"status": "ok", "job_id": job_id}


@router.post("/api/enrichment-jobs/{job_id}/status")
async def update_enrichment_job_status(
    job_id: str,
    body: EnrichmentStatusUpdateRequest,
    request: Request,
    api_key: Optional[str] = Depends(get_optional_api_key),
    session: AsyncSession = Depends(get_session),
):
    job = (
        await session.execute(text("SELECT * FROM enrichment_jobs WHERE id = :id"), {"id": job_id})
    ).mappings().first()
    if not job:
        raise HTTPException(status_code=404, detail="Enrichment job not found")
    machine_id = body.machine_id or await _resolve_enrichment_runner_identity(request, job.get("provider") or "", api_key)
    if body.status not in JOB_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid job status")
    now = _utc_now_iso()
    completed_at = now if body.status in {"completed", "failed", "review_required", "workflow_not_taught", "replay_failed", "no_results"} else None
    await session.execute(
        text(
            """
            UPDATE enrichment_jobs
            SET status = :status,
                matched_address = COALESCE(:matched_address, matched_address),
                machine_id = :machine_id,
                error_message = COALESCE(:error_message, error_message),
                updated_at = :updated_at,
                completed_at = COALESCE(:completed_at, completed_at)
            WHERE id = :id
            """
        ),
        {
            "id": job_id,
            "status": body.status,
            "matched_address": body.matched_address,
            "machine_id": machine_id,
            "error_message": body.error_message or body.note,
            "updated_at": now,
            "completed_at": completed_at,
        },
    )
    await session.execute(
        text(
            """
            UPDATE leads
            SET enrichment_status = :status, enrichment_last_synced_at = :now
            WHERE id = :lead_id
            """
        ),
        {"lead_id": job["lead_id"], "status": body.status, "now": now},
    )
    await session.commit()
    return {"status": body.status, "job_id": job_id}


@router.post("/api/enrichment-jobs/{job_id}/result")
async def submit_enrichment_job_result(
    job_id: str,
    body: EnrichmentResultRequest,
    request: Request,
    api_key: Optional[str] = Depends(get_optional_api_key),
    session: AsyncSession = Depends(get_session),
):
    now = _utc_now_iso()
    job = (
        await session.execute(text("SELECT * FROM enrichment_jobs WHERE id = :id"), {"id": job_id})
    ).mappings().first()
    if not job:
        raise HTTPException(status_code=404, detail="Enrichment job not found")
    machine_id = await _resolve_enrichment_runner_identity(request, job.get("provider") or "", api_key)
    lead_row = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": job["lead_id"]})
    ).mappings().first()
    if not lead_row:
        raise HTTPException(status_code=404, detail="Lead not found for enrichment job")

    requested_fields = _safe_json_loads(job.get("requested_fields_json"), ALLOWED_COTALITY_FIELDS)
    raw_payload = body.raw_payload_json.model_dump()
    proposed_updates_source = body.proposed_updates_json.model_dump(exclude_unset=True)
    section_text = _section_text_blob(raw_payload)
    if section_text:
        inferred_updates = parse_property_payload(section_text, "")
        inferred_suburb_stats = inferred_updates.pop("suburb_market_stats", None)
        if inferred_suburb_stats and not raw_payload.get("suburb_market_stats"):
            raw_payload["suburb_market_stats"] = inferred_suburb_stats
        for key, value in inferred_updates.items():
            if key not in proposed_updates_source and value not in (None, "", []):
                proposed_updates_source[key] = value
    proposed_updates = {
        key: value
        for key, value in proposed_updates_source.items()
        if key in requested_fields and key in ALLOWED_COTALITY_FIELDS and value not in (None, "")
    }
    reported_status = body.status or body.final_status
    should_auto_apply = reported_status == "completed"
    stored_status = reported_status if reported_status in {"failed", "login_required", "workflow_not_taught", "replay_failed"} else "review_required"
    if should_auto_apply:
        stored_status = "completed"

    result_id = str(uuid.uuid4())
    await session.execute(
        text(
            """
            INSERT INTO enrichment_results (
                id, enrichment_job_id, source, raw_payload_json, proposed_updates_json,
                screenshot_path, confidence, created_at
            ) VALUES (
                :id, :job_id, :source, :raw_payload_json, :proposed_updates_json,
                :screenshot_path, :confidence, :created_at
            )
            """
        ),
        {
            "id": result_id,
            "job_id": job_id,
            "source": "cotality",
            "raw_payload_json": json.dumps(raw_payload),
            "proposed_updates_json": json.dumps(proposed_updates),
            "screenshot_path": body.screenshot_path,
            "confidence": body.confidence,
            "created_at": now,
        },
    )
    await session.execute(
        text(
            """
            UPDATE enrichment_jobs
            SET status = :status,
                matched_address = COALESCE(:matched_address, matched_address),
                machine_id = :machine_id,
                error_message = :error_message,
                updated_at = :updated_at,
                completed_at = CASE WHEN :status IN ('review_required', 'completed', 'failed', 'workflow_not_taught', 'replay_failed') THEN :updated_at ELSE completed_at END
            WHERE id = :id
            """
        ),
        {
            "id": job_id,
            "status": stored_status,
            "matched_address": body.matched_address,
            "machine_id": machine_id,
            "error_message": body.error_message,
            "updated_at": now,
        },
    )
    await session.execute(
        text("UPDATE leads SET enrichment_status = :status, enrichment_last_synced_at = :now WHERE id = :lead_id"),
        {"lead_id": job["lead_id"], "status": stored_status, "now": now},
    )
    await _upsert_suburb_market_stats(session, dict(lead_row), raw_payload, now)
    await session.commit()

    if should_auto_apply:
        lead_row = (
            await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": job["lead_id"]})
        ).mappings().first()
        applied = await _apply_cotality_updates(
            session,
            lead_row=dict(lead_row),
            job=dict(job),
            result={
                "id": result_id,
                "raw_payload_json": json.dumps(raw_payload),
                "proposed_updates_json": json.dumps(proposed_updates),
            },
            selected_fields=[field for field in requested_fields if field in COTALITY_AUTO_APPLY_FIELDS],
            actor="cotality_auto_apply",
            auto_apply=True,
        )
        return {
            "status": applied["status"],
            "job_id": job_id,
            "result_id": result_id,
            "lead": applied["lead"],
            "applied_fields": applied["applied_fields"],
            "applied_changes": applied["applied_changes"],
            "warnings": applied.get("warnings", []),
        }

    return {"status": stored_status, "job_id": job_id, "result_id": result_id}


@router.post("/api/enrichment-jobs/{job_id}/id4me-result")
async def submit_id4me_job_result(
    job_id: str,
    body: Id4MeJobResultRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    machine_id = _require_machine_token(request)
    now = _utc_now_iso()
    job = (
        await session.execute(text("SELECT * FROM enrichment_jobs WHERE id = :id"), {"id": job_id})
    ).mappings().first()
    if not job:
        raise HTTPException(status_code=404, detail="Enrichment job not found")
    if job.get("provider") != "id4me":
        raise HTTPException(status_code=400, detail="Job is not an id4me enrichment job")

    stored_status = body.status if body.status in {"completed", "failed", "login_required", "no_results"} else "completed"
    enriched_lead = None
    if stored_status == "completed":
        enriched_lead = await _persist_manual_enrichment(job["lead_id"], body.payload, session)

    result_id = str(uuid.uuid4())
    proposed_updates = body.payload.model_dump(exclude_none=True)
    raw_payload = {
        "matched_address": body.matched_address,
        "csv_path": body.csv_path,
        "raw_result": body.raw_result,
    }
    await session.execute(
        text(
            """
            INSERT INTO enrichment_results (
                id, enrichment_job_id, source, raw_payload_json, proposed_updates_json,
                screenshot_path, confidence, created_at
            ) VALUES (
                :id, :job_id, :source, :raw_payload_json, :proposed_updates_json,
                :screenshot_path, :confidence, :created_at
            )
            """
        ),
        {
            "id": result_id,
            "job_id": job_id,
            "source": "id4me",
            "raw_payload_json": json.dumps(raw_payload),
            "proposed_updates_json": json.dumps(proposed_updates),
            "screenshot_path": body.csv_path,
            "confidence": 1.0 if stored_status == "completed" else None,
            "created_at": now,
        },
    )
    await session.execute(
        text(
            """
            UPDATE enrichment_jobs
            SET status = :status,
                matched_address = COALESCE(:matched_address, matched_address),
                machine_id = :machine_id,
                error_message = :error_message,
                updated_at = :updated_at,
                completed_at = CASE WHEN :status IN ('completed', 'failed', 'login_required', 'no_results') THEN :updated_at ELSE completed_at END
            WHERE id = :id
            """
        ),
        {
            "id": job_id,
            "status": stored_status,
            "matched_address": body.matched_address,
            "machine_id": machine_id,
            "error_message": body.error_message,
            "updated_at": now,
        },
    )
    await session.execute(
        text(
            """
            UPDATE leads
            SET enrichment_status = :status,
                enrichment_last_synced_at = :now,
                updated_at = :now
            WHERE id = :lead_id
            """
        ),
        {"lead_id": job["lead_id"], "status": stored_status, "now": now},
    )
    await session.commit()
    return {
        "status": stored_status,
        "job_id": job_id,
        "result_id": result_id,
        "lead": enriched_lead,
    }


@router.get("/api/leads/{lead_id}/enrich/cotality/status")
async def get_cotality_enrichment_status(
    lead_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    lead = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    ).mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    latest_job = _serialize_job(await _get_latest_cotality_job(session, lead_id))
    latest_result = _serialize_result(await _get_latest_cotality_result(session, lead_id))
    override_suburb = ((latest_result or {}).get("raw_payload_json", {}).get("suburb_market_stats", {}) or {}).get("suburb")
    suburb_market_stats = await _get_latest_suburb_market_stats(session, dict(lead), override_suburb=override_suburb)
    return {
        "lead_id": lead_id,
        "teach_profile_exists": FULL_ENRICH_WORKFLOW_PATH.exists() or COTALITY_PROFILE_PATH.exists(),
        "job": latest_job,
        "result": latest_result,
        "lead": _hydrate_lead(lead),
        "suburb_market_stats": suburb_market_stats,
        "applied_summary": (latest_result or {}).get("raw_payload_json", {}).get("applied_changes"),
    }


@router.get("/api/leads/{lead_id}/enrich/id4me/status")
async def get_id4me_enrichment_status(
    lead_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    lead = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    ).mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    latest_job = _serialize_job(await _get_latest_enrichment_job(session, lead_id, "id4me"))
    latest_result = _serialize_result(await _get_latest_enrichment_result(session, lead_id, "id4me"))
    return {
        "lead_id": lead_id,
        "job": latest_job,
        "result": latest_result,
        "lead": _hydrate_lead(lead),
    }


@router.post("/api/leads/{lead_id}/enrich/cotality/apply")
async def apply_cotality_enrichment(
    lead_id: str,
    body: ApplyCotalityUpdatesRequest,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    lead_row = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    ).mappings().first()
    if not lead_row:
        raise HTTPException(status_code=404, detail="Lead not found")
    job = await _get_latest_cotality_job(session, lead_id)
    result = await _get_latest_cotality_result(session, lead_id)
    if not job or not result:
        raise HTTPException(status_code=404, detail="No Cotality enrichment result available")

    return await _apply_cotality_updates(
        session,
        lead_row=dict(lead_row),
        job=dict(job),
        result=dict(result),
        selected_fields=body.fields,
        actor="cotality_manual_apply",
        auto_apply=False,
    )


@router.post("/api/leads/{lead_id}/enrich/cotality/reject")
async def reject_cotality_enrichment(
    lead_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    job = await _get_latest_cotality_job(session, lead_id)
    if not job:
        raise HTTPException(status_code=404, detail="No Cotality enrichment job found")
    now = _utc_now_iso()
    await session.execute(
        text(
            """
            UPDATE enrichment_jobs
            SET status = 'failed',
                error_message = 'Rejected from CRM',
                updated_at = :now,
                completed_at = :now
            WHERE id = :id
            """
        ),
        {"id": job["id"], "now": now},
    )
    await session.execute(
        text("UPDATE leads SET enrichment_status = 'failed', enrichment_last_synced_at = :now WHERE id = :lead_id"),
        {"lead_id": lead_id, "now": now},
    )
    await session.commit()
    return {"status": "failed", "lead_id": lead_id}
