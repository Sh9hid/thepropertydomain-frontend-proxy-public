import datetime
import hashlib
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import ensure_revenue_engine_schema
from core.logic import _resolve_zoom_account
from models.sql_models import CallLog
from services.integrations import _zoom_request
from services.speech_pipeline_service import ensure_speech_schema, shadow_write_call_log_row
from core.config import RECORDINGS_ROOT

try:
    import phonenumbers
except Exception:  # pragma: no cover - optional dependency fallback
    phonenumbers = None

_logger = logging.getLogger(__name__)
_SYDNEY_TZ = ZoneInfo("Australia/Sydney")
_PAGE_SIZE = 100
_SYNC_TTL_SECONDS = 300
_SYNC_ATTEMPTS: Dict[str, float] = {}


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _digits_tail(value: Any, size: int = 9) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[-size:] if digits else ""


def _json_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _au_phone_keys(value: Any) -> set[str]:
    text_value = _normalize_text(value)
    if not text_value:
        return set()

    digits = re.sub(r"\D", "", text_value)
    keys: set[str] = set()
    if digits:
        keys.add(digits)
        if len(digits) >= 8:
            keys.add(digits[-8:])
        if len(digits) >= 9:
            keys.add(digits[-9:])
        if len(digits) >= 10:
            keys.add(digits[-10:])
        if digits.startswith("61") and len(digits) >= 11:
            national = digits[2:]
            keys.add(national)
            if national and not national.startswith("0"):
                keys.add(f"0{national}")

    if phonenumbers:
        try:
            parsed = phonenumbers.parse(text_value, "AU")
            if phonenumbers.is_possible_number(parsed):
                e164 = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                national = str(parsed.national_number or "")
                for candidate in (e164, national):
                    candidate_digits = re.sub(r"\D", "", candidate or "")
                    if not candidate_digits:
                        continue
                    keys.add(candidate_digits)
                    if len(candidate_digits) >= 8:
                        keys.add(candidate_digits[-8:])
                    if len(candidate_digits) >= 9:
                        keys.add(candidate_digits[-9:])
                    if len(candidate_digits) >= 10:
                        keys.add(candidate_digits[-10:])
                if national and not national.startswith("0"):
                    keys.add(f"0{national}")
        except Exception:
            pass

    return {key for key in keys if key}


def _lead_phone_values(lead: Dict[str, Any]) -> List[str]:
    phones: List[str] = []
    phones.extend(str(phone or "").strip() for phone in _json_list(lead.get("contact_phones")))
    phones.extend(str(phone or "").strip() for phone in _json_list(lead.get("alternate_phones")))
    for contact in _json_list(lead.get("contacts")):
        if isinstance(contact, dict):
            phone = _normalize_text(contact.get("phone"))
            if phone:
                phones.append(phone)
    return [phone for phone in phones if phone]


def _lead_phone_keys(lead: Dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for phone in _lead_phone_values(lead):
        keys.update(_au_phone_keys(phone))
    return keys


def _zoom_match_numbers(raw_call: Dict[str, Any], fallback_number: str = "") -> Dict[str, List[str]]:
    direction = _extract_direction(raw_call)
    from_number, to_number = _extract_phone_fields(raw_call)
    remote_primary = from_number if _is_inbound(direction) else to_number
    local_primary = to_number if _is_inbound(direction) else from_number

    remote_candidates = [
        remote_primary,
        raw_call.get("remote_number"),
        raw_call.get("callee_number") if not _is_inbound(direction) else raw_call.get("caller_number"),
        raw_call.get("to") if not _is_inbound(direction) else raw_call.get("from"),
        fallback_number,
    ]
    all_candidates = [
        remote_primary,
        local_primary,
        from_number,
        to_number,
        raw_call.get("caller_number"),
        raw_call.get("callee_number"),
        raw_call.get("from"),
        raw_call.get("to"),
        raw_call.get("phone_number"),
        raw_call.get("remote_number"),
        fallback_number,
    ]

    def _dedupe(values: List[Any]) -> List[str]:
        seen: set[str] = set()
        cleaned: List[str] = []
        for value in values:
            text_value = _normalize_text(value)
            if not text_value or text_value in seen:
                continue
            seen.add(text_value)
            cleaned.append(text_value)
        return cleaned

    return {"remote": _dedupe(remote_candidates), "all": _dedupe(all_candidates)}


def _lead_match_score(lead: Dict[str, Any], remote_keys: set[str], all_keys: set[str]) -> int:
    lead_keys = _lead_phone_keys(lead)
    if not lead_keys:
        return -1
    if not (lead_keys & all_keys):
        return -1

    score = 0
    if lead_keys & remote_keys:
        score += 100
    if lead_keys & all_keys:
        score += 50
    score += min(len(_json_list(lead.get("contact_phones"))), 3)
    score += min(len(_json_list(lead.get("alternate_phones"))), 2)
    score += min(len(_json_list(lead.get("contacts"))), 2)
    score += min(int(lead.get("call_today_score") or 0), 10)
    score += min(int(lead.get("evidence_score") or 0) // 10, 10)
    return score


async def find_best_matching_lead(
    session: AsyncSession,
    raw_call: Optional[Dict[str, Any]] = None,
    *fallback_numbers: str,
) -> tuple[Optional[Dict[str, Any]], str]:
    candidate_numbers = _zoom_match_numbers(raw_call or {}, next((num for num in fallback_numbers if _normalize_text(num)), ""))
    remote_keys: set[str] = set()
    all_keys: set[str] = set()
    suffixes: set[str] = set()

    for value in candidate_numbers["remote"]:
        keys = _au_phone_keys(value)
        remote_keys.update(keys)
        all_keys.update(keys)
    for value in candidate_numbers["all"]:
        keys = _au_phone_keys(value)
        all_keys.update(keys)

    for key in all_keys:
        if len(key) >= 9:
            suffixes.add(key[-9:])
        elif len(key) >= 8:
            suffixes.add(key[-8:])

    if not suffixes:
        return None, "no_phone_number_on_zoom_call"

    result = await session.execute(
        text(
            """
            SELECT id, address, updated_at, call_today_score, evidence_score, contact_phones, alternate_phones, contacts
            FROM leads
            WHERE COALESCE(contact_phones, '[]') != '[]'
               OR COALESCE(alternate_phones, '[]') != '[]'
               OR COALESCE(contacts, '[]') != '[]'
            ORDER BY COALESCE(call_today_score, 0) DESC, COALESCE(evidence_score, 0) DESC, updated_at DESC
            LIMIT 500
            """
        ),
    )
    candidates = [dict(row) for row in result.mappings().all()]
    if not candidates:
        return None, "phone_match_not_found"

    best_lead: Optional[Dict[str, Any]] = None
    best_score = -1
    for candidate in candidates:
        score = _lead_match_score(candidate, remote_keys, all_keys)
        if score > best_score:
            best_score = score
            best_lead = candidate

    if best_lead is None or best_score < 0:
        return None, "phone_match_not_found"
    return best_lead, ""


def _extract_scalar(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("phone_number", "number", "extension_number", "id", "value", "name"):
            nested = _normalize_text(value.get(key))
            if nested:
                return nested
        return ""
    if isinstance(value, list):
        for item in value:
            nested = _extract_scalar(item)
            if nested:
                return nested
        return ""
    return _normalize_text(value)


def _extract_owner_metadata(raw_call: Dict[str, Any]) -> Dict[str, Any]:
    owner = raw_call.get("owner") if isinstance(raw_call.get("owner"), dict) else {}
    return {
        "owner": owner,
        "zoom_owner_name": _normalize_text(owner.get("name")),
        "zoom_owner_id": _normalize_text(owner.get("id")),
        "zoom_owner_extension": _extract_scalar(owner.get("extension_number")),
    }


def _parse_timestamp(value: Any) -> Optional[datetime.datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000.0
            return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        except Exception:
            return None

    text_value = _normalize_text(value)
    if not text_value:
        return None

    if text_value.endswith("Z"):
        text_value = text_value[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(text_value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.astimezone(datetime.timezone.utc)
    except Exception:
        pass

    for pattern in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ):
        try:
            parsed = datetime.datetime.strptime(text_value, pattern)
            return parsed.replace(tzinfo=datetime.timezone.utc)
        except Exception:
            continue
    return None


def _extract_call_entries(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    for key in (
        "call_history_records",
        "call_history",
        "call_logs",
        "calls",
        "records",
        "phone_records",
    ):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    nested_payload = payload.get("data")
    if isinstance(nested_payload, dict):
        return _extract_call_entries(nested_payload)
    return []


def _extract_duration_seconds(raw_call: Dict[str, Any]) -> int:
    for key in ("duration_seconds", "duration", "call_duration", "talk_time"):
        value = raw_call.get(key)
        if value in (None, ""):
            continue
        try:
            return max(0, int(float(value)))
        except Exception:
            continue
    return 0


def _extract_result_label(raw_call: Dict[str, Any]) -> str:
    for key in ("result", "status", "call_result", "outcome", "disposition"):
        label = _extract_scalar(raw_call.get(key))
        if label:
            return label
    return ""


def _extract_direction(raw_call: Dict[str, Any]) -> str:
    for key in ("direction", "call_type", "type"):
        direction = _extract_scalar(raw_call.get(key)).lower()
        if direction:
            return direction
    return ""


def _is_inbound(direction: str) -> bool:
    direction_value = _normalize_text(direction).lower()
    if not direction_value:
        return False
    return "inbound" in direction_value or direction_value in {"in", "incoming", "inbound_call", "received"}


def _extract_timestamp(raw_call: Dict[str, Any]) -> Optional[datetime.datetime]:
    for key in (
        "date_time",
        "call_start_time",
        "start_time",
        "create_time",
        "timestamp",
        "time",
        "answer_start_time",
    ):
        parsed = _parse_timestamp(raw_call.get(key))
        if parsed:
            return parsed
    return None


def _extract_phone_fields(raw_call: Dict[str, Any]) -> tuple[str, str]:
    from_number = ""
    to_number = ""

    for key in ("from", "caller_number", "from_number", "caller", "caller_id"):
        from_number = _extract_scalar(raw_call.get(key))
        if from_number:
            break

    for key in ("to", "callee_number", "phone_number", "to_number", "callee", "remote_number"):
        to_number = _extract_scalar(raw_call.get(key))
        if to_number:
            break

    return from_number, to_number


def _derive_outcome(result_label: str, duration_seconds: int) -> tuple[str, bool]:
    label = _normalize_text(result_label).lower()
    if "voicemail" in label or "voice mail" in label:
        return "left_voicemail", False
    if any(token in label for token in ("answered", "accepted", "connected", "completed", "success", "live")):
        return "spoke", True
    if any(token in label for token in ("missed", "no answer", "busy", "cancel", "failed", "reject", "unavailable", "abandoned")):
        return "no_answer", False
    if duration_seconds > 0:
        return "spoke", True
    return "no_answer", False


def _provider_call_id(raw_call: Dict[str, Any], logged_at: str, remote_number: str, direction: str) -> str:
    for key in ("call_id", "session_id", "id", "call_log_id", "recording_id"):
        provider_id = _extract_scalar(raw_call.get(key))
        if provider_id:
            return provider_id
    digest_basis = f"{logged_at}|{remote_number}|{direction}|{_extract_result_label(raw_call)}"
    return hashlib.md5(digest_basis.encode("utf-8")).hexdigest()


def _relative_recording_uri(local_path: str) -> str:
    path = Path(str(local_path or "")).resolve()
    try:
        return str(path.relative_to(RECORDINGS_ROOT.parent)).replace("\\", "/")
    except Exception:
        return f"recordings/{path.name}" if path.name else ""


def _extract_recording_payload(raw_call: Dict[str, Any], recording_meta: Dict[str, Any], fallback_duration_seconds: int) -> Dict[str, Any]:
    source = recording_meta if isinstance(recording_meta, dict) and recording_meta else raw_call
    duration_value = (
        source.get("duration")
        or source.get("duration_seconds")
        or raw_call.get("recording_duration")
        or fallback_duration_seconds
    )
    try:
        duration_seconds = max(0, int(float(duration_value or 0)))
    except Exception:
        duration_seconds = max(0, int(fallback_duration_seconds or 0))

    return {
        "recording_id": str(
            source.get("id")
            or raw_call.get("recording_id")
            or ""
        ).strip(),
        "download_url": str(
            source.get("download_url")
            or raw_call.get("download_url")
            or ""
        ).strip(),
        "file_url": str(
            source.get("file_url")
            or raw_call.get("file_url")
            or raw_call.get("recording_url")
            or ""
        ).strip(),
        "file_type": str(
            source.get("file_type")
            or raw_call.get("file_type")
            or "mp3"
        ).strip().lower(),
        "duration_seconds": duration_seconds,
        "recording_start": str(
            source.get("recording_start")
            or source.get("start_time")
            or source.get("date_time")
            or raw_call.get("recording_start")
            or ""
        ).strip(),
        "recording_end": str(
            source.get("recording_end")
            or source.get("end_time")
            or raw_call.get("recording_end")
            or ""
        ).strip(),
    }


def _build_zoom_raw_payload(raw_call: Dict[str, Any], normalized: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw_call)
    payload.update(_extract_owner_metadata(raw_call))
    payload["zoom_source_endpoint"] = str(raw_call.get("__zoom_source_endpoint") or "").strip()
    payload["zoom_object_type"] = "phone_call_log" if payload["zoom_source_endpoint"] == "/phone/call_logs" else "phone_call_history"
    payload["zoom_call_log_id"] = _extract_scalar(raw_call.get("call_log_id") or raw_call.get("id"))
    payload["zoom_call_history_id"] = _extract_scalar(raw_call.get("call_history_id"))
    payload["provider_call_id"] = normalized["provider_call_id"]
    payload["ai_call_summary_id"] = _extract_scalar(raw_call.get("ai_call_summary_id"))
    payload["has_recording"] = bool(raw_call.get("has_recording") or raw_call.get("recording_id") or raw_call.get("recording_type"))
    payload["has_transcript"] = bool(
        raw_call.get("transcript")
        or raw_call.get("transcript_url")
        or raw_call.get("transcript_file")
        or raw_call.get("audio_transcript")
    )
    payload["has_ai_summary"] = bool(
        raw_call.get("summary")
        or raw_call.get("ai_summary")
        or raw_call.get("smart_summary")
    )
    return payload


def _recording_lookup_id(raw_payload: Dict[str, Any]) -> str:
    source_endpoint = str(raw_payload.get("zoom_source_endpoint") or "").strip()
    if source_endpoint == "/phone/call_logs":
        return _extract_scalar(raw_payload.get("zoom_call_log_id") or raw_payload.get("call_log_id"))
    return _extract_scalar(raw_payload.get("zoom_call_log_id") or raw_payload.get("call_log_id") or raw_payload.get("id"))


def _log_zoom_call_trace(raw_payload: Dict[str, Any], recording_payload: Optional[Dict[str, Any]] = None) -> None:
    recording_payload = recording_payload or {}
    trace = {
        "source_endpoint": str(raw_payload.get("zoom_source_endpoint") or ""),
        "object_type": str(raw_payload.get("zoom_object_type") or ""),
        "provider_call_id": str(raw_payload.get("provider_call_id") or raw_payload.get("call_id") or ""),
        "call_log_id": str(raw_payload.get("zoom_call_log_id") or ""),
        "call_history_id": str(raw_payload.get("zoom_call_history_id") or ""),
        "owner": str(raw_payload.get("zoom_owner_name") or raw_payload.get("zoom_owner_extension") or ""),
        "recording_present": bool(
            recording_payload.get("file_url")
            or recording_payload.get("download_url")
            or raw_payload.get("has_recording")
            or raw_payload.get("recording_id")
        ),
        "ai_summary_present": bool(
            raw_payload.get("has_ai_summary")
            or recording_payload.get("summary")
            or recording_payload.get("ai_summary")
        ),
        "transcript_present": bool(
            raw_payload.get("has_transcript")
            or recording_payload.get("transcript")
            or recording_payload.get("transcript_url")
        ),
    }
    _logger.info("zoom_call_trace=%s", json.dumps(trace, sort_keys=True))


def normalize_zoom_call_entry(raw_call: Dict[str, Any], target_date: str = "") -> Optional[Dict[str, Any]]:
    timestamp = _extract_timestamp(raw_call)
    if not timestamp:
        # Extreme fallback: use now if we absolutely must, but usually better to have a date
        # If target_date is provided, assume start of that day
        if target_date:
            try:
                timestamp = datetime.datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=_SYDNEY_TZ)
            except:
                return None
        else:
            return None

    local_timestamp = timestamp.astimezone(_SYDNEY_TZ)
    logged_date = local_timestamp.strftime("%Y-%m-%d")
    if target_date and logged_date != target_date:
        return None

    direction = _extract_direction(raw_call)
    from_number, to_number = _extract_phone_fields(raw_call)
    remote_number = to_number or from_number
    duration_seconds = _extract_duration_seconds(raw_call)
    result_label = _extract_result_label(raw_call)
    outcome, connected = _derive_outcome(result_label, duration_seconds)
    
    # Allow inbound only if it was a connect (spoke)
    if _is_inbound(direction) and not connected:
        return None

    logged_at = local_timestamp.isoformat()

    normalized = {
        "provider": "zoom",
        "provider_call_id": _provider_call_id(raw_call, logged_at, remote_number, direction),
        "outcome": outcome,
        "connected": connected,
        "user_id": "Zoom",
        "timestamp": logged_at,
        "call_duration_seconds": duration_seconds,
        "duration_seconds": duration_seconds,
        "note": result_label,
        "operator": "Zoom",
        "logged_at": logged_at,
        "logged_date": logged_date,
        "next_action_due": None,
        "direction": direction,
        "from_number": from_number,
        "to_number": to_number,
        "remote_number": remote_number,
    }
    normalized["raw_payload"] = json.dumps(_build_zoom_raw_payload(raw_call, normalized), ensure_ascii=True)
    return normalized


async def ensure_call_log_schema(session: AsyncSession) -> None:
    conn = await session.connection()
    await conn.run_sync(lambda sync_conn: CallLog.__table__.create(sync_conn, checkfirst=True))
    await ensure_revenue_engine_schema(conn)
    for ddl in (
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS provider TEXT DEFAULT 'manual'",
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS provider_call_id TEXT",
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS direction TEXT DEFAULT ''",
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS from_number TEXT DEFAULT ''",
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS to_number TEXT DEFAULT ''",
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS raw_payload TEXT DEFAULT '{}'",
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS next_action_due TEXT",
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS recording_url TEXT",
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS recording_status TEXT",
        "ALTER TABLE call_log ADD COLUMN IF NOT EXISTS recording_duration_seconds INTEGER",
    ):
        try:
            await session.execute(text(ddl))
        except Exception:
            continue
    for ddl in (
        "CREATE INDEX IF NOT EXISTS idx_call_log_logged_date ON call_log(logged_date)",
        "CREATE INDEX IF NOT EXISTS idx_call_log_lead_id ON call_log(lead_id)",
        "CREATE INDEX IF NOT EXISTS idx_call_log_user_id ON call_log(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_call_log_provider ON call_log(provider)",
        "CREATE INDEX IF NOT EXISTS idx_call_log_provider_call_id ON call_log(provider_call_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_call_log_provider_unique ON call_log(provider, provider_call_id)",
        "CREATE INDEX IF NOT EXISTS idx_call_recording_status ON call_log(recording_status)",
    ):
        try:
            await session.execute(text(ddl))
        except Exception:
            continue
    await session.commit()


async def _find_matching_lead(session: AsyncSession, phone_number: str) -> Optional[Dict[str, Any]]:
    matched, _ = await find_best_matching_lead(session, None, phone_number)
    return matched


def _zoom_query_paths(target_date: str, next_page_token: str = "") -> List[str]:
    candidates = [
        ("/phone/call_logs", {"page_size": _PAGE_SIZE, "from": target_date, "to": target_date}),
        ("/phone/call_history", {"page_size": _PAGE_SIZE, "from": target_date, "to": target_date}),
        ("/phone/call_logs", {"page_size": _PAGE_SIZE}),
    ]
    paths: List[str] = []
    for base_path, params in candidates:
        query_params = dict(params)
        if next_page_token:
            query_params["next_page_token"] = next_page_token
        paths.append(f"{base_path}?{urlencode(query_params)}")
    return paths


def _fetch_zoom_calls_for_date(account: Dict[str, Any], target_date: str) -> Dict[str, Any]:
    errors: List[str] = []
    successful_endpoint = ""
    successful_payload = False

    for candidate_path in _zoom_query_paths(target_date):
        next_token = ""
        calls: List[Dict[str, Any]] = []
        successful_endpoint = candidate_path.split("?", 1)[0]
        for _ in range(10):
            path = candidate_path
            if next_token:
                params = dict(pair.split("=", 1) for pair in candidate_path.split("?", 1)[1].split("&"))
                params["next_page_token"] = next_token
                path = f"{successful_endpoint}?{urlencode(params)}"

            response = _zoom_request(account, "GET", path)
            if not response.get("ok"):
                errors.append(f"{successful_endpoint}:{response.get('status')}:{response.get('error', '')}")
                calls = []
                successful_payload = False
                break

            successful_payload = True
            data = response.get("data") or {}
            for item in _extract_call_entries(data):
                if isinstance(item, dict):
                    item = dict(item)
                    item["__zoom_source_endpoint"] = successful_endpoint
                    calls.append(item)
            next_token = _normalize_text(data.get("next_page_token") or data.get("nextPageToken"))
            if not next_token:
                break

        if calls:
            return {"ok": True, "endpoint": successful_endpoint, "calls": calls}
        if successful_payload:
            return {"ok": True, "endpoint": successful_endpoint, "calls": []}

    return {
        "ok": False,
        "endpoint": successful_endpoint,
        "calls": [],
        "error": errors[-1] if errors else "No compatible Zoom call endpoint responded",
    }


def _fetch_zoom_recording_metadata(account: Dict[str, Any], provider_call_id: str) -> Dict[str, Any]:
    if not provider_call_id:
        return {}
    response = _zoom_request(account, "GET", f"/phone/call_logs/{provider_call_id}/recordings")
    if not response.get("ok"):
        return {}

    payload = response.get("data") or {}
    if isinstance(payload, dict):
        recordings = payload.get("recordings")
        if isinstance(recordings, list) and recordings:
            first = recordings[0]
            return first if isinstance(first, dict) else {}
        return payload
    if isinstance(payload, list) and payload:
        first = payload[0]
        return first if isinstance(first, dict) else {}
    return {}


def _should_attempt_sync(target_date: str, force: bool) -> bool:
    if force:
        return True
    last_attempt = _SYNC_ATTEMPTS.get(target_date)
    if last_attempt is None:
        return True
    return (time.monotonic() - last_attempt) >= _SYNC_TTL_SECONDS


async def sync_zoom_calls_for_date(
    session: AsyncSession,
    target_date: str,
    *,
    force: bool = False,
) -> Dict[str, Any]:
    await ensure_call_log_schema(session)
    await ensure_speech_schema(session)

    if not _should_attempt_sync(target_date, force):
        return {"ok": False, "attempted": False, "reason": "throttled"}

    _SYNC_ATTEMPTS[target_date] = time.monotonic()

    try:
        account = await _resolve_zoom_account(session)
    except HTTPException as exc:
        return {"ok": False, "attempted": True, "reason": "not_configured", "detail": str(exc.detail)}

    fetched = _fetch_zoom_calls_for_date(account, target_date)
    if not fetched.get("ok"):
        return {
            "ok": False,
            "attempted": True,
            "reason": "zoom_fetch_failed",
            "detail": fetched.get("error", "Unknown Zoom call sync error"),
        }

    imported = 0
    updated = 0
    skipped = 0
    postprocess_call_ids: list[str] = []

    for raw_call in fetched.get("calls", []):
        normalized = normalize_zoom_call_entry(raw_call, target_date)
        if not normalized:
            skipped += 1
            continue

        matched_lead, unmatched_reason = await find_best_matching_lead(
            session,
            raw_call,
            normalized.get("remote_number") or "",
        )
        row_payload = {
            "id": hashlib.md5(f"zoom-call:{normalized['provider_call_id']}".encode("utf-8")).hexdigest(),
            "lead_id": matched_lead.get("id") if matched_lead else "",
            "lead_address": matched_lead.get("address") if matched_lead else "",
            "outcome": normalized["outcome"],
            "connected": 1 if normalized["connected"] else 0,
            "duration_seconds": normalized["duration_seconds"],
            "call_duration_seconds": normalized["duration_seconds"],
            "note": normalized["note"],
            "operator": normalized["operator"],
            "user_id": normalized["user_id"],
            "timestamp": normalized["timestamp"],
            "logged_at": normalized["logged_at"],
            "logged_date": normalized["logged_date"],
            "provider": normalized["provider"],
            "provider_call_id": normalized["provider_call_id"],
            "direction": normalized["direction"],
            "from_number": normalized["from_number"],
            "to_number": normalized["to_number"],
            "next_action_due": normalized["next_action_due"],
            "raw_payload": normalized["raw_payload"],
        }
        if row_payload["raw_payload"]:
            try:
                raw_payload_data = json.loads(row_payload["raw_payload"])
            except Exception:
                raw_payload_data = {}
            if isinstance(raw_payload_data, dict):
                raw_payload_data["zoom_unmatched_reason"] = unmatched_reason if not matched_lead else ""
                row_payload["raw_payload"] = json.dumps(raw_payload_data, ensure_ascii=True)
                normalized["raw_payload"] = row_payload["raw_payload"]

        local_recording_path = ""
        recording_url = ""
        recording_payload: Dict[str, Any] = {}
        from services.recording_service import download_recording
        if normalized.get("raw_payload"):
            raw = normalized["raw_payload"]
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    raw = {}

            recording_url = str(raw.get("recording_url") or raw.get("download_url") or raw.get("file_url") or "").strip()
            recording_payload = _extract_recording_payload(raw, {}, normalized["duration_seconds"])
            if not recording_url:
                recording_lookup_id = _recording_lookup_id(raw)
                recording_meta = _fetch_zoom_recording_metadata(account, recording_lookup_id)
                if recording_meta:
                    recording_payload = _extract_recording_payload(raw, recording_meta, normalized["duration_seconds"])
                    recording_url = str(
                        recording_payload.get("file_url")
                        or recording_payload.get("download_url")
                        or recording_meta.get("recording_url")
                        or ""
                    ).strip()
            if recording_url:
                for attempt in range(2):
                    local_download = await download_recording(recording_url, normalized["provider_call_id"], account=account)
                    if local_download:
                        local_recording_path = str(local_download or "")
                        break
                    if attempt == 0:
                        _logger.warning("Retrying Zoom recording download for %s", normalized["provider_call_id"])
                if not local_recording_path:
                    _logger.error("Failed to download Zoom recording for %s", normalized["provider_call_id"])

            raw["recording_id"] = recording_payload.get("recording_id") or raw.get("recording_id") or ""
            raw["download_url"] = recording_payload.get("download_url") or raw.get("download_url") or ""
            raw["file_url"] = recording_payload.get("file_url") or raw.get("file_url") or ""
            raw["file_type"] = recording_payload.get("file_type") or raw.get("file_type") or ""
            raw["recording_duration_seconds"] = int(recording_payload.get("duration_seconds") or normalized["duration_seconds"] or 0)
            raw["recording_start"] = recording_payload.get("recording_start") or raw.get("recording_start") or ""
            raw["recording_end"] = recording_payload.get("recording_end") or raw.get("recording_end") or ""
            raw["zoom_unmatched_reason"] = unmatched_reason if not matched_lead else ""
            _log_zoom_call_trace(raw, recording_payload)
            normalized["raw_payload"] = json.dumps(raw, ensure_ascii=True)

        row_payload["recording_url"] = recording_url or None
        row_payload["recording_status"] = (
            "available"
            if local_recording_path
            else ("download_failed" if recording_url else "missing")
        )
        row_payload["recording_duration_seconds"] = int(
            recording_payload.get("duration_seconds") or normalized["duration_seconds"] or 0
        )
        row_payload["recording_id"] = str(recording_payload.get("recording_id") or "")
        row_payload["audio_uri"] = _relative_recording_uri(local_recording_path) if local_recording_path else ""
        row_payload["audio_storage_status"] = "stored" if local_recording_path else "not_downloaded"
        row_payload["raw_payload"] = normalized["raw_payload"]

        existing = await session.execute(
            text("SELECT id FROM call_log WHERE provider = :provider AND provider_call_id = :provider_call_id"),
            {"provider": "zoom", "provider_call_id": normalized["provider_call_id"]},
        )
        existing_row = existing.mappings().first()
        if existing_row:
            await session.execute(
                text(
                    """
                    UPDATE call_log
                    SET lead_id = :lead_id,
                        lead_address = :lead_address,
                        outcome = :outcome,
                        connected = :connected,
                        user_id = :user_id,
                        timestamp = :timestamp,
                        call_duration_seconds = :call_duration_seconds,
                        duration_seconds = :duration_seconds,
                        note = :note,
                        operator = :operator,
                        logged_at = :logged_at,
                        logged_date = :logged_date,
                        next_action_due = :next_action_due,
                        direction = :direction,
                        from_number = :from_number,
                        to_number = :to_number,
                        raw_payload = :raw_payload,
                        recording_url = :recording_url,
                        recording_status = :recording_status,
                        recording_duration_seconds = :recording_duration_seconds
                    WHERE provider = :provider AND provider_call_id = :provider_call_id
                    """
                ),
                row_payload,
            )
            await shadow_write_call_log_row(session, row_payload)
            updated += 1
            if row_payload.get("recording_url"):
                postprocess_call_ids.append(str(existing_row.get("id") or row_payload["id"]))
            continue

        await session.execute(
            text(
                """
                INSERT INTO call_log (
                    id, lead_id, lead_address, user_id, outcome, connected, timestamp, call_duration_seconds,
                    duration_seconds, note, operator, logged_at, logged_date, next_action_due, provider,
                    provider_call_id, direction, from_number, to_number, raw_payload, recording_url,
                    recording_status, recording_duration_seconds
                ) VALUES (
                    :id, :lead_id, :lead_address, :user_id, :outcome, :connected, :timestamp, :call_duration_seconds,
                    :duration_seconds, :note, :operator, :logged_at, :logged_date, :next_action_due, :provider,
                    :provider_call_id, :direction, :from_number, :to_number, :raw_payload, :recording_url,
                    :recording_status, :recording_duration_seconds
                )
                """
            ),
            row_payload,
        )
        await shadow_write_call_log_row(session, row_payload)
        imported += 1
        if row_payload.get("recording_url"):
            postprocess_call_ids.append(str(row_payload["id"]))

    await session.commit()
    if postprocess_call_ids:
        from services.call_runtime_service import schedule_call_postprocess

        for call_id in dict.fromkeys(postprocess_call_ids):
            schedule_call_postprocess(call_id)
    _logger.info(
        "Zoom call sync %s: endpoint=%s fetched=%s imported=%s updated=%s skipped=%s",
        target_date,
        fetched.get("endpoint", ""),
        len(fetched.get("calls", [])),
        imported,
        updated,
        skipped,
    )
    return {
        "ok": True,
        "attempted": True,
        "endpoint": fetched.get("endpoint", ""),
        "fetched": len(fetched.get("calls", [])),
        "imported": imported,
        "updated": updated,
        "skipped": skipped,
    }


__all__ = [
    "ensure_call_log_schema",
    "find_best_matching_lead",
    "sync_zoom_calls_for_date",
    "normalize_zoom_call_entry",
]


def _normalize_zoom_call_entry(raw_call: Dict[str, Any], target_date: str = "") -> Optional[Dict[str, Any]]:
    return normalize_zoom_call_entry(raw_call, target_date)
