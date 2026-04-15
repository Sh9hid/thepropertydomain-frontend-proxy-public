import datetime
import json
import re
import sqlite3
from typing import Any, Dict, List, Optional
from fastapi import HTTPException
from core.config import SYDNEY_TZ
from models.schemas import JSON_COLUMNS

def now_sydney() -> datetime.datetime:
    return datetime.datetime.now(SYDNEY_TZ).replace(microsecond=0)

def now_iso() -> str:
    return now_sydney().isoformat()

def format_sydney(ts: Optional[datetime.datetime] = None) -> str:
    return (ts or now_sydney()).astimezone(SYDNEY_TZ).strftime("%d/%m/%Y %I:%M %p")

def parse_client_datetime(value: str) -> str:
    text = (value or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Date/time value is required")
    try:
        parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date/time: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SYDNEY_TZ)
    return parsed.astimezone(SYDNEY_TZ).replace(microsecond=0).isoformat()

def _first_non_empty(*values: Any, fallback: str = "") -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return fallback

def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default

def _format_moneyish(value: Any, fallback: str = "Not available") -> str:
    if value in (None, "", 0, "0"):
        return fallback
    try:
        return f"${float(str(value).replace(',', '')):,.0f}"
    except (TypeError, ValueError):
        return str(value)

def _parse_json_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []

def _encode_value(column: str, value: Any) -> Any:
    if column in JSON_COLUMNS:
        if value is None:
            return json.dumps([])
        if isinstance(value, str):
            return value
        return json.dumps(value)
    return value

def _decode_row(row: sqlite3.Row) -> Dict[str, Any]:
    lead = dict(row)
    for column in JSON_COLUMNS:
        lead[column] = _parse_json_list(lead.get(column))
    return lead

def _dedupe_text_list(values: Any) -> List[str]:
    seen: set[str] = set()
    cleaned: List[str] = []
    for value in _parse_json_list(values):
        text = str(value).strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        cleaned.append(text)
    return cleaned

def _normalize_phone(value: str) -> str:
    digits = re.sub(r"\D+", "", value or "")
    return digits[-10:] if len(digits) >= 8 else digits

def _normalize_au_mobile(value: str) -> str:
    digits = re.sub(r"\D+", "", value or "")
    if not digits:
        return ""
    if digits.startswith("61") and len(digits) >= 11:
        digits = "0" + digits[2:]
    if digits.startswith("04") and len(digits) >= 10:
        return digits[:10]
    return digits

def _is_sms_mobile_au(value: str) -> bool:
    normalized = _normalize_au_mobile(value)
    return bool(re.fullmatch(r"04\d{8}", normalized))

def _dedupe_by_phone(values: Any) -> List[str]:
    seen: set[str] = set()
    cleaned: List[str] = []
    for value in _parse_json_list(values):
        text = str(value).strip()
        if not text:
            continue
        normalized = _normalize_phone(text)
        key = normalized or text.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(text)
    return cleaned

def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime.datetime]:
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return None

def _parse_calendar_date(value: Optional[str]) -> datetime.date:
    text = (value or "").strip()
    if not text:
        return now_sydney().date()
    try:
        return datetime.date.fromisoformat(text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {value}") from exc

def _month_range_from_date(anchor: datetime.date) -> tuple[datetime.datetime, datetime.datetime, datetime.date]:
    month_start = anchor.replace(day=1)
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1, day=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1, day=1)
    return (
        datetime.datetime.combine(month_start, datetime.time.min, tzinfo=SYDNEY_TZ),
        datetime.datetime.combine(next_month, datetime.time.min, tzinfo=SYDNEY_TZ),
        month_start,
    )

def get_openai_api_key() -> str:
    """Returns the OpenAI API key based on the active profile."""
    import os
    profile = os.getenv("OPENAI_PROFILE", "codex").lower()
    if profile == "alhayat":
        return os.getenv("OPENAI_API_KEY_ALHAYAT", "")
    return os.getenv("OPENAI_API_KEY_CODEX", os.getenv("OPENAI_API_KEY", ""))

def _bool_db(value: Any) -> bool:
    return bool(value) and str(value).lower() not in {"0", "false", "none", ""}


__all__ = ['now_sydney', 'now_iso', 'format_sydney', 'parse_client_datetime', '_first_non_empty', '_safe_int', '_format_moneyish', '_parse_json_list', '_encode_value', '_decode_row', '_dedupe_text_list', '_normalize_phone', '_normalize_au_mobile', '_is_sms_mobile_au', '_dedupe_by_phone', '_parse_iso_datetime', '_parse_calendar_date', '_month_range_from_date', 'get_openai_api_key', '_bool_db']
