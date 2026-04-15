"""
Support models and helpers for Zoom Phone recording routes.
"""

import json
import re
from typing import Any, Dict, Optional, Set

from pydantic import BaseModel


class CallTranscriptIngestRequest(BaseModel):
    transcript: str


def phone_suffix(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[-9:] if digits else ""


def lead_phone_suffixes(lead: Dict[str, Any]) -> Set[str]:
    return {phone_suffix(phone) for phone in lead.get("contact_phones", []) if phone_suffix(phone)}


def recording_date(call: Dict[str, Any], recording: Optional[Dict[str, Any]] = None) -> str:
    return str(
        (recording or {}).get("date_time")
        or call.get("date_time")
        or call.get("start_time")
        or call.get("call_end_time")
        or ""
    )


def call_to_recording_payload(call: Dict[str, Any], recording: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "id": str(call.get("recording_id") or (recording or {}).get("id") or call.get("id") or ""),
        "call_id": str(call.get("id") or ""),
        "call_history_id": str((recording or {}).get("call_history_id") or ""),
        "zoom_call_id": str(call.get("call_id") or ""),
        "date": recording_date(call, recording),
        "duration": call.get("duration") or (recording or {}).get("duration") or 0,
        "direction": call.get("direction"),
        "from": call.get("caller_did_number") or call.get("caller_number"),
        "to": call.get("callee_did_number") or call.get("callee_number"),
        "result": call.get("result"),
        "recording_type": call.get("recording_type") or (recording or {}).get("recording_type"),
        "has_voicemail": bool(call.get("has_voicemail")),
        "ai_call_summary_id": call.get("ai_call_summary_id"),
        "download_url": (recording or {}).get("download_url"),
        "file_url": (recording or {}).get("file_url"),
        "owner": (recording or {}).get("owner") or call.get("owner"),
    }


def match_sms_session(session_payload: Dict[str, Any], phone_suffixes: Set[str]) -> bool:
    participants = session_payload.get("participants", [])
    for participant in participants:
        if phone_suffix(participant.get("phone_number")) in phone_suffixes:
            return True
    for history in session_payload.get("sms_histories", []):
        sender = history.get("sender", {}) or {}
        if phone_suffix(sender.get("phone_number")) in phone_suffixes:
            return True
        for member in history.get("to_members", []) or []:
            if phone_suffix(member.get("phone_number")) in phone_suffixes:
                return True
    return False


def session_preview(session_payload: Dict[str, Any]) -> Dict[str, Any]:
    histories = session_payload.get("sms_histories", []) or []
    last_message = histories[-1] if histories else {}
    participants = session_payload.get("participants")
    if not participants and histories:
        participants = []
        seen: Set[str] = set()
        for item in histories:
            sender = item.get("sender", {}) or {}
            sender_number = str(sender.get("phone_number") or "")
            if sender_number and sender_number not in seen:
                participants.append(
                    {
                        "phone_number": sender_number,
                        "display_name": sender.get("display_name"),
                        "owner": sender.get("owner"),
                    }
                )
                seen.add(sender_number)
            for member in item.get("to_members", []) or []:
                member_number = str(member.get("phone_number") or "")
                if member_number and member_number not in seen:
                    participants.append(member)
                    seen.add(member_number)
    return {
        "session_id": session_payload.get("session_id"),
        "session_type": session_payload.get("session_type"),
        "last_access_time": session_payload.get("last_access_time") or last_message.get("date_time"),
        "participants": participants or [],
        "message_count": len(histories),
        "last_message": last_message.get("message"),
        "last_message_at": last_message.get("date_time"),
        "messages": histories,
    }


def json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def remote_number(direction: str, from_number: str, to_number: str) -> str:
    direction_value = str(direction or "").lower()
    if "inbound" in direction_value or direction_value in {"in", "incoming", "received"}:
        return from_number or to_number
    return to_number or from_number


def recording_source_url(
    row: Optional[Dict[str, Any]] = None,
    raw_payload: Optional[Dict[str, Any]] = None,
) -> str:
    payload = raw_payload or {}
    source_row = row or {}
    return str(
        source_row.get("recording_url")
        or payload.get("file_url")
        or payload.get("download_url")
        or ""
    ).strip()
