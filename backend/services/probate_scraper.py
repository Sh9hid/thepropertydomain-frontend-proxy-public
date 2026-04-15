"""
NSW Government Gazette probate notice scraper.

Fetches deceased estate notices from the NSW Government Gazette search
and upserts them into the leads table for target postcodes.

Gemini extraction is used first for structured fields, with deterministic
regex-based fallback to keep ingestion resilient if the model is unavailable.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List
from urllib.parse import urlencode

import httpx
from pydantic import BaseModel, Field, ValidationError

from core.config import (
    PROBATE_EXTRACT_FALLBACK_MODEL,
    PROBATE_EXTRACT_MODEL,
    PROBATE_LLM_EXTRACTION_ENABLED,
)
from core.utils import now_iso
from services.ai_router import _call_gemini_json
from services.scrape_stealth import build_headers, get_rotating_proxy_url, jitter_sleep

TARGET_POSTCODES = {"2765", "2517", "2518"}

# NSW Gazette search endpoint (public, no auth)
_GAZETTE_SEARCH_URL = "https://gazette.legislation.nsw.gov.au/api/v1/search"

# Fallback: ePlanning gazette endpoint (used if primary returns empty)
_GAZETTE_HTML_URL = (
    "https://www.nsw.gov.au/departments-and-agencies/the-cabinet-office/services/"
    "nsw-government-gazette"
)

_ADDRESS_PATTERN = re.compile(
    r"(?:property at|situated at|located at|at)\s+"
    r"([^\n,;]+(?:Street|Road|Avenue|Drive|Close|Place|Way|Crescent|Court|Lane|"
    r"Parade|Terrace|Boulevard|Highway|Circuit|Grove|Rise|Mews|Approach)\b[^\n.;]*)",
    re.I,
)
_EXECUTOR_PATTERN = re.compile(
    r"\b(?:executor|executrix|administrator|administrators|trustee|trustees)\b"
    r"[:\s,-]+([^\n.;]+)",
    re.I,
)
_PROPERTY_REF_PATTERNS = [
    re.compile(r"\bLot\s+\d+(?:/\d+)?(?:\s+in)?\s+DP\s+\d+\b", re.I),
    re.compile(r"\bDP\s+\d+\b", re.I),
    re.compile(r"\bDeposited Plan\s+\d+\b", re.I),
    re.compile(r"\bSP\s+\d+\b", re.I),
    re.compile(r"\bFolio Identifier\s+[A-Za-z0-9/.-]+\b", re.I),
]


class ProbateNoticeExtraction(BaseModel):
    deceased_name: str = ""
    suburb: str = ""
    postcode: str = ""
    executor: str = ""
    property_refs: List[str] = Field(default_factory=list)
    address: str = ""
    confidence: int = 0


_PROBATE_EXTRACTION_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "deceased_name": {
            "type": "string",
            "description": "Exact deceased person's name if explicitly stated. Empty string if missing.",
        },
        "suburb": {
            "type": "string",
            "description": "NSW suburb tied to the estate notice. Empty string if not explicit.",
        },
        "postcode": {
            "type": "string",
            "description": "Four-digit NSW postcode tied to the estate notice. Empty string if not explicit.",
        },
        "executor": {
            "type": "string",
            "description": "Executor, administrator, trustee, or legal estate contact if explicitly named.",
        },
        "property_refs": {
            "type": "array",
            "description": "Property reference strings explicitly present, such as Lot/DP/SP/Folio identifiers.",
            "items": {"type": "string"},
        },
        "address": {
            "type": "string",
            "description": "Street address if explicitly stated in the notice text.",
        },
        "confidence": {
            "type": "integer",
            "description": "Confidence from 0 to 100 based only on explicit text in the notice.",
        },
    },
    "required": [
        "deceased_name",
        "suburb",
        "postcode",
        "executor",
        "property_refs",
        "address",
        "confidence",
    ],
}


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _dedupe_strings(values: List[str]) -> List[str]:
    seen: set[str] = set()
    deduped: List[str] = []
    for value in values:
        cleaned = _clean_text(value)
        key = cleaned.lower()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped


def _notice_content(notice: Dict[str, Any]) -> str:
    return (
        notice.get("content")
        or notice.get("body")
        or notice.get("text")
        or json.dumps(notice, ensure_ascii=True)
    )


def _extract_property_refs(text: str) -> List[str]:
    refs: List[str] = []
    for pattern in _PROPERTY_REF_PATTERNS:
        refs.extend(match.group(0) for match in pattern.finditer(text))
    return _dedupe_strings(refs)


def _extract_executor(text: str) -> str:
    match = _EXECUTOR_PATTERN.search(text)
    return _clean_text(match.group(1)) if match else ""


def _extract_address_candidate(text: str) -> str:
    match = _ADDRESS_PATTERN.search(text)
    return _clean_text(match.group(1)) if match else ""


def _fetch_probate_notices() -> List[Dict[str, Any]]:
    """
    Query the NSW Gazette for probate notices.
    Returns a list of raw notice dicts. Returns [] on any failure - caller handles.
    """
    params = urlencode({
        "query": "probate",
        "category": "probate",
        "limit": 100,
    })
    url = f"{_GAZETTE_SEARCH_URL}?{params}"
    try:
        jitter_sleep()
        proxy = get_rotating_proxy_url()
        headers = build_headers({"Accept": "application/json"})
        with httpx.Client(timeout=15.0, headers=headers, proxy=proxy, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
        return data.get("notices", data.get("results", []))
    except Exception:
        return []


def _extract_suburb_postcode(text: str):
    """
    Heuristic: look for NSW suburb/postcode patterns in free text.
    Returns (suburb, postcode) or (None, None).
    """
    normalized = _clean_text(text)
    nsw_match = re.search(r"\b([A-Za-z][A-Za-z\s'-]+?)\s+NSW\s+(2[0-9]{3})\b", normalized)
    if nsw_match:
        locality = _clean_text(nsw_match.group(1))
        street_split = re.split(
            r"\b(?:Street|Road|Avenue|Drive|Close|Place|Way|Crescent|Court|Lane|"
            r"Parade|Terrace|Boulevard|Highway|Circuit|Grove|Rise|Mews|Approach)\b",
            locality,
            flags=re.I,
        )
        suburb = _clean_text(street_split[-1] if len(street_split) > 1 else locality).title()
        return suburb or None, nsw_match.group(2)

    postcode_match = re.search(r"\b(2[0-9]{3})\b", normalized)
    postcode = postcode_match.group(1) if postcode_match else None

    suburb = None
    if postcode_match:
        before = normalized[: postcode_match.start()].strip()
        suburb_match = re.search(r"([A-Z][A-Za-z\s'-]+)$", before)
        if suburb_match:
            suburb = suburb_match.group(1).strip().title()

    return suburb, postcode


def _heuristic_extract_notice(notice: Dict[str, Any]) -> ProbateNoticeExtraction:
    content = _notice_content(notice)
    deceased = _clean_text(notice.get("deceased_name") or notice.get("title") or "")
    address = _clean_text(notice.get("address") or notice.get("property_address") or "")
    if not address:
        address = _extract_address_candidate(content)
    suburb, postcode = _extract_suburb_postcode(f"{address} {content}")
    executor = _extract_executor(content)
    property_refs = _extract_property_refs(f"{address} {content}")

    confidence = 38
    if postcode:
        confidence = 58
    if address:
        confidence = max(confidence, 68)
    if deceased:
        confidence = max(confidence, 72)

    return ProbateNoticeExtraction(
        deceased_name=deceased,
        suburb=suburb or "",
        postcode=postcode or "",
        executor=executor,
        property_refs=property_refs,
        address=address,
        confidence=confidence,
    )


def _build_probate_prompt(notice: Dict[str, Any]) -> str:
    title = _clean_text(notice.get("title") or notice.get("deceased_name") or "")
    address = _clean_text(notice.get("address") or notice.get("property_address") or "")
    content = _notice_content(notice)
    return (
        "Extract structured data from this NSW probate / deceased-estate gazette notice.\n"
        "Rules:\n"
        "- Use only explicit text from the notice.\n"
        "- Do not invent missing fields.\n"
        "- Keep postcode as a 4-digit string when present.\n"
        "- property_refs should include explicit Lot/DP/SP/Folio identifiers only.\n"
        "- confidence should be 0-100 based on how explicit the notice is.\n\n"
        f"TITLE:\n{title or '(blank)'}\n\n"
        f"ADDRESS FIELD:\n{address or '(blank)'}\n\n"
        f"NOTICE BODY:\n{content}\n"
    )


async def _extract_notice_with_gemini(notice: Dict[str, Any]) -> ProbateNoticeExtraction | None:
    if not PROBATE_LLM_EXTRACTION_ENABLED:
        return None

    models = [PROBATE_EXTRACT_MODEL]
    if PROBATE_EXTRACT_FALLBACK_MODEL and PROBATE_EXTRACT_FALLBACK_MODEL not in models:
        models.append(PROBATE_EXTRACT_FALLBACK_MODEL)

    system = (
        "You extract fields from NSW probate notices for a backend ingestion pipeline. "
        "Return valid JSON that exactly matches the schema. "
        "Prefer empty strings or an empty array over guessing."
    )
    prompt = _build_probate_prompt(notice)

    for model in models:
        payload = await _call_gemini_json(
            prompt,
            _PROBATE_EXTRACTION_SCHEMA,
            system,
            model=model,
            temperature=0.1,
            max_output_tokens=512,
        )
        if not payload:
            continue
        try:
            extraction = ProbateNoticeExtraction.model_validate(payload)
        except ValidationError:
            continue
        extraction.confidence = max(0, min(100, int(extraction.confidence or 0)))
        extraction.property_refs = _dedupe_strings(extraction.property_refs)
        return extraction
    return None


async def extract_probate_notice_fields(notice: Dict[str, Any]) -> ProbateNoticeExtraction:
    heuristic = _heuristic_extract_notice(notice)
    llm = await _extract_notice_with_gemini(notice)
    if not llm:
        return heuristic

    return ProbateNoticeExtraction(
        deceased_name=_clean_text(llm.deceased_name or heuristic.deceased_name),
        suburb=_clean_text(llm.suburb or heuristic.suburb),
        postcode=_clean_text(llm.postcode or heuristic.postcode),
        executor=_clean_text(llm.executor or heuristic.executor),
        property_refs=_dedupe_strings(llm.property_refs or heuristic.property_refs),
        address=_clean_text(llm.address or heuristic.address),
        confidence=max(llm.confidence, heuristic.confidence),
    )


async def scrape_and_upsert(session) -> Dict[str, Any]:
    """
    Fetch NSW Gazette probate notices, filter to target postcodes, and upsert
    into the leads table. New leads are pushed to ticker_events.

    Returns a summary dict: { fetched, matched, upserted, skipped }.
    """
    from sqlalchemy import text
    from services.ticker_push import push_ticker_event

    notices = _fetch_probate_notices()
    fetched = len(notices)
    matched = 0
    upserted = 0
    skipped = 0

    for notice in notices:
        parsed = await extract_probate_notice_fields(notice)
        deceased = parsed.deceased_name or _clean_text(notice.get("title")) or "Deceased Estate"
        address_raw = parsed.address
        suburb = parsed.suburb
        postcode = parsed.postcode

        if not postcode or postcode not in TARGET_POSTCODES:
            skipped += 1
            continue

        matched += 1
        external_ref = _clean_text(notice.get("gazette_id") or notice.get("id") or notice.get("slug") or "")
        lead_id = hashlib.md5(
            f"probate:{external_ref}:{deceased}:{address_raw}:{postcode}".encode("utf-8")
        ).hexdigest()
        now = now_iso()

        existing = (
            await session.execute(text("SELECT id FROM leads WHERE id = :id"), {"id": lead_id})
        ).fetchone()

        if existing:
            skipped += 1
            continue

        await session.execute(
            text(
                """
                INSERT INTO leads (
                    id, owner_name, address, suburb, postcode,
                    trigger_type, opportunity_vectors, heat_score,
                    signal_status, status, route_queue,
                    activity_log, created_at, updated_at
                ) VALUES (
                    :id, :owner_name, :address, :suburb, :postcode,
                    'probate', :opp_vectors, 65,
                    'OFF-MARKET', 'captured', 'real_estate',
                    :activity_log, :now, :now
                )
                """
            ),
            {
                "id": lead_id,
                "owner_name": deceased,
                "address": address_raw or "Address pending",
                "suburb": suburb or "",
                "postcode": postcode,
                "opp_vectors": json.dumps(["PROBATE"]),
                "activity_log": json.dumps(
                    [
                        {
                            "type": "probate_detected",
                            "note": f"Gazette notice ingested: {external_ref or 'unknown'}",
                            "timestamp": now,
                            "executor": parsed.executor,
                            "property_refs": parsed.property_refs,
                            "confidence": parsed.confidence,
                        }
                    ]
                ),
                "now": now,
            },
        )
        await push_ticker_event(
            session,
            event_type="PROBATE",
            source="probate_gazette",
            address=address_raw or "Address pending",
            suburb=suburb or "",
            postcode=postcode,
            owner_name=deceased,
            heat_score=65,
            lead_id=lead_id,
            headline=f"Probate notice | {deceased}",
            extra={
                "gazette_id": external_ref,
                "postcode": postcode,
                "executor": parsed.executor,
                "property_refs": parsed.property_refs,
                "llm_confidence": parsed.confidence,
            },
        )
        upserted += 1

    if upserted > 0:
        await session.commit()

    return {
        "fetched": fetched,
        "matched": matched,
        "upserted": upserted,
        "skipped": skipped,
    }
