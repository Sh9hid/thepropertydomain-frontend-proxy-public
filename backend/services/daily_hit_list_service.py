from __future__ import annotations

import datetime as dt
import re
from typing import Any

from core.utils import _dedupe_by_phone, _dedupe_text_list

DAILY_HIT_LIST_NAME = "Daily_Hit_List_Ranked"
_WINDOW_50_START = dt.date(2021, 1, 1)
_WINDOW_50_END = dt.date(2022, 12, 31)
_WINDOW_40_START = dt.date(2016, 1, 1)
_WINDOW_40_END = dt.date(2019, 12, 31)


def _subtract_years(value: dt.date, years: int) -> dt.date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        # Feb 29 -> Feb 28 fallback
        return value.replace(month=2, day=28, year=value.year - years)


def _normalize_rank_address(address: str | None, suburb: str | None = None) -> str:
    text = str(address or "").upper().strip()
    if not text:
        return ""
    text = text.replace(",", " ")
    text = re.sub(r"\bNSW\b", " ", text)
    text = re.sub(r"\bAUSTRALIA\b", " ", text)
    text = re.sub(r"\b(APARTMENT|UNIT|TOWNHOUSE)\s+(\d+[A-Z]?)\s+(\d+[A-Z]?)\b", r"\2/\3", text)
    text = re.sub(r"\b(\d+[A-Z]?)\s*/\s*(\d+[A-Z]?)\b", r"\1/\2", text)
    substitutions = {
        "PLACE": "PL",
        "CRESCENT": "CRES",
        "ROAD": "RD",
        "STREET": "ST",
        "DRIVE": "DR",
        "AVENUE": "AVE",
        "TERRACE": "TCE",
        "BOULEVARD": "BLVD",
        "LANE": "LN",
        "COURT": "CT",
    }
    for long_form, short_form in substitutions.items():
        text = re.sub(rf"\b{long_form}\b", short_form, text)
    text = re.sub(r"\b\d{4}\b", " ", text)
    if suburb:
        text = text.replace(str(suburb).upper().strip(), " ")
    text = re.sub(r"[^A-Z0-9/]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _valid_mobiles(raw_phones: Any) -> list[str]:
    mobiles: list[str] = []
    for phone in _dedupe_by_phone(raw_phones):
        digits = "".join(ch for ch in str(phone) if ch.isdigit())
        if digits.startswith("61") and len(digits) == 11:
            digits = f"0{digits[2:]}"
        if len(digits) == 10 and digits.startswith("04"):
            mobiles.append(digits)
    return mobiles


def _parse_sale_date(value: Any) -> dt.date | None:
    text = str(value or "").strip()
    if not text or text == "-":
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%d %b %Y", "%d %B %Y"):
        try:
            target = text[:10] if fmt == "%Y-%m-%d" else text
            return dt.datetime.strptime(target, fmt).date()
        except ValueError:
            continue
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError:
        return None


def _latest_sale_date(lead: dict[str, Any]) -> dt.date | None:
    candidates = [
        _parse_sale_date(lead.get("sale_date")),
        _parse_sale_date(lead.get("settlement_date")),
        _parse_sale_date(lead.get("last_settlement_date")),
    ]
    parsed = [item for item in candidates if item]
    return max(parsed) if parsed else None


def _phone_priority(lead: dict[str, Any], mobiles: list[str]) -> tuple[int, int, int, str]:
    trigger = str(lead.get("trigger_type") or "").strip().lower()
    return (
        0 if trigger == "marketing_list" else 1,
        -len(mobiles),
        -int(lead.get("call_today_score") or 0),
        str(lead.get("id") or ""),
    )


def _build_summary(latest_sale: dt.date, intent_score: int, today: dt.date) -> str:
    sale_text = latest_sale.strftime("%b %Y")
    if intent_score == 50:
        return f"Direct 04 mobile on file. Last recorded sale was {sale_text}, which sits in the 2021-2022 priority window."
    if intent_score == 40:
        return f"Direct 04 mobile on file. Last recorded sale was {sale_text}, which sits in the 2016-2019 priority window."
    cutoff_text = _subtract_years(today, 2).strftime("%b %Y")
    return f"Direct 04 mobile on file. Last recorded sale was {sale_text}, which clears the {cutoff_text} recency filter."


def _trigger_summary(lead: dict[str, Any]) -> str:
    trigger = str(lead.get("trigger_type") or "").strip().lower()
    archetype = str(lead.get("lead_archetype") or "").strip().lower()
    signal_status = str(lead.get("signal_status") or "").strip().upper()
    route_queue = str(lead.get("route_queue") or "").strip().upper()

    if "withdrawn" in trigger or "withdrawn" in archetype or signal_status == "WITHDRAWN":
        return "Withdrawn listing signal is attached."
    if "probate" in trigger or "estate" in archetype:
        return "Probate or estate signal is attached."
    if "mortgage" in trigger or "cliff" in trigger or route_queue == "MORTGAGE":
        return "Mortgage pressure signal is attached."
    if "da" in trigger or "development" in trigger or route_queue == "DEVELOPMENT":
        return "DA / development signal is attached."
    if trigger == "marketing_list":
        return "Direct marketing contact record is already in the book."
    if trigger == "cotality_import":
        return "Property ownership and sale-history record is attached."
    if "delta" in trigger or signal_status == "DELTA":
        return "Delta signal is attached."
    return "Property signal is attached."


def _contact_summary(lead: dict[str, Any], mobiles: list[str]) -> str:
    phones = _dedupe_by_phone(lead.get("contact_phones"))
    emails = _dedupe_text_list(lead.get("contact_emails"))
    if mobiles:
        return "Direct 04 mobile is on file."
    if phones:
        return "A phone is on file, but it is not a clean 04 mobile yet."
    if emails:
        return "No direct phone is on file yet, but an email contact is available."
    return "No direct phone or email is on file yet, so this needs enrichment before outreach."


def _build_non_ranked_summary(lead: dict[str, Any]) -> str:
    mobiles = _valid_mobiles(lead.get("contact_phones"))
    parts = [
        _trigger_summary(lead),
        _contact_summary(lead, mobiles),
    ]
    latest_sale = _latest_sale_date(lead)
    if latest_sale:
        parts.append(f"Last recorded sale was {latest_sale.strftime('%b %Y')}.")
    return " ".join(part for part in parts if part).strip()


def enrich_leads_with_daily_hit_list(
    leads: list[dict[str, Any]],
    *,
    limit: int = 200,
    today: dt.date | None = None,
) -> list[dict[str, Any]]:
    if not leads:
        return []

    today = today or dt.date.today()
    cutoff = _subtract_years(today, 2)
    lead_copies = [dict(lead) for lead in leads]
    grouped: dict[str, dict[str, Any]] = {}

    for lead in lead_copies:
        key = _normalize_rank_address(lead.get("address"), lead.get("suburb"))
        if not key:
            continue
        bucket = grouped.setdefault(key, {"phones": [], "sales": []})
        mobiles = _valid_mobiles(lead.get("contact_phones"))
        if mobiles:
            bucket["phones"].append((lead, mobiles))
        latest_sale = _latest_sale_date(lead)
        if latest_sale:
            bucket["sales"].append((latest_sale, lead))

    ranked_candidates: list[dict[str, Any]] = []
    for bucket in grouped.values():
        if not bucket["phones"] or not bucket["sales"]:
            continue
        latest_sale = max(sale_date for sale_date, _ in bucket["sales"])
        if latest_sale >= cutoff:
            continue
        phone_lead, mobiles = min(bucket["phones"], key=lambda item: _phone_priority(item[0], item[1]))
        intent_score = 0
        if _WINDOW_50_START <= latest_sale <= _WINDOW_50_END:
            intent_score = 50
        elif _WINDOW_40_START <= latest_sale <= _WINDOW_40_END:
            intent_score = 40
        ranked_candidates.append(
            {
                "lead_id": phone_lead.get("id"),
                "intent_score": intent_score,
                "intent_summary": _build_summary(latest_sale, intent_score, today),
                "call_today_score": int(phone_lead.get("call_today_score") or 0),
                "heat_score": int(phone_lead.get("heat_score") or 0),
                "latest_sale_ordinal": latest_sale.toordinal(),
            }
        )

    ranked_candidates.sort(
        key=lambda item: (
            -item["intent_score"],
            -item["call_today_score"],
            -item["heat_score"],
            -item["latest_sale_ordinal"],
            str(item["lead_id"] or ""),
        )
    )

    top_candidates = ranked_candidates[: max(1, limit)]
    overrides: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(top_candidates, start=1):
        overrides[str(item["lead_id"])] = {
            "intent_score": item["intent_score"],
            "intent_summary": item["intent_summary"],
            "daily_hit_list_rank": index,
            "daily_hit_list_name": DAILY_HIT_LIST_NAME,
        }

    for lead in lead_copies:
        override = overrides.get(str(lead.get("id") or ""))
        if override:
            tags = _dedupe_text_list(lead.get("source_tags"))
            if DAILY_HIT_LIST_NAME not in tags:
                tags.append(DAILY_HIT_LIST_NAME)
            lead.update(override)
            lead["source_tags"] = tags
        else:
            lead["intent_score"] = 0
            lead["intent_summary"] = _build_non_ranked_summary(lead)
            lead["daily_hit_list_rank"] = None
            lead["daily_hit_list_name"] = ""

    lead_copies.sort(
        key=lambda lead: (
            0 if lead.get("daily_hit_list_rank") else 1,
            int(lead.get("daily_hit_list_rank") or 999999),
            -int(lead.get("call_today_score") or 0),
            -int(lead.get("heat_score") or 0),
            str(lead.get("address") or ""),
            str(lead.get("id") or ""),
        )
    )
    return lead_copies
