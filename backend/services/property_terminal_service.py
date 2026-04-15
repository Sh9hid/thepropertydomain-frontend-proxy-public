import hashlib
import math
import re
import threading
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import BRAND_NAME, PRINCIPAL_NAME, STOCK_ROOT
from core.logic import _hydrate_lead
from core.utils import now_iso, now_sydney, format_sydney

SUBURB_REPORTS_ROOT = Path(STOCK_ROOT) / "Suburb Reports"


def _normalize_token(value: Any) -> str:
    text_value = str(value or "").strip().lower()
    text_value = re.sub(r"\bnsw\b", "", text_value)
    text_value = re.sub(r"\b\d{4}\b", "", text_value)
    text_value = re.sub(r"[^a-z0-9/ ]+", " ", text_value)
    return re.sub(r"\s+", " ", text_value).strip()


def _core_address(value: Any) -> str:
    text_value = str(value or "").strip()
    return text_value.split(",")[0].strip()


def _address_key(address: Any, suburb: Any) -> str:
    return f"{_normalize_token(_core_address(address))}|{_normalize_token(suburb)}".strip("|")


def _street_name(value: Any) -> str:
    address = _normalize_token(_core_address(value))
    address = re.sub(r"^\d+[a-z]?(?:/\d+[a-z]?)?\s+", "", address)
    return address


def _parse_float(value: Any) -> float | None:
    if value in (None, "", "-", "--"):
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", str(value))
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    return int(round(parsed))


def _format_number(value: float | int | None, suffix: str = "") -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    if isinstance(value, float) and not value.is_integer():
        return f"{value:,.1f}{suffix}"
    return f"{int(value):,}{suffix}"


def _format_currency(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    return f"${int(round(float(value))):,}"


def _clean_header(value: Any) -> str:
    text_value = str(value or "").strip().lower()
    text_value = text_value.replace("(m²)", "sqm").replace("(m2)", "sqm")
    text_value = text_value.replace("m²", "sqm").replace("m2", "sqm")
    text_value = re.sub(r"[^a-z0-9]+", "_", text_value)
    return text_value.strip("_")


def _find_header_index(raw: pd.DataFrame) -> int:
    for index in range(min(len(raw.index), 12)):
        row = [_normalize_token(value) for value in raw.iloc[index].tolist()]
        if "street address" in row and "suburb" in row:
            return index
    return 2


def _extract_report_suburb(path: Path, raw: pd.DataFrame) -> str:
    for index in range(min(len(raw.index), 3)):
        values = [str(value).strip() for value in raw.iloc[index].tolist() if value not in (None, "")]
        for value in values:
            lowered = value.lower()
            if "nsw" in lowered and any(char.isalpha() for char in value):
                return value.split("NSW")[0].replace("Search String", "").strip(" -")
    return path.stem.replace(" report", "").strip()


def _rename_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = frame.rename(
        columns={
            "street_address": "street_address",
            "suburb": "suburb",
            "state": "state",
            "postcode": "postcode",
            "council_area": "council_area",
            "property_type": "property_type",
            "bed": "bedrooms",
            "bath": "bathrooms",
            "car": "car_spaces",
            "land_size_sqm": "land_size_sqm",
            "floor_size_sqm": "floor_size_sqm",
            "year_built": "year_built",
            "last_sale_price": "sale_price",
            "sale_price": "sale_price",
            "price": "sale_price",
            "property_photo": "property_photo",
        }
    )
    for column in (
        "street_address",
        "suburb",
        "state",
        "postcode",
        "council_area",
        "property_type",
        "property_photo",
    ):
        if column not in renamed.columns:
            renamed[column] = ""
    for column in ("bedrooms", "bathrooms", "car_spaces", "land_size_sqm", "floor_size_sqm", "year_built", "sale_price"):
        if column not in renamed.columns:
            renamed[column] = None
    return renamed


class _SuburbReportWarehouse:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: dict[str, dict[str, Any]] = {}

    def _matching_files(self, suburb: str) -> list[Path]:
        target = _normalize_token(suburb)
        if not target or not SUBURB_REPORTS_ROOT.exists():
            return []
        matches: list[Path] = []
        for path in SUBURB_REPORTS_ROOT.glob("*.xlsx"):
            path_key = _normalize_token(path.stem.replace(" report", ""))
            if path_key == target:
                matches.append(path)
        if matches:
            return sorted(matches)
        for path in SUBURB_REPORTS_ROOT.glob("*.xlsx"):
            path_key = _normalize_token(path.stem)
            if target and target in path_key:
                matches.append(path)
        return sorted(matches)

    def _fingerprint(self, files: list[Path]) -> str:
        parts = [f"{path.name}:{path.stat().st_size}:{path.stat().st_mtime_ns}" for path in files]
        return hashlib.md5("|".join(parts).encode()).hexdigest() if parts else ""

    def _load_file(self, path: Path) -> pd.DataFrame:
        raw = pd.read_excel(path, header=None)
        header_index = _find_header_index(raw)
        header = [_clean_header(value) for value in raw.iloc[header_index].tolist()]
        body = raw.iloc[header_index + 1 :].copy()
        body.columns = header
        body = body.dropna(how="all")
        body = _rename_columns(body)
        body["report_suburb"] = _extract_report_suburb(path, raw)
        body["source_file"] = path.name
        body["source_path"] = str(path)
        body["source_mtime"] = path.stat().st_mtime
        body["street_address"] = body["street_address"].astype(str).str.strip()
        body["suburb"] = body["suburb"].astype(str).str.strip().replace({"": body["report_suburb"].iloc[0] if not body.empty else ""})
        body["state"] = body["state"].astype(str).str.strip().replace({"": "NSW"})
        body["postcode"] = body["postcode"].astype(str).str.strip()
        body = body[body["street_address"].astype(bool)].copy()
        for column in ("bedrooms", "bathrooms", "car_spaces", "land_size_sqm", "floor_size_sqm", "year_built", "sale_price"):
            body[column] = body[column].map(_parse_float)
        body["address_key"] = body.apply(lambda row: _address_key(row["street_address"], row["suburb"]), axis=1)
        body["street_key"] = body["street_address"].map(_street_name)
        body["report_suburb_key"] = body["report_suburb"].map(_normalize_token)
        body["suburb_key"] = body["suburb"].map(_normalize_token)
        return body[
            [
                "source_file",
                "source_path",
                "source_mtime",
                "report_suburb",
                "report_suburb_key",
                "street_address",
                "address_key",
                "street_key",
                "suburb",
                "suburb_key",
                "state",
                "postcode",
                "council_area",
                "property_type",
                "bedrooms",
                "bathrooms",
                "car_spaces",
                "land_size_sqm",
                "floor_size_sqm",
                "year_built",
                "sale_price",
                "property_photo",
            ]
        ]

    def get_suburb_frame(self, suburb: str) -> pd.DataFrame:
        files = self._matching_files(suburb)
        fingerprint = self._fingerprint(files)
        cache_key = _normalize_token(suburb)
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached and cached["fingerprint"] == fingerprint:
                return cached["frame"]
            if not files:
                empty = pd.DataFrame()
                self._cache[cache_key] = {"fingerprint": fingerprint, "frame": empty}
                return empty
            frames = [self._load_file(path) for path in files]
            frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            self._cache[cache_key] = {"fingerprint": fingerprint, "frame": frame}
            return frame


_report_warehouse = _SuburbReportWarehouse()


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text_value = str(value or "").strip()
        if text_value and text_value.lower() != "nan":
            return text_value
    return ""


def _row_to_snapshot(row: pd.Series | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {
        "street_address": _first_non_empty(row.get("street_address")),
        "suburb": _first_non_empty(row.get("suburb"), row.get("report_suburb")),
        "postcode": _first_non_empty(row.get("postcode")),
        "council_area": _first_non_empty(row.get("council_area")),
        "property_type": _first_non_empty(row.get("property_type")),
        "bedrooms": _parse_int(row.get("bedrooms")),
        "bathrooms": _parse_int(row.get("bathrooms")),
        "car_spaces": _parse_int(row.get("car_spaces")),
        "land_size_sqm": _parse_float(row.get("land_size_sqm")),
        "floor_size_sqm": _parse_float(row.get("floor_size_sqm")),
        "year_built": _parse_int(row.get("year_built")),
        "sale_price": _parse_int(row.get("sale_price")),
        "source_file": _first_non_empty(row.get("source_file")),
        "source_path": _first_non_empty(row.get("source_path")),
    }


def _merge_snapshot_into_lead(lead: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    merged = dict(lead)
    if not snapshot:
        return merged
    for lead_key, snapshot_key in (
        ("postcode", "postcode"),
        ("bedrooms", "bedrooms"),
        ("bathrooms", "bathrooms"),
        ("car_spaces", "car_spaces"),
        ("land_size_sqm", "land_size_sqm"),
        ("floor_size_sqm", "floor_size_sqm"),
        ("year_built", "year_built"),
        ("sale_price", "sale_price"),
    ):
        if not merged.get(lead_key) and snapshot.get(snapshot_key) not in (None, "", 0):
            merged[lead_key] = snapshot[snapshot_key]
    if not merged.get("development_zone") and snapshot.get("council_area"):
        merged["development_zone"] = snapshot["council_area"]
    return merged


def _build_market_context(lead: dict[str, Any], frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "source_file": "",
            "match_confidence": "none",
            "match_type": "none",
            "matched_snapshot": {},
            "suburb_record_count": 0,
            "same_street_count": 0,
            "suburb_median_land_sqm": None,
            "suburb_median_sale_price": None,
            "suburb_median_bedrooms": None,
            "suburb_median_bathrooms": None,
        }

    con = duckdb.connect(database=":memory:")
    con.register("report_rows", frame)
    address_key = _address_key(lead.get("address"), lead.get("suburb"))
    street_key = _street_name(lead.get("address"))
    suburb_key = _normalize_token(lead.get("suburb"))

    exact_rows = con.execute(
        """
        SELECT *
        FROM report_rows
        WHERE address_key = ?
        ORDER BY sale_price DESC NULLS LAST, source_file ASC
        """,
        [address_key],
    ).df()
    street_rows = con.execute(
        """
        SELECT *
        FROM report_rows
        WHERE street_key = ?
        ORDER BY sale_price DESC NULLS LAST, source_file ASC
        """,
        [street_key],
    ).df()
    suburb_stats = con.execute(
        """
        SELECT
            COUNT(*) AS suburb_record_count,
            MEDIAN(land_size_sqm) AS suburb_median_land_sqm,
            MEDIAN(sale_price) AS suburb_median_sale_price,
            MEDIAN(bedrooms) AS suburb_median_bedrooms,
            MEDIAN(bathrooms) AS suburb_median_bathrooms
        FROM report_rows
        WHERE suburb_key = ?
        """,
        [suburb_key],
    ).fetchone()

    property_mix = con.execute(
        """
        SELECT property_type, COUNT(*) AS c
        FROM report_rows
        WHERE suburb_key = ? AND COALESCE(property_type, '') <> ''
        GROUP BY property_type
        ORDER BY c DESC, property_type ASC
        LIMIT 3
        """,
        [suburb_key],
    ).fetchall()

    matched_row = exact_rows.iloc[0] if not exact_rows.empty else street_rows.iloc[0] if not street_rows.empty else None
    match_type = "exact" if not exact_rows.empty else "street" if not street_rows.empty else "suburb_only"
    match_confidence = "high" if match_type == "exact" else "medium" if match_type == "street" else "low"

    context = {
        "source_file": _first_non_empty(matched_row["source_file"]) if matched_row is not None else _first_non_empty(frame.iloc[0]["source_file"]) if not frame.empty else "",
        "match_confidence": match_confidence,
        "match_type": match_type,
        "matched_snapshot": _row_to_snapshot(matched_row),
        "suburb_record_count": int(suburb_stats[0] or 0) if suburb_stats else 0,
        "same_street_count": int(len(street_rows.index)),
        "suburb_median_land_sqm": _parse_float(suburb_stats[1]) if suburb_stats else None,
        "suburb_median_sale_price": _parse_float(suburb_stats[2]) if suburb_stats else None,
        "suburb_median_bedrooms": _parse_float(suburb_stats[3]) if suburb_stats else None,
        "suburb_median_bathrooms": _parse_float(suburb_stats[4]) if suburb_stats else None,
        "property_mix": [{"label": row[0], "count": int(row[1])} for row in property_mix if row[0]],
    }
    con.close()
    return context


async def _build_withdrawn_analysis(session: AsyncSession, lead: dict[str, Any]) -> dict[str, Any]:
    normalized_key = _address_key(lead.get("address"), lead.get("suburb"))
    image_key = Path(str(lead.get("main_image") or "")).name.split("?")[0].lower()
    owner_key = _normalize_token(lead.get("owner_name"))
    suburb_key = _normalize_token(lead.get("suburb"))
    is_withdrawn = "withdrawn" in _normalize_token(lead.get("trigger_type")) or lead.get("signal_status") == "WITHDRAWN"

    result = await session.execute(
        text(
            """
            SELECT id, address, suburb, owner_name, trigger_type, main_image, domain_listing_id
            FROM leads
            WHERE LOWER(COALESCE(suburb, '')) = :suburb
               OR id = :id
            """
        ),
        {"suburb": suburb_key, "id": lead.get("id")},
    )
    rows = [dict(row) for row in result.mappings().all()]

    duplicate_address_records = 0
    same_image_other_addresses = 0
    same_owner_suburb_records = 0
    distinct_listing_ids: set[str] = set()
    for row in rows:
        row_key = _address_key(row.get("address"), row.get("suburb"))
        if row_key == normalized_key:
            duplicate_address_records += 1
            listing_id = str(row.get("domain_listing_id") or "").strip()
            if listing_id:
                distinct_listing_ids.add(listing_id)
        row_image_key = Path(str(row.get("main_image") or "")).name.split("?")[0].lower()
        if image_key and row_image_key == image_key and _address_key(row.get("address"), row.get("suburb")) != normalized_key:
            same_image_other_addresses += 1
        if owner_key and _normalize_token(row.get("owner_name")) == owner_key:
            same_owner_suburb_records += 1

    sold_result = await session.execute(
        text(
            """
            SELECT COUNT(*) AS c
            FROM sold_events
            WHERE LOWER(COALESCE(address, '')) = LOWER(:address)
               OR (LOWER(COALESCE(suburb, '')) = :suburb AND LOWER(COALESCE(address, '')) LIKE :address_like)
            """
        ),
        {
            "address": _core_address(lead.get("address")),
            "suburb": suburb_key,
            "address_like": f"%{_normalize_token(_core_address(lead.get('address')))}%",
        },
    )
    sold_event_matches = int(sold_result.scalar_one() or 0)

    evidence: list[str] = []
    if is_withdrawn:
        evidence.append("A withdrawn-style trigger is already attached to this property.")
    else:
        evidence.append("No withdrawn trigger is attached to this property in the current book.")
    if duplicate_address_records > 1:
        evidence.append(f"{duplicate_address_records} records share the same normalized address in the live book.")
    if len(distinct_listing_ids) > 1:
        evidence.append(f"{len(distinct_listing_ids)} distinct Domain listing IDs were seen against the same address.")
    if same_image_other_addresses:
        evidence.append(f"The current hero image appears on {same_image_other_addresses} other address records, which is a relist/spoof warning.")
    if sold_event_matches:
        evidence.append(f"{sold_event_matches} sold-event match(es) exist for this address, so off-market confidence should be reduced.")
    if same_owner_suburb_records > 1:
        evidence.append(f"{same_owner_suburb_records} records in this suburb share the same owner name.")

    if not is_withdrawn:
        confidence = "not_applicable"
        headline = "No withdrawn listing signal is attached yet."
    elif sold_event_matches:
        confidence = "low"
        headline = "Sold evidence exists, so treat the withdrawn signal as unreliable."
    elif same_image_other_addresses or len(distinct_listing_ids) > 1 or duplicate_address_records > 1:
        confidence = "medium"
        headline = "Withdrawn signal exists, but relist or duplicate risk still needs review."
    else:
        confidence = "high"
        headline = "Withdrawn signal looks clean across current in-house records."

    return {
        "is_withdrawn_signal": is_withdrawn,
        "confidence": confidence,
        "headline": headline,
        "duplicate_address_records": duplicate_address_records,
        "same_image_other_addresses": same_image_other_addresses,
        "same_owner_suburb_records": same_owner_suburb_records,
        "distinct_listing_ids": len(distinct_listing_ids),
        "sold_event_matches": sold_event_matches,
        "evidence": evidence,
    }


def _build_opportunity_insights(lead: dict[str, Any], market_context: dict[str, Any], withdrawn_analysis: dict[str, Any]) -> list[str]:
    insights: list[str] = []
    snapshot = market_context.get("matched_snapshot") or {}
    suburb_count = market_context.get("suburb_record_count") or 0
    if suburb_count:
        insights.append(f"The suburb report contains {suburb_count:,} mapped properties for {lead.get('suburb') or 'this area'}.")
    if market_context.get("match_type") == "exact" and snapshot.get("source_file"):
        insights.append(f"An exact row for this address was found in {snapshot['source_file']}, which gives you a stable property fact base before calling.")
    elif market_context.get("match_type") == "street":
        insights.append("This address does not have a clean exact row yet, but the same street appears in the suburb report and can anchor the call.")
    if snapshot.get("land_size_sqm") and market_context.get("suburb_median_land_sqm"):
        land_size = float(snapshot["land_size_sqm"])
        median_land = float(market_context["suburb_median_land_sqm"])
        if land_size >= median_land * 1.2:
            insights.append(f"The site size ({_format_number(land_size, 'sqm')}) is materially larger than the suburb median ({_format_number(median_land, 'sqm')}).")
        elif land_size <= median_land * 0.8:
            insights.append(f"The site size ({_format_number(land_size, 'sqm')}) is tighter than the suburb median ({_format_number(median_land, 'sqm')}).")
    if lead.get("settlement_date"):
        insights.append(f"The recorded settlement date ({lead['settlement_date'][:10]}) gives you a concrete ownership-timing hook for the opening.")
    elif lead.get("sale_date"):
        insights.append(f"The recorded sale date ({lead['sale_date'][:10]}) gives you a clean ownership-history hook before pitching value.")
    if lead.get("contact_phones"):
        insights.append(f"A direct phone is already on file, so this is a genuine call-first record rather than an enrichment-only lead.")
    if withdrawn_analysis.get("confidence") == "high":
        insights.append("Current in-house evidence does not show obvious relist or sold conflicts against the withdrawn signal.")
    elif withdrawn_analysis.get("confidence") == "medium":
        insights.append("Use the withdrawn story carefully; duplicate or relist risk is present and should be referenced softly, not asserted.")
    property_mix = market_context.get("property_mix") or []
    if property_mix:
        top_mix = ", ".join(f"{item['label']} ({item['count']})" for item in property_mix[:2])
        insights.append(f"The dominant local stock mix in the suburb report is {top_mix}.")
    return insights[:6]


def _build_recommended_email(lead: dict[str, Any], market_context: dict[str, Any], opportunity_insights: list[str]) -> dict[str, str]:
    first_name = (str(lead.get("owner_name") or "").split(" ")[0] or "there").strip()
    address = lead.get("address") or lead.get("suburb") or "your property"
    suburb = lead.get("suburb") or "your area"
    anchor_line = opportunity_insights[0] if opportunity_insights else lead.get("why_now") or f"I have been reviewing current property signals in {suburb}."
    subject = f"Quick property update for {address}"
    body = (
        f"Hi {first_name},\n\n"
        f"I have pulled together a quick view on {address}. {anchor_line}\n\n"
        f"If helpful, I can send you a concise update on where the property sits in the current {suburb} market and what the strongest next move looks like.\n\n"
        f"Regards,\n{PRINCIPAL_NAME}\n{BRAND_NAME}"
    )
    return {
        "recipient": (lead.get("contact_emails") or [""])[0],
        "subject": subject,
        "body": body,
    }


def _build_evidence_panel(lead: dict[str, Any], market_context: dict[str, Any]) -> dict[str, Any]:
    linked_files = [str(item) for item in (lead.get("linked_files") or []) if str(item).strip()]
    source_evidence = [str(item) for item in (lead.get("source_evidence") or []) if str(item).strip()]
    data_sources = []
    
    if market_context.get("source_file"):
        data_sources.append(f"Suburb report: {market_context['source_file']}")
    if lead.get("domain_listing_id"):
        data_sources.append(f"Domain listing: {lead['domain_listing_id']}")
    if lead.get("agency_name"):
        data_sources.append(f"Agency: {lead['agency_name']}")
    if linked_files:
        data_sources.append(f"Linked files: {len(linked_files)}")
    if source_evidence:
        data_sources.append(f"Evidence notes: {len(source_evidence)}")
        
    external_link = lead.get("external_link")
    signal_date = lead.get("date_found")
    
    sig_ts = lead.get("created_at") or lead.get("date_found") or "N/A"
    
    return {
        "data_sources": data_sources,
        "linked_files": linked_files[:8],
        "source_evidence": source_evidence[:8],
        "external_link": external_link,
        "signal_date": signal_date,
        "freshness_note": f"Intelligence refreshed: {format_sydney(now_sydney())} · Signal detected: {sig_ts}",
    }


async def get_property_terminal(session: AsyncSession, lead_id: str) -> dict[str, Any]:
    result = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead = _hydrate_lead(row)
    frame = _report_warehouse.get_suburb_frame(str(lead.get("suburb") or ""))
    market_context = _build_market_context(lead, frame)
    merged_lead = _merge_snapshot_into_lead(lead, market_context.get("matched_snapshot") or {})
    withdrawn_analysis = await _build_withdrawn_analysis(session, merged_lead)
    opportunity_insights = _build_opportunity_insights(merged_lead, market_context, withdrawn_analysis)
    evidence_panel = _build_evidence_panel(merged_lead, market_context)
    recommended_email = _build_recommended_email(merged_lead, market_context, opportunity_insights)

    return {
        "lead": merged_lead,
        "terminal": {
            "market_context": {
                **market_context,
                "suburb_median_land_label": _format_number(market_context.get("suburb_median_land_sqm"), "sqm"),
                "suburb_median_sale_label": _format_currency(market_context.get("suburb_median_sale_price")),
                "suburb_median_bedrooms_label": _format_number(market_context.get("suburb_median_bedrooms")),
                "suburb_median_bathrooms_label": _format_number(market_context.get("suburb_median_bathrooms")),
            },
            "withdrawn_analysis": withdrawn_analysis,
            "opportunity_insights": opportunity_insights,
            "recommended_email": recommended_email,
            "evidence_panel": evidence_panel,
        },
    }
