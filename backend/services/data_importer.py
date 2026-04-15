"""
Local data importer â€” reads Cotality xlsx reports and marketing CSVs from
D:\\L+S Stock\\Suburb reports directly from disk. No file upload needed.

Call:
    from services.data_importer import run_local_import
    result = await run_local_import(db_session)
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import os
import uuid
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import (
    MORTGAGE_CLIFF_MIN_YEARS, MORTGAGE_CLIFF_MAX_YEARS,
    QUEUE_RE, QUEUE_MORTGAGE, SYDNEY_TZ,
)
from core.utils import now_iso
from services.cadastral_identity import build_storage_address, extract_lot_plan, is_subdivision_signal
from services.audit_log_service import write_lead_audit_log

logger = logging.getLogger(__name__)

STOCK_ROOT = Path(os.getenv("STOCK_ROOT", r"D:\L+S Stock"))
SUBURB_REPORTS_DIR = STOCK_ROOT / "Suburb reports"
MARKETING_REPORT_DIR = SUBURB_REPORTS_DIR / "Marketing report"
_MISSING_MARKERS = {"", "-", "N/A", "n/a", "na", "None", "none", "null", "NULL"}
_XLSX_NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _clean_optional_text(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return None if text in _MISSING_MARKERS else text


def _clean_dict(values: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    for key, value in values.items():
        if value in (None, "", [], {}):
            continue
        if isinstance(value, str):
            text = value.strip()
            if not text or text in _MISSING_MARKERS:
                continue
            cleaned[key] = text
            continue
        cleaned[key] = value
    return cleaned


def _safe_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
            return loaded if isinstance(loaded, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _dedupe_provenance(entries: List[Any]) -> List[Any]:
    deduped: List[Any] = []
    seen: set[str] = set()
    for entry in entries:
        normalized = entry if isinstance(entry, dict) else {"value": str(entry)}
        fingerprint = json.dumps(normalized, sort_keys=True, default=str)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        deduped.append(normalized)
    return deduped


def _parse_float(value: Any) -> Optional[float]:
    raw = _clean_optional_text(value)
    if raw is None:
        return None
    try:
        return float(raw.replace(",", ""))
    except (TypeError, ValueError):
        return None


def _normalize_gender(value: Any) -> Optional[str]:
    raw = _clean_optional_text(value)
    if raw is None:
        return None
    lowered = raw.lower()
    if lowered.startswith("m"):
        return "male"
    if lowered.startswith("f"):
        return "female"
    return lowered


def _split_name_parts(name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    cleaned = _clean_optional_text(name)
    if not cleaned:
        return None, None
    parts = [part for part in cleaned.split() if part]
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])

# â”€â”€â”€ Phone normalisation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _normalise_phone(raw: str) -> Optional[str]:
    """Convert bare 9-digit AU mobiles to 0XXXXXXXXX format."""
    if not raw or raw.strip() in ("-", "", "N/A", "n/a"):
        return None
    digits = "".join(c for c in raw if c.isdigit())
    if not digits:
        return None
    if len(digits) == 9 and digits[0] in ("4", "2", "3", "7", "8"):
        digits = "0" + digits
    if len(digits) == 10 and digits.startswith("0"):
        return digits
    if len(digits) == 11 and digits.startswith("61"):
        return "0" + digits[2:]
    return digits  # return whatever we have â€” better than nothing


# â”€â”€â”€ Mortgage cliff detection â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _mortgage_cliff_queue(settlement_date_str: Optional[str]) -> Optional[str]:
    """
    Return QUEUE_MORTGAGE if settlement date falls in the cliff window
    (MORTGAGE_CLIFF_MIN_YEARS â€“ MORTGAGE_CLIFF_MAX_YEARS ago), else None.
    """
    if not settlement_date_str:
        return None
    today = date.today()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%-d/%-m/%Y",
                "%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y"):
        try:
            sd = datetime.strptime(settlement_date_str.strip(), fmt).date()
            age_years = (today - sd).days / 365.25
            if MORTGAGE_CLIFF_MIN_YEARS <= age_years <= MORTGAGE_CLIFF_MAX_YEARS:
                return QUEUE_MORTGAGE
            return None
        except ValueError:
            continue
    return None


# â”€â”€â”€ Upsert helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def _upsert_lead(db: AsyncSession, data: Dict[str, Any], *, batch_id: str) -> str:
    """
    Insert or update a lead row. Uses address as unique key.
    Returns 'inserted' | 'updated' | 'skipped'.
    """
    canonical_address = (data.get("canonical_address") or data.get("address") or "").strip()
    address = (data.get("address") or "").strip()
    if not address:
        return "skipped"

    lead_id = data.get("id") or str(uuid.uuid4())
    now = now_iso()

    phones = _safe_json_list(data.get("contact_phones"))
    phones = [p for p in phones if p]
    source_provenance = _safe_json_list(data.get("source_provenance"))
    existing = (
        await db.execute(text("SELECT * FROM leads WHERE address = :address"), {"address": address})
    ).mappings().first()
    if existing:
        existing_provenance = _safe_json_list(existing.get("source_provenance"))
        source_provenance = _dedupe_provenance([*existing_provenance, *source_provenance])
    else:
        source_provenance = _dedupe_provenance(source_provenance)

    try:
        await db.execute(
            text("""
                INSERT INTO leads (
                    id, address, canonical_address, suburb, postcode, owner_name,
                    contact_phones, bedrooms, bathrooms, car_spaces,
                    land_size_sqm, floor_size_sqm, year_built, property_type, state,
                    sale_price, sale_date, settlement_date, last_settlement_date,
                    agency_name, agent_name, owner_type, land_use,
                    development_zone, parcel_details, parcel_lot, parcel_plan,
                    owner_first_name, owner_last_name,
                    do_not_call, source_provenance,
                    trigger_type, lead_archetype, route_queue, queue_bucket,
                    contactability_status, status,
                    source_tags, activity_log, stage_note,
                    heat_score, evidence_score,
                    created_at, updated_at
                ) VALUES (
                    :id, :address, :canonical_address, :suburb, :postcode, :owner_name,
                    :phones, :bedrooms, :bathrooms, :car_spaces,
                    :land_size_sqm, :floor_size_sqm, :year_built, :property_type, :state,
                    :sale_price, :sale_date, :settlement_date, :settlement_date,
                    :agency_name, :agent_name, :owner_type, :land_use,
                    :development_zone, :parcel_details, :parcel_lot, :parcel_plan,
                    :owner_first_name, :owner_last_name,
                    :do_not_call, :source_provenance,
                    :trigger_type, :lead_archetype, :route_queue, :route_queue,
                    :contactability_status, 'captured',
                    :source_tags, :activity_log, :stage_note,
                    :heat_score, :evidence_score,
                    :now, :now
                )
                ON CONFLICT (address) DO UPDATE SET
                    owner_name = COALESCE(EXCLUDED.owner_name, leads.owner_name),
                    contact_phones = CASE
                        WHEN leads.contact_phones IS NULL OR leads.contact_phones = '[]' OR leads.contact_phones = ''
                        THEN EXCLUDED.contact_phones
                        ELSE leads.contact_phones
                    END,
                    settlement_date      = COALESCE(EXCLUDED.settlement_date, leads.settlement_date),
                    last_settlement_date = COALESCE(EXCLUDED.settlement_date, leads.last_settlement_date),
                    sale_date            = COALESCE(EXCLUDED.sale_date, leads.sale_date),
                    sale_price           = COALESCE(EXCLUDED.sale_price, leads.sale_price),
                    property_type        = COALESCE(EXCLUDED.property_type, leads.property_type),
                    floor_size_sqm       = COALESCE(EXCLUDED.floor_size_sqm, leads.floor_size_sqm),
                    state                = COALESCE(EXCLUDED.state, leads.state),
                    owner_first_name     = COALESCE(EXCLUDED.owner_first_name, leads.owner_first_name),
                    owner_last_name      = COALESCE(EXCLUDED.owner_last_name, leads.owner_last_name),
                    do_not_call          = COALESCE(leads.do_not_call, 0) OR COALESCE(EXCLUDED.do_not_call, 0),
                    source_provenance    = EXCLUDED.source_provenance,
                    route_queue          = CASE WHEN EXCLUDED.route_queue IS NOT NULL AND EXCLUDED.route_queue != '' THEN EXCLUDED.route_queue ELSE leads.route_queue END,
                    queue_bucket         = CASE WHEN EXCLUDED.queue_bucket IS NOT NULL AND EXCLUDED.queue_bucket != '' THEN EXCLUDED.queue_bucket ELSE leads.queue_bucket END,
                    lead_archetype       = CASE WHEN EXCLUDED.lead_archetype IS NOT NULL AND EXCLUDED.lead_archetype != '' THEN EXCLUDED.lead_archetype ELSE leads.lead_archetype END,
                    parcel_lot           = COALESCE(EXCLUDED.parcel_lot, leads.parcel_lot),
                    parcel_plan          = COALESCE(EXCLUDED.parcel_plan, leads.parcel_plan),
                    heat_score           = CASE WHEN EXCLUDED.heat_score > leads.heat_score THEN EXCLUDED.heat_score ELSE leads.heat_score END,
                    updated_at           = :now
            """),
            {
                "id": lead_id,
                "address": address,
                "canonical_address": canonical_address,
                "suburb": data.get("suburb"),
                "postcode": data.get("postcode"),
                "owner_name": data.get("owner_name"),
                "phones": json.dumps(phones),
                "bedrooms": data.get("bedrooms"),
                "bathrooms": data.get("bathrooms"),
                "car_spaces": data.get("car_spaces"),
                "land_size_sqm": data.get("land_size_sqm"),
                "floor_size_sqm": data.get("floor_size_sqm"),
                "year_built": _clean_optional_text(data.get("year_built")),
                "property_type": _clean_optional_text(data.get("property_type")),
                "state": _clean_optional_text(data.get("state")),
                "sale_price": _clean_optional_text(data.get("sale_price")),
                "sale_date": _clean_optional_text(data.get("sale_date")),
                "settlement_date": _clean_optional_text(data.get("settlement_date")),
                "agency_name": _clean_optional_text(data.get("agency_name")),
                "agent_name": _clean_optional_text(data.get("agent_name")),
                "owner_type": _clean_optional_text(data.get("owner_type")),
                "land_use": _clean_optional_text(data.get("land_use")),
                "development_zone": _clean_optional_text(data.get("development_zone")),
                "parcel_details": _clean_optional_text(data.get("parcel_details")),
                "parcel_lot": _clean_optional_text(data.get("parcel_lot")),
                "parcel_plan": _clean_optional_text(data.get("parcel_plan")),
                "owner_first_name": _clean_optional_text(data.get("owner_first_name")),
                "owner_last_name": _clean_optional_text(data.get("owner_last_name")),
                "do_not_call": bool(data.get("do_not_call")),
                "source_provenance": json.dumps(source_provenance),
                "trigger_type": data.get("trigger_type", "cotality_import"),
                "lead_archetype": data.get("lead_archetype", ""),
                "route_queue": data.get("route_queue", QUEUE_RE),
                "contactability_status": data.get("contactability_status", ""),
                "source_tags": json.dumps(data.get("source_tags", ["cotality"])),
                "activity_log": json.dumps(data.get("activity_log", [])),
                "stage_note": data.get("stage_note"),
                "heat_score": data.get("heat_score", 10),
                "evidence_score": data.get("evidence_score", 5),
                "now": now,
            }
        )
        post = (await db.execute(text("SELECT * FROM leads WHERE address = :address"), {"address": address})).mappings().first()
        if post:
            await write_lead_audit_log(
                db,
                lead_id=str(post.get("id") or lead_id),
                action="upsert",
                source=str(data.get("trigger_type") or "import"),
                actor="importer",
                batch_id=batch_id,
                before_state=dict(existing) if existing else {},
                after_state=dict(post),
                payload={
                    "address": address,
                    "canonical_address": canonical_address,
                    "source_tags": data.get("source_tags", []),
                },
            )
        return "inserted"
    except Exception as e:
        logger.warning(f"[Import] Upsert failed for '{address}': {e}")
        return "skipped"


# â”€â”€â”€ Marketing CSV importer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _parse_marketing_csv(filepath: Path) -> List[Dict[str, Any]]:
    """
    Parse a Cotality marketing CSV (First Name, Surname, Street Address,
    Suburb, Phone, On Do Not Mail Register, Gender, Call back notes, Feedback/Summary).
    Skips the 2 metadata header rows.
    """
    records = []
    with open(filepath, encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        rows = list(reader)

    # Find the data header row (contains "First Name")
    header_idx = None
    for i, row in enumerate(rows):
        if row and row[0].strip() == "First Name":
            header_idx = i
            break
    if header_idx is None:
        logger.warning(f"[Marketing CSV] Header not found in {filepath.name}")
        return records

    headers = rows[header_idx]
    report_scope = rows[0][0].strip() if rows and rows[0] and rows[0][0].strip() else None
    for row in rows[header_idx + 1:]:
        if not row or not any(c.strip() for c in row):
            continue

        def g(col):
            try:
                idx = headers.index(col)
                return row[idx].strip() if idx < len(row) else ""
            except ValueError:
                return ""

        first = g("First Name")
        last = g("Surname")
        if not first and not last:
            continue

        full_name = f"{first} {last}".strip()
        street = g("Street Address").upper()
        suburb = g("Suburb").title()
        raw_phone = g("Phone")
        dnc = g("On Do Not Mail Register").lower() == "yes"
        gender = _normalize_gender(g("Gender"))
        notes_parts = [n for n in [g("Call back notes"), g("Feedback/Summary")] if n and n not in ("-", "")]
        stage_note = " | ".join(notes_parts) if notes_parts else None

        phone = _normalise_phone(raw_phone)

        # Build full address
        address = f"{street}, {suburb} NSW"

        row_data = {headers[idx]: (row[idx].strip() if idx < len(row) else "") for idx in range(len(headers))}
        notes_fields = _clean_dict(
            {
                "call_back_notes": row_data.get("Call back notes"),
                "feedback_summary": row_data.get("Feedback/Summary"),
            }
        )
        records.append({
            "owner_name": full_name,
            "address": address,
            "suburb": suburb,
            "contact_phones": [phone] if phone else [],
            "contactability_status": "dnc" if dnc else ("needs_enrichment" if not phone else "ready"),
            "trigger_type": "marketing_list",
            "lead_archetype": "default",
            "route_queue": QUEUE_RE,
            "do_not_call": dnc,
            "source_tags": ["marketing_csv", "cotality"],
            "source_provenance": [
                {
                    "source_type": "marketing_csv",
                    "file": filepath.name,
                    "report_scope": report_scope,
                    "gender": gender,
                    "on_do_not_mail_register": dnc,
                    **notes_fields,
                    "raw_columns": _clean_dict(row_data),
                }
            ],
            "stage_note": stage_note,
            "heat_score": 5,  # cold list â€” low initial score
            "evidence_score": 3,
            "activity_log": [
                {"ts": now_iso(), "action": f"Imported from {filepath.name}",
                 "note": stage_note or ""}
            ] if stage_note else [],
        })
    return records


# â”€â”€â”€ Cotality xlsx importer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _xlsx_col_key(cell_ref: str) -> str:
    letters = []
    for ch in cell_ref:
        if ch.isalpha():
            letters.append(ch)
        else:
            break
    return "".join(letters)


def _xlsx_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out: List[str] = []
    for si in root.findall("x:si", _XLSX_NS):
        text = "".join(t.text or "" for t in si.findall(".//x:t", _XLSX_NS))
        out.append(text)
    return out


def _xlsx_first_sheet_path(zf: zipfile.ZipFile) -> Optional[str]:
    wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
    rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib.get("Id"): rel.attrib.get("Target")
        for rel in rels_root.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship")
    }
    first_sheet = wb_root.find("x:sheets/x:sheet", _XLSX_NS)
    if first_sheet is None:
        return None
    rid = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
    target = rel_map.get(rid)
    if not target:
        return None
    return "xl/" + str(target).replace("\\", "/").lstrip("/")


def _xlsx_cell_text(cell, shared: List[str]) -> str:
    inline = cell.find("x:is", _XLSX_NS)
    if inline is not None:
        return "".join(t.text or "" for t in inline.findall(".//x:t", _XLSX_NS)).strip()
    value_el = cell.find("x:v", _XLSX_NS)
    if value_el is None:
        return ""
    raw = (value_el.text or "").strip()
    if cell.attrib.get("t") == "s":
        try:
            return str(shared[int(raw)]).strip()
        except Exception:
            return raw
    return raw


def _load_xlsx_rows_without_openpyxl(filepath: Path) -> List[List[str]]:
    with zipfile.ZipFile(filepath, "r") as zf:
        shared = _xlsx_shared_strings(zf)
        sheet_path = _xlsx_first_sheet_path(zf)
        if not sheet_path:
            return []
        root = ET.fromstring(zf.read(sheet_path))
    rows: List[List[str]] = []
    for row in root.findall(".//x:sheetData/x:row", _XLSX_NS):
        cells: Dict[str, str] = {}
        max_len = 0
        for cell in row.findall("x:c", _XLSX_NS):
            key = _xlsx_col_key(cell.attrib.get("r", ""))
            if not key:
                continue
            text_value = _xlsx_cell_text(cell, shared)
            if text_value:
                cells[key] = text_value
            if key:
                col_index = 0
                for ch in key:
                    col_index = col_index * 26 + (ord(ch.upper()) - 64)
                max_len = max(max_len, col_index)
        if max_len == 0:
            rows.append([])
            continue
        output = [""] * max_len
        for key, value in cells.items():
            col_index = 0
            for ch in key:
                col_index = col_index * 26 + (ord(ch.upper()) - 64)
            output[col_index - 1] = value
        rows.append(output)
    return rows


def _parse_cotality_xlsx(filepath: Path) -> List[Dict[str, Any]]:
    """
    Parse a Cotality property report xlsx (RPData/CoreLogic format).
    Row 1: search string metadata, Row 2: blank, Row 3: column headers, Row 4+: data.
    """
    records = []
    rows: List[List[Any]] = []
    try:
        import openpyxl
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
    except Exception:
        rows = _load_xlsx_rows_without_openpyxl(filepath)

    if len(rows) < 3:
        return records

    headers = [str(h).strip() if h else "" for h in rows[2]]

    def col(row, *names):
        for name in names:
            try:
                idx = headers.index(name)
                v = row[idx]
                return str(v).strip() if v is not None else ""
            except (ValueError, IndexError):
                continue
        return ""

    for row in rows[3:]:
        if not row or not any(c for c in row):
            continue

        street = col(row, "Street Address").upper()
        suburb = col(row, "Suburb").title()
        state = _clean_optional_text(col(row, "State")) or "NSW"
        postcode = _clean_optional_text(col(row, "Postcode"))
        if not street or not suburb:
            continue

        address = f"{street}, {suburb} NSW" + (f" {postcode}" if postcode else "")

        owner1 = (col(row, "Owner 1 Name") or "").title()
        owner2 = (col(row, "Owner 2 Name") or "").title()
        owner3 = (col(row, "Owner 3 Name") or "").title()
        owner_name = owner1 or owner2 or owner3 or None

        settlement_date = _clean_optional_text(col(row, "Settlement Date"))
        sale_date = _clean_optional_text(col(row, "Sale Date"))
        sale_price = _clean_optional_text(col(row, "Sale Price"))

        beds = _parse_float(col(row, "Bed"))
        baths = _parse_float(col(row, "Bath"))
        cars = _parse_float(col(row, "Car"))
        land = _parse_float(col(row, "Land Size (m²)", "Land Size (mÂ²)", "Land Size (m?)", "Land Size"))
        floor = _parse_float(col(row, "Floor Size (m²)", "Floor Size (mÂ²)", "Floor Size (m?)", "Floor Size"))
        property_type = _clean_optional_text(col(row, "Property Type"))
        owner_first_name, owner_last_name = _split_name_parts(owner1)

        vendor_names = [
            name
            for name in [
                _clean_optional_text(col(row, "Vendor 1 Name")),
                _clean_optional_text(col(row, "Vendor 2 Name")),
                _clean_optional_text(col(row, "Vendor 3 Name")),
            ]
            if name
        ]
        raw_row = _clean_dict({header: (str(row[idx]).strip() if idx < len(row) and row[idx] is not None else "") for idx, header in enumerate(headers) if header})
        source_provenance = [
            {
                "source_type": "cotality_xlsx",
                "file": filepath.name,
                "council_area": _clean_optional_text(col(row, "Council Area")),
                "sale_type": _clean_optional_text(col(row, "Sale Type")),
                "vendor_names": vendor_names,
                "extra_owner_names": [name for name in [_clean_optional_text(owner3)] if name],
                "open_in_rpdata": _clean_optional_text(col(row, "Open in RPData")),
                "property_photo": _clean_optional_text(col(row, "Property Photo")),
                "raw_columns": raw_row,
            }
        ]

        cliff_queue = _mortgage_cliff_queue(settlement_date)
        route_queue = cliff_queue or QUEUE_RE
        archetype = "mortgage_cliff" if cliff_queue else "default"
        heat = 60 if cliff_queue else 20
        evidence = 40 if cliff_queue else 10
        parcel_details = _clean_optional_text(col(row, "Parcel Details"))
        parcel_lot, parcel_plan = extract_lot_plan(parcel_details or "", address)
        subdivision = is_subdivision_signal(_clean_optional_text(col(row, "Development Zone")) or "", "cotality_import")
        # If Lot/DP exists, always persist a lot-specific storage key to avoid parent-lot clobbering.
        storage_address = build_storage_address(
            address,
            parcel_lot,
            parcel_plan,
            subdivision=(subdivision or bool(parcel_lot and parcel_plan)),
        )

        records.append(
            {
                "address": storage_address,
                "canonical_address": address,
                "suburb": suburb,
                "postcode": postcode or None,
                "owner_name": owner_name,
                "contact_phones": [],
                "contactability_status": "needs_enrichment",
                "bedrooms": beds,
                "bathrooms": baths,
                "car_spaces": cars,
                "land_size_sqm": land,
                "floor_size_sqm": floor,
                "property_type": property_type,
                "state": state,
                "owner_first_name": owner_first_name,
                "owner_last_name": owner_last_name,
                "year_built": _clean_optional_text(col(row, "Year Built")),
                "sale_price": sale_price,
                "sale_date": sale_date,
                "settlement_date": settlement_date,
                "agency_name": _clean_optional_text(col(row, "Agency")),
                "agent_name": _clean_optional_text(col(row, "Agent")),
                "owner_type": _clean_optional_text(col(row, "Owner Type")),
                "land_use": _clean_optional_text(col(row, "Land Use")),
                "development_zone": _clean_optional_text(col(row, "Development Zone")),
                "parcel_details": parcel_details,
                "parcel_lot": parcel_lot,
                "parcel_plan": parcel_plan,
                "trigger_type": "cotality_import",
                "lead_archetype": archetype,
                "route_queue": route_queue,
                "source_tags": ["cotality_xlsx", filepath.stem.lower().replace(" ", "_")],
                "source_provenance": source_provenance,
                "heat_score": heat,
                "evidence_score": evidence,
            }
        )

    return records

# â”€â”€â”€ Main orchestrator â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def run_local_import(db: AsyncSession) -> Dict[str, Any]:
    """
    Scan D:\\L+S Stock\\Suburb reports for all xlsx and CSV files,
    parse them, and upsert into the leads table.
    Returns a summary dict.
    """
    if not SUBURB_REPORTS_DIR.exists():
        return {"error": f"Directory not found: {SUBURB_REPORTS_DIR}"}

    total_inserted = 0
    total_skipped = 0
    batch_id = str(uuid.uuid4())
    files_processed = []

    # 1. Marketing CSVs
    if MARKETING_REPORT_DIR.exists():
        for csv_file in sorted(MARKETING_REPORT_DIR.glob("*.csv")):
            logger.info(f"[Import] Parsing marketing CSV: {csv_file.name}")
            # Offload heavy CSV parsing so FastAPI event loop stays responsive.
            records = await asyncio.to_thread(_parse_marketing_csv, csv_file)
            file_inserted = 0
            for rec in records:
                result = await _upsert_lead(db, rec, batch_id=batch_id)
                if result == "inserted":
                    file_inserted += 1
                    total_inserted += 1
                else:
                    total_skipped += 1
            await db.commit()
            files_processed.append({"file": csv_file.name, "type": "marketing_csv", "leads": file_inserted})
            logger.info(f"[Import] {csv_file.name}: {file_inserted} leads upserted")

    # 2. Cotality xlsx reports
    for xlsx_file in sorted(SUBURB_REPORTS_DIR.glob("*.xlsx")):
        logger.info(f"[Import] Parsing Cotality xlsx: {xlsx_file.name}")
        # Offload heavy XLSX parsing/XML decode to a worker thread.
        records = await asyncio.to_thread(_parse_cotality_xlsx, xlsx_file)
        file_inserted = 0
        for rec in records:
            result = await _upsert_lead(db, rec, batch_id=batch_id)
            if result == "inserted":
                file_inserted += 1
                total_inserted += 1
            else:
                total_skipped += 1
        await db.commit()
        files_processed.append({"file": xlsx_file.name, "type": "cotality_xlsx", "leads": file_inserted})
        logger.info(f"[Import] {xlsx_file.name}: {file_inserted} leads upserted")

    return {
        "status": "complete",
        "batch_id": batch_id,
        "total_inserted_or_updated": total_inserted,
        "total_skipped": total_skipped,
        "files": files_processed,
    }

