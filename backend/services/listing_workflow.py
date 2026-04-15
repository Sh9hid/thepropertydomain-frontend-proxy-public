import hashlib
import json
import math
import mimetypes
import re
import shutil
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Template
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import (
    BRAND_LOGO_URL,
    BRAND_NAME,
    GENERATED_REPORTS_ROOT,
    PRINCIPAL_EMAIL,
    PRINCIPAL_NAME,
    PRINCIPAL_PHONE,
    PROJECT_ROOT,
)
from core.logic import _append_activity, _build_activity_entry
from core.utils import _parse_json_list, format_sydney, now_iso, now_sydney
from pdf_generator import html_to_pdf

FTR32_GUIDE_URL = "https://www.fairtrading.nsw.gov.au/__data/assets/pdf_file/0009/1015569/FTR32-Agency-agreements-for-residential-property-guide.pdf"
LISTING_REPORT_ROOT = GENERATED_REPORTS_ROOT / "listing_workflows"
REQUIRED_SEND_DOCUMENT_KINDS = ["marketing_schedule", "material_facts_annexure", "consumer_fact_sheet"]
OPTIONAL_DOCUMENT_KINDS = ["cos", "section_66w", "authority_pack", "signed_authority_pack", "other"]
ALL_DOCUMENT_KINDS = REQUIRED_SEND_DOCUMENT_KINDS + OPTIONAL_DOCUMENT_KINDS

AUTHORITY_PACK_TEMPLATE = Template(
    """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body { font-family: Helvetica, Arial, sans-serif; color: #171717; margin: 0; padding: 28px; }
    .header { display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 2px solid #E21937; padding-bottom: 14px; margin-bottom: 22px; }
    .brand { display: flex; flex-direction: column; gap: 4px; }
    .title { font-size: 24px; font-weight: 800; color: #E21937; }
    .sub { font-size: 11px; color: #666; }
    .logo { max-height: 52px; max-width: 180px; }
    .section { margin-bottom: 18px; page-break-inside: avoid; }
    .section h2 { font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: #555; margin: 0 0 8px; }
    .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
    .card { border: 1px solid #e7e7e7; border-radius: 8px; padding: 12px; background: #fafafa; }
    .metric { font-size: 10px; color: #666; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 4px; }
    .value { font-size: 14px; font-weight: 700; color: #111; line-height: 1.4; }
    .list { margin: 0; padding-left: 18px; line-height: 1.5; }
    .muted { color: #666; font-size: 11px; line-height: 1.5; }
    .comps { width: 100%; border-collapse: collapse; }
    .comps th, .comps td { border-bottom: 1px solid #ececec; padding: 8px 6px; text-align: left; font-size: 11px; }
    .comps th { color: #666; text-transform: uppercase; letter-spacing: 0.06em; font-size: 9px; }
    .pill { display: inline-block; padding: 5px 10px; border-radius: 999px; background: rgba(226,25,55,0.08); color: #E21937; font-size: 10px; font-weight: 700; }
    .signature { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 20px; margin-top: 26px; }
    .sigbox { border-top: 1px solid #171717; padding-top: 6px; min-height: 36px; font-size: 11px; }
    .footer { margin-top: 20px; border-top: 1px solid #efefef; padding-top: 10px; font-size: 10px; color: #777; }
  </style>
</head>
<body>
  <div class="header">
    <div class="brand">
      <div class="title">{{ authority_heading }}</div>
      <div class="sub">{{ brand_name }} | {{ address }}</div>
      <div class="sub">Prepared {{ generated_at }} Sydney time</div>
    </div>
    {% if logo_url %}
    <img class="logo" src="{{ logo_url }}" alt="{{ brand_name }}">
    {% endif %}
  </div>

  <div class="section">
    <h2>Property</h2>
    <div class="grid">
      <div class="card">
        <div class="metric">Vendor</div>
        <div class="value">{{ owner_name }}</div>
      </div>
      <div class="card">
        <div class="metric">Authority Type</div>
        <div class="value">{{ authority_type_label }}</div>
      </div>
      <div class="card">
        <div class="metric">Price Guidance</div>
        <div class="value">${{ estimate_low }} - ${{ estimate_high }}</div>
      </div>
      <div class="card">
        <div class="metric">Inspector</div>
        <div class="value">{{ inspected_by }} | {{ inspection_at }}</div>
      </div>
    </div>
  </div>

  <div class="section">
    <h2>Inspection Notes</h2>
    <div class="card">
      <div class="value">{{ inspection_summary }}</div>
      {% if inspection_notes %}
      <p class="muted">{{ inspection_notes }}</p>
      {% endif %}
    </div>
  </div>

  <div class="section">
    <h2>Price Evidence Pack</h2>
    <div class="card">
      <div class="pill">Human approved by {{ approved_by }} at {{ approved_at }}</div>
      {% if rationale %}
      <p class="muted">{{ rationale }}</p>
      {% endif %}
      <table class="comps">
        <thead>
          <tr>
            <th>Comparable</th>
            <th>Sale Date</th>
            <th>Sale Price</th>
            <th>Source</th>
          </tr>
        </thead>
        <tbody>
          {% for comp in comparables %}
          <tr>
            <td>{{ comp.address }}</td>
            <td>{{ comp.sale_date or "-" }}</td>
            <td>{% if comp.sale_price %}${{ "{:,}".format(comp.sale_price) }}{% else %}-{% endif %}</td>
            <td>{{ comp.source }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>Included Documents</h2>
    <ul class="list">
      <li>NSW Fair Trading residential agency agreement guide (FTR32): {{ fair_trading_guide }}</li>
      {% for item in included_documents %}
      <li>{{ item.label }} ({{ item.kind }})</li>
      {% endfor %}
    </ul>
  </div>

  <div class="section">
    <h2>Compliance Status</h2>
    <ul class="list">
      <li>Preliminary inspection completed before authority send.</li>
      <li>Price guidance drafted from local data and approved before send.</li>
      <li>Lawyer-approved annexure set marked approved for live use.</li>
      <li>Contract for Sale must be uploaded before market-ready status is applied.</li>
    </ul>
  </div>

  <div class="signature">
    <div class="sigbox">Vendor signature{% if signed_vendor %}<br>{{ signed_vendor }}{% endif %}</div>
    <div class="sigbox">Agent signature<br>{{ agent_name }}</div>
  </div>

  <div class="footer">
    {{ brand_name }} | {{ agent_name }} | {{ agent_phone }} | {{ agent_email }} | Lead {{ lead_id }}
  </div>
</body>
</html>
"""
)


def _ensure_root() -> None:
    LISTING_REPORT_ROOT.mkdir(parents=True, exist_ok=True)


def _listing_dir(lead_id: str) -> Path:
    root = LISTING_REPORT_ROOT / f"lead_{lead_id}"
    (root / "uploads").mkdir(parents=True, exist_ok=True)
    (root / "generated").mkdir(parents=True, exist_ok=True)
    return root


def _safe_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name or "document")
    cleaned = cleaned.strip("._") or "document"
    return cleaned[:120]


def _to_download_url(relative_path: str) -> str:
    relative = relative_path.replace("\\", "/").lstrip("/")
    return f"/api/forms/download/{relative}"


def _file_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(row)
    payload["generated"] = bool(payload.get("generated"))
    payload["download_url"] = _to_download_url(payload.get("relative_path") or "")
    return payload


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    raw = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%d/%m/%Y", "%d/%m/%Y %H:%M", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _parse_money(value: Any) -> Optional[int]:
    if value in (None, "", "-", "N/A", "n/a"):
        return None
    digits = re.sub(r"[^\d.]", "", str(value))
    if not digits:
        return None
    try:
        return int(float(digits))
    except ValueError:
        return None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
    if not all([lat1, lon1, lat2, lon2]):
        return None
    radius = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return radius * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


async def _append_lead_activity(session: AsyncSession, lead_id: str, activity_type: str, note: str, channel: str = "listing") -> None:
    lead_row = (await session.execute(text("SELECT activity_log FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    if not lead_row:
        return
    activity_log = _append_activity(
        lead_row.get("activity_log"),
        _build_activity_entry(activity_type, note, "listing_workflow", channel),
    )
    await session.execute(
        text("UPDATE leads SET activity_log = :log, updated_at = :now WHERE id = :id"),
        {"log": json.dumps(activity_log), "now": now_iso(), "id": lead_id},
    )


async def ensure_listing_workflow(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    existing = (
        await session.execute(text("SELECT * FROM listing_workflows WHERE lead_id = :lead_id"), {"lead_id": lead_id})
    ).mappings().first()
    if existing:
        return dict(existing)
    now = now_iso()
    await session.execute(
        text(
            """
            INSERT INTO listing_workflows (
                lead_id, authority_type, stage, inspection_required, inspection_complete,
                price_guidance_required, price_guidance_status, authority_pack_status, market_ready,
                lawyer_signoff_status, marketing_payment_status, workflow_notes, created_at, updated_at
            ) VALUES (
                :lead_id, 'exclusive', 'documents', 1, 0,
                1, 'draft_missing', 'draft_missing', 0,
                'pending', 'not_requested', '', :created_at, :updated_at
            )
            """
        ),
        {"lead_id": lead_id, "created_at": now, "updated_at": now},
    )
    await session.commit()
    created = (
        await session.execute(text("SELECT * FROM listing_workflows WHERE lead_id = :lead_id"), {"lead_id": lead_id})
    ).mappings().first()
    return dict(created or {})


async def get_listing_documents(session: AsyncSession, lead_id: str) -> List[Dict[str, Any]]:
    rows = (
        await session.execute(
            text(
                """
                SELECT * FROM listing_documents
                WHERE lead_id = :lead_id
                ORDER BY kind ASC, version DESC, created_at DESC
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()
    return [_file_payload(dict(row)) for row in rows]


async def _latest_document_by_kind(session: AsyncSession, lead_id: str) -> Dict[str, Dict[str, Any]]:
    docs = await get_listing_documents(session, lead_id)
    latest: Dict[str, Dict[str, Any]] = {}
    for doc in docs:
        latest.setdefault(doc["kind"], doc)
    return latest


async def _latest_price_guidance_rows(session: AsyncSession, lead_id: str) -> List[Dict[str, Any]]:
    rows = (
        await session.execute(
            text(
                """
                SELECT * FROM price_guidance_logs
                WHERE lead_id = :lead_id
                ORDER BY created_at DESC, version DESC
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()
    payloads: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["comparables"] = _parse_json_list(item.get("comparables"))
        payloads.append(item)
    return payloads


async def _latest_offer_events(session: AsyncSession, lead_id: str) -> List[Dict[str, Any]]:
    rows = (
        await session.execute(
            text(
                """
                SELECT * FROM offer_events
                WHERE lead_id = :lead_id
                ORDER BY received_at DESC, created_at DESC
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()
    return [dict(row) for row in rows]


async def _latest_signing_session(session: AsyncSession, lead_id: str) -> Optional[Dict[str, Any]]:
    row = (
        await session.execute(
            text(
                """
                SELECT * FROM signing_sessions
                WHERE lead_id = :lead_id
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().first()
    if not row:
        return None
    payload = dict(row)
    payload["signing_url"] = f"/api/signing/{lead_id}?session={payload['token']}"
    return payload


async def build_listing_workflow_payload(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    workflow = await ensure_listing_workflow(session, lead_id)
    documents = await get_listing_documents(session, lead_id)
    inspection = (
        await session.execute(
            text("SELECT * FROM inspection_reports WHERE lead_id = :lead_id ORDER BY inspection_at DESC LIMIT 1"),
            {"lead_id": lead_id},
        )
    ).mappings().first()
    inspection_payload = dict(inspection) if inspection else None
    if inspection_payload:
        inspection_payload["approved"] = bool(inspection_payload.get("approved"))

    price_history = await _latest_price_guidance_rows(session, lead_id)
    approved_price = next((item for item in price_history if item.get("status") == "approved"), None)
    draft_price = next((item for item in price_history if item.get("status") in {"draft", "ready_for_review"}), None)
    offer_events = await _latest_offer_events(session, lead_id)
    latest_signing = await _latest_signing_session(session, lead_id)
    latest_docs = await _latest_document_by_kind(session, lead_id)

    can_send = bool(
        inspection_payload
        and approved_price
        and workflow.get("lawyer_signoff_status") == "approved"
        and all(kind in latest_docs for kind in REQUIRED_SEND_DOCUMENT_KINDS)
    )
    can_market_ready = bool(can_send and "cos" in latest_docs)

    return {
        "workflow": {
            **workflow,
            "inspection_complete": bool(workflow.get("inspection_complete")),
            "market_ready": bool(workflow.get("market_ready")),
        },
        "documents": documents,
        "inspection_report": inspection_payload,
        "approved_price_guidance": approved_price,
        "draft_price_guidance": draft_price,
        "price_guidance_history": price_history,
        "offer_events": offer_events,
        "latest_signing_session": latest_signing,
        "required_document_kinds": REQUIRED_SEND_DOCUMENT_KINDS,
        "can_send_authority_pack": can_send,
        "can_mark_market_ready": can_market_ready,
    }


def _candidate_sort_key(candidate: Dict[str, Any], lead_value: Optional[int]) -> tuple:
    distance = candidate.get("distance_km")
    price = candidate.get("sale_price")
    date_val = candidate.get("sale_date_obj")
    distance_missing = 1 if distance is None else 0
    lead_delta = abs(price - lead_value) if (lead_value and price) else 0
    timestamp = date_val.timestamp() if date_val else 0
    return (distance_missing, distance or 9999, lead_delta, -timestamp)


async def draft_price_guidance(
    session: AsyncSession,
    lead_id: str,
    override_low: Optional[int] = None,
    override_high: Optional[int] = None,
    override_rationale: Optional[str] = None,
) -> Dict[str, Any]:
    lead_row = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    if not lead_row:
        raise ValueError("Lead not found")
    lead = dict(lead_row)
    suburb = lead.get("suburb") or ""
    lead_lat = float(lead.get("lat") or 0)
    lead_lng = float(lead.get("lng") or 0)
    lead_value = _parse_money(lead.get("est_value"))
    subject_has_geo = bool(lead_lat and lead_lng)
    six_month_cutoff = now_sydney() - timedelta(days=183)

    strict_candidates: List[Dict[str, Any]] = []
    fallback_candidates: List[Dict[str, Any]] = []
    lead_rows = (
        await session.execute(
            text(
                """
                SELECT address, suburb, sale_price, sale_date, lat, lng
                FROM leads
                WHERE id != :lead_id
                  AND COALESCE(suburb, '') = :suburb
                  AND COALESCE(sale_price, '') != ''
                  AND COALESCE(sale_date, '') != ''
                LIMIT 80
                """
            ),
            {"lead_id": lead_id, "suburb": suburb},
        )
    ).mappings().all()
    for row in lead_rows:
        sale_price = _parse_money(row.get("sale_price"))
        sale_date_obj = _parse_dt(row.get("sale_date"))
        if not sale_price or not sale_date_obj:
            continue
        if sale_date_obj < six_month_cutoff:
            continue
        distance = _haversine_km(lead_lat, lead_lng, float(row.get("lat") or 0), float(row.get("lng") or 0))
        candidate = {
            "address": row.get("address"),
            "suburb": row.get("suburb") or suburb,
            "sale_price": sale_price,
            "sale_date": sale_date_obj.date().isoformat(),
            "sale_date_obj": sale_date_obj,
            "source": "local_stock_leads",
            "distance_km": round(distance, 2) if distance is not None else None,
        }
        if distance is not None and distance <= 2:
            strict_candidates.append(candidate)
        elif not subject_has_geo:
            fallback_candidates.append({**candidate, "source": "local_stock_leads (same suburb fallback)"})

    sold_rows = (
        await session.execute(
            text(
                """
                SELECT address, suburb, sale_price, sale_date, lat, lng, source_name
                FROM sold_events
                WHERE COALESCE(suburb, '') = :suburb
                  AND COALESCE(sale_price, '') != ''
                  AND COALESCE(sale_date, '') != ''
                LIMIT 80
                """
            ),
            {"suburb": suburb},
        )
    ).mappings().all()
    for row in sold_rows:
        sale_price = _parse_money(row.get("sale_price"))
        sale_date_obj = _parse_dt(row.get("sale_date"))
        if not sale_price or not sale_date_obj:
            continue
        if sale_date_obj < six_month_cutoff:
            continue
        distance = _haversine_km(lead_lat, lead_lng, float(row.get("lat") or 0), float(row.get("lng") or 0))
        candidate = {
            "address": row.get("address"),
            "suburb": row.get("suburb") or suburb,
            "sale_price": sale_price,
            "sale_date": sale_date_obj.date().isoformat(),
            "sale_date_obj": sale_date_obj,
            "source": row.get("source_name") or "sold_events",
            "distance_km": round(distance, 2) if distance is not None else None,
        }
        if distance is not None and distance <= 2:
            strict_candidates.append(candidate)
        elif not subject_has_geo:
            fallback_candidates.append({**candidate, "source": f"{candidate['source']} (same suburb fallback)"})

    strict_by_address: Dict[str, Dict[str, Any]] = {}
    for candidate in strict_candidates:
        key = str(candidate["address"]).strip().lower()
        if key and key not in strict_by_address:
            strict_by_address[key] = candidate

    fallback_by_address: Dict[str, Dict[str, Any]] = {}
    for candidate in fallback_candidates:
        key = str(candidate["address"]).strip().lower()
        if key and key not in strict_by_address and key not in fallback_by_address:
            fallback_by_address[key] = candidate

    comps = sorted(strict_by_address.values(), key=lambda item: _candidate_sort_key(item, lead_value))[:3]
    if len(comps) < 3:
        fallback_pool = sorted(fallback_by_address.values(), key=lambda item: _candidate_sort_key(item, lead_value))
        comps.extend(fallback_pool[: max(0, 3 - len(comps))])
    comp_prices = [item["sale_price"] for item in comps if item.get("sale_price")]
    if override_low is not None and override_high is not None:
        estimate_low, estimate_high = override_low, override_high
    elif len(comp_prices) >= 3:
        estimate_low, estimate_high = min(comp_prices), max(comp_prices)
    elif lead_value:
        estimate_low = int(lead_value * 0.96)
        estimate_high = int(lead_value * 1.04)
    elif comp_prices:
        pivot = int(sum(comp_prices) / len(comp_prices))
        estimate_low = int(pivot * 0.97)
        estimate_high = int(pivot * 1.03)
    else:
        estimate_low = estimate_high = 0

    strict_comp_count = len(
        [
            item
            for item in comps
            if item.get("distance_km") is not None and float(item.get("distance_km") or 0) <= 2
        ]
    )
    rationale = override_rationale or (
        f"Drafted from local {suburb or 'suburb'} sold evidence using the 2km / 6-month filter where geocoded matches were available. "
        f"Human approval is required before any quoted price is sent or published."
    )
    if strict_comp_count < 3:
        rationale = (
            f"{rationale} Only {strict_comp_count} strict comparable sale(s) met the 2km / 6-month rule from local data, "
            "so any additional matches shown are same-suburb fallbacks pending operator review."
        )
    version_row = (
        await session.execute(
            text("SELECT COALESCE(MAX(version), 0) AS version FROM price_guidance_logs WHERE lead_id = :lead_id"),
            {"lead_id": lead_id},
        )
    ).mappings().first()
    version = int((version_row or {}).get("version") or 0) + 1
    await session.execute(
        text(
            """
            INSERT INTO price_guidance_logs (
                id, lead_id, kind, status, version, estimate_low, estimate_high, rationale, comparables, created_at, updated_at
            ) VALUES (
                :id, :lead_id, 'guidance', 'ready_for_review', :version, :estimate_low, :estimate_high, :rationale, :comparables, :created_at, :updated_at
            )
            """
        ),
        {
            "id": uuid.uuid4().hex,
            "lead_id": lead_id,
            "version": version,
            "estimate_low": estimate_low,
            "estimate_high": estimate_high,
            "rationale": rationale,
            "comparables": json.dumps(
                [
                    {
                        "address": item["address"],
                        "suburb": item["suburb"],
                        "sale_price": item["sale_price"],
                        "sale_date": item["sale_date"],
                        "source": item["source"],
                        "distance_km": item["distance_km"],
                    }
                    for item in comps
                ]
            ),
            "created_at": now_iso(),
            "updated_at": now_iso(),
        },
    )
    await session.execute(
        text(
            """
            UPDATE listing_workflows
            SET price_guidance_status = 'ready_for_review',
                stage = CASE WHEN stage = 'documents' THEN 'price' ELSE stage END,
                updated_at = :now
            WHERE lead_id = :lead_id
            """
        ),
        {"lead_id": lead_id, "now": now_iso()},
    )
    await _append_lead_activity(session, lead_id, "price_guidance_drafted", f"Draft price guidance created at ${estimate_low:,} - ${estimate_high:,}.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def update_price_guidance(session: AsyncSession, lead_id: str, guidance_id: str, estimate_low: int, estimate_high: int, rationale: str, comparables: List[Dict[str, Any]]) -> Dict[str, Any]:
    row = (
        await session.execute(
            text("SELECT * FROM price_guidance_logs WHERE id = :id AND lead_id = :lead_id"),
            {"id": guidance_id, "lead_id": lead_id},
        )
    ).mappings().first()
    if not row:
        raise ValueError("Price guidance record not found")
    await session.execute(
        text(
            """
            UPDATE price_guidance_logs
            SET estimate_low = :estimate_low,
                estimate_high = :estimate_high,
                rationale = :rationale,
                comparables = :comparables,
                status = 'ready_for_review',
                updated_at = :updated_at
            WHERE id = :id
            """
        ),
        {
            "id": guidance_id,
            "estimate_low": estimate_low,
            "estimate_high": estimate_high,
            "rationale": rationale,
            "comparables": json.dumps(comparables),
            "updated_at": now_iso(),
        },
    )
    await _append_lead_activity(session, lead_id, "price_guidance_edited", f"Price guidance draft updated to ${estimate_low:,} - ${estimate_high:,}.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def approve_price_guidance(session: AsyncSession, lead_id: str, guidance_id: str, approved_by: str = "operator") -> Dict[str, Any]:
    row = (
        await session.execute(
            text("SELECT * FROM price_guidance_logs WHERE id = :id AND lead_id = :lead_id"),
            {"id": guidance_id, "lead_id": lead_id},
        )
    ).mappings().first()
    if not row:
        raise ValueError("Price guidance record not found")
    approved_at = now_iso()
    await session.execute(
        text(
            """
            UPDATE price_guidance_logs
            SET status = 'approved',
                approved_by = :approved_by,
                approved_at = :approved_at,
                updated_at = :updated_at
            WHERE id = :id
            """
        ),
        {"id": guidance_id, "approved_by": approved_by, "approved_at": approved_at, "updated_at": approved_at},
    )
    await session.execute(
        text(
            """
            UPDATE listing_workflows
            SET approved_price_guidance_id = :guidance_id,
                price_guidance_status = 'approved',
                stage = CASE WHEN stage IN ('documents', 'price') THEN 'authority_pack' ELSE stage END,
                updated_at = :updated_at
            WHERE lead_id = :lead_id
            """
        ),
        {"guidance_id": guidance_id, "lead_id": lead_id, "updated_at": approved_at},
    )
    row_dict = dict(row)
    await _append_lead_activity(
        session,
        lead_id,
        "price_guidance_approved",
        f"Price guidance approved at ${int(row_dict.get('estimate_low') or 0):,} - ${int(row_dict.get('estimate_high') or 0):,}.",
    )
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def set_lawyer_signoff(session: AsyncSession, lead_id: str, status: str) -> Dict[str, Any]:
    now = now_iso()
    await ensure_listing_workflow(session, lead_id)
    await session.execute(
        text("UPDATE listing_workflows SET lawyer_signoff_status = :status, updated_at = :now WHERE lead_id = :lead_id"),
        {"status": status, "now": now, "lead_id": lead_id},
    )
    await _append_lead_activity(session, lead_id, "lawyer_signoff_updated", f"Lawyer sign-off status set to {status}.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def set_marketing_status(session: AsyncSession, lead_id: str, status: str, note: Optional[str]) -> Dict[str, Any]:
    now = now_iso()
    await ensure_listing_workflow(session, lead_id)
    await session.execute(
        text(
            """
            UPDATE listing_workflows
            SET marketing_payment_status = :status,
                workflow_notes = CASE WHEN :note = '' THEN workflow_notes ELSE :note END,
                updated_at = :now
            WHERE lead_id = :lead_id
            """
        ),
        {"status": status, "note": note or "", "now": now, "lead_id": lead_id},
    )
    await _append_lead_activity(session, lead_id, "marketing_status_updated", f"Marketing payment status set to {status}.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def save_uploaded_document(session: AsyncSession, lead_id: str, kind: str, source_path: Path, original_name: str, uploaded_by: str = "operator") -> Dict[str, Any]:
    if kind not in ALL_DOCUMENT_KINDS:
        raise ValueError(f"Unsupported document kind: {kind}")
    _ensure_root()
    listing_dir = _listing_dir(lead_id)
    version_row = (
        await session.execute(
            text("SELECT COALESCE(MAX(version), 0) AS version FROM listing_documents WHERE lead_id = :lead_id AND kind = :kind"),
            {"lead_id": lead_id, "kind": kind},
        )
    ).mappings().first()
    version = int((version_row or {}).get("version") or 0) + 1
    target_dir = listing_dir / "uploads" / kind
    target_dir.mkdir(parents=True, exist_ok=True)
    stored_name = f"{version:02d}_{_safe_name(original_name)}"
    target_path = target_dir / stored_name
    shutil.copy2(source_path, target_path)
    now = now_iso()
    relative_path = str(target_path.relative_to(GENERATED_REPORTS_ROOT)).replace("\\", "/")
    await session.execute(
        text(
            """
            INSERT INTO listing_documents (
                id, lead_id, kind, label, original_name, stored_name, relative_path,
                mime_type, version, source, generated, uploaded_by, created_at, updated_at
            ) VALUES (
                :id, :lead_id, :kind, :label, :original_name, :stored_name, :relative_path,
                :mime_type, :version, 'upload', 0, :uploaded_by, :created_at, :updated_at
            )
            """
        ),
        {
            "id": uuid.uuid4().hex,
            "lead_id": lead_id,
            "kind": kind,
            "label": kind.replace("_", " ").title(),
            "original_name": original_name,
            "stored_name": stored_name,
            "relative_path": relative_path,
            "mime_type": mimetypes.guess_type(original_name)[0] or "application/octet-stream",
            "version": version,
            "uploaded_by": uploaded_by,
            "created_at": now,
            "updated_at": now,
        },
    )
    await _append_lead_activity(session, lead_id, "listing_document_uploaded", f"{kind.replace('_', ' ')} uploaded: {original_name}.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def create_inspection_report(session: AsyncSession, lead_id: str, inspected_by: str, inspection_at: str, occupancy: str, condition_rating: str, summary: str, notes: Optional[str]) -> Dict[str, Any]:
    await ensure_listing_workflow(session, lead_id)
    report_id = uuid.uuid4().hex
    now = now_iso()
    await session.execute(
        text(
            """
            INSERT INTO inspection_reports (
                id, lead_id, inspected_by, inspection_at, occupancy, condition_rating, summary, notes, approved, created_at, updated_at
            ) VALUES (
                :id, :lead_id, :inspected_by, :inspection_at, :occupancy, :condition_rating, :summary, :notes, 1, :created_at, :updated_at
            )
            """
        ),
        {
            "id": report_id,
            "lead_id": lead_id,
            "inspected_by": inspected_by,
            "inspection_at": inspection_at,
            "occupancy": occupancy,
            "condition_rating": condition_rating,
            "summary": summary,
            "notes": notes or "",
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.execute(
        text(
            """
            UPDATE listing_workflows
            SET inspection_complete = 1,
                inspection_report_id = :report_id,
                stage = CASE WHEN stage = 'documents' THEN 'price' ELSE stage END,
                updated_at = :updated_at
            WHERE lead_id = :lead_id
            """
        ),
        {"report_id": report_id, "updated_at": now, "lead_id": lead_id},
    )
    await _append_lead_activity(session, lead_id, "inspection_completed", f"Inspection logged by {inspected_by}.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def record_offer_event(session: AsyncSession, lead_id: str, amount: int, buyer_name: Optional[str], conditions: Optional[str], channel: str, status: str, received_at: str, notes: Optional[str]) -> Dict[str, Any]:
    now = now_iso()
    await session.execute(
        text(
            """
            INSERT INTO offer_events (
                id, lead_id, amount, buyer_name, conditions, channel, status, received_at, notes, created_at, updated_at
            ) VALUES (
                :id, :lead_id, :amount, :buyer_name, :conditions, :channel, :status, :received_at, :notes, :created_at, :updated_at
            )
            """
        ),
        {
            "id": uuid.uuid4().hex,
            "lead_id": lead_id,
            "amount": amount,
            "buyer_name": buyer_name or "",
            "conditions": conditions or "",
            "channel": channel,
            "status": status,
            "received_at": received_at,
            "notes": notes or "",
            "created_at": now,
            "updated_at": now,
        },
    )
    await _append_lead_activity(session, lead_id, "offer_logged", f"Offer logged at ${amount:,} via {channel}.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def _create_generated_document(session: AsyncSession, lead_id: str, kind: str, original_name: str, html: str, source: str = "system") -> Dict[str, Any]:
    _ensure_root()
    listing_dir = _listing_dir(lead_id)
    target_dir = listing_dir / "generated"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / original_name
    await html_to_pdf(html, str(target_path))
    version_row = (
        await session.execute(
            text("SELECT COALESCE(MAX(version), 0) AS version FROM listing_documents WHERE lead_id = :lead_id AND kind = :kind"),
            {"lead_id": lead_id, "kind": kind},
        )
    ).mappings().first()
    version = int((version_row or {}).get("version") or 0) + 1
    relative_path = str(target_path.relative_to(GENERATED_REPORTS_ROOT)).replace("\\", "/")
    doc_id = uuid.uuid4().hex
    now = now_iso()
    await session.execute(
        text(
            """
            INSERT INTO listing_documents (
                id, lead_id, kind, label, original_name, stored_name, relative_path,
                mime_type, version, source, generated, uploaded_by, created_at, updated_at
            ) VALUES (
                :id, :lead_id, :kind, :label, :original_name, :stored_name, :relative_path,
                'application/pdf', :version, :source, 1, 'system', :created_at, :updated_at
            )
            """
        ),
        {
            "id": doc_id,
            "lead_id": lead_id,
            "kind": kind,
            "label": kind.replace("_", " ").title(),
            "original_name": original_name,
            "stored_name": original_name,
            "relative_path": relative_path,
            "version": version,
            "source": source,
            "created_at": now,
            "updated_at": now,
        },
    )
    return {"id": doc_id, "kind": kind, "relative_path": relative_path, "download_url": _to_download_url(relative_path)}


def _render_authority_pack_html(lead: Dict[str, Any], workflow: Dict[str, Any], inspection_report: Dict[str, Any], approved_guidance: Dict[str, Any], documents: List[Dict[str, Any]], signed_vendor: Optional[str] = None) -> str:
    comparables = _parse_json_list(approved_guidance.get("comparables"))
    return AUTHORITY_PACK_TEMPLATE.render(
        authority_heading="Residential Listing Authority Pack",
        brand_name=BRAND_NAME,
        logo_url=BRAND_LOGO_URL,
        address=lead.get("address") or "Property address",
        owner_name=lead.get("owner_name") or "Vendor",
        authority_type_label=(workflow.get("authority_type") or "exclusive").replace("_", " ").title(),
        estimate_low=f"{int(approved_guidance.get('estimate_low') or 0):,}",
        estimate_high=f"{int(approved_guidance.get('estimate_high') or 0):,}",
        inspected_by=inspection_report.get("inspected_by") or "Agent",
        inspection_at=inspection_report.get("inspection_at") or "",
        inspection_summary=inspection_report.get("summary") or "",
        inspection_notes=inspection_report.get("notes") or "",
        approved_by=approved_guidance.get("approved_by") or "Operator",
        approved_at=approved_guidance.get("approved_at") or "",
        rationale=approved_guidance.get("rationale") or "",
        comparables=comparables,
        included_documents=documents,
        fair_trading_guide=FTR32_GUIDE_URL,
        agent_name=PRINCIPAL_NAME,
        agent_phone=PRINCIPAL_PHONE,
        agent_email=PRINCIPAL_EMAIL,
        lead_id=lead.get("id") or "",
        generated_at=format_sydney(now_sydney()),
        signed_vendor=signed_vendor,
    )


async def generate_authority_pack(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    workflow_payload = await build_listing_workflow_payload(session, lead_id)
    workflow = workflow_payload["workflow"]
    if not workflow_payload["inspection_report"]:
        raise ValueError("Inspection report is required before drafting the authority pack")
    if not workflow_payload["approved_price_guidance"]:
        raise ValueError("Approved price guidance is required before drafting the authority pack")
    if workflow.get("lawyer_signoff_status") != "approved":
        raise ValueError("Lawyer sign-off must be approved before drafting the authority pack")
    docs_by_kind = {doc["kind"]: doc for doc in workflow_payload["documents"]}
    missing = [kind for kind in REQUIRED_SEND_DOCUMENT_KINDS if kind not in docs_by_kind]
    if missing:
        raise ValueError(f"Missing required documents: {', '.join(missing)}")

    lead_row = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    if not lead_row:
        raise ValueError("Lead not found")
    included_docs = [docs_by_kind[kind] for kind in REQUIRED_SEND_DOCUMENT_KINDS if kind in docs_by_kind]
    html = _render_authority_pack_html(
        dict(lead_row),
        workflow,
        workflow_payload["inspection_report"],
        workflow_payload["approved_price_guidance"],
        included_docs,
    )
    generated_doc = await _create_generated_document(
        session,
        lead_id,
        "authority_pack",
        f"Authority_Pack_{lead_id}_{now_sydney().strftime('%Y%m%d_%H%M%S')}.pdf",
        html,
    )
    now = now_iso()
    await session.execute(
        text(
            """
            UPDATE listing_workflows
            SET authority_pack_status = 'ready',
                pack_document_id = :pack_document_id,
                stage = 'send_sign',
                updated_at = :updated_at
            WHERE lead_id = :lead_id
            """
        ),
        {"pack_document_id": generated_doc["id"], "updated_at": now, "lead_id": lead_id},
    )
    await _append_lead_activity(session, lead_id, "authority_pack_generated", "Authority pack PDF regenerated from approved workflow data.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def send_authority_pack(session: AsyncSession, lead_id: str, recipient_email: str, recipient_name: Optional[str]) -> Dict[str, Any]:
    payload = await build_listing_workflow_payload(session, lead_id)
    if not payload["can_send_authority_pack"]:
        raise ValueError("Authority pack send is blocked until inspection, approved price guidance, required documents, and lawyer sign-off are complete")
    if not payload["workflow"].get("pack_document_id"):
        payload = await generate_authority_pack(session, lead_id)
    approved = payload["approved_price_guidance"] or {}
    token = hashlib.md5(f"{lead_id}:{recipient_email}:{now_iso()}".encode()).hexdigest()[:18]
    now = now_iso()
    session_id = uuid.uuid4().hex
    await session.execute(
        text(
            """
            INSERT INTO signing_sessions (
                id, lead_id, token, status, authority_pack_document_id, sent_to, sent_at, created_at, updated_at
            ) VALUES (
                :id, :lead_id, :token, 'sent', :authority_pack_document_id, :sent_to, :sent_at, :created_at, :updated_at
            )
            """
        ),
        {
            "id": session_id,
            "lead_id": lead_id,
            "token": token,
            "authority_pack_document_id": payload["workflow"].get("pack_document_id"),
            "sent_to": recipient_email,
            "sent_at": now,
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.execute(
        text(
            """
            UPDATE listing_workflows
            SET latest_signing_session_id = :session_id,
                pack_sent_at = :pack_sent_at,
                stage = 'send_sign',
                updated_at = :updated_at
            WHERE lead_id = :lead_id
            """
        ),
        {"session_id": session_id, "pack_sent_at": now, "updated_at": now, "lead_id": lead_id},
    )
    await session.execute(
        text(
            """
            INSERT INTO price_guidance_logs (
                id, lead_id, kind, status, version, estimate_low, estimate_high, rationale, comparables,
                quoted_channel, quoted_to, quoted_at, created_at, updated_at
            ) VALUES (
                :id, :lead_id, 'quote', 'recorded', :version, :estimate_low, :estimate_high, :rationale, :comparables,
                'email', :quoted_to, :quoted_at, :created_at, :updated_at
            )
            """
        ),
        {
            "id": uuid.uuid4().hex,
            "lead_id": lead_id,
            "version": int(approved.get("version") or 1),
            "estimate_low": approved.get("estimate_low"),
            "estimate_high": approved.get("estimate_high"),
            "rationale": "Authority pack send recorded as quoted-price communication.",
            "comparables": json.dumps(_parse_json_list(approved.get("comparables"))),
            "quoted_to": recipient_email,
            "quoted_at": now,
            "created_at": now,
            "updated_at": now,
        },
    )
    await _append_lead_activity(session, lead_id, "authority_pack_sent", f"Authority pack sent to {recipient_name or recipient_email}. Signing link created.", "email")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def set_market_ready(session: AsyncSession, lead_id: str, market_ready: bool = True) -> Dict[str, Any]:
    payload = await build_listing_workflow_payload(session, lead_id)
    docs_by_kind = {doc["kind"]: doc for doc in payload["documents"]}
    if market_ready and "cos" not in docs_by_kind:
        raise ValueError("COS must be uploaded before the listing can be marked market ready")
    now = now_iso()
    await session.execute(
        text(
            """
            UPDATE listing_workflows
            SET market_ready = :market_ready,
                market_ready_at = CASE WHEN :market_ready = 1 THEN :now ELSE NULL END,
                stage = CASE WHEN :market_ready = 1 THEN 'market_ready' ELSE stage END,
                updated_at = :now
            WHERE lead_id = :lead_id
            """
        ),
        {"market_ready": 1 if market_ready else 0, "now": now, "lead_id": lead_id},
    )
    await _append_lead_activity(session, lead_id, "market_ready_updated", f"Market-ready set to {market_ready}.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)


async def get_signing_session(session: AsyncSession, lead_id: str, token: str) -> Optional[Dict[str, Any]]:
    row = (
        await session.execute(
            text(
                """
                SELECT * FROM signing_sessions
                WHERE lead_id = :lead_id AND token = :token
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"lead_id": lead_id, "token": token},
        )
    ).mappings().first()
    return dict(row) if row else None


async def mark_signing_session_viewed(session: AsyncSession, lead_id: str, token: str) -> None:
    now = now_iso()
    await session.execute(
        text(
            """
            UPDATE signing_sessions
            SET viewed_at = COALESCE(viewed_at, :now),
                status = CASE WHEN status = 'sent' THEN 'viewed' ELSE status END,
                updated_at = :now
            WHERE lead_id = :lead_id AND token = :token
            """
        ),
        {"lead_id": lead_id, "token": token, "now": now},
    )
    await session.commit()


async def build_signing_context(session: AsyncSession, lead_id: str, token: str) -> Dict[str, Any]:
    signing_session = await get_signing_session(session, lead_id, token)
    if not signing_session:
        raise ValueError("Signing session not found")
    payload = await build_listing_workflow_payload(session, lead_id)
    lead_row = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    if not lead_row:
        raise ValueError("Lead not found")
    documents = [doc for doc in payload["documents"] if doc["kind"] in REQUIRED_SEND_DOCUMENT_KINDS]
    html = _render_authority_pack_html(
        dict(lead_row),
        payload["workflow"],
        payload["inspection_report"],
        payload["approved_price_guidance"],
        documents,
    )
    return {"signing_session": signing_session, "lead": dict(lead_row), "workflow_payload": payload, "authority_html": html}


async def complete_signing_session(session: AsyncSession, lead_id: str, token: str, signer_name: str, signer_email: Optional[str], signer_ip: Optional[str], signer_user_agent: Optional[str]) -> Dict[str, Any]:
    context = await build_signing_context(session, lead_id, token)
    payload = context["workflow_payload"]
    documents = [doc for doc in payload["documents"] if doc["kind"] in REQUIRED_SEND_DOCUMENT_KINDS]
    signed_html = _render_authority_pack_html(
        context["lead"],
        payload["workflow"],
        payload["inspection_report"],
        payload["approved_price_guidance"],
        documents,
        signed_vendor=signer_name,
    )
    signed_doc = await _create_generated_document(
        session,
        lead_id,
        "signed_authority_pack",
        f"Signed_Authority_Pack_{lead_id}_{now_sydney().strftime('%Y%m%d_%H%M%S')}.pdf",
        signed_html,
    )
    now = now_iso()
    await session.execute(
        text(
            """
            UPDATE signing_sessions
            SET status = 'signed',
                signer_name = :signer_name,
                signer_email = :signer_email,
                signer_ip = :signer_ip,
                signer_user_agent = :signer_user_agent,
                signed_at = :signed_at,
                serviced_at = :serviced_at,
                archive_path = :archive_path,
                updated_at = :updated_at
            WHERE lead_id = :lead_id AND token = :token
            """
        ),
        {
            "signer_name": signer_name,
            "signer_email": signer_email or "",
            "signer_ip": signer_ip or "",
            "signer_user_agent": signer_user_agent or "",
            "signed_at": now,
            "serviced_at": now,
            "archive_path": signed_doc["relative_path"],
            "updated_at": now,
            "lead_id": lead_id,
            "token": token,
        },
    )
    await session.execute(
        text(
            """
            UPDATE listing_workflows
            SET authority_pack_status = 'signed',
                pack_signed_at = :pack_signed_at,
                stage = 'signed',
                updated_at = :updated_at
            WHERE lead_id = :lead_id
            """
        ),
        {"pack_signed_at": now, "updated_at": now, "lead_id": lead_id},
    )
    await _append_lead_activity(session, lead_id, "authority_pack_signed", f"Authority pack signed by {signer_name}.")
    await session.commit()
    return await build_listing_workflow_payload(session, lead_id)
