from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import core.database as db_module
from core.config import GENERATED_REPORTS_ROOT


DocumentPayload = Dict[str, Any]


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _compact_dict(payload: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    for key, value in payload.items():
        if value in (None, "", [], {}):
            continue
        cleaned[key] = value
    return cleaned


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        item = _clean_text(value)
        if not item:
            continue
        normalized = item.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(item)
    return ordered


def _format_money(value: Any) -> Optional[str]:
    if value in (None, "", 0, "0"):
        return None
    try:
        return f"${int(round(float(value))):,}"
    except (TypeError, ValueError):
        text_value = _clean_text(value)
        return text_value or None


def _format_number(value: Any, suffix: str = "") -> Optional[str]:
    if value in (None, "", 0, 0.0, "0", "0.0"):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return _clean_text(value) or None
    if numeric.is_integer():
        rendered = f"{int(numeric):,}"
    else:
        rendered = f"{numeric:,.1f}".rstrip("0").rstrip(".")
    return f"{rendered}{suffix}"


def _html_paragraphs(lines: Iterable[str]) -> str:
    parts = [f"<p>{html.escape(line)}</p>" for line in lines if _clean_text(line)]
    return "".join(parts)


def _html_list(items: Iterable[str]) -> str:
    cleaned = [item for item in items if _clean_text(item)]
    if not cleaned:
        return ""
    return "<ul>" + "".join(f"<li>{html.escape(item)}</li>" for item in cleaned) + "</ul>"


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "document"


async def _fetch_lead_bundle(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    lead_row = (
        await session.execute(text("SELECT * FROM leads WHERE id = :lead_id"), {"lead_id": lead_id})
    ).mappings().first()
    if not lead_row:
        raise ValueError("Lead not found")

    notes_rows = (
        await session.execute(
            text(
                """
                SELECT note_type, content, created_at
                FROM notes
                WHERE lead_id = :lead_id
                ORDER BY created_at DESC, id DESC
                LIMIT 6
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()

    call_rows = (
        await session.execute(
            text(
                """
                SELECT outcome, connected, timestamp, logged_at, note, summary
                FROM call_log
                WHERE lead_id = :lead_id
                ORDER BY COALESCE(timestamp, logged_at, '') DESC, id DESC
                LIMIT 5
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()

    guidance_rows = (
        await session.execute(
            text(
                """
                SELECT kind, status, estimate_low, estimate_high, rationale, comparables, created_at, updated_at
                FROM price_guidance_logs
                WHERE lead_id = :lead_id
                ORDER BY CASE WHEN status = 'approved' THEN 0 ELSE 1 END, COALESCE(updated_at, created_at, '') DESC
                LIMIT 3
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()

    sold_rows = (
        await session.execute(
            text(
                """
                SELECT address, suburb, postcode, sale_price, sale_date, source_name
                FROM sold_events
                WHERE COALESCE(suburb, '') = COALESCE(:suburb, '')
                ORDER BY COALESCE(sale_date, created_at, '') DESC
                LIMIT 5
                """
            ),
            {"suburb": lead_row.get("suburb")},
        )
    ).mappings().all()

    return {
        "lead": dict(lead_row),
        "notes": [dict(row) for row in notes_rows],
        "calls": [dict(row) for row in call_rows],
        "price_guidance": [dict(row) for row in guidance_rows],
        "sold_events": [dict(row) for row in sold_rows],
    }


def _property_summary(lead: Dict[str, Any]) -> Dict[str, Any]:
    return _compact_dict(
        {
            "address": _clean_text(lead.get("address")),
            "suburb": _clean_text(lead.get("suburb")),
            "postcode": _clean_text(lead.get("postcode")),
            "bedrooms": _format_number(lead.get("bedrooms")),
            "bathrooms": _format_number(lead.get("bathrooms")),
            "car_spaces": _format_number(lead.get("car_spaces")),
            "land_size_sqm": _format_number(lead.get("land_size_sqm"), " sqm"),
            "floor_size_sqm": _format_number(lead.get("floor_size_sqm"), " sqm"),
            "estimated_value": _format_money(lead.get("est_value")),
            "listing_headline": _clean_text(lead.get("listing_headline")),
        }
    )


def _opportunity_summary(lead: Dict[str, Any]) -> Dict[str, Any]:
    return _compact_dict(
        {
            "status": _clean_text(lead.get("status")),
            "trigger_type": _clean_text(lead.get("trigger_type") or lead.get("lead_archetype")),
            "signal_status": _clean_text(lead.get("signal_status")),
            "scenario": _clean_text(lead.get("scenario")),
        }
    )


def _time_context(lead: Dict[str, Any]) -> Dict[str, Any]:
    return _compact_dict(
        {
            "days_on_market": lead.get("days_on_market") if lead.get("days_on_market") not in (None, "", 0) else None,
            "list_date": _clean_text(lead.get("list_date")),
            "date_found": _clean_text(lead.get("date_found")),
            "created_at": _clean_text(lead.get("created_at")),
            "updated_at": _clean_text(lead.get("updated_at")),
            "last_contacted_at": _clean_text(lead.get("last_contacted_at")),
        }
    )


def _collect_evidence(bundle: Dict[str, Any]) -> List[str]:
    lead = bundle["lead"]
    evidence: List[str] = []
    evidence.extend(lead.get("source_evidence") or [])
    evidence.extend(
        filter(
            None,
            [
                _clean_text(lead.get("why_now")),
                _clean_text(lead.get("stage_note")),
                _clean_text(lead.get("recommended_next_step")),
                _clean_text(lead.get("what_to_say")),
            ],
        )
    )

    for item in lead.get("activity_log") or []:
        if isinstance(item, dict):
            evidence.append(_clean_text(item.get("note") or item.get("headline") or item.get("subject")))

    for note in bundle["notes"]:
        evidence.append(_clean_text(note.get("content")))

    for call in bundle["calls"]:
        summary = _clean_text(call.get("summary"))
        note = _clean_text(call.get("note"))
        outcome = _clean_text(call.get("outcome"))
        if summary:
            evidence.append(summary)
        if note:
            evidence.append(note)
        if outcome:
            evidence.append(f"Recent call outcome: {outcome.replace('_', ' ')}")

    return _dedupe_keep_order(evidence)[:8]


def _extract_comparables(bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    comparables: List[Dict[str, Any]] = []
    for guidance in bundle["price_guidance"]:
        for item in guidance.get("comparables") or []:
            if not isinstance(item, dict):
                continue
            comparable = _compact_dict(
                {
                    "address": _clean_text(item.get("address")),
                    "suburb": _clean_text(item.get("suburb")),
                    "sale_price": _format_money(item.get("sale_price")),
                    "sale_date": _clean_text(item.get("sale_date")),
                    "source": _clean_text(item.get("source")),
                }
            )
            if comparable.get("address"):
                comparables.append(comparable)

    for sold in bundle["sold_events"]:
        comparable = _compact_dict(
            {
                "address": _clean_text(sold.get("address")),
                "suburb": _clean_text(sold.get("suburb")),
                "sale_price": _format_money(sold.get("sale_price")),
                "sale_date": _clean_text(sold.get("sale_date")),
                "source": _clean_text(sold.get("source_name")),
            }
        )
        if comparable.get("address"):
            comparables.append(comparable)

    unique: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in comparables:
        key = item.get("address", "").lower()
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique[:6]


def _pricing_summary(bundle: Dict[str, Any]) -> Dict[str, Any]:
    pricing: Dict[str, Any] = {}
    preferred = bundle["price_guidance"][0] if bundle["price_guidance"] else None
    if preferred:
        indicative_range = _compact_dict(
            {
                "low": _format_money(preferred.get("estimate_low")),
                "high": _format_money(preferred.get("estimate_high")),
            }
        )
        if indicative_range:
            pricing["indicative_range"] = indicative_range
        rationale = _clean_text(preferred.get("rationale"))
        if rationale:
            pricing["rationale"] = rationale

    comparables = _extract_comparables(bundle)
    if comparables:
        pricing["comparables"] = comparables
    return pricing


def _sales_advice_data(bundle: Dict[str, Any]) -> Dict[str, Any]:
    lead = bundle["lead"]
    data = _compact_dict(
        {
            "property_summary": _property_summary(lead),
            "opportunity": _opportunity_summary(lead),
            "time_context": _time_context(lead),
            "key_evidence": _collect_evidence(bundle),
            "pricing": _pricing_summary(bundle),
            "strategy_notes": _dedupe_keep_order(
                [
                    _clean_text(lead.get("conversion_strategy")),
                    _clean_text(lead.get("recommended_next_step")),
                    "Use evidence-led pricing language and keep the discussion anchored in current market feedback."
                    if _collect_evidence(bundle)
                    else "",
                ]
            ),
        }
    )
    return data


def _cma_data(bundle: Dict[str, Any]) -> Dict[str, Any]:
    lead = bundle["lead"]
    pricing = _pricing_summary(bundle)
    comparables = pricing.pop("comparables", [])
    observations = _dedupe_keep_order(
        [
            (
                f"Indicative range currently sits between {pricing['indicative_range'].get('low')} and {pricing['indicative_range'].get('high')}."
                if pricing.get("indicative_range")
                else ""
            ),
            pricing.get("rationale", ""),
        ]
    )
    notes = []
    if not comparables:
        notes.append("Comparable sales were not available in current backend data.")

    payload = _compact_dict(
        {
            "subject_property": _property_summary(lead),
            "time_context": _time_context(lead),
            "comparable_properties": comparables,
            "pricing_observations": observations,
            "notes": notes,
        }
    )
    return payload


def _seller_insight_data(bundle: Dict[str, Any]) -> Dict[str, Any]:
    lead = bundle["lead"]
    evidence = _collect_evidence(bundle)
    time_observations = _dedupe_keep_order(
        [
            (
                f"The property has been active for {lead.get('days_on_market')} days."
                if lead.get("days_on_market") not in (None, "", 0)
                else ""
            ),
            (
                f"Lead record was updated on {lead.get('updated_at')}."
                if _clean_text(lead.get("updated_at"))
                else ""
            ),
            (
                f"Last contact was recorded on {lead.get('last_contacted_at')}."
                if _clean_text(lead.get("last_contacted_at"))
                else ""
            ),
        ]
    )

    recommended_talking_points = _dedupe_keep_order(
        [
            _clean_text(lead.get("what_to_say")),
            _clean_text(lead.get("recommended_next_step")),
            "Lead with current market feedback and ask what result would make a move worthwhile."
            if evidence
            else "",
            "Keep the call focused on timing, price confidence, and what has changed since the property first came to market."
            if lead.get("days_on_market") not in (None, "", 0)
            else "",
        ]
    )

    return _compact_dict(
        {
            "property_summary": _property_summary(lead),
            "why_actionable_now": evidence[:3],
            "time_observations": time_observations,
            "lead_evidence": evidence,
            "recommended_talking_points": recommended_talking_points,
        }
    )


def _render_html(document_type: str, title: str, data: Dict[str, Any]) -> str:
    def section(label: str, body: str) -> str:
        if not body:
            return ""
        return f"<section><h2>{html.escape(label)}</h2>{body}</section>"

    property_block = ""
    if isinstance(data.get("property_summary"), dict):
        items = [
            f"<li><strong>{html.escape(key.replace('_', ' ').title())}:</strong> {html.escape(str(value))}</li>"
            for key, value in data["property_summary"].items()
        ]
        property_block = "<ul>" + "".join(items) + "</ul>"

    opportunity_block = ""
    if isinstance(data.get("opportunity"), dict):
        items = [
            f"<li><strong>{html.escape(key.replace('_', ' ').title())}:</strong> {html.escape(str(value))}</li>"
            for key, value in data["opportunity"].items()
        ]
        opportunity_block = "<ul>" + "".join(items) + "</ul>"

    time_block = ""
    if isinstance(data.get("time_context"), dict):
        items = [
            f"<li><strong>{html.escape(key.replace('_', ' ').title())}:</strong> {html.escape(str(value))}</li>"
            for key, value in data["time_context"].items()
        ]
        time_block = "<ul>" + "".join(items) + "</ul>"

    pricing_block = ""
    pricing = data.get("pricing")
    if isinstance(pricing, dict):
        lines: List[str] = []
        if isinstance(pricing.get("indicative_range"), dict):
            range_bits = [str(v) for v in pricing["indicative_range"].values() if _clean_text(v)]
            if range_bits:
                lines.append(f"Indicative range: {' to '.join(range_bits)}")
        if pricing.get("rationale"):
            lines.append(str(pricing["rationale"]))
        if pricing.get("comparables"):
            for comp in pricing["comparables"]:
                comp_bits = [comp.get("address"), comp.get("sale_price"), comp.get("sale_date")]
                lines.append(" | ".join(bit for bit in comp_bits if _clean_text(bit)))
        pricing_block = _html_list(lines)

    comparable_block = ""
    if isinstance(data.get("comparable_properties"), list):
        lines = []
        for comp in data["comparable_properties"]:
            if isinstance(comp, dict):
                bits = [comp.get("address"), comp.get("sale_price"), comp.get("sale_date")]
                lines.append(" | ".join(bit for bit in bits if _clean_text(bit)))
        comparable_block = _html_list(lines)

    html_body = (
        section("Property Summary", property_block)
        + section("Opportunity", opportunity_block)
        + section("Time Context", time_block)
        + section("Key Evidence", _html_list(data.get("key_evidence", [])))
        + section("Pricing", pricing_block)
        + section("Comparable Properties", comparable_block)
        + section("Pricing Observations", _html_list(data.get("pricing_observations", [])))
        + section("Why This Lead Is Actionable", _html_list(data.get("why_actionable_now", [])))
        + section("Time-Based Observations", _html_list(data.get("time_observations", [])))
        + section("Lead Opportunity Evidence", _html_list(data.get("lead_evidence", [])))
        + section("Strategy Notes", _html_list(data.get("strategy_notes", [])))
        + section("Recommended Talking Points", _html_list(data.get("recommended_talking_points", [])))
        + section("Notes", _html_list(data.get("notes", [])))
    )
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; color: #14213d; margin: 36px; line-height: 1.45; }}
    h1 {{ font-size: 26px; margin-bottom: 6px; }}
    h2 {{ font-size: 15px; margin: 18px 0 8px; text-transform: uppercase; letter-spacing: 0.04em; }}
    p.meta {{ color: #5c677d; font-size: 12px; margin: 0 0 18px; }}
    section {{ margin-bottom: 10px; }}
    ul {{ margin: 0; padding-left: 18px; }}
    li {{ margin: 0 0 6px; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <p class="meta">{html.escape(document_type.replace('_', ' ').title())} generated {html.escape(generated_at)}</p>
  {html_body}
</body>
</html>"""


def _render_pdf(html_preview: str, lead_id: str, document_type: str) -> Optional[str]:
    try:
        from weasyprint import HTML  # type: ignore
    except Exception:
        return None

    output_dir = GENERATED_REPORTS_ROOT / "documents" / lead_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{_slug(document_type)}.pdf"
    try:
        HTML(string=html_preview).write_pdf(str(output_path))
        return str(output_path)
    except Exception:
        return None


async def _generate_document(
    lead_id: str,
    document_type: str,
    title: str,
    builder: Callable[[Dict[str, Any]], Dict[str, Any]],
    session: Optional[AsyncSession] = None,
) -> DocumentPayload:
    owns_session = session is None
    if session is None:
        session = db_module._async_session_factory()

    assert session is not None
    try:
        bundle = await _fetch_lead_bundle(session, lead_id)
        data = builder(bundle)
        html_preview = _render_html(document_type, title, data)
        pdf_path = _render_pdf(html_preview, lead_id, document_type)
        render_notes = [] if pdf_path else ["PDF generation unavailable or failed for this document."]
        return {
            "lead_id": lead_id,
            "document_type": document_type,
            "title": title,
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "data": data,
            "html_preview": html_preview,
            "pdf_path": pdf_path,
            "render_notes": render_notes,
        }
    finally:
        if owns_session:
            await session.close()


async def generate_sales_advice(lead_id: str, session: Optional[AsyncSession] = None) -> DocumentPayload:
    return await _generate_document(
        lead_id=lead_id,
        document_type="sales_advice",
        title="Sales Advice Report",
        builder=_sales_advice_data,
        session=session,
    )


async def generate_cma(lead_id: str, session: Optional[AsyncSession] = None) -> DocumentPayload:
    return await _generate_document(
        lead_id=lead_id,
        document_type="cma",
        title="Comparative Market Analysis",
        builder=_cma_data,
        session=session,
    )


async def generate_seller_insight(lead_id: str, session: Optional[AsyncSession] = None) -> DocumentPayload:
    return await _generate_document(
        lead_id=lead_id,
        document_type="seller_insight",
        title="Seller Insight Report",
        builder=_seller_insight_data,
        session=session,
    )
