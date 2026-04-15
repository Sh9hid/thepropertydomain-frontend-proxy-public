from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.logic import _compute_ownership_years, _derive_contactability_tier, _derive_refinance_signals, _derive_seller_intent_signals
from core.utils import _safe_int


def _list_of_dicts(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _value_range(lead: Dict[str, Any]) -> Dict[str, Any]:
    low = _safe_int(lead.get("estimated_value_low"), 0) or None
    high = _safe_int(lead.get("estimated_value_high"), 0) or None
    est_value = _safe_int(lead.get("est_value"), 0) or None
    if est_value and not low and not high:
        low = int(est_value * 0.95)
        high = int(est_value * 1.05)
    if low and not high:
        high = low
    if high and not low:
        low = high
    midpoint = int((low + high) / 2) if low and high else est_value
    return {"low": low, "high": high, "midpoint": midpoint, "valuation_date": lead.get("valuation_date")}


def _rental_range(lead: Dict[str, Any]) -> Dict[str, Any]:
    low = _safe_int(lead.get("rental_estimate_low"), 0) or None
    high = _safe_int(lead.get("rental_estimate_high"), 0) or None
    midpoint = int((low + high) / 2) if low and high else None
    return {"low": low, "high": high, "midpoint": midpoint, "yield_estimate": lead.get("yield_estimate")}


def build_lead_tools_payload(lead: Dict[str, Any], linked_portfolio_count: int = 1, linked_addresses: List[str] | None = None) -> Dict[str, Any]:
    linked_addresses = linked_addresses or []
    ownership_years = lead.get("ownership_duration_years") or _compute_ownership_years(lead)
    contact_tier, contact_reasons = _derive_contactability_tier(lead)
    contact_tier = lead.get("contactability_tier") or contact_tier
    contact_reasons = lead.get("contactability_reasons") or contact_reasons
    seller_signals = _list_of_dicts(lead.get("seller_intent_signals")) or _derive_seller_intent_signals(lead)
    refinance_signals = _list_of_dicts(lead.get("refinance_signals")) or _derive_refinance_signals(lead)
    value_range = _value_range(lead)
    rental_range = _rental_range(lead)
    nearby_sales = _list_of_dicts(lead.get("nearby_sales"))
    sale_history = _list_of_dicts(lead.get("sale_history"))
    listing_history = _list_of_dicts(lead.get("listing_status_history"))
    contact_numbers = [*(lead.get("contact_phones") or []), *(lead.get("alternate_phones") or [])]
    contact_emails = [*(lead.get("contact_emails") or []), *(lead.get("alternate_emails") or [])]
    comparables = [
        sale.get("sale_price")
        for sale in nearby_sales
        if isinstance(sale.get("sale_price"), (int, float))
    ]
    simple_sale_band = {
        "low": min(comparables) if comparables else None,
        "high": max(comparables) if comparables else None,
        "count": len(comparables),
    }
    last_sale = _safe_int(lead.get("sale_price"), 0) or None
    equity_estimate = None
    if value_range.get("midpoint") and last_sale:
        equity_estimate = max(0, value_range["midpoint"] - last_sale)

    contradictions: List[str] = []
    if lead.get("mailing_address") and lead.get("mailing_address_matches_property") and lead.get("absentee_owner"):
        contradictions.append("Mailing address is marked as matching the property while absentee_owner is true.")
    if lead.get("owner_type") == "investor" and lead.get("likely_owner_occupier"):
        contradictions.append("Owner type is investor but likely_owner_occupier is also true.")

    portfolio_clues = []
    if lead.get("absentee_owner"):
        portfolio_clues.append({"key": "absentee_owner", "label": "Absentee owner", "evidence": "Mailing address differs from property"})
    if linked_portfolio_count > 1:
        portfolio_clues.append({"key": "multi_property_owner", "label": "Multi-property linkage", "evidence": f"{linked_portfolio_count} records share owner or mailing identity"})

    return {
        "lead_id": lead.get("id"),
        "property_understanding": {
            "owner": {
                "owner_name": lead.get("owner_name"),
                "owner_persona": lead.get("owner_persona"),
                "owner_type": lead.get("owner_type"),
                "owner_occupancy_status": lead.get("owner_occupancy_status"),
                "absentee_owner": bool(lead.get("absentee_owner")),
                "likely_landlord": bool(lead.get("likely_landlord")),
            },
            "property": {
                "address": lead.get("address"),
                "canonical_address": lead.get("canonical_address"),
                "property_type": lead.get("property_type"),
                "bedrooms": lead.get("bedrooms"),
                "bathrooms": lead.get("bathrooms"),
                "car_spaces": lead.get("car_spaces"),
                "land_size_sqm": lead.get("land_size_sqm"),
                "floor_size_sqm": lead.get("floor_size_sqm"),
                "parcel_details": {
                    "parcel_details": lead.get("parcel_details"),
                    "parcel_lot": lead.get("parcel_lot"),
                    "parcel_plan": lead.get("parcel_plan"),
                    "title_reference": lead.get("title_reference"),
                },
            },
            "timeline": {
                "ownership_years": ownership_years,
                "sale_history": sale_history,
                "listing_history": listing_history,
                "last_listing_status": lead.get("last_listing_status"),
                "last_listing_date": lead.get("last_listing_date"),
            },
            "market_context": {
                "value_range": value_range,
                "rental_range": rental_range,
                "nearby_sales": nearby_sales[:5],
            },
        },
        "underwriting": {
            "value_range": value_range,
            "rental_range": rental_range,
            "simple_sale_band": simple_sale_band,
            "equity_estimate": equity_estimate,
            "assumptions": [
                assumption
                for assumption in [
                    "Value range defaults to +/-5% around est_value when no explicit valuation band exists." if value_range.get("midpoint") and not lead.get("estimated_value_low") else "",
                    "Equity estimate is gross uplift versus last recorded sale, not current debt." if equity_estimate is not None else "",
                ]
                if assumption
            ],
            "editable_inputs": {
                "estimated_value_low": lead.get("estimated_value_low"),
                "estimated_value_high": lead.get("estimated_value_high"),
                "rental_estimate_low": lead.get("rental_estimate_low"),
                "rental_estimate_high": lead.get("rental_estimate_high"),
            },
        },
        "contactability": {
            "tier": contact_tier,
            "do_not_call": bool(lead.get("do_not_call")),
            "consent_status": lead.get("consent_status"),
            "phone_status": lead.get("phone_status"),
            "phone_line_type": lead.get("phone_line_type"),
            "email_status": lead.get("email_status"),
            "numbers": contact_numbers,
            "emails": contact_emails,
            "reasons": contact_reasons,
            "last_verified_at": lead.get("enrichment_last_synced_at") or lead.get("updated_at"),
        },
        "portfolio": {
            "portfolio_property_count": linked_portfolio_count,
            "linked_addresses": linked_addresses[:8],
            "portfolio_clues": portfolio_clues,
        },
        "seller_intent": {
            "signals": seller_signals,
            "refinance_signals": refinance_signals,
        },
        "research_console": {
            "canonical_profile": {
                "owner_name": lead.get("owner_name"),
                "owner_first_name": lead.get("owner_first_name"),
                "owner_last_name": lead.get("owner_last_name"),
                "mailing_address": lead.get("mailing_address"),
                "contact_numbers": contact_numbers,
                "emails": contact_emails,
            },
            "raw_facts": {
                "source_tags": lead.get("source_tags") or [],
                "deterministic_tags": lead.get("deterministic_tags") or [],
                "ownership_notes": lead.get("ownership_notes"),
            },
            "source_provenance": _list_of_dicts(lead.get("source_provenance")),
            "contradictions": contradictions,
            "last_sync_times": {
                "enrichment_last_synced_at": lead.get("enrichment_last_synced_at"),
                "updated_at": lead.get("updated_at"),
            },
            "manual_verification_needed": [
                item
                for item in [
                    "Confirm mailing address against title records" if lead.get("mailing_address") else "",
                    "Verify rental estimate with current rental comps" if rental_range.get("midpoint") else "",
                    "Validate alternate contact points before outreach" if lead.get("alternate_phones") or lead.get("alternate_emails") else "",
                ]
                if item
            ],
        },
    }


async def build_lead_tools_payload_for_id(session: AsyncSession, lead_id: str) -> Dict[str, Any] | None:
    result = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    row = result.mappings().first()
    if not row:
        return None
    from core.logic import _hydrate_lead

    lead = _hydrate_lead(dict(row))
    owner_name = str(lead.get("owner_name") or "").strip()
    mailing_address = str(lead.get("mailing_address") or "").strip()
    linked = await session.execute(
        text(
            """
            SELECT address
            FROM leads
            WHERE id != :lead_id
              AND (
                (:owner_name != '' AND LOWER(COALESCE(owner_name, '')) = LOWER(:owner_name))
                OR (:mailing_address != '' AND LOWER(COALESCE(mailing_address, '')) = LOWER(:mailing_address))
              )
            ORDER BY updated_at DESC, created_at DESC
            """
        ),
        {"lead_id": lead_id, "owner_name": owner_name, "mailing_address": mailing_address},
    )
    linked_addresses = [str(item["address"]) for item in linked.mappings().all() if item.get("address")]
    return build_lead_tools_payload(lead, linked_portfolio_count=1 + len(linked_addresses), linked_addresses=linked_addresses)


async def build_suburb_opportunity_payload(session: AsyncSession, suburb: str) -> Dict[str, Any]:
    result = await session.execute(
        text("SELECT * FROM leads WHERE LOWER(COALESCE(suburb, '')) = LOWER(:suburb)"),
        {"suburb": suburb},
    )
    from core.logic import _hydrate_lead

    leads = [_hydrate_lead(dict(row)) for row in result.mappings().all()]
    absentee_count = sum(1 for lead in leads if lead.get("absentee_owner"))
    landlord_count = sum(1 for lead in leads if lead.get("likely_landlord"))
    contactable_count = sum(1 for lead in leads if (lead.get("contact_phones") or lead.get("contact_emails")) and not lead.get("do_not_call"))
    signal_count = sum(1 for lead in leads if lead.get("seller_intent_signals"))
    avg_value = [
        (_value_range(lead).get("midpoint"))
        for lead in leads
        if _value_range(lead).get("midpoint") is not None
    ]
    opportunity_reasons = []
    if absentee_count:
        opportunity_reasons.append(f"{absentee_count} absentee-owner records are present in {suburb}.")
    if landlord_count:
        opportunity_reasons.append(f"{landlord_count} likely landlord records are present in {suburb}.")
    if signal_count:
        opportunity_reasons.append(f"{signal_count} leads have deterministic seller-intent evidence.")

    return {
        "suburb": suburb.title(),
        "totals": {
            "lead_count": len(leads),
            "absentee_owner_count": absentee_count,
            "likely_landlord_count": landlord_count,
            "contactable_count": contactable_count,
            "seller_signal_count": signal_count,
            "average_value_midpoint": int(sum(avg_value) / len(avg_value)) if avg_value else None,
        },
        "opportunity_reasons": opportunity_reasons,
        "sample_leads": [
            {
                "lead_id": lead.get("id"),
                "address": lead.get("address"),
                "owner_name": lead.get("owner_name"),
                "contactability_tier": lead.get("contactability_tier"),
                "seller_intent_signal_count": len(lead.get("seller_intent_signals") or []),
            }
            for lead in leads[:10]
        ],
    }
