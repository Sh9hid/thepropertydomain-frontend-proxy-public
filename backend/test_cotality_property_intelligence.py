from api.routes import enrichment
from models import schemas, sql_models


def test_cotality_property_intelligence_contract_surfaces_rich_sections_and_structured_fields():
    expected_fields = {
        "property_type",
        "bedrooms",
        "bathrooms",
        "car_spaces",
        "land_size_sqm",
        "building_size_sqm",
        "year_built",
        "ownership_duration_years",
        "tenure_bucket",
        "owner_occupancy_status",
        "absentee_owner",
        "likely_landlord",
        "likely_owner_occupier",
        "estimated_value_low",
        "estimated_value_high",
        "valuation_date",
        "rental_estimate_low",
        "rental_estimate_high",
        "yield_estimate",
        "last_sale_price",
        "last_sale_date",
        "sale_history",
        "last_listing_status",
        "last_listing_date",
        "listing_status_history",
        "nearby_sales",
        "ownership_notes",
        "source_evidence",
        "summary_points",
        "key_details",
        "seller_intent_signals",
        "refinance_signals",
    }

    assert sql_models.COTALITY_PROPERTY_INTELLIGENCE_WORKFLOW_NAME == "cotality_full_enrich"
    assert expected_fields.issubset(set(sql_models.COTALITY_PROPERTY_INTELLIGENCE_ALLOWED_FIELDS))
    assert sql_models.COTALITY_PROPERTY_INTELLIGENCE_FIELD_TO_LEAD_COLUMN["nearby_sales"] == "nearby_sales"
    assert sql_models.COTALITY_PROPERTY_INTELLIGENCE_FIELD_TO_LEAD_COLUMN["sale_history"] == "sale_history"
    assert sql_models.COTALITY_PROPERTY_INTELLIGENCE_FIELD_TO_LEAD_COLUMN["rental_estimate_low"] == "rental_estimate_low"
    assert tuple(sql_models.COTALITY_PROPERTY_INTELLIGENCE_RAW_SECTION_NAMES) == (
        "property_overview",
        "valuation",
        "sale_history",
        "listing_history",
        "nearby_sales",
        "mortgage_signals",
    )
    assert enrichment.ALLOWED_COTALITY_FIELDS == sql_models.COTALITY_PROPERTY_INTELLIGENCE_ALLOWED_FIELDS
    assert enrichment.PROPOSED_TO_LEAD_FIELD == sql_models.COTALITY_PROPERTY_INTELLIGENCE_FIELD_TO_LEAD_COLUMN


def test_cotality_property_intelligence_models_round_trip_raw_sections_and_updates():
    raw_payload = schemas.CotalityPropertyIntelligenceRawPayload(
        sections={
            "property_overview": {"text": "House 4 bed 2 bath"},
            "nearby_sales": {
                "rows": [
                    {"address": "1 Test St", "price": "$1,200,000"},
                ]
            },
        },
        discovered_tabs=["Overview", "Sales", "Valuation"],
        section_order=["property_overview", "nearby_sales"],
    )

    structured_updates = schemas.CotalityPropertyIntelligenceStructuredUpdates(
        property_type="House",
        bedrooms=4,
        nearby_sales=[{"address": "1 Test St", "price": 1200000}],
        seller_intent_signals=[{"signal": "recently relisted"}],
        refinance_signals=[{"signal": "fixed-rate ending soon"}],
    )

    result = schemas.CotalityPropertyIntelligenceResult(
        matched_address="10 Example Street, Woonona NSW 2517",
        raw_payload_json=raw_payload,
        proposed_updates_json=structured_updates,
        confidence=0.92,
        final_status="review_required",
    )

    assert result.raw_payload_json.sections["nearby_sales"]["rows"][0]["address"] == "1 Test St"
    assert result.raw_payload_json.discovered_tabs == ["Overview", "Sales", "Valuation"]
    assert result.proposed_updates_json.nearby_sales[0]["price"] == 1200000
    assert result.proposed_updates_json.seller_intent_signals[0]["signal"] == "recently relisted"
    assert result.model_dump()["raw_payload_json"]["workflow_name"] == "cotality_full_enrich"


def test_enrichment_result_request_uses_contract_models():
    request = enrichment.EnrichmentResultRequest(
        matched_address="10 Example Street, Woonona NSW 2517",
        raw_payload_json={
            "sections": {
                "listing_history": {"rows": [{"status": "listed", "date": "2025-02-01"}]},
            },
            "discovered_tabs": ["Listing history"],
            "section_order": ["listing_history"],
        },
        proposed_updates_json={
            "listing_status_history": [{"status": "listed", "date": "2025-02-01"}],
            "ownership_notes": "Owner appears to be keeping the property on market.",
            "source_evidence": ["Listing history tab"],
        },
    )

    assert isinstance(request.raw_payload_json, schemas.CotalityPropertyIntelligenceRawPayload)
    assert isinstance(request.proposed_updates_json, schemas.CotalityPropertyIntelligenceStructuredUpdates)
    assert request.raw_payload_json.sections["listing_history"]["rows"][0]["status"] == "listed"
    assert request.proposed_updates_json.listing_status_history[0]["status"] == "listed"
