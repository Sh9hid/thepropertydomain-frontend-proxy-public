import datetime as dt

import pytest


def test_intelligence_functions_placeholder_import():
    from services.lead_intelligence_service import (  # noqa: F401
        build_lead_intelligence_snapshot,
        compute_company_owner_flag,
        compute_contactability_score,
        compute_equity_proxy,
        compute_listing_failure_signal,
        compute_ownership_years,
        determine_freshness_winner,
        extract_intelligence_tags,
        identity_key_for_lead,
    )


def test_extract_intelligence_tags_is_precise_and_grounded():
    from services.lead_intelligence_service import extract_intelligence_tags

    tags = extract_intelligence_tags(
        "Owner is interstate and asked for a call back. Tenant says this is the wrong number for the investor."
    )

    assert tags == ["call_back", "interstate_owner", "investor", "tenant", "wrong_number"]
    assert extract_intelligence_tags("The owner is interested in the property market.") == []


def test_compute_ownership_years_and_equity_proxy_handle_missing_inputs():
    from services.lead_intelligence_service import compute_equity_proxy, compute_ownership_years

    today = dt.date(2026, 3, 30)
    assert compute_ownership_years("2020-03-30", today=today) == 6.0
    assert compute_ownership_years(None, today=today) is None
    assert compute_equity_proxy(1450000, 930000) == 520000
    assert compute_equity_proxy(None, 930000) is None
    assert compute_equity_proxy(1450000, None) is None


def test_company_owner_and_listing_failure_are_deterministic():
    from services.lead_intelligence_service import compute_company_owner_flag, compute_listing_failure_signal

    assert compute_company_owner_flag("ACME HOLDINGS PTY LTD") is True
    assert compute_company_owner_flag("Jane Citizen Family Trust") is True
    assert compute_company_owner_flag("NSW Department of Housing") is False
    assert compute_company_owner_flag("Jane Citizen") is False

    assert compute_listing_failure_signal("withdrawn", []) == 1.0
    assert compute_listing_failure_signal(None, [{"status": "Expired"}]) == 1.0
    assert compute_listing_failure_signal("sold", [{"status": "sold"}]) == 0.0


def test_contactability_score_rewards_connected_and_callback_but_penalizes_wrong_number_and_staleness():
    from services.lead_intelligence_service import compute_contactability_score

    today = dt.datetime(2026, 3, 30, 10, 0, 0)
    strong = compute_contactability_score(
        call_attempts=3,
        connected_calls=1,
        last_contact_at="2026-03-29T08:00:00",
        last_outcome="call_back",
        tags=["call_back"],
        today=today,
    )
    weak = compute_contactability_score(
        call_attempts=6,
        connected_calls=0,
        last_contact_at="2025-12-01T08:00:00",
        last_outcome="wrong_number",
        tags=["wrong_number"],
        today=today,
    )

    assert strong > weak
    assert strong > 0
    assert weak == 0


def test_identity_key_uses_parcel_when_available_and_falls_back_to_address_plus_owner():
    from services.lead_intelligence_service import identity_key_for_lead

    lead = {
        "canonical_address": "10 EXAMPLE STREET WOONONA NSW 2517",
        "parcel_details": "Lot 1 DP 12345",
        "owner_name": "Jane Citizen",
    }
    assert identity_key_for_lead(lead) == "10 EXAMPLE STREET WOONONA NSW 2517|LOT 1 DP 12345|JANE CITIZEN"

    lead["parcel_details"] = None
    assert identity_key_for_lead(lead) == "10 EXAMPLE STREET WOONONA NSW 2517|JANE CITIZEN"


def test_determine_freshness_winner_prefers_newer_source_value_and_preserves_existing_on_tie():
    from services.lead_intelligence_service import determine_freshness_winner

    assert determine_freshness_winner(
        existing_value="House",
        existing_seen_at="2026-02-01T10:00:00",
        incoming_value="Duplex",
        incoming_seen_at="2026-03-01T10:00:00",
    ) == "Duplex"
    assert determine_freshness_winner(
        existing_value="House",
        existing_seen_at="2026-03-01T10:00:00",
        incoming_value="Duplex",
        incoming_seen_at="2026-03-01T10:00:00",
    ) == "House"


def test_build_lead_intelligence_snapshot_is_explainable_and_applies_decay():
    from services.lead_intelligence_service import build_lead_intelligence_snapshot

    today = dt.datetime(2026, 3, 30, 10, 0, 0)
    snapshot = build_lead_intelligence_snapshot(
        lead={
            "id": "lead-1",
            "canonical_address": "10 EXAMPLE STREET WOONONA NSW 2517",
            "address": "10 Example Street, Woonona NSW 2517",
            "suburb": "Woonona",
            "postcode": "2517",
            "owner_name": "ACME HOLDINGS PTY LTD",
            "owner_type": "company",
            "mailing_address": "PO Box 10 Sydney NSW 2000",
            "property_type": "House",
            "land_size_sqm": 650,
            "sale_date": "2016-02-01",
            "sale_price": "600000",
            "estimated_value_high": 1450000,
            "last_listing_status": "withdrawn",
            "contact_phones": ["0400111222"],
            "updated_at": "2026-02-01T09:00:00",
            "stage_note": "Investor owner based interstate, requested call back.",
        },
        property_profile={
            "same_owner_property_count": 3,
            "nearby_sales_count": 2,
            "agent_dominance_score": 0.65,
        },
        call_profile={
            "call_attempts": 2,
            "connected_calls": 1,
            "last_contact_at": "2026-03-29T11:00:00",
            "last_outcome": "call_back",
        },
        today=today,
    )

    assert snapshot["intent_score"] > 0
    assert snapshot["contactability_score"] > 0
    assert snapshot["priority_rank"] == pytest.approx(snapshot["intent_score"] * snapshot["contactability_score"], rel=1e-6)
    assert "absentee_owner" in snapshot["tags"]
    assert "investor" in snapshot["tags"]
    assert any(reason["code"] == "equity_proxy" for reason in snapshot["reasons"])
    assert any(reason["code"] == "stale_lead_decay" for reason in snapshot["reasons"])
