import asyncio
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import core.config
import core.database as db_module
from api.routes import leads as leads_router
from core.config import API_KEY
from core.logic import _hydrate_lead
from models.sql_models import Lead


app = FastAPI()
app.include_router(leads_router.router)


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine\backend\test_dbs") / f"lead-tools-{uuid.uuid4().hex}.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db.as_posix()}")

    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(
        db_module,
        "_async_session_factory",
        sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False),
    )
    db_module.init_db()
    yield test_db
    asyncio.run(test_engine.dispose())
    try:
        test_db.unlink(missing_ok=True)
    except PermissionError:
        pass


async def seed_lead(session: AsyncSession) -> None:
    session.add(
        Lead(
            id="lead-tools-1",
            address="12 Example Street, Windsor NSW 2756",
            suburb="Windsor",
            postcode="2756",
            owner_name="Jane Example",
            trigger_type="marketing_list",
            status="qualified",
            contact_phones=["0411222333"],
            contact_emails=["jane@example.com"],
            owner_type="investor",
            property_type="House",
            land_size_sqm=720,
            floor_size_sqm=180,
            bedrooms=4,
            bathrooms=2,
            car_spaces=2,
            sale_price="875000",
            sale_date="2016-02-03",
            settlement_date="2016-03-01",
            est_value=1180000,
            days_on_market=52,
            last_contacted_at="2026-03-01T10:30:00+11:00",
            route_queue="real_estate",
            contactability_status="phone_ready",
            source_tags=["marketing_csv", "domain"],
            source_evidence=["Domain valuation refreshed 2026-03-24"],
            linked_files=["windsor-owner-report.pdf"],
            created_at="2026-03-24T08:00:00+11:00",
            updated_at="2026-03-28T08:00:00+11:00",
            mailing_address="PO Box 77, Parramatta NSW 2150",
            mailing_address_matches_property=False,
            absentee_owner=True,
            likely_landlord=True,
            owner_occupancy_status="absentee_owner",
            owner_first_name="Jane",
            owner_last_name="Example",
            owner_persona="investor_landlord",
            alternate_phones=["0298765432"],
            alternate_emails=["accounts@example.com"],
            phone_status="verified",
            phone_line_type="mobile",
            email_status="deliverable",
            do_not_call=False,
            consent_status="unknown",
            contactability_tier="high",
            contactability_reasons=["Verified mobile", "Secondary landline on file"],
            ownership_duration_years=10.0,
            tenure_bucket="10y_plus",
            estimated_value_low=1140000,
            estimated_value_high=1210000,
            valuation_date="2026-03-24",
            rental_estimate_low=760,
            rental_estimate_high=820,
            yield_estimate=3.5,
            last_listing_status="withdrawn",
            last_listing_date="2025-12-15",
            sale_history=[{"date": "2016-02-03", "price": 875000, "source": "cotality"}],
            listing_status_history=[
                {"status": "listed", "date": "2025-10-01", "source": "domain"},
                {"status": "withdrawn", "date": "2025-12-15", "source": "domain"},
            ],
            nearby_sales=[
                {"address": "8 Sample Ave, Windsor NSW 2756", "sale_price": 1215000, "sale_date": "2026-02-01"},
                {"address": "4 River Rd, Windsor NSW 2756", "sale_price": 1160000, "sale_date": "2026-01-20"},
            ],
            deterministic_tags=["absentee_owner", "landlord_signal", "long_tenure"],
            seller_intent_signals=[
                {"key": "long_ownership", "label": "Long ownership", "evidence": "Held since 2016-03-01"},
                {"key": "absentee_owner", "label": "Absentee owner", "evidence": "Mailing address differs from property"},
            ],
            refinance_signals=[
                {"key": "equity_position", "label": "Likely equity position", "evidence": "Estimated value materially above last sale"},
            ],
            ownership_notes="Investor-owner pattern from deterministic address and tenure data.",
            source_provenance=[
                {"field": "sale_date", "source_name": "cotality_xlsx", "source_type": "file", "verification_status": "verified"},
                {"field": "estimated_value_high", "source_name": "domain", "source_type": "api", "verification_status": "fetched"},
            ],
            enrichment_status="ready",
            enrichment_last_synced_at="2026-03-28T08:00:00+11:00",
            research_status="needs_manual_review",
        )
    )
    await session.commit()


def test_hydrate_lead_exposes_intelligence_fields_and_signals():
    lead = _hydrate_lead(
        {
            "id": "lead-hydrate-1",
            "address": "99 Hydrate Avenue, Windsor NSW 2756",
            "suburb": "Windsor",
            "postcode": "2756",
            "owner_name": "John Hydrate",
            "contact_phones": '["0411000000"]',
            "contact_emails": '["john@example.com"]',
            "owner_type": "investor",
            "mailing_address": "PO Box 5, Richmond NSW 2753",
            "mailing_address_matches_property": 0,
            "absentee_owner": 1,
            "likely_landlord": 1,
            "ownership_duration_years": 9.4,
            "tenure_bucket": "7_10y",
            "contactability_tier": "high",
            "contactability_reasons": '["Verified mobile"]',
            "seller_intent_signals": '[{"key":"absentee_owner","label":"Absentee owner","evidence":"Mailing address differs"}]',
            "source_provenance": '[{"field":"owner_name","source_name":"marketing_csv"}]',
            "source_tags": '["marketing_csv"]',
            "risk_flags": "[]",
            "activity_log": "[]",
            "stage_note_history": "[]",
            "property_images": "[]",
            "linked_files": "[]",
            "source_evidence": "[]",
        }
    )

    assert lead["absentee_owner"] is True
    assert lead["likely_landlord"] is True
    assert lead["contactability_tier"] == "high"
    assert lead["contactability_reasons"] == ["Verified mobile"]
    assert lead["seller_intent_signals"][0]["key"] == "absentee_owner"
    assert lead["source_provenance"][0]["field"] == "owner_name"


@pytest.mark.asyncio
async def test_lead_tools_endpoint_returns_operator_modules(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_lead(session)

    headers = {"X-API-KEY": API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/leads/lead-tools-1/tools", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["lead_id"] == "lead-tools-1"
    assert payload["property_understanding"]["owner"]["owner_persona"] == "investor_landlord"
    assert payload["contactability"]["tier"] == "high"
    assert payload["underwriting"]["value_range"]["high"] == 1210000
    assert payload["portfolio"]["portfolio_clues"][0]["key"] == "absentee_owner"
    assert payload["seller_intent"]["signals"][0]["key"] == "long_ownership"
    assert payload["research_console"]["source_provenance"][0]["field"] == "sale_date"


@pytest.mark.asyncio
async def test_suburb_opportunity_endpoint_returns_deterministic_rollup(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_lead(session)
        session.add(
            Lead(
                id="lead-tools-2",
                address="44 Investor Parade, Windsor NSW 2756",
                suburb="Windsor",
                postcode="2756",
                owner_name="Sam Investor",
                status="captured",
                owner_type="investor",
                absentee_owner=True,
                likely_landlord=True,
                contact_phones=["0400000000"],
                contactability_tier="medium",
                ownership_duration_years=8.2,
                tenure_bucket="7_10y",
                last_contacted_at="2026-02-15T10:30:00+11:00",
                seller_intent_signals=[
                    {"key": "long_ownership", "label": "Long ownership", "evidence": "Held for more than 8 years"}
                ],
                estimated_value_low=920000,
                estimated_value_high=980000,
                created_at="2026-03-20T08:00:00+11:00",
                updated_at="2026-03-28T08:00:00+11:00",
            )
        )
        await session.commit()

    headers = {"X-API-KEY": API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/tools/suburb-opportunity?suburb=Windsor", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["suburb"] == "Windsor"
    assert payload["totals"]["lead_count"] == 2
    assert payload["totals"]["absentee_owner_count"] == 2
    assert payload["totals"]["likely_landlord_count"] == 2
    assert payload["totals"]["contactable_count"] == 2
    assert payload["opportunity_reasons"]
