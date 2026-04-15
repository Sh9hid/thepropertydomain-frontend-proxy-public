import asyncio
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

from api.routes import leads
import core.config
import core.database as db_module
from models.sql_models import CallLog, Lead


app = FastAPI()
app.include_router(leads.router)


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine") / f"lead_intel_{uuid.uuid4().hex}.db"
    intelligence_db = Path(r"D:\woonona-lead-machine") / f"lead_intel_{uuid.uuid4().hex}.intelligence.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db.as_posix()}")
    monkeypatch.setattr(leads, "USE_POSTGRES", False)

    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    @event.listens_for(test_engine.sync_engine, "connect")
    def _attach_intelligence(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"ATTACH DATABASE '{intelligence_db.as_posix()}' AS intelligence")
        finally:
            cursor.close()
    test_factory = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(db_module, "_async_session_factory", test_factory)

    async def override_get_session():
        async with test_factory() as session:
            yield session

    app.dependency_overrides[db_module.get_session] = override_get_session

    async def init_models():
        async with test_engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

    asyncio.run(init_models())
    try:
        yield test_factory
    finally:
        app.dependency_overrides.pop(db_module.get_session, None)
        asyncio.run(test_engine.dispose())
        test_db.unlink(missing_ok=True)
        intelligence_db.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_sync_lead_intelligence_projects_profiles_links_and_rank_fields(isolated_db):
    from services.lead_intelligence_service import sync_lead_intelligence_for_lead

    async with isolated_db() as session:
        session.add(
            Lead(
                id="lead-proj-1",
                address="10 Example Street, Woonona NSW 2517",
                canonical_address="10 EXAMPLE STREET WOONONA NSW 2517",
                suburb="Woonona",
                postcode="2517",
                owner_name="ACME HOLDINGS PTY LTD",
                owner_type="company",
                parcel_details="Lot 1 DP 12345",
                property_type="House",
                land_size_sqm=650,
                sale_date="2016-02-01",
                sale_price="600000",
                estimated_value_high=1450000,
                agent_name="Sarah Agent",
                agency_name="Harbour Realty",
                mailing_address="PO Box 10 Sydney NSW 2000",
                last_listing_status="withdrawn",
                stage_note="Investor owner based interstate, requested call back.",
                contact_phones=["0400111222"],
                updated_at="2026-02-01T09:00:00",
                created_at="2026-02-01T09:00:00",
            )
        )
        session.add(
            CallLog(
                id="call-1",
                lead_id="lead-proj-1",
                lead_address="10 Example Street, Woonona NSW 2517",
                outcome="call_back",
                connected=True,
                timestamp="2026-03-29T10:00:00",
                logged_at="2026-03-29T10:00:00",
                logged_date="2026-03-29",
                call_duration_seconds=210,
            )
        )
        await session.commit()

        snapshot = await sync_lead_intelligence_for_lead(session, "lead-proj-1", as_of="2026-03-30T10:00:00")
        await session.commit()

        assert snapshot["intent_score"] > 0
        assert snapshot["contactability_score"] > 0

        property_count = (
            await session.execute(text("SELECT COUNT(*) FROM intelligence.property WHERE address = :address"), {"address": "10 Example Street, Woonona NSW 2517"})
        ).scalar_one()
        person_count = (await session.execute(text("SELECT COUNT(*) FROM intelligence.party"))).scalar_one()
        link_count = (await session.execute(text("SELECT COUNT(*) FROM intelligence.property_party"))).scalar_one()
        agent_count = (
            await session.execute(text("SELECT COUNT(*) FROM intelligence.agent_profile WHERE agent_name = 'Sarah Agent' AND agency_name = 'Harbour Realty'"))
        ).scalar_one()
        intelligence_row = (
            await session.execute(text("SELECT tags_json, reasons_json, priority_rank FROM intelligence.lead_intelligence WHERE property_id = :property_id"), {"property_id": snapshot["property_id"]})
        ).mappings().one()

        assert property_count == 1
        assert person_count == 1
        assert link_count == 1
        assert agent_count == 1
        assert "absentee_owner" in intelligence_row["tags_json"]
        assert "equity_proxy" in intelligence_row["reasons_json"]
        assert float(intelligence_row["priority_rank"]) == snapshot["priority_rank"]


@pytest.mark.asyncio
async def test_sync_lead_intelligence_is_idempotent_and_owner_rollup_counts_multiple_properties(isolated_db):
    from services.lead_intelligence_service import sync_all_lead_intelligence

    async with isolated_db() as session:
        session.add_all(
            [
                Lead(
                    id="lead-a",
                    address="10 Example Street, Woonona NSW 2517",
                    canonical_address="10 EXAMPLE STREET WOONONA NSW 2517",
                    suburb="Woonona",
                    postcode="2517",
                    owner_name="Jane Citizen",
                    parcel_details="Lot 1 DP 12345",
                    property_type="House",
                    sale_date="2018-02-01",
                    sale_price="700000",
                    estimated_value_high=1200000,
                    updated_at="2026-03-01T09:00:00",
                    created_at="2026-03-01T09:00:00",
                ),
                Lead(
                    id="lead-b",
                    address="12 Example Street, Woonona NSW 2517",
                    canonical_address="12 EXAMPLE STREET WOONONA NSW 2517",
                    suburb="Woonona",
                    postcode="2517",
                    owner_name="Jane Citizen",
                    parcel_details=None,
                    property_type="House",
                    sale_date="2017-02-01",
                    sale_price="650000",
                    estimated_value_high=1100000,
                    updated_at="2026-03-05T09:00:00",
                    created_at="2026-03-05T09:00:00",
                ),
            ]
        )
        await session.commit()

        first = await sync_all_lead_intelligence(session, as_of="2026-03-30T10:00:00")
        await session.commit()
        second = await sync_all_lead_intelligence(session, as_of="2026-03-30T10:00:00")
        await session.commit()

        property_count = (await session.execute(text("SELECT COUNT(*) FROM intelligence.property"))).scalar_one()
        party_count = (await session.execute(text("SELECT COUNT(*) FROM intelligence.party"))).scalar_one()
        link_count = (await session.execute(text("SELECT COUNT(*) FROM intelligence.property_party"))).scalar_one()
        rollup = (
            await session.execute(text("SELECT same_owner_property_count FROM intelligence.lead_intelligence ORDER BY property_id LIMIT 1"))
        ).scalar_one()

        assert first["processed"] == 2
        assert second["processed"] == 2
        assert property_count == 2
        assert party_count == 1
        assert link_count == 2
        assert rollup == 2


@pytest.mark.asyncio
async def test_api_leads_exposes_priority_rank_tags_and_reasons_after_sync(isolated_db):
    from services.lead_intelligence_service import sync_all_lead_intelligence

    async with isolated_db() as session:
        session.add(
            Lead(
                id="lead-api-1",
                address="10 Example Street, Woonona NSW 2517",
                canonical_address="10 EXAMPLE STREET WOONONA NSW 2517",
                suburb="Woonona",
                postcode="2517",
                owner_name="ACME HOLDINGS PTY LTD",
                owner_type="company",
                parcel_details="Lot 1 DP 12345",
                property_type="House",
                sale_date="2016-02-01",
                sale_price="600000",
                estimated_value_high=1450000,
                mailing_address="PO Box 10 Sydney NSW 2000",
                last_listing_status="withdrawn",
                stage_note="Investor owner based interstate, requested call back.",
                contact_phones=["0400111222"],
                updated_at="2026-02-01T09:00:00",
                created_at="2026-02-01T09:00:00",
            )
        )
        await session.commit()
        await sync_all_lead_intelligence(session, as_of="2026-03-30T10:00:00")
        await session.commit()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/leads", headers={"X-API-KEY": core.config.API_KEY})

    assert response.status_code == 200
    lead = response.json()["leads"][0]
    assert lead["priority_rank"] > 0
    assert "investor" in lead["intelligence_tags"]
    assert any(reason["code"] == "listing_failure_signal" for reason in lead["intelligence_reasons"])
