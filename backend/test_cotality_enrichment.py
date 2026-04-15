import asyncio
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

import core.config
import core.database as db_module
from api.routes import enrichment
from core.config import API_KEY
from models.sql_models import EnrichmentJob, EnrichmentResult, Lead


app = FastAPI()
app.include_router(enrichment.router)


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine") / f"cotality_enrichment_{uuid.uuid4().hex}.db"
    profile_path = Path(r"D:\woonona-lead-machine") / f"cotality_profile_{uuid.uuid4().hex}.json"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db.as_posix()}")
    monkeypatch.setattr(core.config, "ENRICHMENT_MACHINE_TOKEN", "machine-secret")
    monkeypatch.setattr(enrichment.config, "ENRICHMENT_MACHINE_TOKEN", "machine-secret")
    monkeypatch.setattr(enrichment, "COTALITY_PROFILE_PATH", profile_path)

    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    test_factory = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(db_module, "_async_session_factory", test_factory)

    async def override_get_session():
        async with test_factory() as session:
            yield session

    app.dependency_overrides[db_module.get_session] = override_get_session

    async def init_models():
        async with test_engine.begin() as conn:
            await conn.run_sync(
                SQLModel.metadata.create_all,
                tables=[Lead.__table__, EnrichmentJob.__table__, EnrichmentResult.__table__],
            )

    asyncio.run(init_models())
    try:
        yield test_db
    finally:
        app.dependency_overrides.pop(db_module.get_session, None)
        asyncio.run(test_engine.dispose())
        test_db.unlink(missing_ok=True)
        profile_path.unlink(missing_ok=True)


async def _seed_lead(session: AsyncSession):
    session.add(
        Lead(
            id="lead-cotality-1",
            address="10 Example Street, Woonona NSW 2517",
            suburb="Woonona",
            postcode="2517",
            owner_name="Existing Owner",
            property_type="House",
            bedrooms=3,
            bathrooms=1,
            car_spaces=1,
            land_size_sqm=520,
            floor_size_sqm=140,
            sale_price="950000",
            sale_date="2023-01-01",
            estimated_value_low=980000,
            estimated_value_high=1010000,
            created_at="2026-03-30T08:00:00+00:00",
            updated_at="2026-03-30T08:00:00+00:00",
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_cotality_job_lifecycle_and_apply_selected_fields(isolated_db):
    async with db_module._async_session_factory() as session:
        await _seed_lead(session)

    headers = {"X-API-KEY": API_KEY}
    machine_headers = {
        "X-Enrichment-Machine-Token": "machine-secret",
        "X-Enrichment-Machine-Id": "local-runner-1",
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        queued = await ac.post(
            "/api/leads/lead-cotality-1/enrich/cotality",
            headers=headers,
            json={"requested_fields": ["property_type", "building_size_sqm", "last_sale_price", "estimated_value_high"]},
        )
        assert queued.status_code == 200
        job_id = queued.json()["job_id"]

        claimed = await ac.get("/api/enrichment-jobs/next", headers=machine_headers)
        assert claimed.status_code == 200
        claim_body = claimed.json()
        assert claim_body["job"]["id"] == job_id
        assert claim_body["lead"]["address"] == "10 Example Street, Woonona NSW 2517"

        submitted = await ac.post(
            f"/api/enrichment-jobs/{job_id}/result",
            headers=machine_headers,
            json={
                "matched_address": "10 Example Street, Woonona NSW 2517",
                "raw_payload_json": {"raw_text": "Valuation $1,180,000"},
                "proposed_updates_json": {
                    "property_type": "Duplex",
                    "building_size_sqm": 172,
                    "last_sale_price": 1125000,
                    "estimated_value_high": 1180000,
                    "ignored_field": "x",
                },
                "confidence": 0.92,
                "screenshot_path": "backend/artifacts/cotality/test.png",
                "status": "completed",
            },
        )
        assert submitted.status_code == 200
        assert submitted.json()["status"] == "review_required"

        status = await ac.get("/api/leads/lead-cotality-1/enrich/cotality/status", headers=headers)
        assert status.status_code == 200
        status_body = status.json()
        assert status_body["job"]["status"] == "review_required"
        assert status_body["result"]["proposed_updates_json"]["property_type"] == "Duplex"
        assert "ignored_field" not in status_body["result"]["proposed_updates_json"]

        applied = await ac.post(
            "/api/leads/lead-cotality-1/enrich/cotality/apply",
            headers=headers,
            json={"fields": ["property_type", "building_size_sqm", "last_sale_price", "estimated_value_high"]},
        )
        assert applied.status_code == 200
        assert applied.json()["status"] == "completed"
        assert applied.json()["lead"]["lead_state"] == "needs_enrichment"
        assert applied.json()["lead"]["next_action"]["type"] == "enrichment"

    async with db_module._async_session_factory() as session:
        lead = await session.get(Lead, "lead-cotality-1")
        assert lead is not None
        assert lead.property_type == "Duplex"
        assert lead.floor_size_sqm == 172
        assert lead.sale_price == "1125000"
        assert lead.estimated_value_high == 1180000
        assert lead.enrichment_status == "completed"

        stored_job = await session.get(EnrichmentJob, job_id)
        assert stored_job is not None
        assert stored_job.status == "completed"

        stored_results = (
            await session.execute(text("SELECT COUNT(*) FROM enrichment_results WHERE enrichment_job_id = :job_id"), {"job_id": job_id})
        ).scalar_one()
        assert stored_results == 1


@pytest.mark.asyncio
async def test_cotality_job_status_preserves_rich_property_intelligence_payload(isolated_db):
    async with db_module._async_session_factory() as session:
        await _seed_lead(session)

    headers = {"X-API-KEY": API_KEY}
    machine_headers = {
        "X-Enrichment-Machine-Token": "machine-secret",
        "X-Enrichment-Machine-Id": "local-runner-1",
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        queued = await ac.post(
            "/api/leads/lead-cotality-1/enrich/cotality",
            headers=headers,
            json={"requested_fields": ["nearby_sales", "seller_intent_signals", "rental_estimate_low", "yield_estimate"]},
        )
        assert queued.status_code == 200
        job_id = queued.json()["job_id"]

        submitted = await ac.post(
            f"/api/enrichment-jobs/{job_id}/result",
            headers=machine_headers,
            json={
                "matched_address": "10 Example Street, Woonona NSW 2517",
                "raw_payload_json": {
                    "workflow_name": "cotality_full_enrich",
                    "discovered_tabs": ["overview", "sales", "comparables"],
                    "section_order": ["property_overview", "nearby_sales", "mortgage_signals"],
                    "sections": {
                        "nearby_sales": {
                            "rows": [{"address": "1 Smith St", "sold_price": "$1,200,000"}]
                        }
                    },
                },
                "proposed_updates_json": {
                    "nearby_sales": [{"address": "1 Smith St", "sold_price": 1200000, "beds": 4}],
                    "seller_intent_signals": [{"signal": "high_equity", "confidence": "high"}],
                    "rental_estimate_low": 780,
                    "yield_estimate": 3.8,
                },
                "confidence": 0.91,
                "final_status": "review_required",
            },
        )
        assert submitted.status_code == 200

        status = await ac.get("/api/leads/lead-cotality-1/enrich/cotality/status", headers=headers)
        assert status.status_code == 200
        payload = status.json()
        assert payload["result"]["raw_payload_json"]["discovered_tabs"] == ["overview", "sales", "comparables"]
        assert payload["result"]["proposed_updates_json"]["nearby_sales"][0]["address"] == "1 Smith St"
        assert payload["result"]["proposed_updates_json"]["seller_intent_signals"][0]["signal"] == "high_equity"

        applied = await ac.post(
            "/api/leads/lead-cotality-1/enrich/cotality/apply",
            headers=headers,
            json={"fields": ["nearby_sales", "seller_intent_signals", "rental_estimate_low", "yield_estimate"]},
        )
        assert applied.status_code == 200
        applied_lead = applied.json()["lead"]
        assert applied_lead["nearby_sales"][0]["address"] == "1 Smith St"
        assert applied_lead["seller_intent_signals"][0]["signal"] == "high_equity"
        assert applied_lead["rental_estimate_low"] == 780
        assert applied_lead["yield_estimate"] == 3.8
