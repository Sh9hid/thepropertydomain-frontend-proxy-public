import asyncio
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
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
    test_db = Path(r"D:\woonona-lead-machine") / f"id4me_queue_{uuid.uuid4().hex}.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db.as_posix()}")
    monkeypatch.setattr(core.config, "ENRICHMENT_MACHINE_TOKEN", "machine-secret")
    monkeypatch.setattr(enrichment.config, "ENRICHMENT_MACHINE_TOKEN", "machine-secret")

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


async def _seed_lead(session: AsyncSession):
    session.add(
        Lead(
            id="lead-id4me-1",
            address="10 Example Street",
            suburb="Woonona",
            state="NSW",
            postcode="2517",
            owner_name="Existing Owner",
            created_at="2026-04-01T08:00:00+00:00",
            updated_at="2026-04-01T08:00:00+00:00",
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_id4me_job_lifecycle_persists_manual_enrichment_result(isolated_db):
    async with db_module._async_session_factory() as session:
        await _seed_lead(session)

    headers = {"X-API-KEY": API_KEY}
    machine_headers = {
        "X-Enrichment-Machine-Token": "machine-secret",
        "X-Enrichment-Machine-Id": "remote-id4me-1",
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        queued = await ac.post("/api/leads/lead-id4me-1/enrich/id4me", headers=headers, json={})
        assert queued.status_code == 200
        job_id = queued.json()["job_id"]

        claimed = await ac.get("/api/enrichment-jobs/next?provider=id4me", headers=machine_headers)
        assert claimed.status_code == 200
        claim_body = claimed.json()
        assert claim_body["job"]["id"] == job_id
        assert claim_body["job"]["provider"] == "id4me"
        assert claim_body["lead"]["address"] == "10 Example Street"

        submitted = await ac.post(
            f"/api/enrichment-jobs/{job_id}/id4me-result",
            headers=machine_headers,
            json={
                "status": "completed",
                "matched_address": "10 Example Street, Woonona NSW 2517",
                "payload": {
                    "owner_name": "Tanaya Luscombe",
                    "phones": ["0426206506"],
                    "emails": ["t.luscombe@hotmail.com"],
                    "date_of_birth": "1995-01-24",
                    "last_seen": "24-Jan-2025",
                },
                "raw_result": {
                    "status": "ok",
                    "count": 1,
                    "file": "results/export_123.csv",
                },
                "csv_path": "results/export_123.csv",
            },
        )
        assert submitted.status_code == 200
        assert submitted.json()["status"] == "completed"

        status = await ac.get("/api/leads/lead-id4me-1/enrich/id4me/status", headers=headers)
        assert status.status_code == 200
        status_body = status.json()
        assert status_body["job"]["status"] == "completed"
        assert status_body["result"]["source"] == "id4me"
        assert status_body["result"]["proposed_updates_json"]["owner_name"] == "Tanaya Luscombe"
        assert bool(status_body["lead"]["id4me_enriched"]) is True
        assert status_body["lead"]["contact_phones"] == ["0426206506"]


@pytest.mark.asyncio
async def test_id4me_job_status_handles_no_results(isolated_db):
    async with db_module._async_session_factory() as session:
        await _seed_lead(session)

    headers = {"X-API-KEY": API_KEY}
    machine_headers = {
        "X-Enrichment-Machine-Token": "machine-secret",
        "X-Enrichment-Machine-Id": "remote-id4me-1",
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        queued = await ac.post("/api/leads/lead-id4me-1/enrich/id4me", headers=headers, json={})
        assert queued.status_code == 200
        job_id = queued.json()["job_id"]

        claimed = await ac.get("/api/enrichment-jobs/next?provider=id4me", headers=machine_headers)
        assert claimed.status_code == 200

        updated = await ac.post(
            f"/api/enrichment-jobs/{job_id}/status",
            headers=machine_headers,
            json={
                "status": "no_results",
                "matched_address": "10 Example Street, Woonona NSW 2517",
                "note": "Search completed but nothing exported",
            },
        )
        assert updated.status_code == 200

        status = await ac.get("/api/leads/lead-id4me-1/enrich/id4me/status", headers=headers)
        assert status.status_code == 200
        status_body = status.json()
        assert status_body["job"]["status"] == "no_results"
        assert status_body["lead"]["id4me_enriched"] in (False, None)
