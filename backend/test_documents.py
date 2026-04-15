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
from api.routes import documents
from core.config import API_KEY
from models.sql_models import CallLog, Lead, LeadNote, PriceGuidanceLog, SoldEvent
from services import document_service


app = FastAPI()
app.include_router(documents.router)


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine") / f"documents_{uuid.uuid4().hex}.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db.as_posix()}")

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
            await conn.run_sync(SQLModel.metadata.create_all)

    asyncio.run(init_models())
    try:
        yield test_db
    finally:
        app.dependency_overrides.pop(db_module.get_session, None)
        asyncio.run(test_engine.dispose())
        if test_db.exists():
            test_db.unlink(missing_ok=True)


async def _seed_sparse_lead(session: AsyncSession) -> None:
    session.add(
        Lead(
            id="lead-doc-1",
            address="10 Example Street, Woonona NSW 2517",
            suburb="Woonona",
            owner_name="",
            trigger_type="withdrawn",
            signal_status="ACTIVE",
            status="qualified",
            why_now="Property has been sitting without movement.",
            days_on_market=57,
            created_at="2026-03-20T09:00:00+11:00",
            updated_at="2026-03-28T10:00:00+11:00",
            source_evidence=["Listing has extended time on market."],
            activity_log=[{"created_at": "2026-03-27T10:30:00+11:00", "note": "Owner requested a price reality check."}],
        )
    )
    session.add(
        CallLog(
            id="call-doc-1",
            lead_id="lead-doc-1",
            lead_address="10 Example Street, Woonona NSW 2517",
            outcome="connected_interested",
            connected=True,
            timestamp="2026-03-27T10:00:00+11:00",
            logged_at="2026-03-27T10:00:00+11:00",
            logged_date="2026-03-27",
            note="Owner is open to practical pricing guidance.",
            summary="Seller is comparing agent opinions and wants evidence.",
        )
    )
    session.add(
        LeadNote(
            lead_id="lead-doc-1",
            note_type="operator_note",
            content="Owner hinted that timing depends on confidence in pricing.",
            created_at="2026-03-27T12:00:00+11:00",
        )
    )
    await session.commit()


async def _seed_comp_data(session: AsyncSession) -> None:
    session.add(
        PriceGuidanceLog(
            id="pg-1",
            lead_id="lead-doc-1",
            kind="sales",
            status="approved",
            estimate_low=1080000,
            estimate_high=1160000,
            rationale="Based on current vendor expectations and nearby evidence.",
            comparables=[
                {
                    "address": "8 Nearby Street, Woonona NSW 2517",
                    "sale_price": 1115000,
                    "sale_date": "2026-02-14",
                    "source": "price_guidance",
                }
            ],
            created_at="2026-03-28T09:00:00+11:00",
            updated_at="2026-03-28T09:00:00+11:00",
        )
    )
    session.add(
        SoldEvent(
            id="sold-1",
            address="12 Sold Street, Woonona NSW 2517",
            suburb="Woonona",
            postcode="2517",
            sale_price="1095000",
            sale_date="2026-02-01",
            source_name="seed",
            match_reason="same_suburb",
            matched_lead_ids=[],
            created_at="2026-03-28T09:00:00+11:00",
            updated_at="2026-03-28T09:00:00+11:00",
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_generate_sales_advice_returns_structured_json_with_sparse_data(isolated_db, monkeypatch):
    async with db_module._async_session_factory() as session:
        await _seed_sparse_lead(session)

    monkeypatch.setattr(document_service, "_render_pdf", lambda *args, **kwargs: None)
    payload = await document_service.generate_sales_advice("lead-doc-1")

    assert payload["document_type"] == "sales_advice"
    assert payload["lead_id"] == "lead-doc-1"
    assert payload["data"]["property_summary"]["address"] == "10 Example Street, Woonona NSW 2517"
    assert payload["data"]["opportunity"]["trigger_type"] == "withdrawn"
    assert payload["data"]["key_evidence"]
    assert payload["data"]["strategy_notes"]


@pytest.mark.asyncio
async def test_generate_cma_omits_fake_fields_when_data_missing(isolated_db, monkeypatch):
    async with db_module._async_session_factory() as session:
        await _seed_sparse_lead(session)

    monkeypatch.setattr(document_service, "_render_pdf", lambda *args, **kwargs: None)
    payload = await document_service.generate_cma("lead-doc-1")

    subject = payload["data"]["subject_property"]
    assert "owner_name" not in subject
    assert "pricing" not in payload["data"]
    assert payload["data"]["notes"]
    assert "Comparable sales" in payload["data"]["notes"][0]


@pytest.mark.asyncio
async def test_generate_seller_insight_includes_html_preview(isolated_db, monkeypatch):
    async with db_module._async_session_factory() as session:
        await _seed_sparse_lead(session)
        await _seed_comp_data(session)

    monkeypatch.setattr(document_service, "_render_pdf", lambda *args, **kwargs: None)
    payload = await document_service.generate_seller_insight("lead-doc-1")

    assert payload["html_preview"].startswith("<!DOCTYPE html>")
    assert "Seller Insight Report" in payload["html_preview"]
    assert payload["data"]["recommended_talking_points"]


@pytest.mark.asyncio
async def test_document_endpoints_return_expected_shape(isolated_db, monkeypatch):
    async with db_module._async_session_factory() as session:
        await _seed_sparse_lead(session)
        await _seed_comp_data(session)

    monkeypatch.setattr(document_service, "_render_pdf", lambda *args, **kwargs: None)
    headers = {"X-API-KEY": API_KEY}

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        sales = await ac.get("/api/documents/lead-doc-1/sales-advice", headers=headers)
        cma = await ac.get("/api/documents/lead-doc-1/cma", headers=headers)
        insight = await ac.get("/api/documents/lead-doc-1/seller-insight", headers=headers)

    for response, expected_type in (
        (sales, "sales_advice"),
        (cma, "cma"),
        (insight, "seller_insight"),
    ):
        assert response.status_code == 200
        payload = response.json()
        assert payload["lead_id"] == "lead-doc-1"
        assert payload["document_type"] == expected_type
        assert isinstance(payload["data"], dict)
        assert isinstance(payload["html_preview"], str)
        assert "pdf_path" in payload
