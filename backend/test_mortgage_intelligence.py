import asyncio
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, select

import core.config
import core.database as db_module
from api.routes import mortgage
from services.call_brief_service import get_mortgage_market_brief_text
from core.config import API_KEY
from models.sql_models import BankDataHolder, CallLog, Lead, LeadMortgageProfile, LenderProduct, LenderProductDelta, LenderProductSnapshot, MortgageOpportunity, MortgageOpportunityFeedback
from services.lender_product_service import best_market_rate, list_recent_lender_deltas, normalize_products_for_lender, sync_lender_products


app = FastAPI()
app.include_router(mortgage.router)


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine") / f"mortgage_intel_{uuid.uuid4().hex}.db"
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
            await conn.run_sync(
                SQLModel.metadata.create_all,
                tables=[
                    Lead.__table__,
                    MortgageOpportunity.__table__,
                    BankDataHolder.__table__,
                    CallLog.__table__,
                    LeadMortgageProfile.__table__,
                    LenderProduct.__table__,
                    LenderProductDelta.__table__,
                    LenderProductSnapshot.__table__,
                    MortgageOpportunityFeedback.__table__,
                ],
            )

    asyncio.run(init_models())
    try:
        yield test_factory
    finally:
        app.dependency_overrides.pop(db_module.get_session, None)
        asyncio.run(test_engine.dispose())
        test_db.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_mortgage_recompute_creates_refinance_and_equity_opportunities(isolated_db):
    async with isolated_db() as session:
        session.add(
            Lead(
                id="lead-mortgage-1",
                address="74 Porpoise Crescent, Bligh Park NSW 2756",
                suburb="Bligh Park",
                postcode="2756",
                owner_name="Sample Owner",
                likely_owner_occupier=True,
                owner_occupancy_status="owner_occupied",
                ownership_duration_years=9.2,
                sale_price="620000",
                estimated_value_low=980000,
                estimated_value_high=1030000,
                refinance_signals=[{"label": "Long hold period"}, {"label": "Likely stale lender pricing"}],
                created_at="2026-03-20T09:00:00+11:00",
                updated_at="2026-03-31T09:00:00+11:00",
            )
        )
        await session.commit()

    headers = {"X-API-KEY": API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        recompute = await ac.post("/api/mortgage/leads/lead-mortgage-1/recompute", headers=headers)
        assert recompute.status_code == 200
        payload = await ac.get("/api/mortgage/leads/lead-mortgage-1/opportunities", headers=headers)
        assert payload.status_code == 200
        body = payload.json()

    assert body["total"] >= 2
    types = {item["opportunity_type"] for item in body["opportunities"]}
    assert "refinance_review" in types
    assert "equity_review" in types
    refi = next(item for item in body["opportunities"] if item["opportunity_type"] == "refinance_review")
    assert refi["estimated_monthly_saving"] > 0
    assert refi["headline"].lower().startswith("estimated refi gap")


@pytest.mark.asyncio
async def test_mortgage_lenders_endpoint_seeds_registry(isolated_db):
    headers = {"X-API-KEY": API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/mortgage/lenders", headers=headers)
        assert response.status_code == 200
        body = response.json()

    assert body["total"] >= 100
    lender_names = {item["name"] for item in body["lenders"]}
    assert "Westpac" in lender_names
    assert "CommBank" in lender_names
    assert "NATIONAL AUSTRALIA BANK" in lender_names


@pytest.mark.asyncio
async def test_sync_lender_products_normalizes_public_rate_payload(isolated_db):
    async with isolated_db() as session:
        session.add(
            BankDataHolder(
                id="cdr-test-bank",
                name="Test Bank",
                brand="Test Bank",
                base_url="https://example.test/cds-au/v1",
                product_path="/banking/products",
                active=True,
                created_at="2026-04-01T09:00:00+11:00",
                updated_at="2026-04-01T09:00:00+11:00",
            )
        )
        await session.commit()

        payload = {
            "data": {
                "products": [
                    {
                        "productId": "prod-1",
                        "name": "Owner Occupier Variable Offset",
                        "description": "Owner occupier variable rate with offset and redraw",
                        "lendingRates": [
                            {"lendingRateType": "VARIABLE", "rate": 5.84, "comparisonRate": 6.01}
                        ],
                    },
                    {
                        "productId": "prod-2",
                        "name": "Investor Fixed 2 Year",
                        "description": "Investor fixed home loan package",
                        "lendingRates": [
                            {"lendingRateType": "FIXED", "rate": 5.69, "comparisonRate": 5.95}
                        ],
                    },
                ]
            }
        }

        class FakeResponse:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return payload

        class FakeClient:
            async def get(self, url, headers=None):
                return FakeResponse()

        result = await sync_lender_products(session, lender_id="cdr-test-bank", client=FakeClient())
        assert result["processed"] == 1
        assert result["updated_products"] == 2

        rows = (await session.execute(select(LenderProduct).where(LenderProduct.lender_id == "cdr-test-bank"))).scalars().all()
        assert len(rows) == 2
        owner_occ = next(item for item in rows if item.external_product_id == "prod-1")
        assert owner_occ.advertised_rate == 5.84
        assert owner_occ.has_offset is True
        assert owner_occ.has_redraw is True
        assert owner_occ.occupancy_target == "owner_occupier"
        investor = next(item for item in rows if item.external_product_id == "prod-2")
        assert investor.rate_type == "fixed"
        assert investor.occupancy_target == "investor"


def test_normalize_products_for_lender_handles_cdr_payload_shape():
    lender = BankDataHolder(
        id="cdr-quick-bank",
        name="Quick Bank",
        brand="Quick Bank",
        base_url="https://quick.example/cds-au/v1",
    )
    normalized = normalize_products_for_lender(
        lender,
        {
            "products": [
                {
                    "productId": "abc",
                    "name": "Basic Variable",
                    "description": "Owner occupier variable redraw loan",
                    "lendingRates": [{"lendingRateType": "VARIABLE", "rate": 6.02, "comparisonRate": 6.2}],
                }
            ]
        },
        fetched_at="2026-04-01T10:00:00+11:00",
    )
    assert len(normalized) == 1
    row = normalized[0]
    assert row["external_product_id"] == "abc"
    assert row["advertised_rate"] == 6.02
    assert row["comparison_rate"] == 6.2
    assert row["has_redraw"] is True


@pytest.mark.asyncio
async def test_best_market_rate_prefers_big_four_and_st_george_when_rates_are_close(isolated_db):
    async with isolated_db() as session:
        session.add_all(
            [
                LenderProduct(
                    id="prod-major",
                    lender_id="cdr-westpac",
                    external_product_id="west-1",
                    name="Westpac Flexi",
                    brand="Westpac",
                    occupancy_target="owner_occupier",
                    rate_type="variable",
                    advertised_rate=5.95,
                    comparison_rate=6.10,
                ),
                LenderProduct(
                    id="prod-cheap-small",
                    lender_id="cdr-small",
                    external_product_id="small-1",
                    name="Regional Basic",
                    brand="Regional Small Bank",
                    occupancy_target="owner_occupier",
                    rate_type="variable",
                    advertised_rate=5.88,
                    comparison_rate=6.00,
                ),
            ]
        )
        await session.commit()
        selected = await best_market_rate(session, occupancy_target="owner_occupier", rate_type="variable")
    assert selected["lender_name"] == "Westpac"


@pytest.mark.asyncio
async def test_recomputed_refi_reason_and_why_now_are_full_sentences(isolated_db):
    from services.mortgage_intelligence_service import recompute_mortgage_opportunities

    async with isolated_db() as session:
        session.add(
            Lead(
                id="lead-mortgage-2",
                address="10 Full Sentence Street, Kellyville NSW 2155",
                suburb="Kellyville",
                postcode="2155",
                owner_name="Sentence Owner",
                likely_owner_occupier=True,
                owner_occupancy_status="owner_occupied",
                ownership_duration_years=8.1,
                sale_price="700000",
                estimated_value_low=1000000,
                estimated_value_high=1040000,
                refinance_signals=[{"label": "Long hold period"}],
            )
        )
        await session.commit()
        await recompute_mortgage_opportunities(session, lead_id="lead-mortgage-2")
        rows = (await session.execute(select(MortgageOpportunity).where(MortgageOpportunity.lead_id == "lead-mortgage-2"))).scalars().all()
    refi = next(item for item in rows if item.opportunity_type == "refinance_review")
    assert refi.reason_to_call.endswith(".")
    assert refi.why_now.endswith(".")
    assert "The estimated repayment gap is about" in refi.reason_to_call


@pytest.mark.asyncio
async def test_extract_mortgage_profile_from_calls_reads_lender_rate_and_balance(isolated_db):
    from services.mortgage_profile_service import extract_mortgage_profile_from_calls

    async with isolated_db() as session:
        session.add(
            Lead(
                id="lead-profile-1",
                address="55 Script Street, Penrith NSW 2750",
                suburb="Penrith",
                postcode="2750",
            )
        )
        session.add(
            CallLog(
                id="call-profile-1",
                lead_id="lead-profile-1",
                lead_address="55 Script Street, Penrith NSW 2750",
                outcome="contacted",
                connected=True,
                timestamp="2026-04-02T11:00:00+11:00",
                logged_at="2026-04-02T11:00:00+11:00",
                logged_date="2026-04-02",
                transcript="I am with Westpac at 6.79% and the balance is about $640,000. It is variable and I do have an offset account.",
            )
        )
        await session.commit()
        profile = await extract_mortgage_profile_from_calls(session, "lead-profile-1")

    assert profile.current_lender == "Westpac"
    assert profile.current_rate == 6.79
    assert profile.loan_balance_estimate == 640000
    assert profile.fixed_or_variable == "variable"
    assert profile.offset_account is True


@pytest.mark.asyncio
async def test_feedback_suppresses_refinance_opportunity(isolated_db):
    from services.mortgage_intelligence_service import recompute_mortgage_opportunities
    from services.mortgage_profile_service import add_feedback

    async with isolated_db() as session:
        session.add(
            Lead(
                id="lead-feedback-1",
                address="12 Quiet Street, Windsor NSW 2756",
                suburb="Windsor",
                postcode="2756",
                likely_owner_occupier=True,
                owner_occupancy_status="owner_occupied",
                ownership_duration_years=8.0,
                sale_price="600000",
                estimated_value_low=960000,
                estimated_value_high=1000000,
            )
        )
        await session.commit()
        await add_feedback(session, "lead-feedback-1", "not_mortgage_lead", opportunity_type="refinance_review")
        await recompute_mortgage_opportunities(session, lead_id="lead-feedback-1")
        rows = (await session.execute(select(MortgageOpportunity).where(MortgageOpportunity.lead_id == "lead-feedback-1"))).scalars().all()

    assert all(row.opportunity_type != "refinance_review" for row in rows)


@pytest.mark.asyncio
async def test_suburb_cross_check_downgrades_priority_when_nearby_sales_disagree(isolated_db):
    from services.mortgage_intelligence_service import recompute_mortgage_opportunities

    async with isolated_db() as session:
        session.add(
            Lead(
                id="lead-crosscheck-1",
                address="9 Median Street, Richmond NSW 2753",
                suburb="Richmond",
                postcode="2753",
                likely_owner_occupier=True,
                owner_occupancy_status="owner_occupied",
                ownership_duration_years=9.0,
                sale_price="650000",
                estimated_value_low=1400000,
                estimated_value_high=1500000,
                nearby_sales=[{"sale_price": 930000}, {"sale_price": 950000}, {"sale_price": 910000}],
            )
        )
        await session.commit()
        await recompute_mortgage_opportunities(session, lead_id="lead-crosscheck-1")
        rows = (await session.execute(select(MortgageOpportunity).where(MortgageOpportunity.lead_id == "lead-crosscheck-1"))).scalars().all()

    refi = next(item for item in rows if item.opportunity_type == "refinance_review")
    assert refi.priority_score < 90
    assert any(entry.get("label") == "Suburb cross-check" for entry in refi.evidence_json)


@pytest.mark.asyncio
async def test_sync_lender_products_records_rate_change_delta(isolated_db):
    async with isolated_db() as session:
        session.add(
            BankDataHolder(
                id="cdr-delta-bank",
                name="Delta Bank",
                brand="Delta Bank",
                base_url="https://delta.test/cds-au/v1",
                product_path="/banking/products",
                active=True,
            )
        )
        session.add(
            LenderProduct(
                id="existing-delta-product",
                lender_id="cdr-delta-bank",
                external_product_id="delta-prod",
                name="Delta Variable",
                brand="Delta Bank",
                occupancy_target="owner_occupier",
                rate_type="variable",
                advertised_rate=6.15,
                comparison_rate=6.31,
                raw_json={"productId": "delta-prod"},
            )
        )
        await session.commit()

        payload = {
            "products": [
                {
                    "productId": "delta-prod",
                    "name": "Delta Variable",
                    "description": "Owner occupier variable redraw",
                    "lendingRates": [{"lendingRateType": "VARIABLE", "rate": 5.95, "comparisonRate": 6.10}],
                }
            ]
        }

        class FakeResponse:
            status_code = 200
            def raise_for_status(self): return None
            def json(self): return payload

        class FakeClient:
            async def get(self, url, headers=None): return FakeResponse()

        await sync_lender_products(session, lender_id="cdr-delta-bank", client=FakeClient())
        deltas = await list_recent_lender_deltas(session, limit=10)

    assert any(item.change_type == "rate_changed" and item.lender_id == "cdr-delta-bank" for item in deltas)


@pytest.mark.asyncio
async def test_mortgage_market_brief_locks_call_order_language(isolated_db):
    async with isolated_db() as session:
        session.add_all(
            [
                LenderProduct(id="w", lender_id="west", external_product_id="w1", name="Westpac Basic", brand="Westpac", occupancy_target="owner_occupier", rate_type="variable", advertised_rate=5.49),
                LenderProduct(id="s", lender_id="stg", external_product_id="s1", name="St.George Basic", brand="St.George Bank", occupancy_target="owner_occupier", rate_type="variable", advertised_rate=5.54),
                LenderProduct(id="n", lender_id="nab", external_product_id="n1", name="NAB Base", brand="NATIONAL AUSTRALIA BANK", occupancy_target="owner_occupier", rate_type="variable", advertised_rate=5.69),
                LenderProduct(id="c", lender_id="cba", external_product_id="c1", name="CBA Standard", brand="CommBank", occupancy_target="owner_occupier", rate_type="variable", advertised_rate=5.84),
            ]
        )
        session.add(
            LenderProductDelta(
                id="delta-anz",
                lender_id="anz",
                external_product_id="anz1",
                change_type="rate_changed",
                headline="ANZ changed ANZ Variable",
                detected_at="2026-03-27T09:00:00+11:00",
                payload_json={"old": {"advertised_rate": 5.60}, "new": {"advertised_rate": 5.85}},
            )
        )
        await session.commit()
        brief = await get_mortgage_market_brief_text(session)

    assert "2. Borrowers currently above Westpac and St.George" in brief
    assert "Reason: those two lenders are now the sharper public comparison anchors in this group" in brief
    assert "3. NAB and CommBank customers" in brief
