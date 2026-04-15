import asyncio
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import core.config
import core.database as db_module
from core.config import API_KEY
from core.utils import now_iso
from hermes.integrations.nvidia_nim import DummyProvider, build_llm_provider
from hermes.models import HermesFinding, HermesSource
from hermes.routes import router as hermes_router


app = FastAPI()
app.include_router(hermes_router)


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine") / f"hermes_{uuid.uuid4().hex}.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db.as_posix()}")
    monkeypatch.setenv("HERMES_LLM_PROVIDER", "dummy")
    monkeypatch.setenv("HERMES_EMBED_PROVIDER", "dummy")

    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(
        db_module,
        "_async_session_factory",
        sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False),
    )

    db_module.init_db()

    try:
        from hermes.controller import reset_controller_for_tests

        reset_controller_for_tests()
    except Exception:
        pass

    yield test_db

    asyncio.run(test_engine.dispose())
    if test_db.exists():
        test_db.unlink()


@pytest.mark.asyncio
async def test_source_sync_dedupes_findings_and_writes_memory(isolated_db, monkeypatch):
    async def fake_fetch_feed(url: str, limit: int = 25):
        return [
            {
                "title": "WhatsApp repo ships broker follow-up flow",
                "url": "https://example.com/posts/whatsapp-flow",
                "published_at": "2026-03-27T09:00:00Z",
                "summary": "A public post explains a low-friction nurture sequence for mortgage leads.",
            },
            {
                "title": "WhatsApp repo ships broker follow-up flow",
                "url": "https://example.com/posts/whatsapp-flow",
                "published_at": "2026-03-27T09:00:00Z",
                "summary": "Duplicate of the same source item and should be deduped.",
            },
            {
                "title": "Proptech changelog adds seller-intent scoring",
                "url": "https://example.com/posts/seller-intent",
                "published_at": "2026-03-27T10:00:00Z",
                "summary": "A changelog note highlights seller scoring and operator workflows.",
            },
        ]

    monkeypatch.setattr("hermes.integrations.rss.fetch_feed_entries", fake_fetch_feed)

    headers = {"X-API-KEY": API_KEY}

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        create_source = await ac.post(
            "/api/hermes/sources",
            json={
                "name": "Test RSS Source",
                "source_type": "rss",
                "base_url": "https://example.com/blog",
                "rss_url": "https://example.com/feed.xml",
                "enabled": True,
                "fetch_frequency_minutes": 60,
                "tags": ["mortgage", "seller"],
                "company_scope": "shared",
                "credibility_score": 0.82,
            },
            headers=headers,
        )
        assert create_source.status_code == 200
        source_id = create_source.json()["source"]["id"]

        sync = await ac.post(
            "/api/hermes/sources/sync",
            json={"source_ids": [source_id], "force": True},
            headers=headers,
        )
        feed = await ac.get("/api/hermes/feed", headers=headers)
        memory = await ac.get("/api/hermes/memory", headers=headers)

    assert sync.status_code == 200
    sync_body = sync.json()
    assert sync_body["summary"]["sources_processed"] == 1
    assert sync_body["summary"]["new_findings"] == 2
    assert sync_body["run"]["status"] == "completed"

    assert feed.status_code == 200
    feed_body = feed.json()
    assert len(feed_body["findings"]) == 2
    assert feed_body["digest"]["top_research_insights"]
    assert feed_body["digest"]["top_recommended_actions"]
    assert all(item["source_url"].startswith("https://example.com/posts/") for item in feed_body["findings"])

    assert memory.status_code == 200
    memory_body = memory.json()
    assert memory_body["entries"]
    assert memory_body["learning_loops"]["source_type_rankings"]

    async with db_module._async_session_factory() as session:
        finding_count = (
            await session.execute(text("SELECT COUNT(*) FROM hermes_findings"))
        ).scalar_one()
        run_count = (
            await session.execute(text("SELECT COUNT(*) FROM hermes_runs"))
        ).scalar_one()

    assert finding_count == 2
    assert run_count >= 1


@pytest.mark.asyncio
async def test_commands_generate_drafts_approval_flow_and_activity_trace(isolated_db):
    async with db_module._async_session_factory() as session:
        session.add(
            HermesSource(
                id="source-seed",
                name="Seed Blog",
                source_type="blog",
                base_url="https://example.com/blog",
                rss_url="https://example.com/feed.xml",
                enabled=True,
                fetch_frequency_minutes=180,
                tags_json=["seller", "content"],
                company_scope="real_estate",
                credibility_score=0.9,
                created_at=now_iso(),
                updated_at=now_iso(),
            )
        )
        session.add(
            HermesFinding(
                id="finding-seed",
                source_id="source-seed",
                source_type="blog",
                source_name="Seed Blog",
                source_url="https://example.com/blog/seed-finding",
                dedupe_key="seed-dedupe-key",
                company_scope="real_estate",
                topic="Seller proof post pattern",
                signal_type="content",
                summary="Operators are posting short proof-led commentary with one concrete lesson.",
                why_it_matters="This format can be repurposed into founder-style native posts and seller nurture.",
                novelty_score=0.74,
                confidence_score=0.81,
                actionability_score=0.92,
                proposed_actions_json=["use in content", "use in outreach", "save for later"],
                published_at="2026-03-26T08:00:00Z",
                created_at=now_iso(),
            )
        )
        await session.commit()

    headers = {"X-API-KEY": API_KEY}

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        generate_content = await ac.post(
            "/api/hermes/command",
            json={
                "command_type": "GENERATE_CONTENT",
                "prompt": "Turn this finding into native social drafts",
                "finding_ids": ["finding-seed"],
            },
            headers=headers,
        )
        build_campaign = await ac.post(
            "/api/hermes/command",
            json={
                "command_type": "BUILD_CAMPAIGN",
                "prompt": "Prepare seller nurture email sequence",
                "campaign_type": "seller",
            },
            headers=headers,
        )
        activity = await ac.get("/api/hermes/activity", headers=headers)
        feed = await ac.get("/api/hermes/feed", headers=headers)

    assert generate_content.status_code == 200
    content_body = generate_content.json()
    assert content_body["run"]["status"] == "completed"
    assert content_body["trace"]
    assert content_body["result"]["content_ids"]

    assert build_campaign.status_code == 200
    campaign_body = build_campaign.json()
    assert campaign_body["run"]["status"] == "completed"
    assert campaign_body["result"]["campaign_ids"]

    assert activity.status_code == 200
    activity_body = activity.json()
    assert len(activity_body["runs"]) >= 2
    assert any(run["job_type"] == "GENERATE_CONTENT" for run in activity_body["runs"])
    assert any(run["job_type"] == "BUILD_CAMPAIGN" for run in activity_body["runs"])

    assert feed.status_code == 200
    feed_body = feed.json()
    assert feed_body["content"]
    assert feed_body["campaigns"]
    assert feed_body["approvals"]["pending_content"]
    assert feed_body["approvals"]["pending_campaigns"]

    content_id = feed_body["content"][0]["id"]
    campaign_id = feed_body["campaigns"][0]["id"]

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        approve_content = await ac.post(
            "/api/hermes/approve-content",
            json={"content_id": content_id, "approved_by": "operator", "note": "Grounded and safe."},
            headers=headers,
        )
        approve_campaign = await ac.post(
            "/api/hermes/approve-campaign",
            json={"campaign_id": campaign_id, "approved_by": "operator", "note": "Queue after infra setup."},
            headers=headers,
        )
        memory = await ac.get("/api/hermes/memory", headers=headers)

    assert approve_content.status_code == 200
    assert approve_content.json()["content"]["status"] == "approved"
    assert approve_campaign.status_code == 200
    assert approve_campaign.json()["campaign"]["status"] == "approved"

    memory_body = memory.json()
    assert any(entry["memory_type"] == "content_memory" for entry in memory_body["entries"])
    assert any(entry["memory_type"] == "channel_memory" for entry in memory_body["entries"])


def test_provider_factory_falls_back_to_dummy_without_nvidia_key(monkeypatch):
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("NGC_API_KEY", raising=False)
    monkeypatch.setenv("HERMES_LLM_PROVIDER", "nvidia_nim")

    provider = build_llm_provider()

    assert isinstance(provider, DummyProvider)
