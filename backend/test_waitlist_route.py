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
from api.routes import waitlist


app = FastAPI()
app.include_router(waitlist.router)


@pytest.fixture
def isolated_db(monkeypatch):
  test_db = Path(r"D:\woonona-lead-machine") / f"waitlist_{uuid.uuid4().hex}.db"
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
  if test_db.exists():
    test_db.unlink()


@pytest.mark.asyncio
async def test_waitlist_captures_email_suburb_and_offer(isolated_db):
  async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
    response = await ac.post(
      "/api/waitlist",
      json={
        "email": "operator@example.com",
        "suburb_interest": "Marsden Park",
        "offer_code": "3_guides_bundle",
      },
    )

  assert response.status_code == 200
  payload = response.json()
  assert payload["ok"] is True

  async with db_module._async_session_factory() as session:
    row = (
      await session.execute(
        text("SELECT email, suburb_interest, offer_code FROM propella_waitlist ORDER BY submitted_at DESC LIMIT 1")
      )
    ).mappings().first()

  assert row["email"] == "operator@example.com"
  assert row["suburb_interest"] == "Marsden Park"
  assert row["offer_code"] == "buyer_guide"


@pytest.mark.asyncio
async def test_waitlist_triggers_autoreply_with_attachments(isolated_db, monkeypatch):
  captured = {}

  def fake_send_email_service(account_data, body):
    captured["recipient"] = body.recipient
    captured["subject"] = body.subject
    captured["attachments"] = list(body.attachment_paths)
    return {"ok": True}

  monkeypatch.setattr(waitlist, "send_email_service", fake_send_email_service)
  monkeypatch.setattr(waitlist, "_resolve_waitlist_guide_attachments", lambda: ["guide-a.txt", "guide-b.txt", "guide-c.txt"])

  async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
    response = await ac.post(
      "/api/waitlist",
      json={
        "email": "buyer@example.com",
        "suburb_interest": "Box Hill",
        "offer_code": "buyer_guide",
      },
    )

  assert response.status_code == 200
  assert captured["recipient"] == "buyer@example.com"
  assert "Box Hill" in captured["subject"]
  assert captured["attachments"] == ["guide-a.txt", "guide-b.txt", "guide-c.txt"]


@pytest.mark.asyncio
async def test_waitlist_still_succeeds_when_autoreply_fails(isolated_db, monkeypatch):
  def fake_send_email_service(*_args, **_kwargs):
    raise RuntimeError("smtp unavailable")

  monkeypatch.setattr(waitlist, "send_email_service", fake_send_email_service)

  async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
    response = await ac.post(
      "/api/waitlist",
      json={
        "email": "seller@example.com",
        "suburb_interest": "Windsor",
        "offer_code": "seller_guide",
      },
    )

  assert response.status_code == 200
  assert response.json()["ok"] is True
