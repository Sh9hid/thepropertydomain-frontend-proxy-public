"""
Ingestion API Routes
POST /api/ingest/psi-upload          — NSW Valuer General PSI weekly CSV
POST /api/ingest/lrs-webhook         — NSW LRS Property Alerts webhook
POST /api/ingest/cotality-import     — Cotality 10k/month marketing contacts CSV
POST /api/ingest/import-local-files  — Scan D:\L+S Stock\Suburb reports and import all xlsx + CSV
POST /api/ingest/domain-withdrawn    — Pull recently withdrawn Domain listings (hot off-market leads)
POST /api/ingest/da-feed             — Pull fresh DAs from NSW Planning Portal by postcode
"""

import csv
import io
import json
import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from core.config import API_KEY
from core.database import get_session
from core.security import get_api_key
from core.utils import now_iso

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ingest", tags=["Ingestion"])


# ─── PSI Upload ───────────────────────────────────────────────────────────────

@router.post("/psi-upload", summary="NSW Valuer General PSI weekly CSV bulk upload")
async def upload_psi_csv(
    file: UploadFile = File(...),
    api_key: str = Depends(get_api_key),
    db: AsyncSession = Depends(get_session),
):
    """
    Accepts a NSW Valuer General Property Sales Information (PSI) CSV.
    Upserts records into intelligence.event (event_type=SOLD, source=psi).
    Also extracts last_settlement_date → updates intelligence.property.
    """
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv")

    content = await file.read()
    try:
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    except UnicodeDecodeError:
        reader = csv.DictReader(io.StringIO(content.decode("latin-1")))

    rows_inserted = 0
    rows_skipped = 0
    now = now_iso()

    for row in reader:
        address = (
            row.get("Property Address") or
            row.get("address") or
            row.get("Address") or ""
        ).strip()
        settlement_date = (
            row.get("Settlement Date") or
            row.get("settlement_date") or
            row.get("Contract Date") or ""
        ).strip()
        sale_price = (
            row.get("Sale Price") or
            row.get("sale_price") or
            row.get("Price") or ""
        ).strip()

        if not address:
            rows_skipped += 1
            continue

        import hashlib
        prop_id = hashlib.md5(address.encode()).hexdigest()

        try:
            # Upsert into intelligence.property
            await db.execute(
                text("""
                    INSERT INTO intelligence.property
                        (id, address, status, last_settlement_date, created_at, updated_at)
                    VALUES (:id, :address, 'sold', :settlement_date, :now, :now)
                    ON CONFLICT (id) DO UPDATE SET
                        status = 'sold',
                        last_settlement_date = EXCLUDED.last_settlement_date,
                        updated_at = :now
                """),
                {
                    "id": prop_id,
                    "address": address,
                    "settlement_date": settlement_date or None,
                    "now": now,
                }
            )
            # Insert SOLD event
            await db.execute(
                text("""
                    INSERT INTO intelligence.event
                        (id, property_id, event_type, source, raw_payload, occurred_at, created_at)
                    VALUES (:id, :property_id, 'SOLD', 'psi', :payload::jsonb, :occurred_at, :now)
                    ON CONFLICT (id) DO NOTHING
                """),
                {
                    "id": str(uuid.uuid4()),
                    "property_id": prop_id,
                    "payload": json.dumps({"sale_price": sale_price, **dict(row)}),
                    "occurred_at": settlement_date or now,
                    "now": now,
                }
            )
            rows_inserted += 1
        except Exception as e:
            logger.warning(f"[PSI] Row error for '{address}': {e}")
            rows_skipped += 1

    await db.commit()
    logger.info(f"[PSI] Upload complete: {rows_inserted} inserted, {rows_skipped} skipped")
    return {"inserted": rows_inserted, "skipped": rows_skipped}


# ─── LRS Webhook ─────────────────────────────────────────────────────────────

@router.post("/lrs-webhook", summary="NSW LRS Property Alerts webhook receiver")
async def lrs_webhook(
    request: Request,
    api_key: str = Depends(get_api_key),
    db: AsyncSession = Depends(get_session),
):
    """
    Receives NSW LRS (Land Registry Services) Property Alert payloads.
    Upserts into intelligence.event (event_type=SOLD, source=lrs).
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    address = (
        payload.get("propertyAddress") or
        payload.get("address") or ""
    ).strip()
    settlement_date = (
        payload.get("settlementDate") or
        payload.get("registrationDate") or ""
    ).strip()

    if not address:
        raise HTTPException(status_code=422, detail="address is required in payload")

    import hashlib
    prop_id = hashlib.md5(address.encode()).hexdigest()
    now = now_iso()

    try:
        await db.execute(
            text("""
                INSERT INTO intelligence.property
                    (id, address, status, last_settlement_date, created_at, updated_at)
                VALUES (:id, :address, 'sold', :settlement_date, :now, :now)
                ON CONFLICT (id) DO UPDATE SET
                    status = 'sold',
                    last_settlement_date = COALESCE(EXCLUDED.last_settlement_date, intelligence.property.last_settlement_date),
                    updated_at = :now
            """),
            {"id": prop_id, "address": address, "settlement_date": settlement_date or None, "now": now}
        )
        await db.execute(
            text("""
                INSERT INTO intelligence.event
                    (id, property_id, event_type, source, raw_payload, occurred_at, created_at)
                VALUES (:id, :property_id, 'SOLD', 'lrs', :payload::jsonb, :occurred_at, :now)
                ON CONFLICT (id) DO NOTHING
            """),
            {
                "id": str(uuid.uuid4()),
                "property_id": prop_id,
                "payload": json.dumps(payload),
                "occurred_at": settlement_date or now,
                "now": now,
            }
        )
        await db.commit()
        logger.info(f"[LRS] Webhook processed: {address}")
        return {"status": "ok", "property_id": prop_id}
    except Exception as e:
        logger.error(f"[LRS] Webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Cotality Bulk Import ─────────────────────────────────────────────────────

@router.post("/cotality-import", summary="Import Cotality 10k/month marketing contacts CSV")
async def cotality_bulk_import(
    file: UploadFile = File(...),
    api_key: str = Depends(get_api_key),
    db: AsyncSession = Depends(get_session),
):
    """
    Imports Cotality marketing contacts CSV (up to 10k/month) into intelligence.party.
    Attempts to match each contact to an existing intelligence.property by address
    and inserts into intelligence.property_party (role=OWNER).
    """
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv")

    content = await file.read()
    try:
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    except UnicodeDecodeError:
        reader = csv.DictReader(io.StringIO(content.decode("latin-1")))

    import hashlib
    rows_inserted = 0
    rows_skipped = 0
    now = now_iso()

    for row in reader:
        full_name = (
            row.get("Full Name") or
            row.get("full_name") or
            row.get("Name") or
            f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip() or ""
        ).strip()
        cotality_contact_id = (
            row.get("Contact ID") or
            row.get("cotality_contact_id") or
            row.get("ID") or ""
        ).strip()
        address = (
            row.get("Property Address") or
            row.get("address") or
            row.get("Address") or ""
        ).strip()

        if not full_name:
            rows_skipped += 1
            continue

        party_id = cotality_contact_id or hashlib.md5(full_name.encode()).hexdigest()

        try:
            await db.execute(
                text("""
                    INSERT INTO intelligence.party
                        (id, full_name, source, cotality_contact_id, created_at)
                    VALUES (:id, :full_name, 'cotality', :cotality_contact_id, :now)
                    ON CONFLICT (id) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        cotality_contact_id = EXCLUDED.cotality_contact_id
                """),
                {
                    "id": party_id,
                    "full_name": full_name,
                    "cotality_contact_id": cotality_contact_id or None,
                    "now": now,
                }
            )

            # Link to property if address provided
            if address:
                prop_id = hashlib.md5(address.encode()).hexdigest()
                link_id = hashlib.md5(f"{prop_id}:{party_id}".encode()).hexdigest()
                await db.execute(
                    text("""
                        INSERT INTO intelligence.property_party
                            (id, property_id, party_id, role)
                        VALUES (:id, :property_id, :party_id, 'OWNER')
                        ON CONFLICT (id) DO NOTHING
                    """),
                    {"id": link_id, "property_id": prop_id, "party_id": party_id}
                )
            rows_inserted += 1
        except Exception as e:
            logger.warning(f"[Cotality] Row error for '{full_name}': {e}")
            rows_skipped += 1

    await db.commit()
    logger.info(f"[Cotality] Import complete: {rows_inserted} contacts, {rows_skipped} skipped")
    return {"inserted": rows_inserted, "skipped": rows_skipped}


# ─── Domain Withdrawn Listings ────────────────────────────────────────────────

@router.post("/domain-withdrawn", summary="Pull recently withdrawn Domain listings (hot off-market leads)")
async def ingest_domain_withdrawn(
    db: AsyncSession = Depends(get_session),
):
    """
    Searches Domain API for listings withdrawn in the last 90 days across target suburbs.
    Withdrawn listings = owners who tested the market, didn't sell — highest conversion probability.
    Upserts new leads with heat_score=75, trigger_type='domain_withdrawn'.
    Uses shared 490 call/day Domain quota.
    """
    from services.domain_withdrawn import ingest_withdrawn_to_leads
    from core.config import ALL_TARGET_SUBURBS
    result = await ingest_withdrawn_to_leads(db, suburbs=ALL_TARGET_SUBURBS)
    return {"status": "ok", **result}


@router.post("/domain-stale-active", summary="Pull Domain listings on market for 70+ days")
async def ingest_domain_stale_active(
    days_min: int = 70,
    db: AsyncSession = Depends(get_session),
):
    """
    Searches Domain API for active listings that have been on the market for 70+ days.
    Warm leads: frustrated owners open to pivot or refinance.
    """
    from services.stale_active_listings import ingest_stale_active_to_leads
    from core.config import ALL_TARGET_SUBURBS
    result = await ingest_stale_active_to_leads(db, suburbs=ALL_TARGET_SUBURBS, days_min=days_min)
    return {"status": "ok", **result}


# ─── Local File Import ────────────────────────────────────────────────────────

@router.post("/import-local-files", summary="Scan D:\\L+S Stock\\Suburb reports and import all xlsx + CSV")
async def import_local_files(
    db: AsyncSession = Depends(get_session),
):
    """
    Scans D:\\L+S Stock\\Suburb reports for all Cotality xlsx and marketing CSV files,
    parses them, and upserts into the leads table.

    - Marketing CSVs in Marketing report/ subdirectory (Bligh Park format)
    - Cotality xlsx reports at the root level (RPData/CoreLogic format)

    No authentication required — only callable from localhost by default.
    Returns a per-file breakdown of leads upserted.
    """
    from services.data_importer import run_local_import
    result = await run_local_import(db)
    return result


# ─── DA Feed ──────────────────────────────────────────────────────────────────

class DAFeedRequest(BaseModel):
    postcodes: list[str] = ["2765", "2517", "2518"]
    days_back: int = 3


@router.post(
    "/da-feed",
    summary="Pull fresh Development Applications from NSW Planning Portal",
)
async def ingest_da_feed(
    body: DAFeedRequest,
    api_key: str = Depends(get_api_key),
    db: AsyncSession = Depends(get_session),
):
    """
    Fetches DAs filed in the last `days_back` days for the given postcodes
    from the NSW Planning Portal DAApplicationTracker API, then upserts
    matching records into the leads table.

    Postcodes are mapped to councils server-side; postcode filtering is applied
    client-side on the returned FULL_ADDRESS strings.

    Supported postcodes and their councils:
      2765 -> Hawkesbury City Council, The Hills Shire Council
      2517 -> Wollongong City Council
      2518 -> Wollongong City Council

    Returns {"fetched": int, "inserted": int, "skipped": int}.
    """
    from services.da_feed_ingestor import ingest_das_to_leads

    if not body.postcodes:
        raise HTTPException(status_code=400, detail="postcodes list must not be empty")
    if body.days_back < 1 or body.days_back > 30:
        raise HTTPException(status_code=400, detail="days_back must be between 1 and 30")

    result = await ingest_das_to_leads(db, body.postcodes, body.days_back)
    return result



@router.post("/probate-scan", summary="Scan NSW Government Gazette for probate notices in target postcodes")
async def ingest_probate_scan(
    api_key: str = Depends(get_api_key),
    db: AsyncSession = Depends(get_session),
):
    """
    Fetches probate notices from the NSW Government Gazette and upserts
    matching leads for postcodes 2765, 2517, 2518.

    Free public data source — no API key required.
    Returns { fetched, matched, upserted, skipped }.
    """
    from services.probate_scraper import scrape_and_upsert
    return await scrape_and_upsert(db)
