"""
Property Intelligence endpoint — aggregates all free data sources for one property.

GET /api/leads/{lead_id}/intel
  Returns: NSW DA history, OSM amenities, Domain comparable sales,
           DuckDB suburb stats, and photo fallback URLs.
  All fields nullable — degrades gracefully if any source fails.
"""

import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from core.database import get_session
from core.security import get_api_key
from services.nsw_property_service import get_property_intel
from services.suburb_intel_service import get_suburb_intel
from services.domain_enrichment import get_suburb_comparables

logger = logging.getLogger(__name__)
router = APIRouter(tags=["property-intel"])


@router.get("/api/leads/{lead_id}/intel")
async def property_intel(
    lead_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """
    Unified property intelligence for a single lead.

    Data sources (all free, all fail silently):
    - NSW Planning Portal: DA history (needs NSW_EPLANNING_KEY env var — apply free)
    - OSM Overpass: schools, stations, shops within 1km
    - Domain API: recent comparable sales in the suburb (uses existing quota)
    - DuckDB: suburb median price and stats from Cotality xlsx reports
    - Nominatim: geocoding if lat/lng missing from lead record
    """
    res = await session.execute(
        text(
            "SELECT id, address, suburb, postcode, lat, lng, "
            "land_size_sqm, development_zone, bedrooms, bathrooms, "
            "year_built, sale_price, settlement_date "
            "FROM leads WHERE id = :id"
        ),
        {"id": lead_id},
    )
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")

    address = row["address"] or ""
    suburb = row["suburb"] or ""
    postcode = str(row["postcode"] or "")
    lat = row["lat"]
    lng = row["lng"]

    # Run all three external sources in parallel
    nsw_task = asyncio.create_task(
        get_property_intel(lead_id, address, suburb, postcode, lat, lng)
    )
    comparables_task = asyncio.create_task(
        get_suburb_comparables(suburb, postcode)
    )

    # DuckDB suburb intel is sync (in-memory cache after first call)
    suburb_intel = await asyncio.to_thread(get_suburb_intel, suburb)

    nsw_data, comparables = await asyncio.gather(nsw_task, comparables_task)

    # Compute derived signals
    mortgage_cliff_months = None
    if row["settlement_date"]:
        from datetime import date
        try:
            from datetime import datetime
            settled = datetime.fromisoformat(str(row["settlement_date"])).date()
            diff = date.today() - settled
            mortgage_cliff_months = diff.days // 30
        except Exception:
            pass

    subdivision_eligible = (
        (row["land_size_sqm"] or 0) >= 800
        and "R2" in (row["development_zone"] or "")
    )

    return {
        # Property facts (from lead record)
        "lead_id": lead_id,
        "address": address,
        "suburb": suburb,
        "land_size_sqm": row["land_size_sqm"],
        "development_zone": row["development_zone"],
        "bedrooms": row["bedrooms"],
        "bathrooms": row["bathrooms"],
        "year_built": row["year_built"],
        "sale_price": row["sale_price"],
        "settlement_date": str(row["settlement_date"]) if row["settlement_date"] else None,

        # Derived signals
        "mortgage_cliff_months": mortgage_cliff_months,
        "subdivision_eligible": subdivision_eligible,

        # NSW government data
        "das": nsw_data.get("das", []),
        "da_count": nsw_data.get("da_count", 0),

        # OSM amenities
        "nearby_amenities": nsw_data.get("nearby_amenities", {}),

        # Photo fallbacks
        "street_view_url": nsw_data.get("street_view_url"),
        "street_view_embed_url": nsw_data.get("street_view_embed_url"),
        "street_view_available": nsw_data.get("street_view_available", False),
        "street_view_status": nsw_data.get("street_view_status"),
        "street_view_date": nsw_data.get("street_view_date"),
        "street_view_copyright": nsw_data.get("street_view_copyright"),
        "map_tile_url": nsw_data.get("map_tile_url"),
        "lat": nsw_data.get("lat"),
        "lng": nsw_data.get("lng"),
        "geocoded": nsw_data.get("geocoded", False),

        # Domain comparable sales
        "comparables": comparables,
        "comparables_count": len(comparables),

        # DuckDB suburb intel (from Cotality xlsx files)
        "suburb_intel": suburb_intel,
    }
