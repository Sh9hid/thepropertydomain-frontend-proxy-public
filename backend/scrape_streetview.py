import os
import sys
import asyncio
import logging
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add backend to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import core.config
from models.sql_models import Lead

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("streetview_scraper")

# Create async engine
engine = create_async_engine(core.config.DATABASE_URL)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

from services.geocoding_service import run_geocoding_batch

async def capture_hq_streetview(lead_id: str, address: str, lat: float, lng: float, output_path: str):
    """
    Uses browser-use CLI to capture a high-quality Street View screenshot.
    Attempts to hide UI and center/zoom on the house.
    """
    import subprocess
    
    # 1. Close any existing sessions
    subprocess.run(["browser-use", "close"], capture_output=True)
    
    # 2. Determine target URL
    # If we have lat/lng, we can go to the direct pano action
    if lat and lng and lat != 0:
        target_url = f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lng}"
    else:
        # Fallback to search. Google often automatically enters street view 
        # if the address is specific enough.
        target_url = f"https://www.google.com/maps/search/{address.replace(' ', '+')}"

    logger.info(f"[{lead_id}] Navigating to REAL HOUSE view: {target_url}")
    subprocess.run(["browser-use", "open", target_url], capture_output=True)
    
    # 3. Wait for high-res load
    # We wait 12 seconds to ensure the 3D imagery and high-res textures are fully rendered
    await asyncio.sleep(12)
    
    # 4. Hide all Google UI clutter to see the actual house clearly
    hide_ui_script = """
    const selectors = [
        '.scene-footer-container',
        '.app-viewcard-strip',
        '.gm-style-cc',
        '#runway',
        '.widget-pane-visible',
        '.widget-pane',
        '#minimap',
        '.watermark',
        '.gm-iv-address-container',
        '.gm-sv-label-container',
        '.gmnoprint'
    ];
    selectors.forEach(s => {
        document.querySelectorAll(s).forEach(el => el.style.display = 'none');
    });
    """
    subprocess.run(["browser-use", "eval", hide_ui_script], capture_output=True)
    
    # 5. Capture the photo
    logger.info(f"[{lead_id}] Capturing HQ photo of house...")
    subprocess.run(["browser-use", "screenshot", output_path], capture_output=True)
    
    # 6. Close browser
    subprocess.run(["browser-use", "close"], capture_output=True)
    
    return Path(output_path).exists()

async def run_scraper(reset=False):
    output_dir = Path("backend/streetview_images")
    output_dir.mkdir(exist_ok=True)

    async with async_session() as session:
        if reset:
            logger.info("Resetting all streetview images in database...")
            await session.execute(
                text("UPDATE leads SET main_image = NULL WHERE main_image LIKE '/streetview_images/%'")
            )
            await session.commit()

        # STEP 1: Geocode missing coordinates to ensure we hit the HOUSE and not a 2D map
        logger.info("Geocoding addresses to find precise house coordinates...")
        geo_result = await run_geocoding_batch(session, limit=50)
        logger.info(f"Geocoding complete: {geo_result}")

        # STEP 2: Get leads without main_image (prioritize highest heat)
        result = await session.execute(
            text("SELECT id, address, suburb, postcode, lat, lng FROM leads WHERE main_image IS NULL OR main_image = '' ORDER BY heat_score DESC LIMIT 100")
        )
        leads = result.mappings().all()
        
        if not leads:
            logger.info("No leads found needing HQ screenshots.")
            return

        logger.info(f"Processing {len(leads)} leads for HQ imagery...")

        for lead in leads:
            full_address = f"{lead['address']}, {lead['suburb']} {lead['postcode']}, Australia"
            filename = f"{lead['id']}.png"
            filepath = output_dir / filename
            
            try:
                success = await capture_hq_streetview(
                    lead['id'], 
                    full_address, 
                    lead['lat'], 
                    lead['lng'], 
                    str(filepath)
                )
                
                if success:
                    logger.info(f"✅ HQ image saved for {lead['id']}")
                    relative_path = f"/streetview_images/{filename}"
                    await session.execute(
                        text("UPDATE leads SET main_image = :path WHERE id = :id"),
                        {"path": relative_path, "id": lead['id']}
                    )
                    await session.commit()
                else:
                    logger.error(f"❌ Failed to capture HQ image for {lead['id']}")

            except Exception as e:
                logger.error(f"Critical error on {lead['id']}: {e}")

if __name__ == "__main__":
    # Check if 'reset' argument passed
    do_reset = "--reset" in sys.argv
    asyncio.run(run_scraper(reset=do_reset))
