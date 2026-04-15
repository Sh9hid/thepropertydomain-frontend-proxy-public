import os
import sys
import asyncio
import logging
import json
import random
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from playwright.async_api import async_playwright

# Add backend to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import core.config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("listing_photo_scraper")

# Create async engine
engine = create_async_engine(core.config.DATABASE_URL)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def scrape_listing_photo(browser_context, address: str):
    """
    Surgically extracts the actual hero photo from OnTheHouse using Playwright.
    """
    search_url = f"https://www.onthehouse.com.au/search/property?q={address.replace(' ', '%20')}"
    logger.info(f"Scraping professional photo for: {address}")
    
    page = await browser_context.new_page()
    try:
        # Randomized human-like navigation
        await page.goto(search_url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(random.uniform(3, 5))
        
        # If on search results, click first property
        if "/search/" in page.url:
            first_result = await page.query_selector('a[href*="/property/nsw/"]')
            if first_result:
                logger.info("Found search result, clicking through...")
                await first_result.click()
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(3)
        
        # Now on property page, find the hero image
        selectors = [
            'img.property-image', 
            'img[alt*="Property Photo"]',
            '.hero-image img',
            '.gallery-image img',
            'img[src*="cloudfront.net"]'
        ]
        
        img_src = None
        for s in selectors:
            el = await page.query_selector(s)
            if el:
                src = await el.get_attribute("src")
                if src and src.startswith("http"):
                    img_src = src
                    break
        
        if not img_src:
            # Fallback: big images
            imgs = await page.query_selector_all("img")
            for img in imgs:
                box = await img.bounding_box()
                if box and box['width'] > 400:
                    src = await img.get_attribute("src")
                    if src and src.startswith("http"):
                        img_src = src
                        break
        
        return img_src
    except Exception as e:
        logger.error(f"Playwright error for {address}: {e}")
        return None
    finally:
        await page.close()

async def run_batch():
    async with async_playwright() as p:
        # Use headed=False for background speed, but valid user agent
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )

        async with async_session() as session:
            # Get leads missing photos
            result = await session.execute(
                text("SELECT id, address, suburb FROM leads WHERE (main_image IS NULL OR main_image = '' OR main_image LIKE '%static-maps%' OR main_image = 'FAILED_SCRAPE') AND address NOT LIKE 'LOT %' ORDER BY heat_score DESC LIMIT 20")
            )
            leads = result.mappings().all()
            
            if not leads:
                logger.info("No leads need scraping.")
                return

            logger.info(f"Processing batch of {len(leads)} professional listing scrapes...")

            for lead in leads:
                full_address = f"{lead['address']}, {lead['suburb']}, NSW"
                
                try:
                    img_url = await scrape_listing_photo(context, full_address)
                    
                    if img_url:
                        logger.info(f"✅ REAL PHOTO FOUND: {img_url}")
                        await session.execute(
                            text("UPDATE leads SET main_image = :url WHERE id = :id"),
                            {"url": img_url, "id": lead['id']}
                        )
                        await session.commit()
                    else:
                        logger.warning(f"❌ Failed to find real photo for {full_address}")
                        await session.execute(
                            text("UPDATE leads SET main_image = 'NOT_FOUND' WHERE id = :id"),
                            {"id": lead['id']}
                        )
                        await session.commit()

                    # Anti-ban delay
                    await asyncio.sleep(random.uniform(5, 10))

                except Exception as e:
                    logger.error(f"Critical error on {lead['id']}: {e}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_batch())
