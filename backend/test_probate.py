import asyncio
import pytest
from playwright.async_api import async_playwright
import datetime

@pytest.mark.asyncio
async def test_probate():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto("https://onlineregistry.lawlink.nsw.gov.au/probate/", wait_until="domcontentloaded")
        await page.wait_for_selector("#searchForm_suburb")
        
        for suburb in ["Windsor", "Oakville", "Richmond", "Riverstone", "South Windsor", "Pitt Town", "McGraths Hill", "Vineyard"]:
            await page.fill("#searchForm_suburb", suburb)
            await page.click("#searchForm_search")
            try:
                await page.wait_for_selector("table tbody tr", timeout=5000)
                rows = await page.query_selector_all("table tbody tr")
                print(f"{suburb}: Found {len(rows)} rows")
            except Exception:
                print(f"{suburb}: No rows")
        
        await browser.close()
