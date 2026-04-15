import asyncio
import pytest
from playwright.async_api import async_playwright

@pytest.mark.asyncio
async def test_gumtree():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto('https://www.gumtree.com.au/s-property-for-sale/windsor-sydney/c18364l3005820', wait_until="domcontentloaded")
        print(await page.title())
        html = await page.content()
        print("HTML length:", len(html))
        await browser.close()
