import asyncio
from playwright.async_api import async_playwright

async def debug_search():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            print("Navigating...")
            await page.goto("https://onlineregistry.lawlink.nsw.gov.au/probate/", wait_until="networkidle")
            
            print("Searching for specific name...")
            await page.fill('#searchForm_surname', "Woods")
            await page.fill('#searchForm_firstname', "Beatrice")
            await page.click('#searchForm_search')
            
            await page.wait_for_timeout(5000)
            
            html = await page.content()
            if "Too many results" in html:
                print("Error: Too many results")
            elif "No results found" in html:
                print("Error: No results found")
            else:
                rows = await page.query_selector_all('table tbody tr')
                print(f"Rows found: {len(rows)}")
                for row in rows[:2]:
                    print("Row:", await row.inner_text())
                
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_search())
