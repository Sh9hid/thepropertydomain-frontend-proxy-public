import asyncio
from playwright.async_api import async_playwright
from pathlib import Path

async def html_to_pdf(html_content: str, output_path: str):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.set_content(html_content, wait_until="networkidle")
        await page.emulate_media(media="screen")
        await page.pdf(
            path=output_path,
            format="A4",
            print_background=True,
            margin={"top": "12mm", "right": "10mm", "bottom": "14mm", "left": "10mm"},
        )
        await browser.close()

if __name__ == "__main__":
    # Test
    asyncio.run(html_to_pdf("<h1>Test Report</h1><p>Hello Shahid!</p>", "test.pdf"))
