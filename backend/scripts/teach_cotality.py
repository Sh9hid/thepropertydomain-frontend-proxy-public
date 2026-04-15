"""
Open RP Data in a persistent browser and wait for you to demonstrate the workflow.
Captures any downloads. Press Enter in the terminal when done.

Usage:
  python teach_cotality.py
"""
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from playwright.async_api import async_playwright

PROFILE_DIR = Path(__file__).resolve().parent / ".cotality-profile"
DOWNLOAD_DIR = Path(__file__).resolve().parent / "valuation_downloads"
BASE_URL = "https://rpp.corelogic.com.au/"
USERNAME = os.getenv("RPDATA_USERNAME", "")
PASSWORD = os.getenv("RPDATA_PASSWORD", "")


async def main():
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    pw = await async_playwright().start()
    ctx = await pw.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=False,
        viewport={"width": 1440, "height": 960},
        channel="msedge",
        accept_downloads=True,
    )
    page = ctx.pages[0] if ctx.pages else await ctx.new_page()

    # Track downloads
    downloaded = []
    def on_download(dl):
        print(f"\n  >> Download captured: {dl.suggested_filename}")
        downloaded.append(dl)
    page.on("download", on_download)

    # Navigate
    print(f"Navigating to {BASE_URL} ...")
    await page.goto(BASE_URL, wait_until="domcontentloaded")
    await asyncio.sleep(5)

    # Login if needed
    url = page.url.lower()
    if "auth" in url or "login" in url:
        print(f"Login page detected — filling credentials as {USERNAME}")
        try:
            await page.fill("#username", USERNAME)
            await page.fill("#password", PASSWORD)
            await page.locator('a:has-text("Log In")').click()
            print("Credentials submitted — waiting...")
            await asyncio.sleep(8)
        except Exception as e:
            print(f"Auto-login failed: {e}")
            print("Please log in manually in the browser.")

    print(f"\nBrowser is open at: {page.url}")
    print()
    print("=" * 60)
    print("  TEACH MODE — Valuation Estimate Download")
    print("=" * 60)
    print()
    print("  1. Search for an address in the search bar")
    print("  2. Click on the property")
    print("  3. Click 'Valuation Estimate' or 'AVM' to download the PDF")
    print("  4. Come back here and press Enter")
    print()
    print("  I'll save any downloads that happen.")
    print()

    # Wait for user — use thread so asyncio loop stays alive for downloads
    await asyncio.to_thread(input, "Press Enter when you're done demonstrating...\n")

    # Save downloads
    if downloaded:
        for dl in downloaded:
            fname = dl.suggested_filename or "valuation.pdf"
            save_path = DOWNLOAD_DIR / fname
            try:
                await dl.save_as(str(save_path))
                sz = save_path.stat().st_size
                print(f"  Saved: {save_path} ({sz:,} bytes)")
            except Exception as e:
                print(f"  Save error: {e}")
                try:
                    p = await dl.path()
                    if p:
                        import shutil
                        shutil.copy2(p, str(save_path))
                        print(f"  Copied from temp: {save_path}")
                except Exception:
                    pass
    else:
        print("  No downloads were captured by Playwright.")
        print("  Check your normal Downloads folder — the file may have gone there.")

    print()
    print("Keep browser open? (y/n, default=y): ", end="", flush=True)
    answer = (await asyncio.to_thread(input, "")).strip().lower()
    if answer in ("n", "no"):
        await ctx.close()
        await pw.stop()
        print("Browser closed.")
    else:
        print("Browser stays open. Close it manually or Ctrl+C here.")
        try:
            while True:
                await asyncio.sleep(60)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    asyncio.run(main())
