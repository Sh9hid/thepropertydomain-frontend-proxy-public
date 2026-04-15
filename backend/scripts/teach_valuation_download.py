"""
Simple teaching script for the Valuation Estimate download workflow.
Opens a persistent browser session, logs in, then waits for operator
to demonstrate the download flow.

Usage:
  python teach_valuation_download.py "51 Thoroughbred Way, Box Hill NSW 2765"
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from playwright.async_api import async_playwright

PROFILE_DIR = Path(__file__).resolve().parent / ".cotality-profile"
WORKFLOW_DIR = Path(__file__).resolve().parent / "workflows"
BASE_URL = "https://rpp.corelogic.com.au/"
USERNAME = os.getenv("RPDATA_USERNAME", "")
PASSWORD = os.getenv("RPDATA_PASSWORD", "")


async def main():
    example_address = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    print(f"[teach] Example address: {example_address or '(none)'}")
    print(f"[teach] Using persistent profile: {PROFILE_DIR}")
    print(f"[teach] RP Data credentials: {USERNAME}")

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    WORKFLOW_DIR.mkdir(parents=True, exist_ok=True)

    pw = await async_playwright().start()

    # Try Chrome first, then Edge, then Playwright chromium
    context = None
    for channel in ["chrome", "msedge", None]:
        try:
            opts = {
                "user_data_dir": str(PROFILE_DIR),
                "headless": False,
                "viewport": {"width": 1440, "height": 960},
                "args": ["--start-maximized"],
                "accept_downloads": True,
            }
            if channel:
                opts["channel"] = channel
            context = await pw.chromium.launch_persistent_context(**opts)
            print(f"[teach] Browser launched: {channel or 'playwright-chromium'}")
            break
        except Exception as e:
            print(f"[teach] {channel or 'chromium'} failed: {e}")

    if not context:
        print("[teach] ERROR: No browser could be launched")
        return

    page = context.pages[0] if context.pages else await context.new_page()

    # Set up download directory
    download_dir = Path(__file__).resolve().parent / "downloads"
    download_dir.mkdir(exist_ok=True)

    # Navigate to RP Data
    print(f"[teach] Navigating to {BASE_URL}")
    await page.goto(BASE_URL, wait_until="domcontentloaded")
    await asyncio.sleep(3)

    # Check if login needed
    body_text = ""
    try:
        body_text = await page.locator("body").inner_text(timeout=5000)
    except Exception:
        pass

    url_lower = page.url.lower()
    needs_login = any(kw in url_lower for kw in ["login", "signin", "auth"])
    if not needs_login:
        needs_login = any(kw in body_text.lower() for kw in ["sign in", "log in", "username", "password"])

    if needs_login and USERNAME and PASSWORD:
        print("[teach] Login page detected — auto-filling credentials...")
        for selector in ['input[name*="user" i]', 'input[type="email"]', 'input[id*="user" i]', 'input[placeholder*="username" i]']:
            try:
                loc = page.locator(selector)
                if await loc.count() > 0 and await loc.first.is_visible(timeout=2000):
                    await loc.first.fill(USERNAME)
                    print(f"[teach] Filled username via {selector}")
                    break
            except Exception:
                continue

        try:
            pwd_field = page.locator('input[type="password"]')
            if await pwd_field.count() > 0:
                await pwd_field.first.fill(PASSWORD)
                print("[teach] Filled password")
                await pwd_field.first.press("Enter")
                print("[teach] Submitted login — waiting for redirect...")
                await asyncio.sleep(5)
        except Exception as e:
            print(f"[teach] Password fill error: {e}")

        # Check if login succeeded
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        print(f"[teach] Post-login URL: {page.url}")
    elif needs_login:
        print("[teach] Login page detected but no credentials — please log in manually")
        input("[teach] Press Enter after you've logged in...")
    else:
        print("[teach] Already logged in or no login needed")

    print()
    print("=" * 60)
    print("  TEACHING MODE — Valuation Estimate Download")
    print("=" * 60)
    print()
    print("The browser is now open and logged in.")
    print()
    print("Please do the following:")
    print(f"  1. Search for: {example_address or 'any property address'}")
    print("  2. Navigate to the property details page")
    print("  3. Find and click the 'Valuation Estimate' button")
    print("  4. Wait for the PDF to download")
    print("  5. Come back here and press Enter")
    print()
    print("I'll record the download and save the workflow.")
    print()

    # Listen for downloads
    downloaded_files = []

    def on_download(download):
        print(f"[teach] Download started: {download.suggested_filename}")
        downloaded_files.append(download)

    page.on("download", on_download)

    input("[teach] Press Enter after you've downloaded the valuation PDF...")

    # Process downloads
    if downloaded_files:
        for dl in downloaded_files:
            filename = dl.suggested_filename
            save_path = download_dir / filename
            try:
                await dl.save_as(str(save_path))
                print(f"[teach] Saved: {save_path} ({save_path.stat().st_size:,} bytes)")
            except Exception as e:
                print(f"[teach] Download save error: {e}")
                # Try to get the path where it was already saved
                try:
                    path = await dl.path()
                    if path:
                        print(f"[teach] Download was at: {path}")
                except Exception:
                    pass
    else:
        print("[teach] No downloads were captured by Playwright.")
        print("[teach] The file may have downloaded directly to your Downloads folder.")
        print("[teach] Please check and tell me the filename.")

    # Save workflow manifest
    manifest = {
        "workflow_name": "cotality_download_valuation",
        "version": 1,
        "site": "cotality",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "description": "Search property, navigate to details, click Valuation Estimate to download PDF",
        "example_address": example_address,
        "steps": [
            {"type": "search_property", "description": "Search for address in RP Data search bar"},
            {"type": "select_property", "description": "Click on the matching property result"},
            {"type": "click_valuation_estimate", "description": "Click the Valuation Estimate button/link"},
            {"type": "wait_download", "description": "Wait for PDF download to complete", "timeout_ms": 30000},
        ],
        "download_filename_pattern": "Valuation_Estimate_AVM_*.pdf",
        "final_url": page.url,
        "downloads_captured": [dl.suggested_filename for dl in downloaded_files],
    }

    manifest_path = WORKFLOW_DIR / "cotality_download_valuation.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[teach] Workflow saved: {manifest_path}")

    print()
    print("Keep the browser open? (y/n, default=y): ", end="", flush=True)
    answer = await asyncio.to_thread(input, "")
    if answer.strip().lower() in ("n", "no"):
        await context.close()
        await pw.stop()
        print("[teach] Browser closed.")
    else:
        print("[teach] Browser stays open. Close it manually when done.")
        # Keep process alive
        try:
            while True:
                await asyncio.sleep(60)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    asyncio.run(main())
