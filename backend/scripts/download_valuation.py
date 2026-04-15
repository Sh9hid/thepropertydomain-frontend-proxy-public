"""
Download a Valuation Estimate PDF from RP Data (Cotality) for a given address.

Fully automated: login → search → property page → download valuation PDF.
Uses persistent browser profile so sessions survive across runs.

Usage:
  python download_valuation.py "51 Thoroughbred Way, Box Hill NSW 2765"
  python download_valuation.py "12 Oak Avenue, Oakville NSW 2765" --output /path/to/save.pdf
  python download_valuation.py "12 Oak Avenue, Oakville NSW 2765" --debug  (saves screenshots)
"""
import asyncio
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from playwright.async_api import (
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

SCRIPTS_DIR = Path(__file__).resolve().parent
PROFILE_DIR = SCRIPTS_DIR / ".cotality-profile"
DOWNLOAD_DIR = SCRIPTS_DIR / "valuation_downloads"
DEBUG_DIR = SCRIPTS_DIR / "debug_screenshots"
BASE_URL = "https://rpp.corelogic.com.au/"
USERNAME = os.getenv("RPDATA_USERNAME", "")
PASSWORD = os.getenv("RPDATA_PASSWORD", "")
DEBUG = "--debug" in sys.argv


def log(msg: str) -> None:
    print(f"[valuation] {msg}", flush=True)


async def screenshot(page: Page, name: str) -> None:
    if not DEBUG:
        return
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    path = DEBUG_DIR / f"{name}_{int(time.time())}.png"
    await page.screenshot(path=str(path))
    log(f"  screenshot: {path}")


async def launch_browser(pw) -> BrowserContext:
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    for channel in ["msedge", None]:
        try:
            opts = {
                "user_data_dir": str(PROFILE_DIR),
                "headless": False,
                "viewport": {"width": 1440, "height": 960},
                "channel": channel,
                "accept_downloads": True,
            } if channel else {
                "user_data_dir": str(PROFILE_DIR),
                "headless": False,
                "viewport": {"width": 1440, "height": 960},
                "accept_downloads": True,
            }
            ctx = await pw.chromium.launch_persistent_context(**opts)
            log(f"Browser launched: {channel or 'playwright-chromium'}")
            return ctx
        except Exception as e:
            log(f"{channel or 'chromium'} failed: {e}")
    raise RuntimeError("No browser could be launched")


async def do_login(page: Page) -> bool:
    """Handle the CoreLogic OAuth2 login page."""
    if not USERNAME or not PASSWORD:
        log("No RPDATA_USERNAME/RPDATA_PASSWORD — cannot login")
        return False

    log(f"Login page — filling as {USERNAME}")
    await screenshot(page, "01_login_page")

    try:
        await page.fill("#username", USERNAME)
        await page.fill("#password", PASSWORD)
        await page.locator('a:has-text("Log In")').click()
        log("Credentials submitted")
    except Exception:
        # Fallback: try generic selectors
        try:
            await page.locator('input[name="pf.username"]').fill(USERNAME)
            await page.locator('input[name="pf.pass"]').fill(PASSWORD)
            await page.locator('a:has-text("Log In"), button:has-text("Log In")').first.click()
        except Exception as e:
            log(f"Login fill failed: {e}")
            return False

    # Wait for redirect
    for _ in range(15):
        await asyncio.sleep(2)
        url = page.url.lower()
        if "rpp.corelogic.com.au" in url and "auth" not in url:
            log(f"Login success — at {page.url}")
            await screenshot(page, "02_post_login")
            return True

    log(f"Login redirect unclear — at {page.url}")
    await screenshot(page, "02_login_unclear")
    return "rpp.corelogic.com.au" in page.url.lower()


async def ensure_search_page(page: Page) -> bool:
    """Make sure we're on the RP Data search page, handle login if needed."""
    await page.goto(BASE_URL, wait_until="domcontentloaded")
    await asyncio.sleep(5)

    url = page.url.lower()

    # If redirected to auth page, login
    if "auth" in url and "corelogic" in url:
        if not await do_login(page):
            return False

    # If on linked-accounts, navigate to base
    if "linked-accounts" in page.url.lower():
        log("On linked-accounts — navigating to search...")
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await asyncio.sleep(3)

    # If on terms-and-conditions page, accept and continue
    if "terms-and-conditions" in page.url.lower():
        log("On terms-and-conditions — accepting...")
        try:
            accept_btn = page.locator('button:has-text("Accept"), a:has-text("Accept"), button:has-text("Agree"), button:has-text("Continue")').first
            if await accept_btn.count() > 0 and await accept_btn.is_visible(timeout=3000):
                await accept_btn.click()
                await asyncio.sleep(3)
            else:
                # Just navigate to base URL
                await page.goto(BASE_URL, wait_until="domcontentloaded")
                await asyncio.sleep(3)
        except Exception:
            await page.goto(BASE_URL, wait_until="domcontentloaded")
            await asyncio.sleep(3)

    await screenshot(page, "03_search_page")
    log(f"On page: {page.url}")
    return True


def _score_suggestion(suggestion_text: str, address: str) -> tuple[int, list[str]]:
    """Score a suggestion against the full address. Returns (score, matched_tokens).

    Scoring logic:
    - Street number match: +4 (must match exactly as a word)
    - Street name match: +3 per word
    - Suburb match: +3
    - State match: +1
    - Postcode match: +2
    Minimum viable match requires street number + street name present.
    """
    norm_suggestion = suggestion_text.strip().upper()
    norm_address = address.strip().upper()

    # Tokenise the address into meaningful parts (skip short connectors)
    raw_tokens = [t.strip(",.") for t in norm_address.split() if len(t.strip(",.")) > 0]

    score = 0
    matched: list[str] = []

    # Street number is always the first token (digits, e.g. "8" or "8A")
    if raw_tokens:
        street_num = raw_tokens[0]
        # Must appear as a standalone word boundary to avoid "8" matching "18"
        import re as _re
        if _re.search(r'(?<!\d)' + _re.escape(street_num) + r'(?!\d)', norm_suggestion):
            score += 4
            matched.append(f"num:{street_num}")

    # Street name words (tokens 1..N until we hit a known suburb/state/postcode marker)
    # Heuristic: tokens before the last 3 are street name; last 3 are suburb/state/postcode
    street_tokens = raw_tokens[1:-3] if len(raw_tokens) > 4 else raw_tokens[1:]
    for tok in street_tokens:
        if len(tok) >= 3 and tok in norm_suggestion:
            score += 3
            matched.append(f"street:{tok}")

    # Last 3 tokens: suburb, state, postcode
    tail_tokens = raw_tokens[-3:] if len(raw_tokens) >= 3 else raw_tokens
    for i, tok in enumerate(tail_tokens):
        if tok not in norm_suggestion:
            continue
        if tok.isdigit() and len(tok) == 4:
            score += 2
            matched.append(f"postcode:{tok}")
        elif tok in ("NSW", "VIC", "QLD", "SA", "WA", "ACT", "TAS", "NT"):
            score += 1
            matched.append(f"state:{tok}")
        else:
            score += 3
            matched.append(f"suburb:{tok}")

    return score, matched


async def _find_search_input(page: Page) -> object | None:
    """Locate the main address search input on the RP Data page."""
    search_selectors = [
        'input[placeholder*="search" i]',
        'input[placeholder*="address" i]',
        'input[aria-label*="search" i]',
        '#search-input',
        '[role="searchbox"]',
        '[role="combobox"] input',
        'input[type="search"]',
        'input[name*="search" i]',
        'input[name*="address" i]',
        '.search-bar input',
        '.rui-search-input input',
        'input.rui-text-input',
    ]
    for sel in search_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible(timeout=2000):
                log(f"  Found search input: {sel}")
                return loc
        except Exception:
            continue

    # Fallback: first visible text input that is not a login field
    log("  Standard selectors failed — scanning all visible text inputs...")
    all_inputs = page.locator('input[type="text"], input:not([type])')
    count = await all_inputs.count()
    for i in range(count):
        inp = all_inputs.nth(i)
        try:
            if not await inp.is_visible(timeout=1000):
                continue
            placeholder = await inp.get_attribute("placeholder") or ""
            name = await inp.get_attribute("name") or ""
            log(f"  Visible input [{i}]: placeholder='{placeholder}' name='{name}'")
            if "username" not in name.lower() and "password" not in name.lower():
                return inp
        except Exception:
            continue
    return None


async def _collect_suggestions(page: Page) -> list[object]:
    """Return all currently visible autocomplete suggestion locators."""
    suggestion_selectors = [
        '[role="listbox"] [role="option"]',
        '[class*="suggestion"] [class*="item"]',
        '[class*="autocomplete"] li',
        '[class*="dropdown"] [class*="item"]',
        '[class*="search-result"]',
        'ul li[class*="result"]',
        '.rui-menu-item',
        '[data-testid*="suggestion"]',
    ]
    for sel in suggestion_selectors:
        try:
            container = page.locator(sel)
            if await container.count() > 0 and await container.first.is_visible(timeout=500):
                count = await container.count()
                items = [container.nth(i) for i in range(count)]
                visible = []
                for item in items:
                    try:
                        if await item.is_visible(timeout=300):
                            visible.append(item)
                    except Exception:
                        continue
                if visible:
                    log(f"  Suggestions found via: {sel} ({len(visible)} items)")
                    return visible
        except Exception:
            continue
    return []


async def search_and_select(page: Page, address: str) -> bool:
    """Type the full address character-by-character, score all autocomplete
    suggestions, and click the best match. Falls back to Enter + results-page
    scoring if no high-confidence suggestion is found.
    """
    log(f"Searching: {address}")

    # --- 1. Locate the search input ---
    search_input = await _find_search_input(page)
    if not search_input:
        log("FAILED: no search input found")
        await screenshot(page, "04_no_search_input")
        return False

    # --- 2. Clear and focus the input ---
    await search_input.click()
    await asyncio.sleep(0.3)
    await search_input.fill("")
    await asyncio.sleep(0.2)

    # --- 3. Type the FULL address character-by-character (~50 ms/char) ---
    # This mimics real keystrokes so the autocomplete service keeps up.
    # We type the full string: number + street + suburb + state + postcode.
    # Commas in the raw address string are stripped to keep the query clean.
    clean_address = address.replace(",", " ").strip()
    # Collapse any double spaces produced by stripping commas
    import re
    clean_address = re.sub(r"\s{2,}", " ", clean_address)

    log(f"  Typing full address: '{clean_address}'")
    await search_input.type(clean_address, delay=50)
    await screenshot(page, "04_typed_address")

    # --- 4. Wait for autocomplete to populate (up to 5 s, polling every 0.5 s) ---
    suggestions: list[object] = []
    for attempt in range(10):
        await asyncio.sleep(0.5)
        suggestions = await _collect_suggestions(page)
        if suggestions:
            log(f"  Autocomplete appeared after ~{(attempt + 1) * 0.5:.1f}s ({len(suggestions)} items)")
            break
    else:
        log("  No autocomplete after 5s — continuing to Enter fallback")

    # --- 5. Score every visible suggestion and pick the best one ---
    CONFIDENCE_THRESHOLD = 7  # must match street number + at least one street word + suburb/postcode

    if suggestions:
        best_score = 0
        best_item = None
        best_text = ""
        for item in suggestions:
            try:
                text = (await item.inner_text(timeout=500)).strip()
            except Exception:
                continue
            score, matched = _score_suggestion(text, address)
            log(f"  Suggestion: '{text[:70]}' → score={score} matched={matched}")
            if score > best_score:
                best_score = score
                best_item = item
                best_text = text

        if best_item and best_score >= CONFIDENCE_THRESHOLD:
            log(f"  Selecting best suggestion (score={best_score}): '{best_text[:70]}'")
            await best_item.click()
            await asyncio.sleep(5)
            await screenshot(page, "05_property_page")
            log(f"  Property page: {page.url}")
            return True
        else:
            log(f"  Best suggestion score {best_score} < threshold {CONFIDENCE_THRESHOLD} — trying Enter")

    # --- 6. Fallback: press Enter and work the results page ---
    log("  Pressing Enter for results page...")
    await search_input.press("Enter")
    await asyncio.sleep(5)
    await screenshot(page, "05_after_enter")
    log(f"  After Enter: {page.url}")

    if "/property/" in page.url.lower():
        return True

    # Collect address tokens for scoring results-page items
    addr_parts = [p.strip(",.").upper() for p in address.split() if len(p.strip(",.")) > 1]

    # Strategy A: direct property links
    result_selectors = [
        'a[href*="/property/"]',
        '[class*="search-result"] a',
        'table tbody tr a',
        '[class*="property"] a[href]',
        'li[class*="result"]',
    ]
    for sel in result_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible(timeout=2000):
                text = (await loc.inner_text(timeout=1000))[:80]
                score, matched = _score_suggestion(text, address)
                log(f"  Result '{text[:60]}' score={score}")
                if score >= CONFIDENCE_THRESHOLD:
                    log(f"  Clicking result (score={score}): '{text[:60]}'")
                    await loc.click()
                    await asyncio.sleep(5)
                    await screenshot(page, "06_clicked_result")
                    if "/property/" in page.url.lower():
                        return True
        except Exception:
            continue

    # Strategy B: broad scored scan of all visible rows/items
    log("  Broad scored scan of visible items...")
    try:
        all_items = page.locator('tr, li, [role="row"], [role="option"], div[class*="item"]')
        count = await all_items.count()
        log(f"  Scanning {count} items against address tokens: {addr_parts[:5]}")
        best_score = 0
        best_match = None
        for i in range(min(count, 30)):
            item = all_items.nth(i)
            try:
                if not await item.is_visible(timeout=400):
                    continue
                text = (await item.inner_text(timeout=400)).strip()
                if not any(kw in text.upper() for kw in ["NSW", "VIC", "QLD", "SA", "WA", "ACT", "TAS", "NT"]):
                    continue
                score, matched = _score_suggestion(text, address)
                log(f"  Item [{i}] score={score}: '{text[:60]}'")
                if score > best_score:
                    best_score = score
                    best_match = (i, item, text)
            except Exception:
                continue

        if best_match and best_score >= CONFIDENCE_THRESHOLD:
            idx, item, text = best_match
            log(f"  Clicking best item [{idx}] (score={best_score}): '{text[:60]}'")
            await item.click()
            await asyncio.sleep(5)
            await screenshot(page, "06_clicked_address_item")
            if "/property/" in page.url.lower():
                return True
        elif best_match:
            log(f"  Best item score {best_score} < threshold — skipping to avoid wrong property")
    except Exception as e:
        log(f"  Broad scan failed: {e}")

    log("FAILED: could not find/click property")
    await screenshot(page, "06_no_result")
    return False


async def download_valuation_report(page: Page, context: BrowserContext) -> str | None:
    """On the property page, find and click the Valuation/AVM download button.

    RP Data layout (as of Apr 2026):
    - Scroll down to "Valuation Estimates" section
    - There are two tabs: "Valuation Estimate" and "Rental Estimate"
    - Within the Valuation Estimate tab, there is a BLUE BUTTON labelled
      "Valuation Estimate" with a download icon — that's the PDF trigger.
    - The blue button is styled differently from the tab (it's a solid blue
      pill-shaped button on the right side of the section).
    """
    log("Looking for Valuation Estimate...")
    await screenshot(page, "07_property_page")

    downloaded = []
    def on_download(dl):
        log(f"  >> Download started: {dl.suggested_filename}")
        downloaded.append(dl)
    page.on("download", on_download)

    # First scroll to the Valuation Estimates section
    try:
        section = page.locator('text="Valuation Estimates"').first
        if await section.count() > 0:
            await section.scroll_into_view_if_needed()
            await asyncio.sleep(1)
            log("  Scrolled to Valuation Estimates section")
    except Exception:
        # Try scrolling down generically
        await page.evaluate("window.scrollBy(0, 800)")
        await asyncio.sleep(1)

    await screenshot(page, "07b_valuation_section")

    # Strategy 1: Click the blue "Valuation Estimate" download button
    # It's a button/anchor with class containing "btn" or styled as a button,
    # distinct from the tab which is just text.
    # Target: the button that actually triggers download, not the tab label.
    blue_btn_selectors = [
        # Button-styled elements with valuation text (not tab links)
        'button:has-text("Valuation Estimate"):not([role="tab"])',
        'a.btn:has-text("Valuation Estimate")',
        '[class*="btn"]:has-text("Valuation Estimate")',
        '[class*="button"]:has-text("Valuation Estimate")',
        # RP Data uses rui- prefixed classes
        '[class*="rui-button"]:has-text("Valuation")',
        '[class*="rui-btn"]:has-text("Valuation")',
        # The Reports dropdown in the top nav
        'button:has-text("Reports")',
    ]

    for sel in blue_btn_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible(timeout=2000):
                text = (await loc.inner_text(timeout=1000)).strip()
                log(f"  Found button: '{text[:40]}' via {sel}")
                await loc.click()

                # A MUI Dialog opens with a loading spinner, then a PDF preview.
                # Wait for the PDF to generate (can take 10-20s).
                log("  Waiting for PDF dialog to load...")
                await asyncio.sleep(3)
                await screenshot(page, "08_dialog_loading")

                # Wait for the PDF to render (loading dots → canvas with PDF)
                # Then find the Download button at the bottom of the dialog
                log("  Waiting for PDF to render in dialog...")
                try:
                    # Wait for the canvas (PDF preview) to appear
                    canvas = page.locator('.MuiDialog-root canvas, .crux-print-dialog canvas')
                    await canvas.first.wait_for(state="visible", timeout=30000)
                    log("  PDF canvas rendered")
                    await asyncio.sleep(2)
                    await screenshot(page, "08_pdf_ready")
                except PlaywrightTimeoutError:
                    log("  PDF canvas didn't render in 30s")
                    await screenshot(page, "08_no_canvas")

                # The Download button is an <a> tag (not <button>) with MuiButton classes
                dl_btn = page.locator('.MuiDialog-root a:has-text("Download")').first
                try:
                    if await dl_btn.count() > 0 and await dl_btn.is_visible(timeout=3000):
                        log("  Clicking Download <a> in dialog")
                        await dl_btn.click()
                        await asyncio.sleep(8)
                        if downloaded:
                            return await _save_download(downloaded[0])
                except Exception as e:
                    log(f"  Download click error: {e}")
                await screenshot(page, "08_after_dl_attempt")

                # Check if download happened anyway
                if downloaded:
                    return await _save_download(downloaded[0])
                break  # Don't try more selectors if the dialog opened
        except Exception:
            continue

    # Check if download already happened from Strategy 1
    if downloaded:
        return await _save_download(downloaded[0])

    # Strategy 2: Find ALL elements with "Valuation Estimate" text and try each
    # The download button is usually the second or third match (first is section title, second is tab)
    log("  Trying all 'Valuation Estimate' elements...")
    all_ve = page.locator(':text("Valuation Estimate")')
    ve_count = await all_ve.count()
    log(f"  Found {ve_count} 'Valuation Estimate' elements")
    for i in range(ve_count):
        try:
            el = all_ve.nth(i)
            if not await el.is_visible(timeout=1000):
                continue
            tag = await el.evaluate("el => el.tagName")
            classes = await el.evaluate("el => el.className")
            text = (await el.inner_text(timeout=1000)).strip()
            log(f"  [{i}] <{tag}> class='{str(classes)[:50]}' text='{text[:40]}'")
            # Skip headings and section titles
            if tag.upper() in ("H1", "H2", "H3", "H4", "SPAN", "P"):
                continue
            # Click if it looks like a button/anchor
            if tag.upper() in ("A", "BUTTON") or "btn" in str(classes).lower() or "button" in str(classes).lower():
                log(f"  Clicking element [{i}]...")
                await el.click()
                await asyncio.sleep(8)
                if downloaded:
                    return await _save_download(downloaded[0])
                await screenshot(page, f"08_ve_click_{i}")
        except Exception as exc:
            log(f"  [{i}] error: {exc}")
            continue

    # Strategy 3: Click the "Reports" dropdown button at the top
    try:
        reports_btn = page.locator('button:has-text("Reports"), a:has-text("Reports")').first
        if await reports_btn.count() > 0 and await reports_btn.is_visible(timeout=2000):
            log("  Clicking Reports dropdown...")
            await reports_btn.click()
            await asyncio.sleep(2)
            await screenshot(page, "08_reports_dropdown")
            # Look for Valuation Estimate in dropdown
            ve_link = page.locator('[role="menu"] :text("Valuation"), [class*="dropdown"] :text("Valuation"), [class*="menu"] :text("Valuation")').first
            if await ve_link.count() > 0 and await ve_link.is_visible(timeout=2000):
                log("  Clicking Valuation in Reports menu...")
                await ve_link.click()
                await asyncio.sleep(8)
                if downloaded:
                    return await _save_download(downloaded[0])
    except Exception:
        pass

    # Wait for any delayed download
    await asyncio.sleep(5)
    if downloaded:
        return await _save_download(downloaded[0])

    # Check for new pages/popups with PDF
    for p in context.pages:
        if ".pdf" in (p.url or "").lower():
            log(f"  PDF in tab: {p.url}")

    log("FAILED: could not trigger valuation download")
    await screenshot(page, "09_no_valuation")
    return None


async def _save_download(dl) -> str | None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    fname = dl.suggested_filename or f"Valuation_{int(time.time())}.pdf"
    save_path = DOWNLOAD_DIR / fname
    try:
        await dl.save_as(str(save_path))
        sz = save_path.stat().st_size
        log(f"  SAVED: {save_path} ({sz:,} bytes)")
        return str(save_path)
    except Exception as e:
        log(f"  Save error: {e}")
        try:
            p = await dl.path()
            if p:
                import shutil
                shutil.copy2(p, str(save_path))
                log(f"  Copied: {save_path}")
                return str(save_path)
        except Exception:
            pass
    return None


async def download_valuation(address: str, output_path: str | None = None) -> str | None:
    """Main flow: login → search → property page → download valuation PDF."""
    pw = await async_playwright().start()
    try:
        context = await launch_browser(pw)
        page = context.pages[0] if context.pages else await context.new_page()

        # Step 1: Get to search page
        if not await ensure_search_page(page):
            return None

        # Step 2: Search and select property
        if not await search_and_select(page, address):
            return None

        # Step 3: Download valuation
        result = await download_valuation_report(page, context)

        if result and output_path:
            import shutil
            shutil.move(result, output_path)
            result = output_path

        return result

    finally:
        try:
            await context.close()
        except Exception:
            pass
        await pw.stop()


async def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        print("Usage: python download_valuation.py \"ADDRESS\" [--debug] [--output PATH]")
        sys.exit(1)

    address = " ".join(args)
    output = None
    if "--output" in sys.argv:
        idx = sys.argv.index("--output")
        if idx + 1 < len(sys.argv):
            output = sys.argv[idx + 1]

    result = await download_valuation(address, output)
    if result:
        print(result)
        sys.exit(0)
    else:
        log("Download failed")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
