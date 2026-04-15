import asyncio
import datetime
import hashlib
import math
import re
from typing import Iterable, Optional

import httpx
from colorama import Fore, Style, init
from playwright.async_api import async_playwright

from core.config import API_KEY
from models.sql_models import Lead
from services.lead_service import save_lead

# --- MODERN TERMINAL INIT ---
init(autoreset=True)

from core.events import event_manager
from core import config

def log_status(category, message, color=Fore.CYAN):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    icons = {
        "hunt": "*",
        "machine": "#",
        "sync": "+",
        "intel": ">",
        "save": "OK",
        "error": "!",
        "info": "i"
    }
    icon = icons.get(category.lower(), "-")
    print(f"{Fore.BLACK}{Style.BRIGHT}[{ts}] {color}{Style.BRIGHT}{icon} {category.upper():<8}{Style.NORMAL} {Fore.WHITE}{message}")
    
    # Broadcast to pulse
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(event_manager.broadcast_log(message, category=category.upper()))
    except Exception:
        pass

API_BASE = f"{config.BASE_URL.rstrip('/')}/api"
BOX_HILL_CENTER = (-33.6545, 150.8415)
TARGET_RADIUS_KM = 30
MAX_LEADS_PER_CYCLE = 40

# Approximate centers used to scope probate searches and geocode fallbacks.
TARGET_SUBURBS = {
    "Windsor": {"postcode": "2756", "lat": -33.6083, "lng": 150.8228},
    "Oakville": {"postcode": "2765", "lat": -33.6150, "lng": 150.8552},
    "South Windsor": {"postcode": "2756", "lat": -33.6267, "lng": 150.8146},
    "Pitt Town": {"postcode": "2756", "lat": -33.5857, "lng": 150.8540},
    "McGraths Hill": {"postcode": "2756", "lat": -33.6166, "lng": 150.8358},
    "Vineyard": {"postcode": "2765", "lat": -33.6457, "lng": 150.8415},
    "Mulgrave": {"postcode": "2756", "lat": -33.6261, "lng": 150.8249},
    "Box Hill": {"postcode": "2765", "lat": -33.6395, "lng": 150.8845},
    "Gables": {"postcode": "2765", "lat": -33.6313, "lng": 150.8732},
    "Rouse Hill": {"postcode": "2155", "lat": -33.6825, "lng": 150.9175},
    "Kellyville": {"postcode": "2155", "lat": -33.7108, "lng": 150.9561},
    "The Ponds": {"postcode": "2769", "lat": -33.7034, "lng": 150.9037},
    "Schofields": {"postcode": "2762", "lat": -33.7046, "lng": 150.8745},
    "Riverstone": {"postcode": "2765", "lat": -33.6837, "lng": 150.8667},
    "Marsden Park": {"postcode": "2765", "lat": -33.6761, "lng": 150.8266},
    "Maraylya": {"postcode": "2765", "lat": -33.5939, "lng": 150.9027},
    "Annangrove": {"postcode": "2156", "lat": -33.6586, "lng": 150.9483},
    "Nelson": {"postcode": "2765", "lat": -33.5649, "lng": 150.9563},
    "Stanhope Gardens": {"postcode": "2768", "lat": -33.7182, "lng": 150.9290},
    "Acacia Gardens": {"postcode": "2763", "lat": -33.7315, "lng": 150.9119},
}


def distance_km(lat: float, lng: float, anchor_lat: float, anchor_lng: float) -> float:
    radius = 6371
    d_lat = math.radians(lat - anchor_lat)
    d_lng = math.radians(lng - anchor_lng)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(anchor_lat))
        * math.cos(math.radians(lat))
        * math.sin(d_lng / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def lead_id(*parts: str) -> str:
    payload = "|".join(part.strip().lower() for part in parts if part)
    return hashlib.md5(payload.encode()).hexdigest()


def normalize_name(raw_name: str) -> str:
    parts = [part.strip() for part in raw_name.split(",") if part.strip()]
    if len(parts) == 2:
        return f"{parts[1]} {parts[0]}"
    return re.sub(r"\s+", " ", raw_name).strip()


def find_address_candidate(texts: Iterable[str], suburb: str, owner_name: str) -> Optional[str]:
    street_tokens = ("street", "st", "road", "rd", "avenue", "ave", "drive", "dr", "close", "cres", "crescent", "place", "pl", "lane", "way", "court", "ct", "highway", "hwy")
    for text in texts:
        cleaned = re.sub(r"\s+", " ", text).strip()
        lowered = cleaned.lower()
        # Check if any token is present as a whole word
        has_street_type = any(re.search(rf"\b{token}\b", lowered) for token in street_tokens)
        
        if has_street_type and re.search(r"\d", cleaned):
            # Skip obvious non-residential / solicitor markers
            if any(bad in lowered for bad in ["po box", "p o box", "gpo box", "dx ", "level ", "suite ", "floor ", "solicitor", "lawyer", "attorney", "chambers", "legal"]):
                continue
            # Also skip very short strings or very long paragraphs
            if 10 < len(cleaned) < 150:
                return cleaned
    return None


def find_solicitor_candidate(texts: Iterable[str]) -> str:
    blocked_markers = (
        "skip to main content",
        "current alerts",
        "online registry",
        "courts and tribunals",
        "file forms",
        "online court",
        "pay your penalty",
        "lawaccess",
        "finding a lawyer",
        "need legal help",
        "frequently asked questions",
        "copyright",
    )
    solicitor_pattern = re.compile(
        r"\b([A-Z][a-zA-Z'.-]+(?:\s+[A-Z][a-zA-Z'.-]+){0,4})\s+(Solicitor|Lawyers?|Law\s+Firm|Attorney)\b",
        re.IGNORECASE,
    )
    for text in texts:
        cleaned = re.sub(r"\s+", " ", text).strip()
        lowered = cleaned.lower()
        if len(cleaned) > 120 or any(marker in lowered for marker in blocked_markers):
            continue
        match = solicitor_pattern.search(cleaned)
        if match:
            return match.group(0).strip()
        if any(keyword in lowered for keyword in ("solicitor", "lawyers", "attorney", "law firm")):
            return cleaned
    return "NSW Supreme Court Probate Registry"


def calculate_probate_score(suburb: str, has_precise_address: bool, has_named_solicitor: bool) -> tuple[int, int]:
    heat = 70
    confidence = 78
    if suburb == "Box Hill":
        heat += 12
        confidence += 8
    if has_precise_address:
        heat += 10
        confidence += 8
    if has_named_solicitor:
        heat += 8
        confidence += 6
    return min(heat, 98), min(confidence, 100)


def build_summary_points(suburb: str, notice_type: str, date_death: str, solicitor: str) -> list[str]:
    points = [f"Verified probate notice in {suburb}", f"Trigger: {notice_type}"]
    if date_death:
        points.append(f"DOD captured: {date_death}")
    if solicitor and solicitor != "NSW Supreme Court Probate Registry":
        points.append("Solicitor or legal office visible on source")
    else:
        points.append("Registry path available for gatekeeper follow-up")
    return points


def qualify_probate_lead(suburb: str, lat: float, lng: float) -> bool:
    return distance_km(lat, lng, BOX_HILL_CENTER[0], BOX_HILL_CENTER[1]) <= TARGET_RADIUS_KM

async def update_agent_status(agent_id, activity=None, status=None, health=None):
    headers = {"X-API-KEY": API_KEY}
    payload = {"id": agent_id}
    if activity: payload["activity"] = activity
    if status: payload["status"] = status
    if health is not None: payload["health"] = health
    
    try:
        async with httpx.AsyncClient() as client:
            await client.post(f"{API_BASE}/system/update_agent", json=payload, headers=headers)
    except Exception as e:
        pass # Silent failure for agent status updates to keep terminal clean

async def geocode_address(address):
    try:
        async with httpx.AsyncClient() as client:
            headers = {"User-Agent": "HillsIntelligenceHub/1.0"}
            params = {"q": address, "format": "json", "limit": 1}
            response = await client.get("https://nominatim.openstreetmap.org/search", params=params, headers=headers)
            data = response.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        log_status("error", f"Geocoding failed for {address}", Fore.RED)
    return None, None

async def extract_detail_texts(page, detail_url: str) -> list[str]:
    try:
        await page.goto(detail_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(800)
        selectors = [
            "main",
            "article",
            ".content",
            ".notice",
            "#content",
            "table",
            "dl",
            "p",
        ]
        collected: list[str] = []
        for selector in selectors:
            for text in await page.locator(selector).all_inner_texts():
                cleaned = re.sub(r"\s+", " ", text).strip()
                if cleaned:
                    collected.append(cleaned)

        if not collected:
            for text in await page.locator("body").all_inner_texts():
                cleaned = re.sub(r"\s+", " ", text).strip()
                if cleaned:
                    collected.append(cleaned)

        blocked_markers = (
            "skip to main content",
            "current alerts",
            "courts and tribunals",
            "need legal help",
            "frequently asked questions",
            "footer menu",
            "copyright",
        )
        return [
            text
            for text in collected
            if len(text) <= 300 and not any(marker in text.lower() for marker in blocked_markers)
        ]
    except Exception:
        return []


def normalize_detail_url(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None

    value = raw_value.strip().replace("&amp;", "&")
    if not value or value in {"#", "javascript:void(0)", "javascript:void(0);"}:
        return None

    if value.startswith(("http://", "https://")):
        return value

    if value.startswith("/"):
        return f"https://onlineregistry.lawlink.nsw.gov.au{value}"

    if value.startswith("javascript:"):
        match = re.search(r"['\"](https?://[^'\"]+|/[^'\"]+)['\"]", value, re.IGNORECASE)
        if match:
            return normalize_detail_url(match.group(1))
        return None

    return f"https://onlineregistry.lawlink.nsw.gov.au/{value.lstrip('/')}"


def extract_url_from_onclick(onclick: Optional[str]) -> Optional[str]:
    if not onclick:
        return None

    # Handle specific dialog function
    dialog_match = re.search(r"prepareDialog\((\d+)\)", onclick)
    if dialog_match:
        notice_id = dialog_match.group(1)
        return f"https://onlineregistry.lawlink.nsw.gov.au/probate/notice?noticeID={notice_id}"

    patterns = [
        r"window\.open\(\s*['\"]([^'\"]+)['\"]",
        r"location\.href\s*=\s*['\"]([^'\"]+)['\"]",
        r"document\.location\s*=\s*['\"]([^'\"]+)['\"]",
        r"submitNotice\(\s*['\"]([^'\"]+)['\"]",
    ]
    for pattern in patterns:
        match = re.search(pattern, onclick, re.IGNORECASE)
        if match:
            return normalize_detail_url(match.group(1))

    generic = re.search(r"['\"](https?://[^'\"]+|/[^'\"]+)['\"]", onclick, re.IGNORECASE)
    if generic:
        return normalize_detail_url(generic.group(1))

    return None


async def resolve_row_detail_url(row) -> Optional[str]:
    anchors = await row.query_selector_all("a")
    for anchor in anchors:
        href = await anchor.get_attribute("href")
        normalized_href = normalize_detail_url(href)
        if normalized_href:
            return normalized_href

        onclick = await anchor.get_attribute("onclick")
        extracted = extract_url_from_onclick(onclick)
        if extracted:
            return extracted

        for attr in ("data-href", "data-url", "data-link"):
            attr_value = await anchor.get_attribute(attr)
            normalized_attr = normalize_detail_url(attr_value)
            if normalized_attr:
                return normalized_attr

    onclick = await row.get_attribute("onclick")
    return extract_url_from_onclick(onclick)


async def scrape_nsw_probate_market():
    log_status("hunt", "Initializing seller-intent probate sweep for Box Hill radius", Fore.CYAN)
    await update_agent_status(
        "agent_intercept",
        activity=f"Scanning probate leads within {TARGET_RADIUS_KM} km of Box Hill...",
        status="Scanning",
        health=92,
    )

    saved_count = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        detail_page = await context.new_page()

        try:
            for suburb, profile in TARGET_SUBURBS.items():
                if saved_count >= MAX_LEADS_PER_CYCLE:
                    break

                log_status("machine", f"Navigating NSW Probate Registry for {suburb}", Fore.MAGENTA)
                await update_agent_status(
                    "agent_intercept",
                    activity=f"Scanning {suburb} for owner-side probate signals...",
                    status="Scanning",
                )
                await page.goto("https://onlineregistry.lawlink.nsw.gov.au/probate/", wait_until="domcontentloaded")
                await page.wait_for_selector("#searchForm_suburb")
                await page.fill("#searchForm_suburb", suburb)
                await page.click("#searchForm_search")

                try:
                    await page.wait_for_selector("table tbody tr", timeout=10000)
                except Exception:
                    log_status("info", f"No probate signals surfaced for {suburb}.", Fore.YELLOW)
                    continue

                rows = await page.query_selector_all("table tbody tr")
                log_status("sync", f"Found {len(rows)} candidate rows in {suburb}.", Fore.GREEN)

                for row in rows:
                    if saved_count >= MAX_LEADS_PER_CYCLE:
                        break

                    cells = await row.query_selector_all("td")
                    if len(cells) < 4:
                        continue

                    cell_texts = [re.sub(r"\s+", " ", (await cell.inner_text())).strip() for cell in cells]
                    raw_name = cell_texts[0]
                    clean_name = normalize_name(raw_name)
                    
                    date_of_notice_raw = cell_texts[2] if len(cell_texts) > 2 else ""
                    try:
                        date_of_notice = datetime.datetime.strptime(date_of_notice_raw, "%d/%m/%Y").date().isoformat()
                    except ValueError:
                        date_of_notice = datetime.date.today().isoformat()
                        
                    notice_type = cell_texts[3] if len(cell_texts) > 3 else "Probate notice"
                    date_death = cell_texts[4] if len(cell_texts) > 4 else ""

                    detail_url = await resolve_row_detail_url(row)

                    detail_texts = list(cell_texts)
                    if detail_url:
                        detail_texts.extend(await extract_detail_texts(detail_page, detail_url))

                    address = find_address_candidate(detail_texts, suburb, clean_name)
                    
                    # Enforce Excel address mapping
                    import sys
                    import os
                    # Ensure excel_lookup is accessible
                    temp_dir = str(config.TEMP_DIR)
                    if temp_dir not in sys.path:
                        sys.path.append(temp_dir)
                    try:
                        import excel_lookup
                        excel_address = excel_lookup.find_address_in_excel(clean_name)
                        if excel_address:
                            # Use the address found in the folder
                            address = excel_address
                            has_precise_address = True
                        else:
                            # If not found in the L+S stock folder, leave it
                            log_status("info", f"No address found in L+S stock folder for {clean_name}, skipping.", Fore.YELLOW)
                            continue
                    except Exception as e:
                        log_status("error", f"Error querying excel stock: {e}", Fore.RED)
                        if not address:
                            continue
                    # -----------------------------------------------

                    if not address:
                        log_status("info", f"No address found for {clean_name}, skipping.", Fore.YELLOW)
                        continue

                    solicitor = find_solicitor_candidate(detail_texts)
                    has_precise_address = True
                    has_named_solicitor = solicitor != "NSW Supreme Court Probate Registry"

                    await update_agent_status("agent_id", activity=f"Resolving probate asset for {clean_name}...", status="Resolving")
                    lat, lng = await geocode_address(address)
                    if not lat or not lng:
                        lat, lng = profile["lat"], profile["lng"]

                    if not qualify_probate_lead(suburb, lat, lng):
                        continue

                    heat_score, confidence_score = calculate_probate_score(suburb, has_precise_address, has_named_solicitor)
                    if confidence_score < 78:
                        continue

                    source_label = "NSW Probate Registry detail" if detail_url else "NSW Probate Registry row"
                    asset_photo = f"https://static-maps.yandex.ru/1.x/?ll={lng},{lat}&z=18&l=sat&size=600,400"
                    lead = Lead(
                        id=lead_id(clean_name, suburb, date_death, notice_type),
                        address=address,
                        suburb=suburb,
                        postcode=profile["postcode"],
                        owner_name=clean_name,
                        trigger_type="Probate",
                        heat_score=heat_score,
                        scenario=f"Verified {notice_type.lower()} signal. Estate-side decision point detected{f' (DOD: {date_death})' if date_death else ''}.",
                        strategic_value="Estate liquidation / executor-led sale",
                        contact_status="GATEKEEPER_READY",
                        ownership_tenure="Estate transition",
                        equity_estimate="High probability of accumulated equity",
                        confidence_score=confidence_score,
                        potential_contacts=[
                            {"type": "Solicitor", "value": solicitor, "probability": 92 if has_named_solicitor else 72, "source": source_label},
                            {"type": "Mail", "value": "Use registry notice to identify acting estate office", "probability": 60, "source": "Probate workflow"},
                        ],
                        lat=lat,
                        lng=lng,
                        est_value=1850000 if suburb == "Box Hill" else 1550000,
                        date_found=date_of_notice,
                        key_details=["Probate notice", notice_type, suburb],
                        main_image=asset_photo,
                        description_deep=(
                            f"Public probate record ties {clean_name} to a likely estate-controlled property in {suburb}. "
                            f"Seller intent is inferred from a formal legal transition rather than portal inventory."
                        ),
                        features=["Owner-side trigger", "Probate verified", "Gatekeeper contact path"],
                        conversion_strategy=(
                            "Approach as an estate-support conversation. Lead with valuation clarity, executor process support, "
                            "and a discreet off-market disposition path rather than a generic listing pitch."
                        ),
                        summary_points=build_summary_points(suburb, notice_type, date_death, solicitor),
                        horizon="IMMEDIATE",
                        last_checked=datetime.datetime.now().strftime("%H:%M"),
                        exhaustive_summary=(
                            f"{clean_name} surfaced via a live probate registry event inside the Box Hill acquisition radius. "
                            "This is a direct ownership transition signal with a lawful gatekeeper route, making it materially stronger than general suburb farming."
                        ),
                        likely_scenario=(
                            "Executor or estate representative will need pricing, sale sequencing, and a low-friction disposal recommendation."
                        ),
                        strategic_why=(
                            "Qualified because the lead is owner-side, time-sensitive, within the Box Hill radius, and reachable through a probate-linked legal contact."
                        ),
                        owner_age=None,
                        suburb_average_tenure=11,
                        propensity_score=min(heat_score + 2, 99),
                        recent_sales_velocity="High growth corridor; estate assets typically attract quick local enquiry when priced correctly.",
                        est_net_profit=0,
                        local_dominance="Strong if approached as estate specialist rather than generic listing pitch.",
                        zoning_type="Probate / residential",
                        status="captured",
                        conversion_score=min(heat_score, 95),
                        compliance_score=95,
                        readiness_score=92 if has_named_solicitor else 78,
                        next_actions=[
                            {"title": "Run appraisal call within 24h", "owner": config.DEFAULT_OPERATOR_NAME, "due_at": datetime.date.today().isoformat(), "channel": "phone", "message_template_id": "estate_call_v1"},
                            {"title": "Send D0 follow-up message", "owner": config.DEFAULT_OPERATOR_NAME, "due_at": datetime.date.today().isoformat(), "channel": "whatsapp", "message_template_id": "estate_followup_d0"},
                        ],
                        source_evidence=[
                            "NSW Online Probate Registry",
                            source_label,
                            detail_url if detail_url else "registry-row-only",
                        ],
                    )
                    await save_lead(lead)
                    saved_count += 1
                    log_status("save", f"Qualified seller-intent lead saved: {clean_name} ({suburb})", Fore.GREEN)

            health = 100 if saved_count >= 10 else 76
            activity = f"Hunt cycle secure. Qualified {saved_count} owner-side probate leads."
            await update_agent_status("agent_intercept", activity=activity, status="Active", health=health)
            await update_agent_status("agent_id", activity=f"Resolved {saved_count} qualified probate leads.", status="Active", health=health)
            await update_agent_status("agent_strategy", activity="Summaries generated for seller-intent leads.", status="Active", health=98)
        except Exception as e:
            log_status("error", f"Scraper failure: {str(e)[:90]}", Fore.RED)
            await update_agent_status("agent_intercept", activity=f"Error: {str(e)[:40]}", status="Degraded", health=50)
        finally:
            await detail_page.close()
            await browser.close()

async def main():
    print(f"\n{Fore.CYAN}{Style.BRIGHT}HILLS INTELLIGENCE HUB | 2026 AUTONOMOUS HOUND ACTIVE")
    print(f"{Fore.BLACK}{Style.BRIGHT}--------------------------------------------------\n")
    while True:
        await scrape_nsw_probate_market()
        log_status("machine", "Cycle complete. Entering standby for 1 hour.", Fore.WHITE)
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
