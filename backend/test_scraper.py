import asyncio
from playwright.async_api import async_playwright
import re

def normalize_detail_url(raw_value):
    if not raw_value: return None
    value = raw_value.strip().replace('&amp;', '&')
    if not value or value in {'#', 'javascript:void(0)', 'javascript:void(0);'}: return None
    if value.startswith(('http://', 'https://')): return value
    if value.startswith('/'): return f'https://onlineregistry.lawlink.nsw.gov.au{value}'
    if value.startswith('javascript:'):
        match = re.search(r"['\"](https?://[^'\"]+|/[^'\"]+)['\"]", value, re.IGNORECASE)
        if match: return normalize_detail_url(match.group(1))
        return None
    return f'https://onlineregistry.lawlink.nsw.gov.au/{value.lstrip("/")}'

def extract_url_from_onclick(onclick):
    if not onclick: return None
    
    dialog_match = re.search(r"prepareDialog\((\d+)\)", onclick)
    if dialog_match:
        notice_id = dialog_match.group(1)
        return f"https://onlineregistry.lawlink.nsw.gov.au/probate/notice?noticeID={notice_id}"
        
    patterns = [
        r"window\.open\(\s*['\"]([^'\"]+)['\"]",
        r"location\.href\s*=\s*['\"]([^'\"]+)['\"]",
        r"document\.location\s*=\s*['\"]([^'\"]+)['\"]",
        r"submitNotice\(\s*['\"]([^'\"]+)['\"]"
    ]
    for pattern in patterns:
        match = re.search(pattern, onclick, re.IGNORECASE)
        if match: return normalize_detail_url(match.group(1))
    generic = re.search(r"['\"](https?://[^'\"]+|/[^'\"]+)['\"]", onclick, re.IGNORECASE)
    if generic: return normalize_detail_url(generic.group(1))
    return None

async def resolve_row_detail_url(row):
    anchors = await row.query_selector_all('a')
    for anchor in anchors:
        href = await anchor.get_attribute('href')
        normalized_href = normalize_detail_url(href)
        if normalized_href: return normalized_href
        onclick = await anchor.get_attribute('onclick')
        extracted = extract_url_from_onclick(onclick)
        if extracted: return extracted
        for attr in ('data-href', 'data-url', 'data-link'):
            attr_value = await anchor.get_attribute(attr)
            normalized_attr = normalize_detail_url(attr_value)
            if normalized_attr: return normalized_attr
    onclick = await row.get_attribute('onclick')
    return extract_url_from_onclick(onclick)

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            print('Navigating...')
            await page.goto('https://onlineregistry.lawlink.nsw.gov.au/probate/', wait_until='networkidle')
            await page.fill('#searchForm_suburb', 'Windsor')
            await page.click('#searchForm_search')
            await page.wait_for_selector('table tbody tr', timeout=10000)
            rows = await page.query_selector_all('table tbody tr')
            print(f'Rows found: {len(rows)}')
            if rows:
                row = rows[0]
                cells = await row.query_selector_all('td')
                texts = [await cell.inner_text() for cell in cells]
                print(f'Row texts: {texts}')
                
                row_html = await row.evaluate("el => el.outerHTML")
                print(f'Row HTML: {row_html}')
                
                detail_url = await resolve_row_detail_url(row)
                print(f'Resolved Detail URL: {detail_url}')
                
                if detail_url:
                    detail_page = await browser.new_page()
                    await detail_page.goto(detail_url, wait_until='domcontentloaded')
                    await detail_page.wait_for_timeout(2000)
                    body_text = await detail_page.locator('body').inner_text()
                    print('--- DETAIL PAGE BODY TEXT ---')
                    print(body_text[:1000]) # Print first 1000 chars
                    print('-----------------------------')
                    
                    selectors = ["main", "article", ".content", ".notice", "#content", "table", "dl", "p"]
                    collected = []
                    for selector in selectors:
                        for text in await detail_page.locator(selector).all_inner_texts():
                            cleaned = re.sub(r"\s+", " ", text).strip()
                            if cleaned: collected.append(cleaned)
                    print(f"Collected texts from selectors: {collected[:5]}")
                    
                    await detail_page.close()
                else:
                    print('No detail URL could be extracted.')
        except Exception as e:
            print(f'Error: {e}')
        finally:
            await browser.close()

asyncio.run(run())