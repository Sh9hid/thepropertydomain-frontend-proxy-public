from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.on('console', lambda msg: print(f'CONSOLE: {msg.type}: {msg.text}'))
    page.on('requestfailed', lambda req: print(f'FAILED: {req.url} - {req.failure}'))
    page.on('response', lambda res: print(f'RESP: {res.status} {res.url}'))
    page.goto('http://localhost:5175/operator.html')
    page.wait_for_load_state('networkidle')
    page.wait_for_timeout(2000)
    print('DOM:', page.evaluate('document.body.innerText')[:1000])
    browser.close()
