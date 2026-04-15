import os
import sys
import asyncio
import subprocess
import json

async def test_extraction(address: str):
    print(f"🕵️ Searching OSINT for: {address}")
    
    # 1. Google Search for the property page
    query = f"{address} Australia property listing"
    search_url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
    
    subprocess.run(["browser-use", "close"], capture_output=True)
    subprocess.run(["browser-use", "open", search_url], capture_output=True)
    await asyncio.sleep(5)
    
    # Find a good property portal link
    extract_portal = """
    const links = Array.from(document.querySelectorAll('a'));
    const portal = links.find(l => 
        l.href.includes('onthehouse.com.au/property/') || 
        l.href.includes('homely.com.au/homes/') ||
        l.href.includes('view.com.au/property/') ||
        l.href.includes('realestate.com.au/property/')
    );
    return portal ? portal.href : null;
    """
    res = subprocess.run(["browser-use", "eval", extract_portal], capture_output=True, text=True)
    portal_url = res.stdout.strip()
    
    if not portal_url or "http" not in portal_url:
        print("❌ No portal URL found in Google search.")
        return
        
    print(f"🔗 Found portal URL: {portal_url}")
    subprocess.run(["browser-use", "open", portal_url], capture_output=True)
    await asyncio.sleep(8) # Wait for page and images
    
    # Extract the main image
    extract_img = """
    // Try common property portal image selectors
    const selectors = [
        'img.property-image', 
        '.property-image img',
        'img[alt*="Property Photo"]',
        '.hero-image img',
        'img.main-image',
        '.gallery img',
        'img[src*="cloudfront.net"]',
        'img[src*="domain.com.au"]',
        'img[src*="realestate.com.au"]'
    ];
    for (const s of selectors) {
        const img = document.querySelector(s);
        if (img && img.src && img.src.startsWith('http')) return img.src;
    }
    // Fallback: find any large image
    const imgs = Array.from(document.querySelectorAll('img'));
    const big = imgs.find(i => i.width > 400 && i.src.startsWith('http'));
    return big ? big.src : null;
    """
    res = subprocess.run(["browser-use", "eval", extract_img], capture_output=True, text=True)
    img_url = res.stdout.strip()
    
    if img_url and "http" in img_url:
        print(f"✅ SUCCESS: {img_url}")
    else:
        print("❌ FAILED: Could not extract image URL from page.")
    
    subprocess.run(["browser-use", "close"], capture_output=True)

if __name__ == "__main__":
    addr = "UNIT 2 123 PORPOISE CRES, Bligh Park NSW"
    if len(sys.argv) > 1:
        addr = sys.argv[1]
    asyncio.run(test_extraction(addr))
