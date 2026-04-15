import asyncio
from playwright.async_api import async_playwright
import os
import sys
from pathlib import Path
import json

# Ensure core is importable
sys.path.append(str(Path(__file__).resolve().parents[2]))
from core import config

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Property Brief</title>
    <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600&family=Inter:wght@300;400;500;600&display=swap" rel="stylesheet">
    <style>
        :root {{
            --brand-dark: #111111;
            --brand-accent: #CFA144; /* Premium Gold/Bronze */
            --text-main: #2D2D2D;
            --text-light: #666666;
            --bg-page: #FDFDFD;
        }}
        
        @page {{ size: A4; margin: 0; }}
        
        body {{
            margin: 0;
            padding: 0;
            font-family: 'Inter', sans-serif;
            background: var(--bg-page);
            color: var(--text-main);
            -webkit-font-smoothing: antialiased;
        }}
        
        .page {{
            width: 210mm;
            height: 297mm;
            padding: 20mm;
            box-sizing: border-box;
            position: relative;
            page-break-after: always;
            background: white;
            overflow: hidden;
        }}

        .cover-page {{
            background: var(--brand-dark);
            color: white;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }}

        .brand-header {{
            position: absolute;
            top: 20mm;
            left: 20mm;
            width: calc(100% - 40mm);
            border-bottom: 1px solid rgba(255,255,255,0.2);
            padding-bottom: 10mm;
        }}

        .logo {{
            font-family: 'Playfair Display', serif;
            font-size: 28px;
            font-weight: 600;
            letter-spacing: 2px;
            text-transform: uppercase;
        }}

        .logo-sub {{
            font-family: 'Inter', sans-serif;
            font-size: 10px;
            letter-spacing: 4px;
            color: var(--brand-accent);
            margin-top: 4px;
            text-transform: uppercase;
        }}

        .title-block {{
            margin-top: 80mm;
        }}

        .report-type {{
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 4px;
            color: var(--brand-accent);
            margin-bottom: 20px;
        }}

        h1 {{
            font-family: 'Playfair Display', serif;
            font-size: 48px;
            line-height: 1.2;
            margin: 0 0 20px 0;
            font-weight: 400;
        }}

        .prepared-for {{
            margin-top: 40px;
            font-size: 14px;
            color: rgba(255,255,255,0.7);
        }}

        .owner-name {{
            font-size: 18px;
            font-weight: 500;
            color: white;
            margin-top: 8px;
        }}

        .dark-footer {{
            position: absolute;
            bottom: 20mm;
            left: 20mm;
            font-size: 10px;
            color: rgba(255,255,255,0.4);
            letter-spacing: 1px;
        }}

        /* --- INTERNAL PAGES --- */
        .page-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #EEEEEE;
            padding-bottom: 10mm;
            margin-bottom: 15mm;
        }}

        .page-logo {{
            color: var(--brand-dark);
        }}

        .page-title {{
            font-family: 'Playfair Display', serif;
            font-size: 32px;
            color: var(--brand-dark);
            margin: 0;
        }}

        .letter-content {{
            font-size: 14px;
            line-height: 1.8;
            color: var(--text-main);
            max-width: 140mm;
        }}

        .letter-content p {{
            margin-bottom: 20px;
        }}

        .signature-block {{
            margin-top: 40px;
            border-top: 1px solid #EEEEEE;
            padding-top: 20px;
            max-width: 80mm;
        }}

        .sig-name {{
            font-family: 'Playfair Display', serif;
            font-size: 20px;
            font-weight: 600;
            color: var(--brand-dark);
        }}

        .sig-title {{
            font-size: 11px;
            color: var(--text-light);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-top: 4px;
        }}

        /* Data Visualization */
        .data-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-top: 30px;
        }}

        .data-card {{
            background: #FAFAFA;
            border: 1px solid #F0F0F0;
            padding: 20px;
            border-radius: 4px;
        }}

        .data-label {{
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-light);
            margin-bottom: 8px;
        }}

        .data-value {{
            font-family: 'Playfair Display', serif;
            font-size: 24px;
            color: var(--brand-dark);
        }}

        .map-placeholder {{
            width: 100%;
            height: 120mm;
            background: #EAEAEA;
            margin-top: 20px;
            position: relative;
            overflow: hidden;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        
        .map-placeholder img {{
            width: 100%;
            height: 100%;
            object-fit: cover;
            opacity: 0.8;
            filter: grayscale(100%) contrast(1.2);
        }}

        .map-overlay {{
            position: absolute;
            background: rgba(17, 17, 17, 0.9);
            color: white;
            padding: 15px 20px;
            bottom: 20px;
            left: 20px;
            border-left: 3px solid var(--brand-accent);
        }}
        
        .disclaimer {{
            position: absolute;
            bottom: 15mm;
            font-size: 9px;
            color: #999;
            line-height: 1.4;
        }}
    </style>
</head>
<body>

    <!-- ASSET 1: THE EXECUTIVE TEAR-SHEET -->
    <div class="page cover-page" id="tear-sheet">
        <div class="brand-header">
            <div class="logo">Laing+Simmons</div>
            <div class="logo-sub">Oakville | Windsor</div>
        </div>

        <div class="title-block">
            <div class="report-type">Executive Tear-Sheet</div>
            <h1>{address}</h1>
            <p style="font-size: 16px; line-height: 1.6; color: #CCCCCC; max-width: 120mm; margin-top: 20px;">
                The landscape in {suburb} has shifted. Based on recent zoning and market velocity, assets matching your profile are currently experiencing high liquidity. This brief is provided for your situational awareness.
            </p>
            
            <div class="data-grid" style="margin-top: 40px; max-width: 140mm;">
                <div class="data-card" style="background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1);">
                    <div class="data-label" style="color: var(--brand-accent);">Target Demographic</div>
                    <div class="data-value" style="color: white; font-size: 20px;">{buyer_profile}</div>
                </div>
                <div class="data-card" style="background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1);">
                    <div class="data-label" style="color: var(--brand-accent);">Action Required</div>
                    <div class="data-value" style="color: white; font-size: 20px;">Hold or Liquidate</div>
                </div>
            </div>
            
            <div class="prepared-for">
                Prepared Exclusively For<br>
                <div class="owner-name">{owner_name}</div>
            </div>
        </div>

        <div class="dark-footer">
            CONFIDENTIAL INTELLIGENCE ASSET | PREPARED BY {principal_name} | {principal_email}
        </div>
    </div>

    <!-- ASSET 2: THE INSTITUTIONAL PROSPECTUS (COVER) -->
    <div class="page cover-page">
        <div class="brand-header">
            <div class="logo">Laing+Simmons</div>
            <div class="logo-sub">Oakville | Windsor</div>
        </div>

        <div class="title-block">
            <div class="report-type">Institutional Prospectus</div>
            <h1>Asset Intelligence<br>& Market Velocity</h1>
            
            <div class="prepared-for" style="margin-top: 80px;">
                Subject Property<br>
                <div class="owner-name" style="color: var(--brand-accent);">{address}</div>
            </div>
        </div>

        <div class="dark-footer">
            CONFIDENTIAL INTELLIGENCE ASSET | PREPARED BY {principal_name} | {principal_email}
        </div>
    </div>

    <!-- PAGE 3: PRINCIPAL'S FORWARD -->
    <div class="page">
        <div class="page-header">
            <div class="logo page-logo">Laing+Simmons</div>
            <div class="logo-sub" style="color: var(--text-light);">Oakville | Windsor</div>
        </div>

        <h2 class="page-title" style="margin-bottom: 30px;">Principal's Forward</h2>

        <div class="letter-content">
            <p>Dear {owner_name},</p>
            <p>We operate on data, not speculation.</p>
            <p>The Hawkesbury region is currently undergoing a rapid wealth shift. I am writing to you not to ask for a listing, but because the profile of your specific block has recently become highly sought-after by a new demographic of institutional and private buyers.</p>
            <p>When assets like yours transition from simple residences to strategic land holdings, the method of representation must evolve. Traditional real estate marketing is insufficient. Maximizing your yield requires surgical demographic targeting and precise financial modeling.</p>
            <p>This prospectus outlines the exact market realities currently affecting <strong>{address}</strong>. It is designed to provide you with the situational awareness required to make an informed decision on your timeline, whether that is to hold, develop, or liquidate.</p>
            
            <div class="signature-block">
                <div class="sig-name">{principal_name}</div>
                <div class="sig-title">Principal, Laing+Simmons {brand_area}</div>
                <div style="font-size: 11px; margin-top: 8px; color: var(--brand-accent);">{principal_email} | {principal_phone}</div>
            </div>
        </div>
        
        <div class="disclaimer">The information provided is derived from public datasets and proprietary intelligence. It does not constitute formal financial advice.</div>
    </div>

    <!-- PAGE 4: THE ASSET -->
    <div class="page">
        <div class="page-header">
            <div class="logo page-logo">Laing+Simmons</div>
            <h2 class="page-title" style="font-size: 24px;">Asset Overview</h2>
        </div>

        <div class="map-placeholder" style="width:800px;height:600px;background:linear-gradient(135deg,#1a1a1a 0%,#2d2d2d 50%,#1a1a1a 100%);border:1px solid var(--brand-accent);display:flex;align-items:center;justify-content:center;position:relative;">
            <div class="map-overlay">
                <div style="font-size: 10px; text-transform: uppercase; letter-spacing: 2px; color: var(--brand-accent);">Target Asset</div>
                <div style="font-family: 'Playfair Display', serif; font-size: 18px; margin-top: 4px;">{address}</div>
            </div>
        </div>

        <div class="data-grid">
            <div class="data-card">
                <div class="data-label">Land Size</div>
                <div class="data-value">{land_size} m²</div>
            </div>
            <div class="data-card">
                <div class="data-label">Last Traded</div>
                <div class="data-value">{sale_date}</div>
            </div>
            <div class="data-card">
                <div class="data-label">Acquisition Value</div>
                <div class="data-value">{sale_price}</div>
            </div>
            <div class="data-card">
                <div class="data-label">Zoning Profile</div>
                <div class="data-value">Low Density</div>
            </div>
        </div>
    </div>

</body>
</html>
"""

async def generate_assets(address: str, owner_name: str, suburb: str, sale_price: str, sale_date: str, land_size: str, output_dir: str):
    # Prepare data
    if not owner_name or owner_name == "Owner record pending":
        owner_name = "The Property Owner"
        
    buyer_profile = "Upsizing Family"
    if land_size and land_size != "None":
        try:
            if float(land_size.replace(',', '')) > 800:
                buyer_profile = "Private Developer"
        except:
            pass

    # Inject data into HTML
    html_content = HTML_TEMPLATE.format(
        address=address,
        owner_name=owner_name,
        suburb=suburb,
        sale_price=sale_price or "Undisclosed",
        sale_date=sale_date or "Historic",
        land_size=land_size or "Unverified",
        buyer_profile=buyer_profile,
        principal_name=config.PRINCIPAL_NAME,
        principal_email=config.PRINCIPAL_EMAIL,
        principal_phone=config.PRINCIPAL_PHONE,
        brand_area=config.BRAND_AREA,
    )

    # Setup directories
    base_dir = Path(output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    
    asset1_path = base_dir / "01_Executive_Tear_Sheet.pdf"
    asset2_path = base_dir / "02_Institutional_Prospectus.pdf"

    print(f"Spinning up headless browser engine...")
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.set_content(html_content, wait_until="networkidle")
        await page.emulate_media(media="screen")
        
        print(f"Rendering Tear-Sheet...")
        await page.pdf(
            path=str(asset1_path),
            format="A4",
            print_background=True,
            page_ranges="1", # Print only the first page for the tear sheet
            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"}
        )
        
        print(f"Rendering Prospectus...")
        await page.pdf(
            path=str(asset2_path),
            format="A4",
            print_background=True,
            page_ranges="2-4", # Print the rest for the prospectus
            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"}
        )
        
        await browser.close()
        
    print(f"Assets successfully generated in: {output_dir}")

if __name__ == "__main__":
    # Selected generic target based on db query
    target = {
        "address": "86 Colonial Drive, South Windsor",
        "owner_name": "J Williams",
        "suburb": "South Windsor",
        "sale_price": "$950,000",
        "sale_date": "12 Aug 2021",
        "land_size": "650"
    }
    
    folder_name = target["address"].replace(" ", "_").replace(",", "")
    out_dir = str(config.TEMP_DIR / "asset_factory_v2" / folder_name)
    
    asyncio.run(generate_assets(
        target["address"], 
        target["owner_name"], 
        target["suburb"],
        target["sale_price"],
        target["sale_date"],
        target["land_size"],
        out_dir
    ))
