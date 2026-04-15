# -*- coding: utf-8 -*-
import asyncio
import os
import sqlite3
import json
import re
import sys
import hashlib
from pathlib import Path
from datetime import datetime

# Add core dir to sys.path
backend_dir = str(Path(__file__).resolve().parent)
if backend_dir not in sys.path:
    sys.path.append(backend_dir)

from core import config
from pdf_generator import html_to_pdf

# ASSET LINKS
BRAND_LOGO = config.BRAND_LOGO_URL
NITIN_PHOTO = ""
DB_PATH = str(config.PROJECT_ROOT / "leads.db")

# GLOBAL CONFIG
NITIN_NAME = config.PRINCIPAL_NAME
NITIN_TITLE = "Local Principal | " + config.BRAND_NAME
NITIN_MOBILE = config.PRINCIPAL_PHONE
NITIN_EMAIL = config.PRINCIPAL_EMAIL

CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;800&family=Playfair+Display:ital,wght@0,700;1,700&display=swap');
:root { --ls-navy: #05070a; --ls-gold: #D4AF37; --ls-accent: #34c759; --ls-paper: #ffffff; --text-main: #1d1d1f; --text-dim: #86868b; }
body { margin: 0; padding: 0; background: var(--ls-paper); font-family: 'Inter', sans-serif; color: var(--text-main); }
.page { width: 210mm; height: 297mm; page-break-after: always; position: relative; background: var(--ls-paper); overflow: hidden; }
.dark-bg { background: var(--ls-navy); color: #fff; }
.grid-12 { display: grid; grid-template-columns: repeat(12, 1fr); gap: 20px; padding: 60px; height: 100%; box-sizing: border-box; align-content: start; }
.header { grid-column: span 12; display: flex; justify-content: space-between; align-items: center; padding-bottom: 30px; border-bottom: 1px solid rgba(0,0,0,0.05); margin-bottom: 40px; }
.dark-bg .header { border-bottom: 1px solid rgba(255,255,255,0.1); }
.logo-container { height: 60px; display: flex; align-items: center; background: #fff; padding: 10px 20px; border-radius: 4px; }
.logo { height: 40px; width: auto; }
h1 { grid-column: span 12; font-family: 'Playfair Display', serif; font-size: 54px; margin: 0; line-height: 1.1; }
h2 { grid-column: span 12; font-family: 'Playfair Display', serif; font-size: 36px; margin: 0; line-height: 1.2; }
.eyebrow { grid-column: span 12; font-size: 12px; font-weight: 800; letter-spacing: 0.25em; text-transform: uppercase; color: var(--ls-gold); margin-bottom: 8px; }
.body-copy { font-size: 17px; line-height: 1.6; color: #334155; }
.dark-bg .body-copy { color: #cbd5e1; }
.hero-box { grid-column: span 12; height: 450px; background-size: cover; background-position: center; margin-bottom: 40px; position: relative; background-color: var(--ls-navy); }
.hero-overlay { position: absolute; inset: 0; background: linear-gradient(to bottom, transparent, rgba(5,7,10,0.8)); }
.stat-card { grid-column: span 4; padding: 32px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 16px; text-align: center; }
.dark-bg .stat-card { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08); }
.stat-val { font-size: 32px; font-weight: 800; color: var(--ls-gold); display: block; }
.stat-lab { font-size: 10px; font-weight: 700; text-transform: uppercase; color: var(--text-dim); }
.mirror-table { grid-column: span 12; width: 100%; border-collapse: collapse; margin-top: 40px; }
.mirror-table th { text-align: left; padding: 24px; font-size: 11px; text-transform: uppercase; border-bottom: 2px solid var(--ls-navy); }
.mirror-table td { padding: 24px; border-bottom: 1px solid #f1f5f9; vertical-align: top; font-size: 15px; }
.fear-col { color: #64748b; background: #fafafa; width: 45%; }
.reality-col { background: #fff; font-weight: 500; }
.footer { position: absolute; bottom: 40px; left: 60px; right: 60px; display: flex; justify-content: space-between; font-size: 9px; color: var(--text-dim); border-top: 1px solid rgba(0,0,0,0.05); padding-top: 20px; }
.dark-bg .footer { border-top: 1px solid rgba(255,255,255,0.1); }
.signature { grid-column: span 12; display: flex; align-items: center; gap: 30px; margin-top: 60px; border-top: 1px solid var(--ls-gold); padding-top: 40px; }
"""

REPORTS = {
    "ALPHA_AUDIT": {
        "title": "The Alpha Audit",
        "eyebrow": "Asset Diagnostics",
        "hook": "Information Asymmetry in the Hawkesbury corridor.",
        "p2_h": "The Yield Gap",
        "p2_p": "Our data indicates your property sits in a data-blind pocket. Standard algorithms fail to account for the RU4 zoning shift near the Aerotropolis Northern Gateway.",
    },
    "EQUITY_VELOCITY": {
        "title": "The Equity Velocity Protocol",
        "eyebrow": "Wealth Engineering",
        "hook": "Fund your next move without impacting your cash flow.",
        "p2_h": "Ghost Equity",
        "p2_p": "Hawkesbury acreage has appreciated by 42% since 2021. Most owners are sitting on 'Lazy Equity' that could be weaponized into a tax-neutral investment lever.",
    },
    "PERMISSIBILITY_PARADOX": {
        "title": "The Permissibility Paradox",
        "eyebrow": "Planning Intelligence",
        "hook": "Why your constraints are your greatest asset.",
        "p2_h": "August 2026 Reforms",
        "p2_p": "The updated Detached Dual Occupancy laws have re-rated land value for RU4 lots. Your parcel footprint allows for a secondary dwelling pivot that standard R2 lots lack.",
    },
    "SLOW_LUXURY": {
        "title": "The Slow Luxury Manifesto",
        "eyebrow": "Lifestyle & Design",
        "hook": "The architecture of space and resilience.",
        "p2_h": "The Hamptons of Sydney",
        "p2_p": "Executive buyers are trading visual opulence for thermal resilience. We position your property as a regenerative legacy asset, not just a house.",
    },
    "INVISIBLE_MARKET": {
        "title": "The Invisible Market",
        "eyebrow": "Methodology",
        "hook": "Precision over portals.",
        "p2_h": "The Dark Market Engine",
        "p2_p": "The best buyers don't wait for notifications. They are already in our private database. We match assets before the public portals even see the price.",
    }
}

def build_page(content_html, class_name=""):
    return f'<div class="page {class_name}">{content_html}</div>'

def build_header():
    return f'<div class="header"><div class="logo-container"><img src="{BRAND_LOGO}" class="logo"></div><div style="text-align:right;"><div style="font-weight:800; font-size:11px; color:var(--ls-gold);">PRIVATE STRATEGY</div><div style="font-size:10px; color:var(--text-dim);">{config.BRAND_AREA.upper()}</div></div></div>'

async def generate_prospectus(lead_id: str, report_key: str, output_path: str):
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    lead = dict(row) if row else {"owner_name": "Property Owner", "address": "Subject Asset"}
    conn.close()

    rep = REPORTS[report_key]
    owner = lead.get('owner_name', 'Property Owner').split(',')[0].split('&')[0].strip().title()
    address = lead.get('address', '').split(',')[0]

    pages = []
    # P1: COVER
    pages.append(build_page(f"""<div class="grid-12"><div class="header" style="border:none;"><div class="logo-container"><img src="{BRAND_LOGO}" class="logo"></div></div><div class="hero-box"><div class="hero-overlay"></div></div><div style="grid-column: span 12; margin-top:-100px; z-index:20; position:relative;"><div class="eyebrow" style="color:#fff;">{rep['eyebrow']}</div><h1 style="color:#fff; font-size:64px;">{rep['title']}</h1><p style="font-size:24px; color:var(--ls-gold); margin-top:20px;">Prepared for {owner}</p><p style="font-size:14px; color:#fff; opacity:0.6;">{address}</p></div><div class="footer"><span>Laing+Simmons</span><span>Confidential Brief</span></div></div>""", "dark-bg"))
    # P2: HOOK
    pages.append(build_page(f"""<div class="grid-12">{build_header()}<div class="eyebrow">Strategic Hook</div><h2>{rep['hook']}</h2><div class="body-copy" style="grid-column: span 8; margin-top:40px;">{rep['p2_p']}</div><div class="stat-card" style="grid-column: span 4; background:var(--ls-navy); color:#fff;"><span class="stat-val">+18%</span><span class="stat-lab">MARKET ALPHA</span></div></div>"""))
    # P3: THE MIRROR (FEARS)
    pages.append(build_page(f"""<div class="grid-12">{build_header()}<div class="eyebrow">The Mirror</div><h2>The Strategic Choice</h2><table class="mirror-table"><thead><tr><th class="fear-col">Common Market Fear</th><th class="reality-col">{NITIN_NAME} Reality</th></tr></thead><tbody><tr><td class="fear-col">Agent underselling due to lack of planning depth.</td><td class="reality-col">Technical planning audit to extract "Development Alpha".</td></tr><tr><td class="fear-col">Market fatigue from stale public listings.</td><td class="reality-col">Dark Market matching against 15,168 private contacts.</td></tr></tbody></table></div>"""))
    # P4-P7: PLACEHOLDERS FOR 8 PAGE DEPTH
    for i in range(4, 8):
        pages.append(build_page(f"""<div class="grid-12">{build_header()}<div class="eyebrow">Data Intelligence 0{i}</div><h2>Institutional Market Briefing</h2><p class="body-copy">Analysis of local infrastructure halo and supply-side anomalies extracted from our proprietary stock ledger.</p></div>"""))
    # P8: CTA
    pages.append(build_page(f"""<div class="grid-12" style="text-align:center; display:block; padding-top:200px;"><h1 style="color:#fff; font-size:72px;">Your Strategy Begins Here.</h1><p class="body-copy" style="font-size:24px; color:var(--text-dim); margin-top:40px;">Private Strategy Workshop with {NITIN_NAME}.</p><div style="margin-top:100px; display:inline-block; text-align:left; padding:50px; border:1px solid var(--ls-gold); border-radius:12px; background:rgba(255,255,255,0.02);"><div style="font-weight:800; font-size:32px;">{NITIN_NAME}</div><div style="color:var(--ls-gold); font-size:13px; margin-bottom:40px;">{NITIN_TITLE.upper()}</div><div style="font-size:20px;">M: {NITIN_MOBILE}</div><div style="font-size:20px;">E: {NITIN_EMAIL}</div></div></div>""", "dark-bg"))

    full_html = f"<html><head><meta charset='UTF-8'><style>{CSS}</style></head><body>" + "".join(pages) + "</body></html>"
    await html_to_pdf(full_html, output_path)
    print(f"Sovereign {report_key} Generated: {output_path}")

if __name__ == "__main__":
    import sys
    lid = sys.argv[1] if len(sys.argv) > 1 else "a21dd0b030064d4fbebdfd0ab2307d6a"
    rk = sys.argv[2] if len(sys.argv) > 2 else "ALPHA_AUDIT"
    out_dir = config.TEMP_DIR / "sovereign_reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = sys.argv[3] if len(sys.argv) > 3 else str(out_dir / f"{rk}.pdf")
    asyncio.run(generate_prospectus(lid, rk, out))
