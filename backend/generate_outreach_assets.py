import asyncio
import os
import json
import sys
from pathlib import Path

# Add current directory to path to find pdf_generator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from pdf_generator import html_to_pdf

BRAND_LOGO = "https://images.squarespace-cdn.com/content/v1/52c0bc66e4b035c2f1f884fc/1473065644926-35LTYYPWD09N2SLU311T/Laing%2B%2B%2BSimmons%2BLogo.jpg"
from core import config

OUTPUT_DIR = Path(config.STOCK_ROOT) / "Important documents"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
body { font-family: 'Helvetica', 'Arial', sans-serif; margin: 0; background: #05070a; color: #ffffff; line-height: 1.5; }
.page { width: 210mm; min-height: 297mm; padding: 50px; box-sizing: border-box; background: #05070a; position: relative; }
.header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 60px; }
.logo { height: 40px; }
.brand-meta { text-align: right; }
.brand-name { font-weight: 800; font-size: 14px; letter-spacing: 0.2em; color: #8bd3ff; }
.brand-sub { font-size: 11px; color: #64748b; margin-top: 4px; }
.eyebrow { color: #34c759; font-size: 12px; font-weight: 800; letter-spacing: 0.3em; margin-bottom: 8px; }
h1 { font-size: 48px; font-weight: 800; margin: 0 0 20px; letter-spacing: -0.03em; color: #f8fafc; }
.hook { font-size: 22px; color: #94a3b8; font-weight: 400; margin-bottom: 50px; border-left: 4px solid #34c759; padding-left: 24px; font-style: italic; }
.card { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08); border-radius: 24px; padding: 32px; margin-bottom: 24px; }
h3 { color: #8bd3ff; font-size: 18px; font-weight: 800; margin-top: 0; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.05em; }
p { color: #cbd5e1; font-size: 16px; margin: 0; }
.infographic { display: flex; gap: 16px; margin: 40px 0; }
.stat { flex: 1; padding: 24px; background: rgba(52, 199, 89, 0.05); border-radius: 20px; border: 1px solid rgba(52, 199, 89, 0.1); text-align: center; }
.stat-val { font-size: 32px; font-weight: 800; color: #34c759; display: block; }
.stat-label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.1em; margin-top: 4px; }
.footer { position: absolute; bottom: 50px; left: 50px; right: 50px; display: flex; justify-content: space-between; font-size: 10px; color: #475569; letter-spacing: 0.1em; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 24px; }
.chart-bar { height: 8px; background: #1e293b; border-radius: 4px; margin-top: 12px; overflow: hidden; }
.chart-fill { height: 100%; background: #34c759; border-radius: 4px; }
"""

DOCS = [
    {
        "filename": "The_Equity_Velocity_Protocol.pdf",
        "eyebrow": "STRATEGIC WEALTH",
        "title": "The Equity Velocity Protocol",
        "hook": "Your mortgage has a Ghost Equity layer. We have identified a strategy to fund a $1M portfolio expansion without impacting your current cash flow.",
        "cards": [
            {"h": "2026 Interest Rate Arbitrage", "p": "By using our proprietary Offset Stacking model, we transition your debt from a standard liability into a high-velocity wealth lever. This is specific to high-equity holdings in the Hawkesbury radius."},
            {"h": "The Lifestyle Multiplier", "p": "We map the exact path from your current asset valuation to a second Legacy home, maintaining a risk-adjusted 55% LVR ceiling."}
        ],
        "stats": [("55%", "LVR CEILING"), ("$1.2M", "AVG. UNLOCKED"), ("48h", "APPROVAL TIME")],
        "chart": 75
    },
    {
        "filename": "The_Permissibility_Paradox.pdf",
        "eyebrow": "LAND INTELLIGENCE",
        "title": "The Permissibility Paradox",
        "hook": "The RU4 vs. R5 Trap: Why the planning constraints on your property title might actually be your greatest financial asset in the 2026 cycle.",
        "cards": [
            {"h": "Secondary Dwelling ROI", "p": "Luxury Studio Pavilions are driving a 22% land premium in Oakville. We provide the feasibility blueprints for Multi-Generational ready acreage."},
            {"h": "2026 Infrastructure Delta", "p": "We verify your property's readiness for EV high-voltage loads and Starlink 3.0 reliability - the new non-negotiables for high-net-worth buyers."}
        ],
        "stats": [("+22%", "VALUE UPLIFT"), ("RU4/R5", "ZONING ALPHA"), ("100%", "DATA READINESS")],
        "chart": 90
    },
    {
        "filename": "The_Slow_Luxury_Manifesto.pdf",
        "eyebrow": "DESIGN & LIFESTYLE",
        "title": "The Slow Luxury Manifesto",
        "hook": "Buyers in 2026 are trading Visual Opulence for Thermal Resilience. If your home is not Passive House ready, you are leaving money on the table.",
        "cards": [
            {"h": "The Hawkesbury Aesthetic", "p": "A fusion of heritage sandstone and ultra-modern glass. We curate the Legacy Build narrative that connects emotionally with high-end tree-changers."},
            {"h": "Landscape as Infrastructure", "p": "Moving from Gardens to Regenerative Ecosystems. Private dams and edible forests are now top-tier appraisal drivers for properties over $3M."}
        ],
        "stats": [("A+", "THERMAL RATING"), ("REGEN", "ECO-DNA"), ("LUX", "MARKET TIER")],
        "chart": 65
    },
    {
        "filename": "The_Invisible_Appraisal.pdf",
        "eyebrow": "MARKET PSYCHOLOGY",
        "title": "The Invisible Appraisal",
        "hook": "We do not list properties. We engineer historical outcomes using Dark Market tactics and algorithmic buyer seeding.",
        "cards": [
            {"h": "The Critical 72 Hours", "p": "Our AI-driven seeding finds your buyer before the first open home. We use first-person narrative cinematic video to build attachment before the public market even sees the price."},
            {"h": "Negotiation War-Room", "p": "We create extreme scarcity through invite-only viewing periods, ensuring the vendor captures the full Emotional Premium of the 2026 market."}
        ],
        "stats": [("72h", "SEEDING PHASE"), ("15%", "OFF-MARKET DELTA"), ("TOP 1%", "CLOSING FOCUS")],
        "chart": 85
    },
    {
        "filename": "Why_Choose_LS_Windsor_Oakville.pdf",
        "eyebrow": "AGENCY ALPHA",
        "title": "The Boutique Advantage",
        "hook": "Real estate is no longer about finding a buyer. It is about Information Asymmetry. We operate 3-6 months ahead of the competition.",
        "cards": [
            {"h": "Market Intelligence Platform", "p": "Our proprietary radar intercepts transition signals (Probates, DAs, Refinances) before they hit the public portals. We give you the unfair advantage."},
            {"h": "Integrated Mortgage Core", "p": "The only agency in the region with Ownit1st - a fully integrated mortgage broking arm. We pre-qualify every buyer to ensure your settlement is guaranteed."}
        ],
        "stats": [("15k+", "ACTIVE LEADS"), ("3mo", "TIME ADVANTAGE"), ("TOP 1%", "LOCAL MARKET")],
        "chart": 95
    }
]

async def generate_all():
    for doc in DOCS:
        cards_html = "".join([f'<div class="card"><h3>{c["h"]}</h3><p>{c["p"]}</p></div>' for c in doc["cards"]])
        stats_html = "".join([f'<div class="stat"><span class="stat-val">{val}</span><span class="stat-label">{lab}</span></div>' for val, lab in doc["stats"]])
        
        html = f"""
        <!DOCTYPE html><html><head><meta charset="UTF-8"><style>{CSS}</style></head>
        <body><div class="page">
            <div class="header">
                <img src="{BRAND_LOGO}" class="logo">
                <div class="brand-meta"><div class="brand-name">Laing+Simmons</div><div class="brand-sub">Oakville | Windsor</div></div>
            </div>
            <div class="eyebrow">{doc["eyebrow"]}</div>
            <h1>{doc["title"]}</h1>
            <div class="hook">{doc["hook"]}</div>
            {cards_html}
            <div style="margin-top: 40px;">
                <div class="brand-sub" style="text-transform: uppercase; font-weight: 800; letter-spacing: 0.1em; color: #34c759;">MARKET PENETRATION INDEX</div>
                <div class="chart-bar"><div class="chart-fill" style="width: {doc["chart"]}%"></div></div>
            </div>
            <div class="infographic">{stats_html}</div>
            <div class="footer">
                <span>(c) 2026 LAING+SIMMONS OAKVILLE | WINDSOR</span>
                <span>STRATEGIC INTEL BRIEF</span>
                <span>LAING+SIMMONS OAKVILLE | WINDSOR</span>
            </div>
        </div></body></html>
        """
        await html_to_pdf(html, str(OUTPUT_DIR / doc["filename"]))
        print(f"Refined: {doc['filename']}")

if __name__ == "__main__":
    asyncio.run(generate_all())
