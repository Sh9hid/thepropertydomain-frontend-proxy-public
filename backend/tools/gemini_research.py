"""
Gemini Pro Research Runner
--------------------------
Runs research tasks using the Gemini API and saves output to research_output/.
Run: python backend/tools/gemini_research.py

Uses GEMINI_API_KEY from .env. Outputs markdown files to research_output/.
"""
import asyncio
import json
import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "backend"))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / "backend/.env", encoding="utf-8-sig")

import httpx

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-2.0-flash-001"  # Pro available as "gemini-2.0-pro-exp" but flash is faster + cheaper

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "research_output"
OUTPUT_DIR.mkdir(exist_ok=True)


async def ask_gemini(prompt: str, model: str = GEMINI_MODEL) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 4096},
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(url, json=body)
    if resp.status_code != 200:
        return f"ERROR {resp.status_code}: {resp.text[:300]}"
    data = resp.json()
    try:
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        return json.dumps(data)[:500]


async def run_all_research():
    tasks = [
        (
            "ai_sales_methods",
            """You are a senior real estate technology consultant.

Research: What are the most effective AI-powered sales and outreach methods for Australian residential real estate agents booking appraisals and mortgage consultations in 2025-2026?

Cover:
1. Best-converting outreach sequences (SMS → call → email timing and content)
2. How top performers use property data signals to trigger personalised outreach
3. Proven tools and frameworks for mortgage broker + real estate agent cross-sell
4. What data points predict likelihood of listing (tenure, DA filed, mortgage cliff, etc.)
5. Open source tools worth integrating (CRM automation, SMS, property data)

Output: concise numbered list of actionable tactics with evidence. Skip theory — only what demonstrably works.
Australia-specific where possible. Do not recommend paid enterprise platforms unless they have a free tier.""",
        ),
        (
            "data_analysis_insights",
            """You are a data analyst specialising in Australian real estate conversion.

We have a database of 13,473 leads for a real estate agent covering Hills District (2765) and Woonona (2517/2518) NSW.

Data breakdown:
- 13,472 leads from Cotality (CoreLogic) property ownership data: has address, owner name, heat_score=60, no phone
- 943 leads from a marketing CSV list: has real mobile numbers, heat_score=65, trigger_type=marketing_list
- 1 contacted lead
- All signal_status currently "OFF-MARKET" except marketing_list leads (now "LIVE")

Questions to answer:
1. What outreach sequence is optimal for the 943 leads with phones? (SMS timing, message angle, follow-up cadence)
2. For the 12k+ Cotality records with no phone — how should we enrich them? (Free methods: LinkedIn, White Pages, electoral roll, letterbox drop)
3. What subset of the 12k Cotality records is most likely to convert to an appraisal without a phone number?
4. What additional data would make this database 10x more valuable? (be specific about free sources in NSW)
5. What patterns predict "this owner is about to sell"? (think: tenure 7+ years, DA filed, rate cliff, etc.)

Output: actionable insights in numbered sections. Be specific about NSW data sources.""",
        ),
        (
            "open_source_tools",
            """Research open source and free tools suitable for a solo real estate agent / mortgage broker operator running a lead machine in 2025-2026.

Context:
- Stack: Python/FastAPI backend, React/TypeScript frontend, SQLite
- Need: SMS automation, email sequences, property data enrichment, CRM workflows
- Budget: $0-50/month on tools
- Already have: Domain API (500 calls/day free), Gemini API, OpenAI API, Zoom API

Evaluate these categories:
1. Open source CRM (n8n, Chatwoot, Cal.com, etc.) — what integrates cleanly with a FastAPI backend?
2. SMS/outreach automation (Twilio alternatives, free SMS gateways in Australia)
3. Australian property data feeds (NSW Open Data, PriceFinder API pricing, etc.)
4. Email deliverability tools (open source SMTP management)
5. Analytics/charting (Bloomberg-style terminal UI components for React)

For each recommendation: name, URL, why it fits, rough integration effort (hours).
Reject anything that adds complexity without clear ROI for a solo operator.""",
        ),
        (
            "revenue_maximisation",
            """You are a strategic advisor for a solo real estate lead generation operator.

Situation:
- Operator: Shahid, based in India, working Australian market remotely
- Revenue source 1: Commission per appraisal booked for L+S Oakville | Windsor (Nitin Puri's agency)
- Revenue source 2: Commission per mortgage originated through Ownit1st Loans
- Has 13,473 property records in target suburbs
- Has 943 direct dial mobile numbers
- Has Domain API access (500 calls/day)
- Has Gemini + OpenAI API access

Question: What is the fastest path to $10,000/month revenue from this asset base?

Constraints:
- No buying leads — generate from owned data
- No cold calling scripts that sound robotic
- Must work remotely (no in-person)
- Australian real estate regulations apply (no unlicensed advice)

Output: a 30-day action plan. Be specific about which 943 leads to call first, what to say, and what metrics to track. Include the cross-sell angle (real estate appraisal → mortgage refinance) and how to execute it as a solo operator.""",
        ),
    ]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    results = {}

    for name, prompt in tasks:
        print(f"[Gemini] Running: {name}...")
        try:
            result = await ask_gemini(prompt)
            out_path = OUTPUT_DIR / f"{name}_{timestamp}.md"
            out_path.write_text(result, encoding="utf-8")
            print(f"  -> Saved to {out_path.name} ({len(result)} chars)")
            results[name] = str(out_path)
        except Exception as exc:
            print(f"  -> FAILED: {exc}")
            results[name] = f"ERROR: {exc}"

    summary_path = OUTPUT_DIR / f"research_summary_{timestamp}.json"
    summary_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nAll research complete. Summary: {summary_path}")
    return results


if __name__ == "__main__":
    asyncio.run(run_all_research())
