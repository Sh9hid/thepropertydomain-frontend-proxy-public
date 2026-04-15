"""
OpenAI o4-mini Task Runner
--------------------------
Delegates heavy code generation and UI work to OpenAI o4-mini (high reasoning).
Run: python backend/tools/openai_tasks.py

Uses OPENAI_API_KEY from .env. Outputs files to research_output/.
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
from core.utils import get_openai_api_key

OPENAI_API_KEY = get_openai_api_key()
OPENAI_MODEL = "o4-mini"

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "research_output"
OUTPUT_DIR.mkdir(exist_ok=True)


async def ask_openai(prompt: str, model: str = OPENAI_MODEL) -> str:
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "reasoning_effort": "high",
        "messages": [{"role": "user", "content": prompt}],
        "max_completion_tokens": 8000,
    }
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(url, headers=headers, json=body)
    if resp.status_code != 200:
        return f"ERROR {resp.status_code}: {resp.text[:500]}"
    data = resp.json()
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError):
        return json.dumps(data)[:500]


PIPELINE_VIEW_CODE = """
Current Pipeline view (CommandLedger.tsx) structure:
- Dark terminal-style UI with monospace font
- Table with columns: SIGNAL DATE, H3 HEX ID, ASSET/LOT-DP, AGENCY/AGENT, STATUS, HEAT (0-100 bar), CALL (numeric), EVID (bar), PHONE (tel link), AGE (days), VECTORS (REFI/SUBDIV/etc tags), Δ PRICE
- Status badges: LIVE (green glow), WITHDRAWN (red), OFF-MKT (amber), UNDER OFFER (purple), SOLD (grey), DELTA (orange)
- Filter bar: status filter chips + queue filter
- Clicking a row → opens EntityOS detail panel
- Uses framer-motion AnimatePresence for row transitions
- Tech: React + TypeScript + Zustand + inline styles (no CSS files)
- Color palette: bg #030303, accent #0a84ff, green #30d158, amber #D6A84F
"""

ENTITY_OS_CODE = """
Current EntityOS (PropertyDetail) structure:
- Split panel: left = property info + action buttons, right = property gallery + activity timeline
- Shows: owner name, address, suburb, heat score bar, call today score, evidence score
- PropertyGallery: grid of Domain API images (main_image + property_images array)
- ActivityTimeline: reads activity_log + stage_note_history JSON fields
- Action buttons: Book Appraisal (L+S), Book Mortgage Consult (Ownit1st), Send SMS
- What-to-say panel: AI-generated call script from backend _derive_intelligence
- Est value display from Domain enrichment
"""

TASKS = [
    (
        "bloomberg_ui_spec",
        f"""You are a senior React/TypeScript engineer specialising in Bloomberg Terminal-style financial data UIs.

Context: Australian real estate lead machine for Laing+Simmons Oakville | Windsor.
Stack: React 18, TypeScript, Vite, Zustand, Framer Motion, Lucide React icons.
No CSS files — all styling via inline style objects.
Color palette: bg #030303, text #e5e5e5, accent #0a84ff, green #30d158, amber #D6A84F, red #ff453a.
Font: system monospace stack.

{PIPELINE_VIEW_CODE}

Task: Design (as detailed specification with exact prop interfaces, layout measurements, and component hierarchy) a Bloomberg Terminal-style enhancement for the Pipeline view. Target:

1. **Keyboard navigation**: j/k to move between rows, Enter to open detail, / to focus search, Escape to close — with visible keyboard shortcut hints in the UI
2. **Inline mini-charts**: Replace the HEAT bar with a 7-point sparkline showing heat trajectory over time (use MiniSparkline component already present)
3. **Density modes**: Compact (1-line per row, 11px) / Normal (current) / Expanded (2 lines + thumbnail) — toggled via D key
4. **Status change animation**: When a lead's signal_status changes via WebSocket, flash the row amber then settle
5. **Quick-action overlay**: Press Space on a focused row → floating overlay with [Book Appraisal] [Send SMS] [Mark Called] buttons
6. **Column sort**: Click any column header to sort; shift+click for secondary sort; show sort indicators
7. **Sticky column groups**: Address columns sticky-left, score columns sticky-right

Output: TypeScript interfaces, component breakdown, exact style measurements, and the complete updated JSX for the top-bar + table header + one data row (enough to implement, not a stub).
Do not output placeholder comments. Output real, working code fragments.""",
    ),
    (
        "sms_sequence_scripts",
        """You are a senior real estate sales consultant and copywriter for Australian residential property.

Context:
- Agent: Nitin Puri, Laing+Simmons Oakville | Windsor
- Phone: 04 85 85 7881, Email: oakville@isre.com.au
- Target suburbs: Bligh Park, Oakville, Windsor, Woonona, Bulli, Thirroul (NSW)
- Lead types:
  a) marketing_list: confirmed mobile number, owner of property, not currently listed — heat_score=65
  b) domain_withdrawn: was listed 0-90 days ago, pulled without sale — heat_score=75
  c) cotality_import: property owner, no phone enriched yet — heat_score=60

Task: Write the complete SMS + follow-up call + email outreach sequences for each lead type.

Requirements:
- SMS max 160 characters (single message, no carrier concat)
- Tone: direct, human, local — NOT a robot, NOT generic
- Include the property address or suburb to prove we're not mass-blasting
- First SMS triggers within 24h of signal capture
- Follow-up call script: 45-second opener, 3 objection handlers
- Follow-up email: subject line + 150-word body if no call answer after 48h
- Cross-sell angle: every appraisal booking → offer to calculate mortgage equity in same call
- Legal: compliant with Australian Spam Act + Do Not Call Register (include opt-out in email)

Output:
For each lead type (marketing_list, domain_withdrawn, cotality_import):
  SMS Day 1 (with address variable)
  SMS Day 3 (if no reply)
  Call script Day 2 (opener + 3 objection handlers)
  Email Day 4 (subject + body)

Keep it tight. Real estate agents will judge these hard — they've heard every cliché.""",
    ),
    (
        "nsw_da_feed_spec",
        """You are a Python/FastAPI backend engineer building a real estate intelligence data pipeline.

Context:
- Stack: Python 3.12, FastAPI, SQLite (AsyncSession + SQLAlchemy), httpx for async HTTP
- NSW Planning Portal DA API base: https://api.apps1.nsw.gov.au/planning/viewApplications/v3
- Target postcodes: 2765 (Hills District: Oakville, Windsor, Bligh Park), 2517, 2518 (Woonona, Bulli)
- Database: `leads` table (SQLite) with fields: id, address, suburb, postcode, trigger_type, status, heat_score, call_today_score, signal_date, source_tags, source_evidence, created_at

Task: Write complete, production-ready Python code for a NSW DA feed ingestor:

1. `backend/services/nsw_da_feed.py` — async service that:
   - GETs DA applications for target postcodes from NSW Planning Portal API (no auth key required)
   - Filters for: new dwellings, subdivisions, demolitions, secondary dwellings (DA types that signal selling intent)
   - Extracts: applicant address, DA type, lodgement date, determination status
   - Upserts into leads table: trigger_type="da_filed", heat_score=70, signal_date=lodgement_date
   - Source evidence: "DA #{number} filed {date}: {description}"
   - Deduplicates by address

2. `backend/api/routes/ingest.py` addition — POST /api/ingest/nsw-da endpoint that calls the service

3. Error handling: API may be slow (up to 30s), rate limit gracefully, log errors without crashing

Output real, complete Python code. No placeholder comments, no stubs, no TODOs.
The function signatures must match the existing codebase pattern:
  async def ingest_nsw_da_to_leads(session: AsyncSession, postcodes: list[str]) -> dict
Return: {fetched: int, inserted: int, skipped: int}""",
    ),
    (
        "scoring_engine_improvements",
        """You are a data scientist building a property lead scoring model for Australian real estate.

Context:
- 13,473 leads in SQLite database
- Current scoring in backend/services/scoring.py calculates heat_score (0-100), call_today_score (0-100), evidence_score (0-100)
- Current signals available per lead: trigger_type, tenure_years, signal_date, suburb, postcode, est_value, opportunity_vectors (REFI/SUBDIV/YIELD/PROBATE/VACANT/DIVORCE)
- New data coming: DA filings (da_filed trigger), withdrawn Domain listings (domain_withdrawn trigger)

Task: Design and write improved scoring logic for:

1. **Decay function**: heat_score should decay over time — a signal from 90 days ago is less urgent than one from yesterday. Write the decay formula and implementation.

2. **Multi-signal boost**: If same address has both a DA filing AND is a marketing_list lead (has phone), score should compound. Write the compound logic.

3. **Suburb velocity**: If a suburb has had 3+ withdrawn listings in 30 days, all remaining off-market leads in that suburb get +5 urgency bonus. Write the implementation.

4. **Best time to call**: Given Australian time zones (AEST), signal_date, and trigger_type — write a function that returns `call_window: "morning" | "afternoon" | "evening" | "avoid"` for each lead.

5. **Probability to list**: Binary classifier using available signals. Write a simple rule-based scoring function that outputs `p_list: float (0.0-1.0)`.

For each section: Python function code + rationale (1-2 sentences explaining why this signal matters).
Pattern to follow:
  def score_lead(lead: dict) -> dict:
      # Returns updated lead dict with new score fields
      ...

No stubs. Real Python code that works with the existing dict-based lead structure.""",
    ),
]


async def run_all_tasks():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    results = {}

    for name, prompt in TASKS:
        print(f"[OpenAI o4-mini] Running: {name}...")
        try:
            result = await ask_openai(prompt)
            out_path = OUTPUT_DIR / f"{name}_{timestamp}.md"
            out_path.write_text(result, encoding="utf-8")
            print(f"  -> Saved to {out_path.name} ({len(result)} chars)")
            results[name] = str(out_path)
        except Exception as exc:
            print(f"  -> FAILED: {exc}")
            results[name] = f"ERROR: {exc}"

    summary_path = OUTPUT_DIR / f"openai_summary_{timestamp}.json"
    summary_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nAll tasks complete. Summary: {summary_path}")
    return results


if __name__ == "__main__":
    asyncio.run(run_all_tasks())
