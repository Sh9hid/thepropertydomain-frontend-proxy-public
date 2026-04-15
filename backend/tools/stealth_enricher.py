import os
import sqlite3
import asyncio
import random
import datetime
from pathlib import Path
from browser_use import Agent, Browser, BrowserConfig
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

# --- CONFIGURATION ---
BACKEND_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BACKEND_DIR / ".env")

DB_PATH = Path(__file__).resolve().parents[2] / "leads.db"
BRAVE_PATH = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
DAILY_CAP = 200 # Safe daily limit
BATCH_SIZE = 5  # Leads per browser session
GEMINI_MODEL = "gemini-2.5-flash"

# --- STEALTH HELPERS ---
def get_sydney_now():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=11)))

def should_be_working():
    now = get_sydney_now()
    # Work between 8 AM and 10 PM Sydney time
    return 8 <= now.hour <= 22

async def human_delay(min_sec=2, max_sec=8):
    delay = random.uniform(min_sec, max_sec)
    await asyncio.sleep(delay)

# --- DATABASE LOGIC ---
def get_enrichment_queue(limit=5):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Priority: 1. Heat Score, 2. Created At (Freshness)
    # Only pick leads without phone/email and not yet enriched
    query = """
        SELECT id, address, suburb FROM leads 
        WHERE (contact_phones IS NULL OR contact_phones = '[]' OR contact_phones = '')
        AND enriched_at IS NULL
        AND status != 'dropped'
        ORDER BY heat_score DESC, created_at DESC
        LIMIT ?
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def update_lead_intel(lead_id, intel):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Simple JSON string storage for phones/emails
    phones = intel.get('phones', [])
    emails = intel.get('emails', [])
    owner = intel.get('name', None)
    dob = intel.get('dob', None)
    
    import json
    cursor.execute("""
        UPDATE leads 
        SET contact_phones = ?, 
            contact_emails = ?, 
            owner_name = COALESCE(?, owner_name),
            stage_note = ?,
            enriched_at = ?
        WHERE id = ?
    """, (
        json.dumps(phones), 
        json.dumps(emails), 
        owner,
        f"Enriched via Stealth Engine: DOB {dob}" if dob else "Enriched via Stealth Engine",
        datetime.datetime.now().isoformat(),
        lead_id
    ))
    conn.commit()
    conn.close()

# --- AGENT LOGIC ---
async def enrich_batch():
    if not should_be_working():
        print("Outside Sydney working hours. Sleeping...")
        return

    queue = get_enrichment_queue(BATCH_SIZE)
    if not queue:
        print("Queue empty. No leads to enrich.")
        return

    print(f"Starting batch of {len(queue)} leads...")

    # Configure Stealth Browser
    # Using a separate user_data_dir prevents locking issues with your main Brave window
    user_data_dir = str(Path(__file__).resolve().parents[2] / "tmp" / "stealth_brave_profile")
    
    browser = Browser(
        config=BrowserConfig(
            headless=False, # Show window so you can see it working
            chrome_instance_path=BRAVE_PATH,
            extra_chromium_args=[
                f"--user-data-dir={user_data_dir}",
                "--disable-blink-features=AutomationControlled",
            ]
        )
    )

    llm = ChatGoogleGenerativeAI(model=GEMINI_MODEL)

    for lead in queue:
        address = f"{lead['address']}, {lead['suburb']}"
        print(f"Researching: {address}")
        
        task = f"""
        Go to https://id4me.com.au. 
        You are already logged into the Brave profile.
        1. Type the address '{address}' into the search bar. Type at human speed (70WPM).
        2. Wait for the results to load (2-5 seconds).
        3. Use your vision to find the 'Owner Name', 'Phone Number', 'Email', and 'Date of Birth'.
        4. DO NOT copy-paste. Read the information from the screen.
        5. If multiple owners exist, capture all of them.
        6. Return the data as a JSON object with keys: name, phones (list), emails (list), dob.
        """

        agent = Agent(task=task, llm=llm, browser=browser)
        result = await agent.run()
        
        # In a real scenario, we'd parse the LLM's final response
        # For now, we simulate the logic of saving
        print(f"Extraction complete for {lead['id']}")
        
        # Add random delay between leads
        await human_delay(30, 90)

    await browser.close()

async def main():
    print("Stealth Enrichment Engine Initialized.")
    while True:
        try:
            await enrich_batch()
            # Wait between batches (10-20 minutes)
            wait_time = random.randint(600, 1200)
            print(f"Batch complete. Sleeping for {wait_time}s...")
            await asyncio.sleep(wait_time)
        except Exception as e:
            print(f"Engine Error: {e}")
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
