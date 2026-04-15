import asyncio
import datetime
import json
import sqlite3
from pathlib import Path
from typing import Dict, Any, List

from core import config
from core.logic import _hydrate_lead, _append_activity
from services.scoring import _score_lead, compute_derived_scores
from ingest_stock_intel import main as run_stock_ingest
from run_scraper_once import run_once as run_live_scraper

DB_PATH = config.PROJECT_ROOT / "leads.db"

async def algorithmic_reprocessing():
    """
    Ensures ALL leads in the database are processed through the latest 
    algorithmic scoring and status logic, rather than manual movement.
    """
    print("[SYSTEM] Starting Global Algorithmic Reprocessing...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 1. Fetch all leads
    cursor.execute("SELECT * FROM leads")
    rows = cursor.fetchall()
    print(f"[SYSTEM] Reprocessing {len(rows)} leads...")
    
    updated_count = 0
    
    for row in rows:
        lead = dict(row)
        original_status = lead.get("status")
        original_call_score = lead.get("call_today_score")
        
        # A. Re-compute derived scores (confidence, propensity, readiness, conversion)
        derived = compute_derived_scores(lead)
        lead.update(derived)
        
        # B. Re-compute final scores (evidence, call_today)
        final_scores = _score_lead(lead)
        lead.update(final_scores)
        
        # C. Algorithmic Status Transition
        # If a lead has very high call_today_score and is still 'captured', 
        # move it to 'outreach_ready' automatically.
        if lead.get("status") == "captured" and lead.get("call_today_score", 0) >= 80:
            lead["status"] = "outreach_ready"
            _append_activity(lead, "SYSTEM_PROMOTION", "Lead automatically promoted to OUTREACH_READY due to high heat/score.")
            
        # D. Lifecycle Stage Alignment
        # Ensure lifecycle_stage matches trigger_type/data
        trigger = (lead.get("trigger_type") or "").lower()
        if "withdrawn" in trigger:
            lead["lifecycle_stage"] = "Market Tested"
        elif "mortgage" in trigger:
            lead["lifecycle_stage"] = "Financial Pivot"
            
        # Update DB if changed
        if lead.get("call_today_score") != original_call_score or lead.get("status") != original_status:
            cursor.execute("""
                UPDATE leads 
                SET call_today_score = ?, 
                    evidence_score = ?,
                    confidence_score = ?,
                    propensity_score = ?,
                    readiness_score = ?,
                    conversion_score = ?,
                    status = ?,
                    lifecycle_stage = ?,
                    updated_at = ?
                WHERE id = ?
            """, (
                lead["call_today_score"],
                lead["evidence_score"],
                lead["confidence_score"],
                lead["propensity_score"],
                lead["readiness_score"],
                lead["conversion_score"],
                lead["status"],
                lead["lifecycle_stage"],
                datetime.datetime.now().isoformat(),
                lead["id"]
            ))
            updated_count += 1
            
    conn.commit()
    conn.close()
    print(f"[SYSTEM] Algorithmic Reprocessing Complete. {updated_count} leads updated.")

async def full_sync_cycle():
    """
    Full coverage procurement cycle:
    1. Stock Ingest (Excel/PDF)
    2. Live Scraper (Web)
    3. Algorithmic Reprocessing (Math)
    """
    print("=== STARTING FULL COVERAGE PROCUREMENT CYCLE ===")
    
    # 1. Local Stock (Primary Intelligence)
    print("\n--- Phase 1: Local Stock Ingestion ---")
    try:
        run_stock_ingest()
    except Exception as e:
        print(f"Stock Ingest Error: {e}")
        
    # 2. Live Market (Live Coverage)
    print("\n--- Phase 2: Live Market Scraping ---")
    try:
        # We run it once for this demonstration
        await run_live_scraper()
    except Exception as e:
        print(f"Live Scraper Error: {e}")
        
    # 3. Algorithm (Systematic Movement)
    print("\n--- Phase 3: Algorithmic Reprocessing ---")
    await algorithmic_reprocessing()
    
    print("\n=== CYCLE COMPLETE: TOTAL SYSTEM COVERAGE SECURED ===")

if __name__ == "__main__":
    asyncio.run(full_sync_cycle())
