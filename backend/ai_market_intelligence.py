import sqlite3
import json
import re
from typing import Dict, Any

class AIMarketBrain:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def analyze_lead(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        scenario = (lead.get('scenario') or "").lower()
        land_size = lead.get('land_size_sqm') or 0
        owner_name = lead.get('owner_name') or "Homeowner"
        suburb = lead.get('suburb') or ""
        
        # 1. Sense-Make Intent from Notes
        intent = "General Interest"
        motivation = 50
        
        if any(x in scenario for x in ["qld", "vic", "interstate", "rent", "invest"]):
            intent = "Investor Exit"
            motivation += 20
        elif any(x in scenario for x in ["downsize", "old", "empty nesters", "retired"]):
            intent = "Downsizer"
            motivation += 25
        elif any(x in scenario for x in ["looking to buy", "ready", "urgent", "now"]):
            intent = "High Intent Seller"
            motivation += 40
            
        # 2. Sense-Make Development Potential (Anomaly Detection)
        dna = "Standard Residential"
        dev_score = 30
        if land_size > 700:
            dna = "Under-capitalized Land"
            dev_score = 85
        elif land_size > 600:
            dna = "Potential Dual Occ"
            dev_score = 65
            
        # 3. Construct Narrative
        # FIX: Get first name properly
        clean_owner = owner_name.split(',')[0].split('&')[0].split('AND')[0].strip()
        first_name = clean_owner.split(' ')[0].title() if clean_owner else "Homeowner"
        
        narrative = f"{first_name}, "
        if intent == "Investor Exit":
            narrative += f"your current portfolio positioning in {suburb} suggests a high-yield liquidation window. "
        if dev_score > 80:
            narrative += f"The land size ({land_size}sqm) is a massive anomaly for the street. "
        
        narrative += f"Our analysis indicates a strategy leading with a high-yield appraisal or a developer-focused buy-out is most optimum right now."

        return {
            "ai_intent_label": intent,
            "dev_potential_score": dev_score,
            "market_narrative": narrative,
            "property_dna": dna,
            "motivation_index": min(motivation, 99)
        }

    def process_all_leads(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        leads = conn.execute("SELECT * FROM leads").fetchall()
        
        print(f"AI Sense-Making Engine analyzing {len(leads)} records...")
        
        for lead in leads:
            lead_dict = dict(lead)
            insights = self.analyze_lead(lead_dict)
            
            conn.execute("""
                UPDATE leads 
                SET ai_intent_label = ?, 
                    dev_potential_score = ?, 
                    market_narrative = ?, 
                    property_dna = ?, 
                    motivation_index = ?
                WHERE id = ?
            """, (
                insights['ai_intent_label'],
                insights['dev_potential_score'],
                insights['market_narrative'],
                insights['property_dna'],
                insights['motivation_index'],
                lead_dict['id']
            ))
            
        conn.commit()
        conn.close()
        print("AI Sense-Making complete.")

if __name__ == "__main__":
    from core import config
    brain = AIMarketBrain(config.DB_PATH)
    brain.process_all_leads()
