import sqlite3
import uuid
import datetime
import json

def test_db():
    conn = sqlite3.connect('backend/leads.db')
    c = conn.cursor()
    
    # Just a minimal insert to verify write access
    lead_id = str(uuid.uuid4())
    today = datetime.date.today().isoformat()
    
    # Matching the 34 columns schema
    lead_data = (
        lead_id, "Test Address", "Suburb", "2000", "Test Owner",
        "Probate", 90, "Scenario", "Strategic Value", "Status",
        "10 Years", "Equity", 95, "[]", -33.0, 151.0,
        1000000, today, "[]", "image_url", "Description", "[]",
        "Conversion", "[]", "IMMEDIATE", "12:00",
        "Summary", "Likely", "Why", 70, 11, 95,
        "Velocity", 900000, "Local", "R2"
    )
    
    try:
        c.execute("INSERT INTO leads VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", lead_data)
        conn.commit()
        print("Successfully inserted test lead.")
    except Exception as e:
        print(f"Insert failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_db()
